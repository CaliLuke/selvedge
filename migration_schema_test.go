package selvedge

import (
	"context"
	"database/sql"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/CaliLuke/selvedge/internal/migrations"
	"github.com/CaliLuke/selvedge/internal/storage"
)

type schemaBackend struct {
	store      storage.Storage
	db         *sql.DB
	dialect    string
	dsn        string
	open       func() (*sql.DB, error)
	initialize func(context.Context) error
}

type schemaBackendFactory func(*testing.T) schemaBackend

func sqliteSchemaBackend(t *testing.T) schemaBackend {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schema.db")
	store, err := storage.NewSQLiteStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return schemaBackend{
		store: store, db: store.DB(), dialect: "sqlite",
		dsn: path + "?_pragma=busy_timeout(5000)&_txlock=immediate",
		open: func() (*sql.DB, error) {
			s, err := storage.NewSQLiteStorage(path)
			if err != nil {
				return nil, err
			}
			return s.DB(), nil
		},
		initialize: func(ctx context.Context) error { return storage.InitializeTestSchema(ctx, store) },
	}
}

func TestMigrationsApplyCleanAndIdempotent(t *testing.T) {
	testMigrationsApplyCleanAndIdempotent(t, sqliteSchemaBackend)
}

func testMigrationsApplyCleanAndIdempotent(t *testing.T, factory schemaBackendFactory) {
	t.Helper()
	t.Run("apply_twice", func(t *testing.T) {
		b := factory(t)
		ctx := context.Background()
		applied, err := migrations.ApplyMigrations(ctx, b.db, b.dialect, EmbeddedMigrationsFS())
		if err != nil {
			t.Fatal(err)
		}
		if len(applied) != 1 {
			t.Fatalf("applied %v, want one schema version", applied)
		}
		before := schemaSnapshot(t, b)
		applied, err = migrations.ApplyMigrations(ctx, b.db, b.dialect, EmbeddedMigrationsFS())
		if err != nil {
			t.Fatal(err)
		}
		if len(applied) != 0 {
			t.Fatalf("second apply: %v", applied)
		}
		if after := schemaSnapshot(t, b); !reflect.DeepEqual(before, after) {
			t.Fatalf("schema changed on second apply: before %v, after %v", before, after)
		}
	})
	t.Run("four_concurrent_workers", func(t *testing.T) {
		b := factory(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Distinct pools model distinct processes, without sharing a Go mutex.
		workers := make([]*sql.DB, 4)
		for i := range workers {
			db, err := b.open()
			if err != nil {
				t.Fatal(err)
			}
			workers[i] = db
			t.Cleanup(func() { _ = db.Close() })
			// Open each pool before the race so the test exercises the
			// migration lock, not concurrent first-open journal pragmas.
			if err := db.PingContext(ctx); err != nil {
				t.Fatal(err)
			}
		}
		start := make(chan struct{})
		type result struct {
			applied []string
			err     error
		}
		results := make(chan result, len(workers))
		for _, db := range workers {
			go func() {
				<-start
				applied, err := migrations.ApplyMigrations(ctx, db, b.dialect, EmbeddedMigrationsFS())
				results <- result{applied, err}
			}()
		}
		close(start)
		total := 0
		for range workers {
			r := <-results
			if r.err != nil {
				t.Errorf("concurrent apply: %v", r.err)
			}
			total += len(r.applied)
		}
		if total != 1 {
			t.Fatalf("workers applied %d versions, want exactly one", total)
		}
		var count int
		if err := b.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("schema_migrations has %d rows", count)
		}
	})
	t.Run("four_concurrent_workers_pending_migration", func(t *testing.T) {
		b := factory(t)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Apply the real schema first: creating a fresh tracking table must not
		// accidentally serialize workers when the transaction lock is missing.
		if _, err := migrations.ApplyMigrations(ctx, b.db, b.dialect, EmbeddedMigrationsFS()); err != nil {
			t.Fatal(err)
		}
		pending := pendingProbeSchema(t, b.dialect)
		probe := newMigrationProbe()
		defer probe.resume()
		workers := make([]*sql.DB, 4)
		for i := range workers {
			db := sql.OpenDB(&migrationProbeConnector{base: b.db.Driver(), dsn: b.dsn, probe: probe})
			workers[i] = db
			t.Cleanup(func() { _ = db.Close() })
			// Open each pool before the race so the test exercises the
			// migration lock, not concurrent first-open journal pragmas.
			if err := db.PingContext(ctx); err != nil {
				t.Fatal(err)
			}
		}
		start := make(chan struct{})
		type result struct {
			applied []string
			err     error
		}
		results := make(chan result, len(workers))
		for _, db := range workers {
			go func() {
				<-start
				applied, err := migrations.ApplyMigrations(ctx, db, b.dialect, pending)
				results <- result{applied, err}
			}()
		}
		close(start)
		select {
		case <-probe.entered:
			assertMigrationLockHeld(t, ctx, b)
		case <-ctx.Done():
			t.Fatal("migration did not reach the probe: ", ctx.Err())
		}
		probe.resume()
		total := 0
		for range workers {
			r := <-results
			if r.err != nil {
				t.Errorf("concurrent pending migration: %v", r.err)
			}
			for _, version := range r.applied {
				if version != "20261011000000" {
					t.Errorf("applied version %q, want pending probe version", version)
				}
			}
			total += len(r.applied)
		}
		if total != 1 {
			t.Fatalf("workers applied %d pending versions, want exactly one", total)
		}
		var versions, probes int
		if err := b.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&versions); err != nil {
			t.Fatal(err)
		}
		if versions != 2 {
			t.Fatalf("schema_migrations has %d rows, want initial and probe versions", versions)
		}
		if err := b.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM migration_concurrency_probe WHERE id = 1").Scan(&probes); err != nil {
			t.Fatal(err)
		}
		if probes != 1 {
			t.Fatalf("probe has %d rows, want one", probes)
		}
		applied, err := migrations.ApplyMigrations(ctx, b.db, b.dialect, pending)
		if err != nil || len(applied) != 0 {
			t.Fatalf("reapply pending schema: applied %v, err %v", applied, err)
		}
	})
}

func TestTestSchemaEqualsMigrations(t *testing.T) {
	testTestSchemaEqualsMigrations(t, sqliteSchemaBackend)
}

func testTestSchemaEqualsMigrations(t *testing.T, factory schemaBackendFactory) {
	t.Helper()
	helper, direct := factory(t), factory(t)
	ctx := context.Background()
	if err := helper.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := migrations.ApplyMigrations(ctx, direct.db, direct.dialect, EmbeddedMigrationsFS()); err != nil {
		t.Fatal(err)
	}
	want, got := schemaSnapshot(t, direct), schemaSnapshot(t, helper)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("helper schema differs from embedded schema\nhelper: %v\nembedded: %v", got, want)
	}
	if err := helper.initialize(ctx); err != nil {
		t.Fatalf("second helper initialization: %v", err)
	}
	if after := schemaSnapshot(t, helper); !reflect.DeepEqual(got, after) {
		t.Fatal("helper changed schema on second initialization")
	}
}

// Include definitions of columns, defaults, constraints, indexes and foreign keys,
// not only table names. PostgreSQL excludes generated sequence names and OIDs.
func schemaSnapshot(t *testing.T, b schemaBackend) []string {
	t.Helper()
	query := "SELECT type || ':' || name || ':' || sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
	if b.dialect == "postgresql" {
		query = `SELECT definition FROM (
   SELECT 'column:' || table_name || ':' || column_name || ':' || data_type || ':' || is_nullable || ':' || COALESCE(column_default, '') AS definition
   FROM information_schema.columns WHERE table_schema = 'public'
   UNION ALL SELECT 'constraint:' || c.relname || ':' || con.conname || ':' || pg_get_constraintdef(con.oid)
   FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public'
   UNION ALL SELECT 'index:' || tablename || ':' || indexname || ':' || indexdef FROM pg_indexes WHERE schemaname = 'public'
  ) definitions ORDER BY definition`
	}
	rows, err := b.db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var snapshot []string
	for rows.Next() {
		var definition string
		if err := rows.Scan(&definition); err != nil {
			t.Fatal(err)
		}
		snapshot = append(snapshot, definition)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestSchemaOwnership(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgresql"} {
		t.Run(dialect, func(t *testing.T) {
			entries, err := fs.ReadDir(EmbeddedMigrationsFS(), dialect)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("want one schema file, got %d", len(entries))
			}
			content, err := fs.ReadFile(EmbeddedMigrationsFS(), dialect+"/"+entries[0].Name())
			if err != nil {
				t.Fatal(err)
			}
			up, down := migrations.ParseMigrationFile(string(content))
			if up == "" || down == "" {
				t.Fatal("schema requires up and down sections")
			}
			for _, leftover := range []string{"workflow_compensations", "outbox_events", "workflow_group_memberships", "workflow_history_archive", "schema_version", "'python'", "'compensating'", "'recurred'"} {
				if strings.Contains(string(content), leftover) {
					t.Errorf("schema still contains %s", leftover)
				}
			}
			if !strings.Contains(up, "DEFAULT 'go'") {
				t.Error("missing Go framework default")
			}
			if !strings.Contains(up, "REFERENCES workflow_definitions") {
				t.Error("missing workflow definition foreign key")
			}
		})
	}
}

func TestMigrationsRollbackOnError(t *testing.T) {
	testMigrationsRollbackOnError(t, sqliteSchemaBackend)
}

func testMigrationsRollbackOnError(t *testing.T, factory schemaBackendFactory) {
	t.Helper()
	for _, tc := range []struct{ name, up string }{
		{"duplicate_table", "CREATE TABLE probe (id INTEGER); CREATE TABLE probe (id INTEGER);"},
		{"duplicate_column", "CREATE TABLE probe (id INTEGER); ALTER TABLE probe ADD COLUMN id INTEGER;"},
		{"invalid_sql", "CREATE TABLE probe (id INTEGER); INVALID SQL;"},
		{"duplicate_data", "CREATE TABLE probe (id INTEGER PRIMARY KEY); INSERT INTO probe VALUES (1); INSERT INTO probe VALUES (1);"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := factory(t)
			before := schemaSnapshot(t, b)
			_, err := migrations.ApplyMigrations(context.Background(), b.db, b.dialect, probeSchema(b.dialect, tc.up))
			if err == nil {
				t.Fatal("schema error was ignored")
			}
			if after := schemaSnapshot(t, b); !reflect.DeepEqual(before, after) {
				t.Fatalf("failed schema left objects behind: %v", after)
			}
			applied, err := migrations.ApplyMigrations(context.Background(), b.db, b.dialect, probeSchema(b.dialect, "CREATE TABLE probe (id INTEGER);"))
			if err != nil || len(applied) != 1 {
				t.Fatalf("retry corrected schema: applied %v, err %v", applied, err)
			}
		})
	}
	t.Run("version_record_failure", func(t *testing.T) {
		b := factory(t)
		_, err := b.db.Exec("CREATE TABLE schema_migrations (version VARCHAR(255) PRIMARY KEY CHECK (version <> '20260101000000'))")
		if err != nil {
			t.Fatal(err)
		}
		before := schemaSnapshot(t, b)
		_, err = migrations.ApplyMigrations(context.Background(), b.db, b.dialect, probeSchema(b.dialect, "CREATE TABLE probe (id INTEGER);"))
		if err == nil {
			t.Fatal("version recording error was ignored")
		}
		if after := schemaSnapshot(t, b); !reflect.DeepEqual(before, after) {
			t.Fatalf("version failure left schema behind: %v", after)
		}
	})
}

func probeSchema(dialect, up string) fs.FS {
	return fstest.MapFS{dialect + "/20260101000000_probe.sql": &fstest.MapFile{Data: []byte("-- migrate:up\n" + up + "\n-- migrate:down\nDROP TABLE probe;")}}
}

func TestMigrationsSQLLiterals(t *testing.T) {
	testMigrationsSQLLiterals(t, sqliteSchemaBackend)
}

func testMigrationsSQLLiterals(t *testing.T, factory schemaBackendFactory) {
	t.Helper()
	b := factory(t)
	up := "CREATE TABLE probe (value TEXT); INSERT INTO probe VALUES ('a;b');"
	if _, err := migrations.ApplyMigrations(context.Background(), b.db, b.dialect, probeSchema(b.dialect, up)); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := b.db.QueryRow("SELECT value FROM probe").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "a;b" {
		t.Fatalf("literal = %q, want a;b", got)
	}
}

// Copy the real schemas and add only a test-local pending migration. The test
// driver pauses at the marker after version lookup and before the first write.
func pendingProbeSchema(t *testing.T, dialect string) fs.FS {
	t.Helper()
	real := EmbeddedMigrationsFS()
	files, err := fs.Glob(real, "*/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	pending := make(fstest.MapFS, len(files)+1)
	for _, name := range files {
		content, err := fs.ReadFile(real, name)
		if err != nil {
			t.Fatal(err)
		}
		pending[name] = &fstest.MapFile{Data: content}
	}
	pending[dialect+"/20261011000000_s0c_concurrency_probe.sql"] = &fstest.MapFile{Data: []byte(`-- migrate:up
-- migration concurrency probe
CREATE TABLE migration_concurrency_probe (id INTEGER PRIMARY KEY);
INSERT INTO migration_concurrency_probe (id) VALUES (1);
-- migrate:down
DROP TABLE migration_concurrency_probe;
`)}
	return pending
}
