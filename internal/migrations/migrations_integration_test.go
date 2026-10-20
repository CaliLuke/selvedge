//go:build integration

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"testing/fstest"

	"github.com/CaliLuke/selvedge/internal/testkit"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Test migrations filesystem with realistic schema
var integrationTestFS = fstest.MapFS{
	"sqlite/20240101000000_initial.sql": &fstest.MapFile{
		Data: []byte(`-- migrate:up
CREATE TABLE IF NOT EXISTS test_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS test_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL
);

-- migrate:down
DROP TABLE IF EXISTS test_orders;
DROP TABLE IF EXISTS test_users;
`),
	},
	"postgresql/20240101000000_initial.sql": &fstest.MapFile{
		Data: []byte(`-- migrate:up
CREATE TABLE IF NOT EXISTS test_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS test_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    amount NUMERIC(10,2) NOT NULL
);

-- migrate:down
DROP TABLE IF EXISTS test_orders;
DROP TABLE IF EXISTS test_users;
`),
	},
}

func TestApplyMigrations_PostgreSQL_Integration(t *testing.T) {
	ctx := context.Background()

	connStr, cleanup := testkit.PostgresDatabase(t)
	defer cleanup()

	db, err := sql.Open("pgx", repeatableReadURL(connStr))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// A server may default to repeatable read. The advisory lock wait must not
	// freeze the snapshot used by the subsequent version lookup.
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "SET default_transaction_isolation = 'repeatable read'"); err != nil {
		t.Fatal(err)
	}
	if err := withMigrationTransaction(ctx, db, "postgresql", func(conn *sql.Conn) error {
		var isolation string
		if err := conn.QueryRowContext(ctx, "SHOW transaction_isolation").Scan(&isolation); err != nil {
			return err
		}
		if isolation != "read committed" {
			return fmt.Errorf("migration isolation = %q, want read committed", isolation)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Apply migrations
	applied, err := ApplyMigrations(ctx, db, "postgresql", integrationTestFS)
	if err != nil {
		t.Fatalf("ApplyMigrations() error = %v", err)
	}

	if len(applied) != 1 {
		t.Errorf("ApplyMigrations() applied %d migrations, want 1", len(applied))
	}

	// Verify tables were created
	var tableName string
	err = db.QueryRowContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_name = 'test_users'").Scan(&tableName)
	if err != nil {
		t.Errorf("test_users table not created: %v", err)
	}

	err = db.QueryRowContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_name = 'test_orders'").Scan(&tableName)
	if err != nil {
		t.Errorf("test_orders table not created: %v", err)
	}

	// Verify schema_migrations was recorded
	var version string
	err = db.QueryRowContext(ctx, "SELECT version FROM schema_migrations WHERE version = '20240101000000'").Scan(&version)
	if err != nil {
		t.Errorf("migration not recorded in schema_migrations: %v", err)
	}

	// Test idempotency - run again
	applied2, err := ApplyMigrations(ctx, db, "postgresql", integrationTestFS)
	if err != nil {
		t.Fatalf("ApplyMigrations() second run error = %v", err)
	}
	if len(applied2) != 0 {
		t.Errorf("ApplyMigrations() second run applied %d migrations, want 0", len(applied2))
	}
}

func TestApplyMigrations_MultiWorker_PostgreSQL_Integration(t *testing.T) {
	ctx := context.Background()

	connStr, cleanup := testkit.PostgresDatabase(t)
	defer cleanup()

	// Simulate multiple workers starting simultaneously
	const numWorkers = 5
	results := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			db, err := sql.Open("pgx", repeatableReadURL(connStr))
			if err != nil {
				results <- err
				return
			}
			defer db.Close()

			_, err = ApplyMigrations(ctx, db, "postgresql", integrationTestFS)
			results <- err
		}(i)
	}

	// Collect results
	var errors []error
	for i := 0; i < numWorkers; i++ {
		if err := <-results; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		t.Errorf("multi-worker migration failed with %d errors: %v", len(errors), errors)
	}

	// Verify migration was applied exactly once
	db, err := sql.Open("pgx", repeatableReadURL(connStr))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count migrations: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 migration record, got %d", count)
	}
}

func repeatableReadURL(connection string) string {
	address, err := url.Parse(connection)
	if err != nil {
		panic(err)
	} // PostgresDatabase supplies validated URLs.
	params := address.Query()
	params.Set("default_transaction_isolation", "repeatable read")
	address.RawQuery = params.Encode()
	return address.String()
}
