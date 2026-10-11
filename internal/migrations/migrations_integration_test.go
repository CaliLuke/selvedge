//go:build integration

package migrations

import (
	"context"
	"database/sql"
	"testing"
	"testing/fstest"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

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

	container, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("migration_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute)),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}
	defer testcontainers.CleanupContainer(t, container)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

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

	container, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("multiworker_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute)),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}
	defer testcontainers.CleanupContainer(t, container)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Simulate multiple workers starting simultaneously
	const numWorkers = 5
	results := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			db, err := sql.Open("pgx", connStr)
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
	db, _ := sql.Open("pgx", connStr)
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
