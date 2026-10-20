//go:build integration

package testkit

import (
	"context"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PostgresDatabase returns an isolated database and idempotent cleanup. When
// SELVEDGE_TEST_POSTGRES_URL is set, its role must be allowed to create databases.
// Otherwise, a disposable PostgreSQL container supplies the database.
func PostgresDatabase(t *testing.T) (databaseURL string, closeDatabase func()) {
	t.Helper()
	if server := os.Getenv("SELVEDGE_TEST_POSTGRES_URL"); server != "" {
		return isolatedPostgresDatabase(t, server)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	container, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("selvedge_test"), postgres.WithUsername("selvedge"), postgres.WithPassword("selvedge"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Minute), wait.ForListeningPort("5432/tcp")))
	var cleanup func()
	if container != nil {
		cleanup = postgresCleanup(t, func(ctx context.Context) error { return container.Terminate(ctx) })
	}
	if err != nil {
		t.Fatalf("start PostgreSQL container: %v", err)
	}
	if container == nil {
		t.Fatal("PostgreSQL container was not created")
	}
	connection, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get PostgreSQL connection string: %v", err)
	}
	return connection, cleanup
}

func isolatedPostgresDatabase(t *testing.T, server string) (databaseURL string, closeDatabase func()) {
	t.Helper()
	address, err := url.Parse(server)
	if err != nil || (address.Scheme != "postgres" && address.Scheme != "postgresql") {
		t.Fatal("SELVEDGE_TEST_POSTGRES_URL must be a PostgreSQL URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, server)
	if err != nil {
		t.Fatalf("connect to PostgreSQL test server: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	name := "selvedge_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	identifier := pgx.Identifier{name}.Sanitize()
	cleanup := postgresCleanup(t, func(ctx context.Context) error {
		admin, err := pgx.Connect(ctx, server)
		if err != nil {
			return err
		}
		defer func() { _ = admin.Close(ctx) }()
		_, err = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+identifier+" WITH (FORCE)")
		return err
	})
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		t.Fatalf("create isolated PostgreSQL test database: %v", err)
	}
	// pgx query parameters override the URL path, so remove both database aliases.
	params := address.Query()
	params.Del("dbname")
	params.Del("database")
	address.RawQuery = params.Encode()
	address.Path = "/" + name
	address.RawPath = ""
	return address.String(), cleanup
}

func postgresCleanup(t *testing.T, remove func(context.Context) error) func() {
	t.Helper()
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := remove(ctx); err != nil {
				t.Errorf("clean up isolated PostgreSQL test database: %v", err)
			}
		})
	}
	t.Cleanup(cleanup)
	return cleanup
}
