//go:build integration

package selvedge

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func postgresSchemaBackend(t *testing.T) schemaBackend {
	t.Helper()
	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("schema_test"), postgres.WithUsername("selvedge"), postgres.WithPassword("selvedge"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewPostgresStorage(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return schemaBackend{
		store: store, db: store.DB(), dialect: "postgresql", dsn: url,
		open: func() (*sql.DB, error) {
			s, err := storage.NewPostgresStorage(url)
			if err != nil {
				return nil, err
			}
			return s.DB(), nil
		},
		initialize: func(ctx context.Context) error { return storage.InitializeTestSchemaPostgres(ctx, store) },
	}
}

func TestMigrationsApplyCleanAndIdempotent_Postgres(t *testing.T) {
	testMigrationsApplyCleanAndIdempotent(t, postgresSchemaBackend)
}

func TestTestSchemaEqualsMigrations_Postgres(t *testing.T) {
	testTestSchemaEqualsMigrations(t, postgresSchemaBackend)
}

func TestMigrationsRollbackOnError_Postgres(t *testing.T) {
	testMigrationsRollbackOnError(t, postgresSchemaBackend)
}

func TestMigrationsSQLLiterals_Postgres(t *testing.T) {
	testMigrationsSQLLiterals(t, postgresSchemaBackend)
}

func TestReceiveGoEnvelopeOnly_Postgres(t *testing.T) {
	testReceiveGoEnvelopeOnly(t, postgresSchemaBackend)
}
