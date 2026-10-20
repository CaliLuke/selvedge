//go:build integration

package selvedge

import (
	"context"
	"database/sql"
	"testing"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/testkit"
)

func postgresSchemaBackend(t *testing.T) schemaBackend {
	t.Helper()
	url, _ := testkit.PostgresDatabase(t)
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
