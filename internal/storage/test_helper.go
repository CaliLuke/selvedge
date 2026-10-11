package storage

import (
	"context"

	"github.com/CaliLuke/selvedge/internal/migrations"
)

// InitializeTestSchema applies the embedded SQLite schema used by the App.
// This is intended for use in tests only.
func InitializeTestSchema(ctx context.Context, s *SQLiteStorage) error {
	_, err := migrations.ApplyMigrations(ctx, s.DB(), "sqlite", migrations.EmbeddedMigrationsFS())
	return err
}

// InitializeTestSchemaPostgres applies the embedded PostgreSQL schema used by the App.
// This is intended for use in tests only.
func InitializeTestSchemaPostgres(ctx context.Context, s *PostgresStorage) error {
	_, err := migrations.ApplyMigrations(ctx, s.DB(), "postgresql", migrations.EmbeddedMigrationsFS())
	return err
}

// InitializeTestSchemaForStorage initializes the embedded schema for a Storage.
// This is useful for external test packages.
func InitializeTestSchemaForStorage(ctx context.Context, s Storage) error {
	switch store := s.(type) {
	case *SQLiteStorage:
		return InitializeTestSchema(ctx, store)
	case *PostgresStorage:
		return InitializeTestSchemaPostgres(ctx, store)
	default:
		return nil
	}
}
