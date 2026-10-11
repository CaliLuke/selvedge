package migrations

import (
	"embed"
	"io/fs"
)

// The SQL files are the schema source of truth. Before the first release,
// keep one initial schema per dialect and edit both in place. Numbered
// migrations start at S20.
//
//go:embed sql/sqlite/*.sql sql/postgresql/*.sql
var embeddedMigrations embed.FS

// EmbeddedMigrationsFS returns the embedded schemas rooted at their dialects.
func EmbeddedMigrationsFS() fs.FS {
	schemas, err := fs.Sub(embeddedMigrations, "sql")
	if err != nil {
		panic("failed to create schema filesystem: " + err.Error())
	}
	return schemas
}
