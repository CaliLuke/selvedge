package selvedge

import (
	"io/fs"

	"github.com/CaliLuke/selvedge/internal/migrations"
)

// EmbeddedMigrationsFS returns the embedded schemas rooted at sqlite/ and
// postgresql/. internal/migrations/sql/ is the only schema source of truth.
func EmbeddedMigrationsFS() fs.FS {
	return migrations.EmbeddedMigrationsFS()
}
