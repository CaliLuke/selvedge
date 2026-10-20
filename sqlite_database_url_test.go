package selvedge

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppStartSQLiteDatabaseURLFormats(t *testing.T) {
	for _, format := range []struct{ name, prefix string }{
		{"bare_path", ""}, {"file_uri", "file:"}, {"sqlite_url", "sqlite://"},
	} {
		t.Run(format.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "workflows.db")
			app := NewApp(WithDatabase(format.prefix + path + "?mode=rwc&_pragma=cache_size(1234)"))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			t.Cleanup(func() { require.NoError(t, app.Shutdown(context.Background())) })

			require.NoError(t, app.Start(ctx))
			_, err := os.Stat(path)
			require.NoError(t, err, "the selected database file must be created")
			var cacheSize int
			require.NoError(t, app.Storage().DB().QueryRowContext(ctx, "PRAGMA cache_size").Scan(&cacheSize))
			require.Equal(t, 1234, cacheSize, "URL options must survive normalization")
			var migrations int
			require.NoError(t, app.Storage().DB().QueryRowContext(ctx, "SELECT count(*) FROM schema_migrations").Scan(&migrations))
			require.Positive(t, migrations, "startup must migrate every supported SQLite URL form")
		})
	}
}
