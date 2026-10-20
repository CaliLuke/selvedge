//go:build integration

package selvedge

import (
	"context"
	"testing"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/testkit"
)

// createTestAppWithPostgres creates a new App with an isolated PostgreSQL backend.
// Additional options can be passed to configure the App.
func createTestAppWithPostgres(t *testing.T, opts ...Option) (*App, func()) {
	t.Helper()
	ctx := context.Background()

	connStr, databaseCleanup := testkit.PostgresDatabase(t)

	// Create PostgreSQL storage directly to initialize schema
	store, err := storage.NewPostgresStorage(connStr)
	if err != nil {
		databaseCleanup()
		t.Fatalf("failed to create PostgreSQL storage: %v", err)
	}

	// Initialize test schema
	if err := storage.InitializeTestSchemaPostgres(ctx, store); err != nil {
		_ = store.Close()
		databaseCleanup()
		t.Fatalf("failed to initialize test schema: %v", err)
	}
	_ = store.Close()

	// Create app with the connection string and any additional options
	allOpts := append([]Option{WithDatabase(connStr)}, opts...)
	app := NewApp(allOpts...)

	cleanup := func() {
		_ = app.Shutdown(context.Background())
		databaseCleanup()
	}

	return app, cleanup
}
