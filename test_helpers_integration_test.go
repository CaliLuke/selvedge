//go:build integration

package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// createTestAppWithPostgres creates a new App with a PostgreSQL testcontainer backend.
// Additional options can be passed to configure the App.
func createTestAppWithPostgres(t *testing.T, opts ...Option) (*App, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("selvedge_test"),
		postgres.WithUsername("selvedge"),
		postgres.WithPassword("selvedge"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute)),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Create PostgreSQL storage directly to initialize schema
	store, err := storage.NewPostgresStorage(connStr)
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("failed to create PostgreSQL storage: %v", err)
	}

	// Initialize test schema
	if err := storage.InitializeTestSchemaPostgres(ctx, store); err != nil {
		_ = store.Close()
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("failed to initialize test schema: %v", err)
	}
	_ = store.Close()

	// Create app with the connection string and any additional options
	allOpts := append([]Option{WithDatabase(connStr)}, opts...)
	app := NewApp(allOpts...)

	cleanup := func() {
		_ = app.Shutdown(context.Background())
		testcontainers.CleanupContainer(t, container)
	}

	return app, cleanup
}
