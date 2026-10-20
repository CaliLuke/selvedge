//go:build integration

package testkit

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestPostgresDatabaseIsolationAndCleanup(t *testing.T) {
	// One server can host fixtures concurrently without table or cleanup collisions.
	server, _ := PostgresDatabase(t)
	address, err := url.Parse(server)
	require.NoError(t, err)
	params := address.Query()
	params.Set("dbname", strings.TrimPrefix(address.Path, "/"))
	params.Set("database", strings.TrimPrefix(address.Path, "/"))
	address.RawQuery = params.Encode()
	t.Setenv("SELVEDGE_TEST_POSTGRES_URL", address.String())
	firstURL, cleanupFirst := PostgresDatabase(t)
	secondURL, _ := PostgresDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := pgx.Connect(ctx, firstURL)
	require.NoError(t, err)
	defer first.Close(ctx)
	second, err := pgx.Connect(ctx, secondURL)
	require.NoError(t, err)
	defer second.Close(ctx)
	_, err = first.Exec(ctx, "CREATE TABLE fixture_probe(value TEXT)")
	require.NoError(t, err)
	var table *string
	require.NoError(t, second.QueryRow(ctx, "SELECT to_regclass('fixture_probe')::text").Scan(&table))
	require.Nil(t, table)

	cleanupFirst() // Also verifies teardown terminates lingering fixture connections.
	cleanupFirst() // Repeated explicit/t.Cleanup calls are harmless.
	require.NoError(t, second.Ping(ctx))
	admin, err := pgx.Connect(ctx, server)
	require.NoError(t, err)
	defer admin.Close(ctx)
	address, err = url.Parse(firstURL)
	require.NoError(t, err)
	var exists bool
	require.NoError(t, admin.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", address.Path[1:]).Scan(&exists))
	require.False(t, exists)
}
