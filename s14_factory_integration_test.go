//go:build integration

package selvedge

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/CaliLuke/selvedge/internal/storage"
)

type s14PostgresPool struct {
	admin     *storage.PostgresStorage
	dsn       string
	next      atomic.Int64
	schemasMu sync.Mutex
	available []string
}

// Each top-level test owns a container and reuses empty schemas across its subcases.
// Cleanup follows the test lifetime, so no package TestMain is needed.
func s14PostgresFactory(t *testing.T) replayAppFactory {
	t.Helper()
	ctx := context.Background()
	c, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("selvedge_test"), postgres.WithUsername("selvedge"), postgres.WithPassword("selvedge"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Minute), wait.ForListeningPort("5432/tcp")))
	require.NoError(t, err)
	suite := new(s14PostgresPool)
	t.Cleanup(func() {
		if suite.admin != nil {
			require.NoError(t, suite.admin.Close())
		}
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, c.Terminate(cleanup))
	})
	suite.dsn, err = c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	suite.admin, err = storage.NewPostgresStorage(suite.dsn)
	require.NoError(t, err)
	return func(t *testing.T, opts ...Option) (*App, func()) {
		t.Helper()
		schema := suite.s14AcquireSchema(t)
		dsn, err := url.Parse(suite.dsn)
		require.NoError(t, err)
		params := dsn.Query()
		params.Set("search_path", schema)
		dsn.RawQuery = params.Encode()
		options := append([]Option{WithDatabase(dsn.String())}, opts...)
		app := NewApp(options...)
		return app, func() {
			_ = app.Shutdown(context.Background())
			suite.s14ReleaseSchema(t, schema)
		}
	}
}

// A schema returns to the pool only after its harness has closed every store.
// Active harnesses (including retained crash baselines) never share schemas.
func (suite *s14PostgresPool) s14AcquireSchema(t *testing.T) string {
	t.Helper()
	suite.schemasMu.Lock()
	defer suite.schemasMu.Unlock()
	if n := len(suite.available); n > 0 {
		schema := suite.available[n-1]
		suite.available = suite.available[:n-1]
		return schema
	}
	schema := fmt.Sprintf("s14_%d", suite.next.Add(1))
	_, err := suite.admin.DB().ExecContext(context.Background(), `CREATE SCHEMA `+schema)
	require.NoError(t, err)
	return schema
}
func (suite *s14PostgresPool) s14ReleaseSchema(t *testing.T, schema string) {
	t.Helper()
	if t.Failed() {
		return
	}
	rows, err := suite.admin.DB().QueryContext(context.Background(), `SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)
 FROM information_schema.tables
 WHERE table_schema=$1 AND table_type='BASE TABLE' AND table_name <> 'schema_migrations'
 ORDER BY table_name`, schema)
	require.NoError(t, err)
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	if len(tables) > 0 {
		_, err = suite.admin.DB().ExecContext(context.Background(), `TRUNCATE TABLE `+strings.Join(tables, ",")+` RESTART IDENTITY CASCADE`)
		require.NoError(t, err)
	}
	_, err = suite.admin.DB().ExecContext(context.Background(), `DROP TABLE IF EXISTS `+schema+`.selvedge_test_clock`)
	require.NoError(t, err)
	suite.schemasMu.Lock()
	suite.available = append(suite.available, schema)
	suite.schemasMu.Unlock()
}
