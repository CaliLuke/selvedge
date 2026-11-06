package sqliteutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

func TestWithoutBusyTimeoutPreservesPooledConnectionSetting(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		name := "success"
		if cancelled {
			name = "cancelled"
		}
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("sqlite", ":memory:?_pragma=busy_timeout(1234)")
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
			conn, err := db.Conn(context.Background())
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err = WithoutBusyTimeout(ctx, conn, func() error {
				if cancelled {
					cancel()
				}
				return ctx.Err()
			})
			if cancelled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, conn.Close())
			var timeout int
			require.NoError(t, db.QueryRow("PRAGMA busy_timeout").Scan(&timeout))
			require.Equal(t, 1234, timeout, "later operations must retain the connection's original lock-wait behavior")
		})
	}
}

var errBusySetting = errors.New("busy timeout setting unavailable")

type busyFaultConnector struct {
	failSet, failRestore bool
	closed               int
}

func (c *busyFaultConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c *busyFaultConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(":memory:?_pragma=busy_timeout(1234)")
	if err != nil {
		return nil, err
	}
	return &busyFaultConn{Conn: conn, owner: c}, nil
}

type busyFaultConn struct {
	driver.Conn
	owner *busyFaultConnector
}

func (c *busyFaultConn) Close() error { c.owner.closed++; return c.Conn.Close() }
func (c *busyFaultConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if (query == "PRAGMA busy_timeout=0" && c.owner.failSet) || (query == "PRAGMA busy_timeout=1234" && c.owner.failRestore) {
		return nil, errBusySetting
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestWithoutBusyTimeoutRejectsUnreadableConnection(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	err = WithoutBusyTimeout(context.Background(), conn, func() error {
		t.Fatal("operation must not run on a closed connection")
		return nil
	})
	require.ErrorIs(t, err, sql.ErrConnDone)
}

func TestWithoutBusyTimeoutRejectsFailedConfiguration(t *testing.T) {
	connector := &busyFaultConnector{failSet: true}
	db := sql.OpenDB(connector)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	err = WithoutBusyTimeout(context.Background(), conn, func() error {
		t.Fatal("operation must not run with SQLite busy waiting still enabled")
		return nil
	})
	require.ErrorIs(t, err, errBusySetting)
	var timeout int
	require.NoError(t, conn.QueryRowContext(context.Background(), "PRAGMA busy_timeout").Scan(&timeout))
	require.Equal(t, 1234, timeout)
}

func TestWithoutBusyTimeoutDiscardsConnectionAfterRestoreFailure(t *testing.T) {
	connector := &busyFaultConnector{failRestore: true}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	operationErr := errors.New("operation failed")
	err = WithoutBusyTimeout(context.Background(), conn, func() error { return operationErr })
	require.ErrorIs(t, err, operationErr)
	require.ErrorIs(t, err, errBusySetting)
	require.Equal(t, 1, connector.closed)
	var timeout int
	require.NoError(t, db.QueryRow("PRAGMA busy_timeout").Scan(&timeout))
	require.Equal(t, 1234, timeout, "pool must open a new connection after restoration fails")
}
