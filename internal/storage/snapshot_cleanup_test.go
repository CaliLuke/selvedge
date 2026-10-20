package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

var errSnapshotRollback = errors.New("snapshot rollback unavailable")

type snapshotRollbackConnector struct {
	path      string
	closed    int
	failBegin bool
}

func (c *snapshotRollbackConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c *snapshotRollbackConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.path)
	if err != nil {
		return nil, err
	}
	return &snapshotRollbackConn{Conn: conn, owner: c}, nil
}

type snapshotRollbackConn struct {
	driver.Conn
	owner *snapshotRollbackConnector
}

func (c *snapshotRollbackConn) Close() error { c.owner.closed++; return c.Conn.Close() }
func (c *snapshotRollbackConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query == "BEGIN" && c.owner.failBegin {
		if _, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args); err != nil {
			return nil, err
		}
		return nil, errSnapshotBegin
	}
	if query == "ROLLBACK" {
		return nil, errSnapshotRollback
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *snapshotRollbackConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestReadSnapshotRollbackFailureDiscardsConnection(t *testing.T) {
	connector := &snapshotRollbackConnector{path: filepath.Join(t.TempDir(), "snapshot.db")}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	store := &SQLiteStorage{db: db, driver: &SQLiteDriver{}}
	require.NoError(t, InitializeTestSchema(context.Background(), store))
	snapshot, err := store.ReadSnapshot(context.Background(), "missing")
	require.Nil(t, snapshot)
	require.ErrorIs(t, err, errSnapshotRollback)
	require.Equal(t, 1, connector.closed)
	// A new borrower must be able to start its own transaction.
	_, err = db.Exec("BEGIN IMMEDIATE")
	require.NoError(t, err)
	db.SetMaxIdleConns(0)
}

var errSnapshotBegin = errors.New("snapshot begin acknowledgement lost")

func TestReadSnapshotBeginFailureDiscardsConnection(t *testing.T) {
	connector := &snapshotRollbackConnector{path: filepath.Join(t.TempDir(), "snapshot.db"), failBegin: true}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	store := &SQLiteStorage{db: db, driver: &SQLiteDriver{}}
	snapshot, err := store.ReadSnapshot(context.Background(), "missing")
	require.Nil(t, snapshot)
	require.ErrorIs(t, err, errSnapshotBegin)
	require.Equal(t, 1, connector.closed)
	_, err = db.Exec("BEGIN IMMEDIATE")
	require.NoError(t, err, "the pool must not retain a transaction after an uncertain BEGIN")
	db.SetMaxIdleConns(0)
}
