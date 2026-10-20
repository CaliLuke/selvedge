package migrations

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"testing"

	"modernc.org/sqlite"
)

var errRollbackProbe = errors.New("rollback unavailable")

type rollbackConnector struct {
	path      string
	closed    int
	failBegin bool
}

func (c *rollbackConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c *rollbackConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.path)
	if err != nil {
		return nil, err
	}
	return &rollbackConn{Conn: conn, owner: c}, nil
}

type rollbackConn struct {
	driver.Conn
	owner *rollbackConnector
}

func (c *rollbackConn) Close() error { c.owner.closed++; return c.Conn.Close() }
func (c *rollbackConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query == "BEGIN IMMEDIATE" && c.owner.failBegin {
		c.owner.failBegin = false
		if _, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args); err != nil {
			return nil, err
		}
		return nil, errBeginProbe
	}
	if query == "ROLLBACK" {
		return nil, errRollbackProbe
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestMigrationRollbackFailureDiscardsConnection(t *testing.T) {
	connector := &rollbackConnector{path: filepath.Join(t.TempDir(), "rollback.db")}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	failure := errors.New("migration failed")
	err := withMigrationTransaction(context.Background(), db, "sqlite", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(), "CREATE TABLE probe (id INTEGER)"); err != nil {
			return err
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Errorf("lost apply error: %v", err)
	}
	if !errors.Is(err, errRollbackProbe) {
		t.Errorf("lost rollback error: %v", err)
	}
	if connector.closed != 1 {
		t.Errorf("closed connections = %d, want 1 before reuse", connector.closed)
	}
	if _, err := db.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("pool returned an open transaction: %v", err)
	}
	// Close the new connection rather than triggering the injected rollback failure.
	db.SetMaxIdleConns(0)
}

var errBeginProbe = errors.New("migration begin acknowledgement lost")

func TestMigrationBeginFailureDiscardsConnection(t *testing.T) {
	connector := &rollbackConnector{path: filepath.Join(t.TempDir(), "begin.db"), failBegin: true}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	err := EnsureSchemaMigrationsTable(context.Background(), db, "sqlite")
	if !errors.Is(err, errBeginProbe) {
		t.Fatalf("lost BEGIN error: %v", err)
	}
	if connector.closed != 1 {
		t.Fatalf("closed connections = %d, want 1 before reuse", connector.closed)
	}
	if _, err := db.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatalf("pool returned an uncertain transaction: %v", err)
	}
	db.SetMaxIdleConns(0)
}
