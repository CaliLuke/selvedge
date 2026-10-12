package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"path/filepath"
	"testing"
	"time"

	"modernc.org/sqlite"
)

func TestSQLiteConcurrentFirstOpen(t *testing.T) {
	const pools = 16
	path := filepath.Join(t.TempDir(), "fresh.db")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, pools)
	for range pools {
		go func() {
			<-start
			s, err := NewSQLiteStorage(path)
			if err != nil {
				results <- err
				return
			}
			t.Cleanup(func() { _ = s.Close() })
			// No pool is opened before the barrier, and none is pre-pinged.
			var mode string
			err = s.DB().QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode)
			if err == nil && mode != "wal" {
				t.Errorf("journal mode = %q, want wal", mode)
			}
			results <- err
		}()
	}
	close(start)
	for range pools {
		if err := <-results; err != nil {
			t.Errorf("concurrent first open: %v", err)
		}
	}
}

func TestSQLiteWALRejectsUnchangedJournalMode(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=OFF"); err != nil {
		t.Fatal(err)
	}
	if err := enableSQLiteWAL(context.Background(), db); err == nil {
		t.Fatal("accepted journal_mode=off after requesting WAL")
	}
}

// Hold a real exclusive lock across the first WAL attempt. Release it only once
// the initializer retries, so deleting the retry deterministically fails.
func TestSQLiteWALRetriesFirstOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked-first-open.db")
	blocker, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	blocker.SetMaxOpenConns(1)
	if _, err := blocker.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	defer blocker.Exec("ROLLBACK")
	connector := &walProbeConnector{path: path, retry: func() {
		if _, err := blocker.Exec("ROLLBACK"); err != nil {
			t.Error(err)
		}
	}}
	db := sql.OpenDB(connector)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := enableSQLiteWAL(ctx, db); err != nil {
		t.Fatalf("first open: %v", err)
	}
	if connector.attempts < 2 {
		t.Fatalf("WAL attempts = %d, want a retry", connector.attempts)
	}
	var mode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("journal mode = %q, want wal", mode)
	}
}

type walProbeConnector struct {
	path     string
	attempts int
	retry    func()
}

func (c *walProbeConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c *walProbeConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.path)
	if err != nil {
		return nil, err
	}
	return &walProbeConn{Conn: conn, probe: c}, nil
}

type walProbeConn struct {
	driver.Conn
	probe *walProbeConnector
}

func (c *walProbeConn) attempt(query string) {
	if query == "PRAGMA journal_mode=WAL" {
		c.probe.attempts++
		if c.probe.attempts == 2 {
			c.probe.retry()
		}
	}
}
func (c *walProbeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.attempt(query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *walProbeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.attempt(query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
