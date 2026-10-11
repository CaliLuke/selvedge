package selvedge

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// The real driver executes every statement. Only the probe batch pauses, after
// version lookup and before DDL can acquire a lock that masks a missing lock.
type migrationProbe struct {
	entered chan struct{}
	release chan struct{}
	arrived sync.Once
	resume  func()
}

func newMigrationProbe() *migrationProbe {
	p := &migrationProbe{entered: make(chan struct{}), release: make(chan struct{})}
	p.resume = sync.OnceFunc(func() { close(p.release) })
	return p
}

type migrationProbeConnector struct {
	base  driver.Driver
	dsn   string
	probe *migrationProbe
}

func (c *migrationProbeConnector) Driver() driver.Driver { return c.base }

func (c *migrationProbeConnector) Connect(_ context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &migrationProbeConn{Conn: conn, probe: c.probe}, nil
}

type migrationProbeConn struct {
	driver.Conn
	probe *migrationProbe
}

func (c *migrationProbeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "-- migration concurrency probe") {
		c.probe.arrived.Do(func() { close(c.probe.entered) })
		select {
		case <-c.probe.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if exec, ok := c.Conn.(driver.ExecerContext); ok {
		return exec.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *migrationProbeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func assertMigrationLockHeld(t *testing.T, ctx context.Context, b schemaBackend) {
	t.Helper()
	conn, err := b.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if b.dialect == "postgresql" {
		// The migration lock namespace is ASCII "SELVEDGE". This statement
		// runs in its own transaction, so even an unexpected lock is released.
		var acquired bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", int64(0x53454c5645444745)).Scan(&acquired); err != nil {
			t.Fatal(err)
		}
		if acquired {
			t.Error("pending migration did not hold the PostgreSQL advisory lock before probe writes")
		}
		return
	}
	// A zero timeout makes this an immediate lock check, not a timed wait.
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=0"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "PRAGMA busy_timeout=5000")
	_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
	if err == nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		t.Error("pending migration did not hold the SQLite writer lock before probe writes")
		return
	}
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_BUSY {
		t.Fatalf("checking SQLite writer lock: %v", err)
	}
}
