// Package sqliteutil provides SQLite connection configuration for initialization.
package sqliteutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
)

// WithoutBusyTimeout lets an operation retry SQLITE_BUSY using its context
// instead of waiting inside SQLite's busy handler. The caller must exclusively
// own conn until this function returns. Its previous timeout is restored before
// reuse, including after cancellation; a failed restoration discards conn.
//
// This works around context-insensitive busy waits in the pinned sqlite v1.40.1:
// https://gitlab.com/cznic/sqlite/-/issues/262 (fixed upstream in v1.61.0).
// Remove it after adopting a fixed driver release and confirming the producer
// WAL/migration lock-cancellation tests pass with the normal busy timeout.
func WithoutBusyTimeout(ctx context.Context, conn *sql.Conn, operation func() error) (err error) {
	var timeout int
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, restoreErr := conn.ExecContext(cleanup, fmt.Sprintf("PRAGMA busy_timeout=%d", timeout)); restoreErr != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			err = errors.Join(err, fmt.Errorf("restore SQLite busy timeout: %w", restoreErr))
		}
	}()
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=0"); err != nil {
		return err
	}
	return operation()
}
