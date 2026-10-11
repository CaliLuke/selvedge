package storage

import (
	"context"
	"database/sql"
)

// WithTestStatementObserver records the SQL protocol without replacing the store.
// Install before concurrent access. The observer must be concurrency-safe.
func WithTestStatementObserver(observe func(string, []any)) func(Storage) {
	return func(s Storage) {
		switch s := s.(type) {
		case *SQLiteStorage:
			s.driver = observingDriver{s.driver, observe}
		case *PostgresStorage:
			s.driver = observingDriver{s.driver, observe}
		}
	}
}

type observingDriver struct {
	Driver
	observe func(string, []any)
}
type observedExecutor struct {
	Executor
	observe func(string, []any)
}

func observeExecutor(d Driver, conn Executor) Executor {
	if d, ok := d.(observingDriver); ok {
		return observedExecutor{conn, d.observe}
	}
	return conn
}

// ExecContext records the statement before executing it.
func (e observedExecutor) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	e.observe(q, args)
	return e.Executor.ExecContext(ctx, q, args...)
}

// QueryContext records the statement before querying rows.
func (e observedExecutor) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	e.observe(q, args)
	return e.Executor.QueryContext(ctx, q, args...)
}

// QueryRowContext records the statement before querying a row.
func (e observedExecutor) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	e.observe(q, args)
	return e.Executor.QueryRowContext(ctx, q, args...)
}
