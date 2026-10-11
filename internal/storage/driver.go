package storage

import "fmt"

// Driver abstracts database-specific SQL operations.
type Driver interface {
	NowMillisExpr() string
	// DriverName returns the driver name (e.g., "sqlite", "postgres")
	DriverName() string

	// GetCurrentTimeExpr returns the SQL expression for current time.
	// SQLite: datetime('now'), PostgreSQL: NOW()
	GetCurrentTimeExpr() string

	// MakeDatetimeComparable wraps a datetime column for comparison.
	// SQLite needs datetime() wrapper, PostgreSQL doesn't.
	MakeDatetimeComparable(column string) string

	// SelectForUpdateSkipLocked returns the clause for row locking.
	// PostgreSQL: "FOR UPDATE SKIP LOCKED", SQLite: "" (uses table-level locking)
	SelectForUpdateSkipLocked() string

	// Placeholder returns the placeholder for the nth parameter.
	// SQLite: ?, PostgreSQL: $n
	Placeholder(n int) string

	// OnConflictDoNothing returns the clause for idempotent inserts.
	// SQLite: "ON CONFLICT DO NOTHING", PostgreSQL: "ON CONFLICT DO NOTHING"
	OnConflictDoNothing(conflictColumns ...string) string

	// ReturningClause returns the RETURNING clause for insert/update.
	// SQLite: "", PostgreSQL: "RETURNING id"
	ReturningClause(columns ...string) string

	// JSONExtract returns the SQL expression to extract a value from a JSON column.
	// The path is a dot-separated path like "order.customer.id".
	// The returned expression extracts the value as text.
	JSONExtract(column, path string) string

	// JSONCompare returns the SQL expression to compare a JSON-extracted value
	// with a given value, handling type coercion appropriately.
	// Returns the comparison expression and any additional args needed.
	JSONCompare(extractExpr string, value any, placeholderNum int) (expr string, args []any)
}

// SQLiteDriver implements Driver for SQLite.
type SQLiteDriver struct{}

func (d *SQLiteDriver) DriverName() string {
	return "sqlite"
}

func (d *SQLiteDriver) GetCurrentTimeExpr() string {
	return "datetime('now')"
}

func (d *SQLiteDriver) MakeDatetimeComparable(column string) string {
	return fmt.Sprintf("datetime(%s)", column)
}

func (d *SQLiteDriver) SelectForUpdateSkipLocked() string {
	// SQLite uses table-level locking, no row-level locking support
	return ""
}

func (d *SQLiteDriver) Placeholder(n int) string {
	return "?"
}

func (d *SQLiteDriver) OnConflictDoNothing(conflictColumns ...string) string {
	if len(conflictColumns) == 0 {
		return "ON CONFLICT DO NOTHING"
	}
	cols := ""
	for i, c := range conflictColumns {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", cols)
}

func (d *SQLiteDriver) ReturningClause(columns ...string) string {
	// SQLite 3.35+ supports RETURNING, but we'll use it minimally
	if len(columns) == 0 {
		return ""
	}
	result := "RETURNING "
	for i, c := range columns {
		if i > 0 {
			result += ", "
		}
		result += c
	}
	return result
}

// PostgresDriver implements Driver for PostgreSQL.
type PostgresDriver struct{}

func (d *PostgresDriver) DriverName() string {
	return "postgres"
}

func (d *PostgresDriver) GetCurrentTimeExpr() string {
	return "NOW()"
}

func (d *PostgresDriver) MakeDatetimeComparable(column string) string {
	// PostgreSQL timestamps are already comparable
	return column
}

func (d *PostgresDriver) SelectForUpdateSkipLocked() string {
	return "FOR UPDATE SKIP LOCKED"
}

func (d *PostgresDriver) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

func (d *PostgresDriver) OnConflictDoNothing(conflictColumns ...string) string {
	if len(conflictColumns) == 0 {
		return "ON CONFLICT DO NOTHING"
	}
	cols := ""
	for i, c := range conflictColumns {
		if i > 0 {
			cols += ", "
		}
		cols += c
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO NOTHING", cols)
}

func (d *PostgresDriver) ReturningClause(columns ...string) string {
	if len(columns) == 0 {
		return ""
	}
	result := "RETURNING "
	for i, c := range columns {
		if i > 0 {
			result += ", "
		}
		result += c
	}
	return result
}

// NewDriver creates a new driver based on the database URL.
func NewDriver(dbURL string) Driver {
	if len(dbURL) >= 8 && dbURL[:8] == "postgres" {
		return &PostgresDriver{}
	}
	// Default to SQLite
	return &SQLiteDriver{}
}

// NowMillisExpr returns SQL for the store clock in Unix milliseconds.
func (d *SQLiteDriver) NowMillisExpr() string { return "CAST(unixepoch('subsec')*1000 AS INTEGER)" }

// NowMillisExpr returns SQL for the store clock in Unix milliseconds.
func (d *PostgresDriver) NowMillisExpr() string {
	return "(EXTRACT(EPOCH FROM clock_timestamp())*1000)::BIGINT"
}

type testClockDriver struct{ Driver }

// NowMillisExpr reads the deterministic clock table installed by the test harness.
func (d testClockDriver) NowMillisExpr() string { return "(SELECT now_ms FROM selvedge_test_clock)" }

// WithTestStoreClock selects a clock table created by the test harness.
// Apply only before concurrent access. Production schemas never create the table.
func WithTestStoreClock() func(Storage) {
	return func(s Storage) {
		switch s := s.(type) {
		case *SQLiteStorage:
			s.driver = testClockDriver{s.driver}
		case *PostgresStorage:
			s.driver = testClockDriver{s.driver}
		}
	}
}
