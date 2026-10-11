// Package migrations provides automatic dbmate-compatible migration support.
//
// This package provides functions to automatically apply dbmate migration files
// at application startup, eliminating the need to manually run `dbmate up`.
//
// The migration system is compatible with dbmate:
//   - Uses the same `schema_migrations` table for tracking applied migrations
//   - Reads the same `-- migrate:up` / `-- migrate:down` SQL format
//   - Supports SQLite and PostgreSQL
package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// DetectDBType detects database type from connection URL.
//
// Returns one of "sqlite", "postgresql".
func DetectDBType(url string) (string, error) {
	url = strings.ToLower(url)

	if strings.Contains(url, "sqlite") || strings.HasPrefix(url, "file:") || strings.HasSuffix(url, ".db") {
		return "sqlite", nil
	}
	if strings.HasPrefix(url, "postgres") {
		return "postgresql", nil
	}

	return "", fmt.Errorf("cannot detect database type from URL: %s", url)
}

// EnsureSchemaMigrationsTable creates the schema_migrations table if it doesn't exist.
// This table is compatible with dbmate's migration tracking.
// Safe for concurrent execution by multiple workers.
func EnsureSchemaMigrationsTable(ctx context.Context, db *sql.DB, dbType string) error {
	return withMigrationTransaction(ctx, db, dbType, func(conn *sql.Conn) error {
		return ensureSchemaMigrationsTable(ctx, conn)
	})
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func ensureSchemaMigrationsTable(ctx context.Context, conn sqlExecutor) error {
	_, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
  version VARCHAR(255) PRIMARY KEY
 )`)
	return err
}

// GetAppliedMigrations returns a set of already applied migration versions.
func GetAppliedMigrations(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	applied := make(map[string]bool)

	rows, err := db.QueryContext(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		// Table might not exist yet
		return applied, nil
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}

	return applied, rows.Err()
}

// RecordMigration records a migration as applied.
//
// Returns true if recorded successfully, false if already recorded (race condition).
func RecordMigration(ctx context.Context, db *sql.DB, dbType, version string) (bool, error) {
	query := "INSERT INTO schema_migrations (version) VALUES (?) ON CONFLICT (version) DO NOTHING"
	if dbType == "postgresql" {
		query = "INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING"
	}
	result, err := db.ExecContext(ctx, query, version)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

// ExtractVersionFromFilename extracts version from migration filename.
//
// dbmate uses format: YYYYMMDDHHMMSS_description.sql
// Example: "20251217000000_initial_schema.sql" -> "20251217000000"
func ExtractVersionFromFilename(filename string) string {
	// Match leading digits followed by underscore
	re := regexp.MustCompile(`^(\d+)_`)
	match := re.FindStringSubmatch(filename)
	if len(match) > 1 {
		return match[1]
	}
	// Fallback: remove .sql extension
	return strings.TrimSuffix(filename, ".sql")
}

// ParseMigrationFile parses dbmate migration file content.
//
// Returns (upSQL, downSQL) extracted from the file.
func ParseMigrationFile(content string) (upSQL, downSQL string) {
	// Extract "-- migrate:up" section
	upRe := regexp.MustCompile(`(?s)-- migrate:up\s*(.*?)(?:-- migrate:down|$)`)
	upMatch := upRe.FindStringSubmatch(content)
	if len(upMatch) > 1 {
		upSQL = strings.TrimSpace(upMatch[1])
	}

	// Extract "-- migrate:down" section
	downRe := regexp.MustCompile(`(?s)-- migrate:down\s*(.*)$`)
	downMatch := downRe.FindStringSubmatch(content)
	if len(downMatch) > 1 {
		downSQL = strings.TrimSpace(downMatch[1])
	}

	return upSQL, downSQL
}

// ExecuteSQLStatements executes a SQL batch atomically and returns every SQL error.
// The database parses statements, including quoted semicolons and comments.
func ExecuteSQLStatements(ctx context.Context, db *sql.DB, sqlContent, dbType string) error {
	if strings.TrimSpace(sqlContent) == "" {
		return nil
	}
	return withMigrationTransaction(ctx, db, dbType, func(conn *sql.Conn) error {
		return executeSQL(ctx, conn, sqlContent)
	})
}

func executeSQL(ctx context.Context, conn sqlExecutor, content string) error {
	if _, err := conn.ExecContext(ctx, content); err != nil {
		return fmt.Errorf("failed to execute SQL: %w", err)
	}
	return nil
}

// migrationLockID is the stable Selvedge advisory-lock namespace, encoded as
// ASCII "SELVEDGE". Lock tracking-table creation too, including on a fresh DB.
const migrationLockID int64 = 0x53454c5645444745

func withMigrationTransaction(ctx context.Context, db *sql.DB, dbType string, apply func(*sql.Conn) error) error {
	begin := "BEGIN"
	switch dbType {
	case "sqlite":
		begin = "BEGIN IMMEDIATE"
	case "postgresql":
	default:
		return fmt.Errorf("unsupported migration database type: %s", dbType)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, begin); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			// Cancellation of application startup must still release the writer lock.
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = conn.ExecContext(rollbackCtx, "ROLLBACK")
		}
	}()
	if dbType == "postgresql" {
		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
			return err
		}
	}
	if err := apply(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

// ApplyMigrations applies dbmate migration files automatically.
//
// This function:
//  1. Finds migration files for the given database type
//  2. Checks which migrations have already been applied
//  3. Applies pending migrations in order
//  4. Records applied migrations in schema_migrations table
//
// Parameters:
//   - ctx: Context for cancellation
//   - db: Database connection
//   - dbType: One of "sqlite", "postgresql"
//   - migrationsFS: Filesystem containing migrations (expects dbType subdirectory)
//
// Returns list of applied migration versions.
func ApplyMigrations(ctx context.Context, db *sql.DB, dbType string, migrationsFS fs.FS) ([]string, error) {
	if migrationsFS == nil {
		slog.Warn("no migrations filesystem provided, skipping automatic migration")
		return nil, nil
	}

	// Find migrations directory for this database type
	migrationsDir := dbType

	// Check if directory exists
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		slog.Warn("migrations directory not found, skipping automatic migration",
			"db_type", dbType, "error", err)
		return nil, nil
	}

	// Get all migration files sorted by name (timestamp order)
	var migrationFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			migrationFiles = append(migrationFiles, entry.Name())
		}
	}
	sort.Strings(migrationFiles)

	var appliedVersions []string

	for _, filename := range migrationFiles {
		version := ExtractVersionFromFilename(filename)

		// Read and parse migration file
		filePath := filepath.Join(migrationsDir, filename)
		content, err := fs.ReadFile(migrationsFS, filePath)
		if err != nil {
			return appliedVersions, fmt.Errorf("failed to read migration file %s: %w", filename, err)
		}

		upSQL, _ := ParseMigrationFile(string(content))
		if upSQL == "" {
			slog.Warn("no '-- migrate:up' section found", "filename", filename)
			continue
		}

		applied, err := applyMigration(ctx, db, dbType, version, upSQL)
		if err != nil {
			return appliedVersions, fmt.Errorf("failed to apply migration %s: %w", version, err)
		}
		if applied {
			appliedVersions = append(appliedVersions, version)
		}
	}

	if len(appliedVersions) > 0 {
		slog.Info("applied migrations", "count", len(appliedVersions))
	} else {
		slog.Debug("no pending migrations to apply")
	}

	return appliedVersions, nil
}

// Version lookup, DDL, and version insertion share the same locked transaction.
func applyMigration(ctx context.Context, db *sql.DB, dbType, version, upSQL string) (bool, error) {
	applied := false
	err := withMigrationTransaction(ctx, db, dbType, func(conn *sql.Conn) error {
		if err := ensureSchemaMigrationsTable(ctx, conn); err != nil {
			return err
		}
		placeholder := "?"
		if dbType == "postgresql" {
			placeholder = "$1"
		}
		var exists bool
		if err := conn.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = "+placeholder+")", version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		if err := executeSQL(ctx, conn, upSQL); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO schema_migrations (version) VALUES ("+placeholder+")", version); err != nil {
			return err
		}
		applied = true
		return nil
	})
	return applied && err == nil, err
}
