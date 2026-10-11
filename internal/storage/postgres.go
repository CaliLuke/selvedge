// Package storage provides the storage layer for Selvedge.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/CaliLuke/selvedge/internal/notify"
)

// PostgresStorage implements the Storage interface using PostgreSQL.
type PostgresStorage struct {
	db            *sql.DB
	driver        Driver
	notifyEnabled bool
}

// NewPostgresStorage creates a new PostgreSQL storage.
// The connStr should be a PostgreSQL connection string:
// "postgres://user:password@localhost:5432/dbname?sslmode=disable"
func NewPostgresStorage(connStr string) (*PostgresStorage, error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return &PostgresStorage{
		db:     db,
		driver: &PostgresDriver{},
	}, nil
}

// Initialize is deprecated. Use dbmate for migrations:
// dbmate -d internal/migrations/sql/postgresql up
func (s *PostgresStorage) Initialize(ctx context.Context) error {
	slog.Warn("Initialize() is deprecated. Use dbmate for migrations: dbmate -d internal/migrations/sql/postgresql up")
	return nil
}

// DB returns the underlying database connection.
func (s *PostgresStorage) DB() *sql.DB {
	return s.db
}

// Close closes the database connection.
func (s *PostgresStorage) Close() error {
	return s.db.Close()
}

// SetNotifyEnabled enables or disables PostgreSQL LISTEN/NOTIFY.
// When enabled, the storage will send pg_notify() calls after key operations.
func (s *PostgresStorage) SetNotifyEnabled(enabled bool) {
	s.notifyEnabled = enabled
}

// IsNotifyEnabled returns whether PostgreSQL LISTEN/NOTIFY is enabled.
func (s *PostgresStorage) IsNotifyEnabled() bool {
	return s.notifyEnabled
}

// getConn returns the appropriate database handle based on context.
func (s *PostgresStorage) getConn(ctx context.Context) Executor {
	if state, ok := ctx.Value(txKey{}).(*txState); ok {
		return observeExecutor(s.driver, state.tx)
	}
	return observeExecutor(s.driver, s.db)
}

// --- Transaction Manager ---

// BeginTransaction starts a new transaction.
func (s *PostgresStorage) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ctx, err
	}
	state := &txState{tx: tx}
	return context.WithValue(ctx, txKey{}, state), nil
}

// CommitTransaction commits the current transaction.
func (s *PostgresStorage) CommitTransaction(ctx context.Context) error {
	state, ok := ctx.Value(txKey{}).(*txState)
	if !ok {
		return fmt.Errorf("no transaction in context")
	}

	// First commit the transaction
	if err := state.tx.Commit(); err != nil {
		return err
	}

	// Execute callbacks AFTER successful commit
	for _, cb := range state.callbacks {
		if err := cb(); err != nil {
			// Log error but don't fail - commit already succeeded
			slog.Debug("post-commit callback error", "error", err)
		}
	}

	return nil
}

// RollbackTransaction rolls back the current transaction.
// Callbacks are not executed on rollback.
func (s *PostgresStorage) RollbackTransaction(ctx context.Context) error {
	state, ok := ctx.Value(txKey{}).(*txState)
	if !ok {
		return nil
	}
	return state.tx.Rollback()
}

// InTransaction returns whether a transaction is in progress.
func (s *PostgresStorage) InTransaction(ctx context.Context) bool {
	_, ok := ctx.Value(txKey{}).(*txState)
	return ok
}

// Conn returns the database executor for the current context.
func (s *PostgresStorage) Conn(ctx context.Context) Executor {
	return s.getConn(ctx)
}

// RegisterPostCommitCallback registers a callback to be executed after a successful commit.
// Returns an error if not currently in a transaction.
func (s *PostgresStorage) RegisterPostCommitCallback(ctx context.Context, cb func() error) error {
	state, ok := ctx.Value(txKey{}).(*txState)
	if !ok {
		return fmt.Errorf("not in a transaction")
	}
	state.callbacks = append(state.callbacks, cb)
	return nil
}

// --- Instance Manager ---

// CreateInstance creates a new workflow instance.
func (s *PostgresStorage) CreateInstance(ctx context.Context, instance *WorkflowInstance) error {
	conn := s.getConn(ctx)
	framework := "go"

	// Ensure workflow definition exists (required by foreign key constraint)
	// Uses ON CONFLICT DO NOTHING for idempotency
	_, err := conn.ExecContext(ctx, `
		INSERT INTO workflow_definitions (workflow_name, source_hash, source_code)
		VALUES ($1, $2, $3)
		ON CONFLICT (workflow_name, source_hash) DO NOTHING
	`, instance.WorkflowName, instance.SourceHash, "")
	if err != nil {
		return fmt.Errorf("failed to ensure workflow definition: %w", err)
	}

	result, err := conn.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, status, input_data, source_hash, owner_service, framework, started_at, updated_at, runnable
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE)
		ON CONFLICT (instance_id) DO NOTHING
	`, instance.InstanceID, instance.WorkflowName, instance.Status,
		string(instance.InputData), instance.SourceHash, instance.OwnerService,
		framework, instance.StartedAt.UTC(), instance.UpdatedAt.UTC())
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrWorkflowAlreadyStarted
	}
	s.notifyRunnable(ctx, instance.InstanceID)
	return nil
}

// GetInstance retrieves a workflow instance by ID.
func (s *PostgresStorage) GetInstance(ctx context.Context, instanceID string) (*WorkflowInstance, error) {
	conn := s.getConn(ctx)
	row := conn.QueryRowContext(ctx, `
		SELECT instance_id, workflow_name, status, input_data, output_data,
			   current_activity_id, source_hash, owner_service, framework,
			   lease_owner, lease_epoch, lease_expires_ms, runnable,
			   started_at, updated_at, COALESCE(cancel_reason,'')
		FROM workflow_instances WHERE instance_id = $1
	`, instanceID)

	var inst WorkflowInstance
	var inputData, outputData, activityID, sourceHash, ownerService, framework sql.NullString
	var leaseOwner sql.NullString
	var leaseExpires sql.NullInt64

	err := row.Scan(
		&inst.InstanceID, &inst.WorkflowName, &inst.Status,
		&inputData, &outputData, &activityID, &sourceHash, &ownerService, &framework,
		&leaseOwner, &inst.LeaseEpoch, &leaseExpires, &inst.Runnable,
		&inst.StartedAt, &inst.UpdatedAt, &inst.CancelReason,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if inputData.Valid {
		inst.InputData = []byte(inputData.String)
	}
	if outputData.Valid {
		inst.OutputData = []byte(outputData.String)
	}
	if activityID.Valid {
		inst.CurrentActivityID = activityID.String
	}
	if sourceHash.Valid {
		inst.SourceHash = sourceHash.String
	}
	if ownerService.Valid {
		inst.OwnerService = ownerService.String
	}
	if framework.Valid {
		inst.Framework = framework.String
	}
	inst.LeaseOwner = leaseOwner.String
	if leaseExpires.Valid {
		inst.LeaseExpiresMS = &leaseExpires.Int64
	}

	return &inst, nil
}

// UpdateInstanceActivity updates the current activity ID.
func (s *PostgresStorage) UpdateInstanceActivity(ctx context.Context, instanceID, activityID string) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		_, err := conn.ExecContext(ctx, `
		UPDATE workflow_instances
		SET current_activity_id = $1, updated_at = NOW()
		WHERE instance_id = $2
	`, activityID, instanceID)
		return err
	})
}

// ListInstances lists workflow instances with cursor-based pagination and filters.
func (s *PostgresStorage) ListInstances(ctx context.Context, opts ListInstancesOptions) (*PaginationResult, error) {
	conn := s.getConn(ctx)
	query := "SELECT instance_id, workflow_name, status, started_at, updated_at, COALESCE(cancel_reason,'') FROM workflow_instances WHERE 1=1"
	args := []any{}
	argN := 1

	// Handle both new and deprecated filter options
	workflowFilter := opts.WorkflowNameFilter
	if workflowFilter == "" {
		workflowFilter = opts.WorkflowName // Deprecated field
	}
	statusFilter := opts.StatusFilter
	if statusFilter == "" {
		statusFilter = opts.Status // Deprecated field
	}

	// Apply filters
	if workflowFilter != "" {
		query += fmt.Sprintf(" AND LOWER(workflow_name) LIKE LOWER($%d)", argN)
		args = append(args, "%"+workflowFilter+"%")
		argN++
	}
	if statusFilter != "" {
		query += fmt.Sprintf(" AND status = $%d", argN)
		args = append(args, statusFilter)
		argN++
	}
	if opts.InstanceIDFilter != "" {
		query += fmt.Sprintf(" AND LOWER(instance_id) LIKE LOWER($%d)", argN)
		args = append(args, "%"+opts.InstanceIDFilter+"%")
		argN++
	}
	if opts.StartedAfter != nil {
		query += fmt.Sprintf(" AND started_at > $%d", argN)
		args = append(args, opts.StartedAfter.UTC())
		argN++
	}
	if opts.StartedBefore != nil {
		query += fmt.Sprintf(" AND started_at < $%d", argN)
		args = append(args, opts.StartedBefore.UTC())
		argN++
	}

	// Handle input filters
	if len(opts.InputFilters) > 0 {
		filterBuilder := NewInputFilterBuilder(s.driver)
		filterConditions, filterArgs, err := filterBuilder.BuildFilterQuery(opts.InputFilters, argN)
		if err != nil {
			return nil, fmt.Errorf("invalid input filter: %w", err)
		}
		for _, cond := range filterConditions {
			query += " AND " + cond
		}
		args = append(args, filterArgs...)
		argN += len(filterArgs)
	}

	// Parse cursor token (format: "ISO_DATETIME||INSTANCE_ID")
	if opts.PageToken != "" {
		parts := strings.SplitN(opts.PageToken, "||", 2)
		if len(parts) == 2 {
			cursorTime, err := time.Parse(time.RFC3339Nano, parts[0])
			if err == nil {
				cursorID := parts[1]
				// For descending order: get rows where (started_at, instance_id) < (cursor_time, cursor_id)
				query += fmt.Sprintf(" AND (started_at < $%d OR (started_at = $%d AND instance_id < $%d))", argN, argN+1, argN+2)
				args = append(args, cursorTime.UTC(), cursorTime.UTC(), cursorID)
				argN += 3
			}
		}
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 50 // Default page size
	}
	// Fetch one extra to determine if there are more pages
	query += fmt.Sprintf(" ORDER BY started_at DESC, instance_id DESC LIMIT $%d", argN)
	args = append(args, limit+1)

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var instances []*WorkflowInstance
	for rows.Next() {
		var inst WorkflowInstance
		if err := rows.Scan(&inst.InstanceID, &inst.WorkflowName, &inst.Status, &inst.StartedAt, &inst.UpdatedAt, &inst.CancelReason); err != nil {
			return nil, err
		}
		instances = append(instances, &inst)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Determine pagination
	hasMore := len(instances) > limit
	if hasMore {
		instances = instances[:limit] // Remove the extra row
	}

	var nextPageToken string
	if hasMore && len(instances) > 0 {
		lastInst := instances[len(instances)-1]
		nextPageToken = lastInst.StartedAt.UTC().Format(time.RFC3339Nano) + "||" + lastInst.InstanceID
	}

	return &PaginationResult{
		Instances:     instances,
		NextPageToken: nextPageToken,
		HasMore:       hasMore,
	}, nil
}

// --- Lock Manager ---

// --- History Manager ---

// GetHistoryPaginated retrieves history events with pagination.
// Returns events with id > afterID, up to limit events.
// Returns (events, hasMore, error).
func (s *PostgresStorage) GetHistoryPaginated(ctx context.Context, instanceID string, afterID int64, limit int) ([]*HistoryEvent, bool, error) {
	conn := s.getConn(ctx)
	// Fetch one extra to check if there are more events
	rows, err := conn.QueryContext(ctx, `
		SELECT id, instance_id, activity_id, command, event_type, event_data, event_data_binary, data_type, reservation_data, created_at
		FROM workflow_history
		WHERE instance_id = $1 AND id > $2
		ORDER BY id ASC
		LIMIT $3
	`, instanceID, afterID, limit+1)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()

	var history []*HistoryEvent
	for rows.Next() {
		var h HistoryEvent
		var eventData, reservation sql.NullString
		var eventDataBinary []byte

		if err := rows.Scan(&h.ID, &h.InstanceID, &h.ActivityID, &h.Command, &h.EventType, &eventData, &eventDataBinary, &h.DataType, &reservation, &h.CreatedAt); err != nil {
			return nil, false, err
		}
		if eventData.Valid {
			h.EventData = []byte(eventData.String)
		}
		h.EventDataBinary = eventDataBinary
		if reservation.Valid {
			h.ReservationData = []byte(reservation.String)
		}
		history = append(history, &h)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	// Check if there are more events
	hasMore := len(history) > limit
	if hasMore {
		history = history[:limit] // Remove the extra event
	}

	return history, hasMore, nil
}

// GetHistoryCount returns the total number of history events for an instance.
func (s *PostgresStorage) GetHistoryCount(ctx context.Context, instanceID string) (int64, error) {
	conn := s.getConn(ctx)
	var count int64
	err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM workflow_history WHERE instance_id = $1
	`, instanceID).Scan(&count)
	return count, err
}

// --- Timer Subscription Manager ---

// RemoveTimerSubscription removes a timer subscription.
func (s *PostgresStorage) RemoveTimerSubscription(ctx context.Context, instanceID, timerID string) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		_, err := conn.ExecContext(ctx, `
		DELETE FROM workflow_timer_subscriptions WHERE instance_id = $1 AND timer_id = $2
	`, instanceID, timerID)
		return err
	})
}

// FindExpiredTimers finds expired timers.
func (s *PostgresStorage) FindExpiredTimers(ctx context.Context, limit int) ([]*TimerSubscription, error) {
	if limit <= 0 {
		limit = 100
	}
	conn := s.getConn(ctx)
	rows, err := conn.QueryContext(ctx, `
		SELECT t.id, t.instance_id, t.timer_id, t.expires_at, t.activity_id, t.created_at
		FROM workflow_timer_subscriptions t
		JOIN workflow_instances w ON t.instance_id = w.instance_id
		WHERE t.expires_at <= NOW()
		ORDER BY t.expires_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var timers []*TimerSubscription
	for rows.Next() {
		var t TimerSubscription
		var activityID sql.NullString
		if err := rows.Scan(&t.ID, &t.InstanceID, &t.TimerID, &t.ExpiresAt, &activityID, &t.CreatedAt); err != nil {
			return nil, err
		}
		if activityID.Valid {
			t.ActivityID = activityID.String
		}
		timers = append(timers, &t)
	}
	return timers, rows.Err()
}

// ========================================
// System Lock Manager
// ========================================

// TryAcquireSystemLock attempts to acquire a system lock.
func (s *PostgresStorage) TryAcquireSystemLock(ctx context.Context, lockName, workerID string, ttl time.Duration) (bool, error) {
	conn := s.getConn(ctx)
	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	// Try to insert or update if expired or same worker
	result, err := conn.ExecContext(ctx, `
		INSERT INTO system_locks (lock_name, locked_by, locked_at, lock_expires_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (lock_name) DO UPDATE SET
			locked_by = EXCLUDED.locked_by,
			locked_at = EXCLUDED.locked_at,
			lock_expires_at = EXCLUDED.lock_expires_at
		WHERE system_locks.lock_expires_at < NOW() OR system_locks.locked_by = EXCLUDED.locked_by
	`, lockName, workerID, now, expiresAt)
	if err != nil {
		return false, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// ReleaseSystemLock releases a system lock.
func (s *PostgresStorage) ReleaseSystemLock(ctx context.Context, lockName, workerID string) error {
	conn := s.getConn(ctx)
	_, err := conn.ExecContext(ctx, `
		DELETE FROM system_locks WHERE lock_name = $1 AND locked_by = $2
	`, lockName, workerID)
	return err
}

// CleanupExpiredSystemLocks removes expired system locks.
func (s *PostgresStorage) CleanupExpiredSystemLocks(ctx context.Context) error {
	conn := s.getConn(ctx)
	_, err := conn.ExecContext(ctx, `
		DELETE FROM system_locks WHERE lock_expires_at < NOW()
	`)
	return err
}

// NotifyRunnable wakes dispatchers after a batch of runnable mutations, such as
// lease releases during shutdown. It is a no-op when notifications are disabled.
func (s *PostgresStorage) NotifyRunnable(ctx context.Context) {
	s.notifyRunnable(ctx, "")
}

// notifyRunnable publishes only after the mutation's transaction commits. The
// notification is a hint to all dispatchers; the runnable flag remains durable.
func (s *PostgresStorage) notifyRunnable(ctx context.Context, instanceID string) {
	if !s.notifyEnabled {
		return
	}
	send := func() error { s.sendRunnableNotify(ctx, instanceID); return nil }
	if s.InTransaction(ctx) {
		_ = s.RegisterPostCommitCallback(ctx, send)
	} else {
		_ = send()
	}
}

// sendRunnableNotify uses the pool because the mutation has already committed.
func (s *PostgresStorage) sendRunnableNotify(ctx context.Context, instanceID string) {
	if !s.notifyEnabled {
		return
	}
	payload, _ := json.Marshal(notify.WorkflowNotification{InstanceID: instanceID})
	_, err := s.db.ExecContext(ctx, "SELECT pg_notify($1, $2)", string(notify.ChannelWorkflowResumable), string(payload))
	if err != nil {
		slog.Debug("failed to notify runnable workflow", "error", err)
	}
}
