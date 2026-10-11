// Package storage provides the storage layer for Selvedge.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// txKey is the context key for transactions.
type txKey struct{}

// txState holds transaction state including post-commit callbacks.
type txState struct {
	tx        *sql.Tx
	callbacks []func() error
}

// SQLiteStorage implements the Storage interface using SQLite.
type SQLiteStorage struct {
	db     *sql.DB
	driver Driver
}

// NewSQLiteStorage creates a new SQLite storage.
func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
	// For in-memory databases, use shared cache mode so all connections share the same database.
	// This is required because database/sql uses connection pooling, and without shared cache,
	// each connection to ":memory:" would get its own separate database.
	connStr := dbPath
	if dbPath == ":memory:" {
		connStr = "file::memory:?cache=shared"
	}
	connStr += "?_pragma=busy_timeout(5000)"

	// For shared memory, the connection string already has query params, so use & instead of ?
	if dbPath == ":memory:" {
		connStr = "file::memory:?cache=shared&_pragma=busy_timeout(5000)"
	}

	connStr += "&_txlock=immediate"
	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	// WAL persists on disk. Set it once per pool, rather than on every new
	// connection. Concurrent journal-mode changes can return BUSY immediately
	// even with busy_timeout set, so retry the pragma within the same budget.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := enableSQLiteWAL(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to enable SQLite WAL: %w", err)
	}

	return &SQLiteStorage{
		db:     db,
		driver: &SQLiteDriver{},
	}, nil
}

func enableSQLiteWAL(ctx context.Context, db *sql.DB) error {
	for {
		_, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL")
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != sqlite3.SQLITE_BUSY {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Initialize is deprecated. Use dbmate for migrations:
// dbmate -d internal/migrations/sql/sqlite up
func (s *SQLiteStorage) Initialize(ctx context.Context) error {
	slog.Warn("Initialize() is deprecated. Use dbmate for migrations: dbmate -d internal/migrations/sql/sqlite up")
	return nil
}

// DB returns the underlying database connection.
func (s *SQLiteStorage) DB() *sql.DB {
	return s.db
}

// Close closes the database connection.
func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}

// parseSQLiteTime parses a SQLite datetime TEXT value into time.Time.
// Handles both "2006-01-02 15:04:05" and "2006-01-02T15:04:05Z" formats.
func parseSQLiteTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	// Try SQLite format first
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err == nil {
		return t, nil
	}
	// Try RFC3339 format
	t, err = time.Parse(time.RFC3339, s)
	if err == nil {
		return t, nil
	}
	// Try RFC3339 without timezone
	t, err = time.Parse("2006-01-02T15:04:05", s)
	if err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse time: %s", s)
}

// getConn returns the appropriate database handle based on context (internal use).
func (s *SQLiteStorage) getConn(ctx context.Context) Executor {
	if state, ok := ctx.Value(txKey{}).(*txState); ok {
		return observeExecutor(s.driver, state.tx)
	}
	return observeExecutor(s.driver, s.db)
}

// --- Transaction Manager ---

// BeginTransaction starts a new transaction.
func (s *SQLiteStorage) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ctx, err
	}
	state := &txState{tx: tx}
	return context.WithValue(ctx, txKey{}, state), nil
}

// CommitTransaction commits the current transaction.
func (s *SQLiteStorage) CommitTransaction(ctx context.Context) error {
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
func (s *SQLiteStorage) RollbackTransaction(ctx context.Context) error {
	state, ok := ctx.Value(txKey{}).(*txState)
	if !ok {
		return nil // No transaction to rollback
	}
	return state.tx.Rollback()
}

// InTransaction returns whether a transaction is in progress.
func (s *SQLiteStorage) InTransaction(ctx context.Context) bool {
	_, ok := ctx.Value(txKey{}).(*txState)
	return ok
}

// Conn returns the database executor for the current context.
// If a transaction is active, returns the transaction; otherwise, returns the database.
// This allows storage operations to share an explicitly opened transaction.
//
// Example:
//
//	conn := ctx.Storage().Conn(ctx.Context())
//	_, err := conn.ExecContext(ctx.Context(), "INSERT INTO orders ...", ...)
func (s *SQLiteStorage) Conn(ctx context.Context) Executor {
	return s.getConn(ctx)
}

// RegisterPostCommitCallback registers a callback to be executed after a successful commit.
// Returns an error if not currently in a transaction.
func (s *SQLiteStorage) RegisterPostCommitCallback(ctx context.Context, cb func() error) error {
	state, ok := ctx.Value(txKey{}).(*txState)
	if !ok {
		return fmt.Errorf("not in a transaction")
	}
	state.callbacks = append(state.callbacks, cb)
	return nil
}

// --- Instance Manager ---

// CreateInstance creates a new workflow instance.
func (s *SQLiteStorage) CreateInstance(ctx context.Context, instance *WorkflowInstance) error {
	conn := s.getConn(ctx)
	framework := instance.Framework
	if framework == "" {
		framework = "go" // Default framework
	}

	// Ensure workflow definition exists (required by foreign key constraint)
	// Uses INSERT OR IGNORE for idempotency
	_, err := conn.ExecContext(ctx, `
		INSERT OR IGNORE INTO workflow_definitions (workflow_name, source_hash, source_code)
		VALUES (?, ?, ?)
	`, instance.WorkflowName, instance.SourceHash, "")
	if err != nil {
		return fmt.Errorf("failed to ensure workflow definition: %w", err)
	}

	result, err := conn.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, framework, status, input_data, source_hash, owner_service, started_at, updated_at, runnable
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		ON CONFLICT (instance_id) DO NOTHING
	`, instance.InstanceID, instance.WorkflowName, framework, instance.Status,
		string(instance.InputData), instance.SourceHash, instance.OwnerService,
		instance.StartedAt.UTC().Format("2006-01-02 15:04:05"),
		instance.UpdatedAt.UTC().Format("2006-01-02 15:04:05"))
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
	return nil
}

// GetInstance retrieves a workflow instance by ID.
func (s *SQLiteStorage) GetInstance(ctx context.Context, instanceID string) (*WorkflowInstance, error) {
	conn := s.getConn(ctx)
	row := conn.QueryRowContext(ctx, `
		SELECT instance_id, workflow_name, framework, status, input_data, output_data,
			   current_activity_id, source_hash, owner_service,
			   lease_owner, lease_epoch, lease_expires_ms, runnable,
			   started_at, updated_at, COALESCE(cancel_reason,'')
		FROM workflow_instances WHERE instance_id = ?
	`, instanceID)

	var inst WorkflowInstance
	var inputData, outputData, framework, activityID, sourceHash, ownerService sql.NullString
	var leaseOwner sql.NullString
	var leaseExpires sql.NullInt64
	var startedAt, updatedAt sql.NullString // SQLite stores datetime as TEXT

	err := row.Scan(
		&inst.InstanceID, &inst.WorkflowName, &framework, &inst.Status,
		&inputData, &outputData, &activityID, &sourceHash, &ownerService,
		&leaseOwner, &inst.LeaseEpoch, &leaseExpires, &inst.Runnable,
		&startedAt, &updatedAt, &inst.CancelReason,
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
	if framework.Valid {
		inst.Framework = framework.String
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
	inst.LeaseOwner = leaseOwner.String
	if leaseExpires.Valid {
		inst.LeaseExpiresMS = &leaseExpires.Int64
	}

	if startedAt.Valid {
		if t, err := time.Parse("2006-01-02 15:04:05", startedAt.String); err == nil {
			inst.StartedAt = t
		}
	}
	if updatedAt.Valid {
		if t, err := time.Parse("2006-01-02 15:04:05", updatedAt.String); err == nil {
			inst.UpdatedAt = t
		}
	}

	return &inst, nil
}

// UpdateInstanceActivity updates the current activity ID.
func (s *SQLiteStorage) UpdateInstanceActivity(ctx context.Context, instanceID, activityID string) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		_, err := conn.ExecContext(ctx, `
		UPDATE workflow_instances
		SET current_activity_id = ?, updated_at = datetime('now')
		WHERE instance_id = ?
	`, activityID, instanceID)
		return err
	})
}

// ListInstances lists workflow instances with cursor-based pagination and filters.
func (s *SQLiteStorage) ListInstances(ctx context.Context, opts ListInstancesOptions) (*PaginationResult, error) {
	conn := s.getConn(ctx)
	query := "SELECT instance_id, workflow_name, status, started_at, updated_at, COALESCE(cancel_reason,'') FROM workflow_instances WHERE 1=1"
	args := []any{}

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
		query += " AND LOWER(workflow_name) LIKE LOWER(?)"
		args = append(args, "%"+workflowFilter+"%")
	}
	if statusFilter != "" {
		query += " AND status = ?"
		args = append(args, statusFilter)
	}
	if opts.InstanceIDFilter != "" {
		query += " AND LOWER(instance_id) LIKE LOWER(?)"
		args = append(args, "%"+opts.InstanceIDFilter+"%")
	}
	if opts.StartedAfter != nil {
		query += " AND datetime(started_at) > datetime(?)"
		args = append(args, opts.StartedAfter.UTC().Format("2006-01-02 15:04:05"))
	}
	if opts.StartedBefore != nil {
		query += " AND datetime(started_at) < datetime(?)"
		args = append(args, opts.StartedBefore.UTC().Format("2006-01-02 15:04:05"))
	}

	// Handle input filters
	if len(opts.InputFilters) > 0 {
		filterBuilder := NewInputFilterBuilder(s.driver)
		filterConditions, filterArgs, err := filterBuilder.BuildFilterQuery(opts.InputFilters, len(args)+1)
		if err != nil {
			return nil, fmt.Errorf("invalid input filter: %w", err)
		}
		for _, cond := range filterConditions {
			query += " AND " + cond
		}
		args = append(args, filterArgs...)
	}

	// Parse cursor token (format: "ISO_DATETIME||INSTANCE_ID")
	if opts.PageToken != "" {
		parts := strings.SplitN(opts.PageToken, "||", 2)
		if len(parts) == 2 {
			cursorTime, err := time.Parse(time.RFC3339Nano, parts[0])
			if err == nil {
				cursorID := parts[1]
				// For descending order: get rows where (started_at, instance_id) < (cursor_time, cursor_id)
				query += " AND (datetime(started_at) < datetime(?) OR (datetime(started_at) = datetime(?) AND instance_id < ?))"
				cursorTimeStr := cursorTime.UTC().Format("2006-01-02 15:04:05")
				args = append(args, cursorTimeStr, cursorTimeStr, cursorID)
			}
		}
	}

	limit := opts.Limit
	if limit <= 0 {
		limit = 50 // Default page size
	}
	// Fetch one extra to determine if there are more pages
	query += " ORDER BY started_at DESC, instance_id DESC LIMIT ?"
	args = append(args, limit+1)

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var instances []*WorkflowInstance
	for rows.Next() {
		var inst WorkflowInstance
		var startedAt, updatedAt sql.NullString
		if err := rows.Scan(&inst.InstanceID, &inst.WorkflowName, &inst.Status, &startedAt, &updatedAt, &inst.CancelReason); err != nil {
			return nil, err
		}
		if startedAt.Valid {
			if t, err := time.Parse("2006-01-02 15:04:05", startedAt.String); err == nil {
				inst.StartedAt = t
			}
		}
		if updatedAt.Valid {
			if t, err := time.Parse("2006-01-02 15:04:05", updatedAt.String); err == nil {
				inst.UpdatedAt = t
			}
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
func (s *SQLiteStorage) GetHistoryPaginated(ctx context.Context, instanceID string, afterID int64, limit int) ([]*HistoryEvent, bool, error) {
	conn := s.getConn(ctx)
	// Fetch one extra to check if there are more events
	rows, err := conn.QueryContext(ctx, `
		SELECT id, instance_id, activity_id, command, event_type, event_data, event_data_binary, data_type, reservation_data, created_at
		FROM workflow_history
		WHERE instance_id = ? AND id > ?
		ORDER BY id ASC
		LIMIT ?
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
		var createdAtStr string

		if err := rows.Scan(&h.ID, &h.InstanceID, &h.ActivityID, &h.Command, &h.EventType, &eventData, &eventDataBinary, &h.DataType, &reservation, &createdAtStr); err != nil {
			return nil, false, err
		}
		h.CreatedAt, _ = parseSQLiteTime(createdAtStr)
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
func (s *SQLiteStorage) GetHistoryCount(ctx context.Context, instanceID string) (int64, error) {
	conn := s.getConn(ctx)
	var count int64
	err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM workflow_history WHERE instance_id = ?
	`, instanceID).Scan(&count)
	return count, err
}

// --- Timer Subscription Manager ---

// RegisterTimerSubscription registers a timer for a workflow.
func (s *SQLiteStorage) RegisterTimerSubscription(ctx context.Context, sub *TimerSubscription) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, sub.InstanceID), func(conn Executor) error {
		expiresStr := sub.ExpiresAt.UTC().Format("2006-01-02 15:04:05")
		_, err := conn.ExecContext(ctx, `
		INSERT INTO workflow_timer_subscriptions (instance_id, timer_id, expires_at, activity_id)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (instance_id, timer_id) DO NOTHING
	`, sub.InstanceID, sub.TimerID, expiresStr, sub.ActivityID)
		return err
	})
}

// RemoveTimerSubscription removes a timer subscription.
func (s *SQLiteStorage) RemoveTimerSubscription(ctx context.Context, instanceID, timerID string) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		_, err := conn.ExecContext(ctx, `
		DELETE FROM workflow_timer_subscriptions WHERE instance_id = ? AND timer_id = ?
	`, instanceID, timerID)
		return err
	})
}

// FindExpiredTimers finds expired timers.
func (s *SQLiteStorage) FindExpiredTimers(ctx context.Context, limit int) ([]*TimerSubscription, error) {
	if limit <= 0 {
		limit = 100
	}
	conn := s.getConn(ctx)
	rows, err := conn.QueryContext(ctx, `
		SELECT t.id, t.instance_id, t.timer_id, t.expires_at, t.activity_id, t.created_at
		FROM workflow_timer_subscriptions t
		JOIN workflow_instances w ON t.instance_id = w.instance_id
		WHERE datetime(t.expires_at) <= datetime('now')
		ORDER BY t.expires_at ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var timers []*TimerSubscription
	for rows.Next() {
		var t TimerSubscription
		var expiresAt, createdAt sql.NullString // SQLite stores datetime as TEXT
		var activityID sql.NullString
		if err := rows.Scan(&t.ID, &t.InstanceID, &t.TimerID, &expiresAt, &activityID, &createdAt); err != nil {
			return nil, err
		}
		if activityID.Valid {
			t.ActivityID = activityID.String
		}
		if expiresAt.Valid {
			if parsed, err := time.Parse("2006-01-02 15:04:05", expiresAt.String); err == nil {
				t.ExpiresAt = parsed
			}
		}
		if createdAt.Valid {
			if parsed, err := time.Parse("2006-01-02 15:04:05", createdAt.String); err == nil {
				t.CreatedAt = parsed
			}
		}
		timers = append(timers, &t)
	}
	return timers, rows.Err()
}

// ========================================
// Channel Manager
// ========================================

// PublishToChannel publishes a message to a channel.
// For direct messages, use dynamic channel names (e.g., "channel:instance_id").
func (s *SQLiteStorage) PublishToChannel(ctx context.Context, channelName string, dataJSON, metadata []byte) (int64, error) {
	return publishChannel(ctx, s, channelName, dataJSON, metadata)
}

// SubscribeToChannel subscribes an instance to a channel.
// For broadcast mode, it initializes the delivery cursor to the current max message ID
// so that only messages published after subscription are received.
func (s *SQLiteStorage) SubscribeToChannel(ctx context.Context, instanceID, channelName string, mode ChannelMode) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		_, err := conn.ExecContext(ctx, `
		INSERT INTO channel_subscriptions (instance_id, channel, mode)
		VALUES (?, ?, ?)
		ON CONFLICT (instance_id, channel) DO UPDATE SET mode = excluded.mode
	`, instanceID, channelName, string(mode))
		if err != nil {
			return err
		}

		// For broadcast mode, initialize the delivery cursor to current max message ID
		// This ensures new subscribers only receive messages published after subscription
		if mode == ChannelModeBroadcast {
			_, err = conn.ExecContext(ctx, `
			INSERT INTO channel_delivery_cursors (instance_id, channel, last_delivered_id)
			SELECT ?, ?, COALESCE(MAX(id), 0)
			FROM channel_messages
			WHERE channel = ?
			ON CONFLICT (instance_id, channel) DO NOTHING
		`, instanceID, channelName, channelName)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// GetChannelSubscription retrieves a subscription for an instance and channel.
func (s *SQLiteStorage) GetChannelSubscription(ctx context.Context, instanceID, channelName string) (*ChannelSubscription, error) {
	conn := s.getConn(ctx)
	row := conn.QueryRowContext(ctx, `
		SELECT id, instance_id, channel, mode, timeout_at, COALESCE(activity_id, ''), subscribed_at
		FROM channel_subscriptions
		WHERE instance_id = ? AND channel = ?
	`, instanceID, channelName)

	var sub ChannelSubscription
	var modeStr string
	var timeoutAt, subscribedAt sql.NullString // SQLite stores datetime as TEXT
	err := row.Scan(&sub.ID, &sub.InstanceID, &sub.Channel, &modeStr, &timeoutAt, &sub.ActivityID, &subscribedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sub.Mode = ChannelMode(modeStr)
	if timeoutAt.Valid {
		if t, err := time.Parse(time.RFC3339Nano, timeoutAt.String); err == nil {
			sub.TimeoutAt = &t
		}
	}
	if subscribedAt.Valid {
		if t, err := time.Parse("2006-01-02 15:04:05", subscribedAt.String); err == nil {
			sub.SubscribedAt = t
		}
	}
	return &sub, nil
}

// GetChannelMode retrieves the mode for a channel (from any existing subscription).
// Returns empty string if no subscriptions exist.
func (s *SQLiteStorage) GetChannelMode(ctx context.Context, channelName string) (ChannelMode, error) {
	conn := s.getConn(ctx)
	row := conn.QueryRowContext(ctx, `
		SELECT mode FROM channel_subscriptions WHERE channel = ? LIMIT 1
	`, channelName)

	var modeStr string
	if err := row.Scan(&modeStr); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return ChannelMode(modeStr), nil
}

// RegisterChannelReceive atomically registers a channel receive wait and releases the lock.
// Waiting state is indicated by activity_id being non-null.
func (s *SQLiteStorage) RegisterChannelReceive(ctx context.Context, instanceID, channelName, activityID string, timeoutAt *time.Time) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		var timeout any
		if timeoutAt != nil {
			timeout = timeoutAt.UTC().Format(time.RFC3339Nano)
		}
		_, err := conn.ExecContext(ctx, `UPDATE channel_subscriptions SET timeout_at=$1, activity_id=$2 WHERE instance_id=$3 AND channel=$4`, timeout, activityID, instanceID, channelName)
		return err
	})
}

// GetPendingChannelMessagesForInstance gets pending messages for a specific subscriber.
// For broadcast mode: Returns messages with id > cursor (messages not yet seen by this instance)
func (s *SQLiteStorage) GetPendingChannelMessagesForInstance(ctx context.Context, instanceID, channelName string) ([]*ChannelMessage, error) {
	conn := s.getConn(ctx)

	// First, get the subscription to determine mode
	var mode string
	err := conn.QueryRowContext(ctx, `
		SELECT mode FROM channel_subscriptions
		WHERE instance_id = ? AND channel = ?
	`, instanceID, channelName).Scan(&mode)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Not subscribed
		}
		return nil, err
	}

	var query string
	var args []any

	if mode == string(ChannelModeBroadcast) {
		// Broadcast mode: Get messages after the cursor
		var cursorID int64
		err := conn.QueryRowContext(ctx, `
			SELECT COALESCE(last_delivered_id, 0) FROM channel_delivery_cursors
			WHERE instance_id = ? AND channel = ?
		`, instanceID, channelName).Scan(&cursorID)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}

		query = `
			SELECT id, channel, data, data_binary, metadata, published_at
			FROM channel_messages
			WHERE channel = ? AND id > ?
			ORDER BY id ASC
			LIMIT 10
		`
		args = []any{channelName, cursorID}
	}

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var messages []*ChannelMessage
	for rows.Next() {
		var msg ChannelMessage
		var data, metadata, publishedAt sql.NullString
		var dataBinary []byte
		err := rows.Scan(&msg.ID, &msg.Channel, &data, &dataBinary, &metadata, &publishedAt)
		if err != nil {
			return nil, err
		}
		if data.Valid {
			msg.Data = []byte(data.String)
		}
		msg.DataBinary = dataBinary
		if metadata.Valid {
			msg.Metadata = []byte(metadata.String)
		}
		if publishedAt.Valid {
			if t, err := time.Parse("2006-01-02 15:04:05", publishedAt.String); err == nil {
				msg.PublishedAt = t
			}
		}
		messages = append(messages, &msg)
	}
	return messages, rows.Err()
}

// UpdateDeliveryCursor updates the delivery cursor for broadcast mode.
func (s *SQLiteStorage) UpdateDeliveryCursor(ctx context.Context, instanceID, channelName string, lastMessageID int64) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		_, err := conn.ExecContext(ctx, `
		INSERT INTO channel_delivery_cursors (instance_id, channel, last_delivered_id)
		VALUES (?, ?, ?)
		ON CONFLICT (instance_id, channel) DO UPDATE SET last_delivered_id = excluded.last_delivered_id, updated_at = datetime('now')
	`, instanceID, channelName, lastMessageID)
		return err
	})
}

// GetChannelSubscribersWaiting finds subscribers waiting for messages on a channel.
// Waiting state is determined by activity_id IS NOT NULL.
// Returns all waiting subscribers regardless of framework - delivery is handled by Lock-First pattern.
func (s *SQLiteStorage) GetChannelSubscribersWaiting(ctx context.Context, channelName string) ([]*ChannelSubscription, error) {
	conn := s.getConn(ctx)
	rows, err := conn.QueryContext(ctx, `
		SELECT id, instance_id, channel, mode, timeout_at, activity_id, subscribed_at
		FROM channel_subscriptions
		WHERE channel = ? AND activity_id IS NOT NULL AND activity_id != ''
	`, channelName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var subs []*ChannelSubscription
	for rows.Next() {
		var sub ChannelSubscription
		var modeStr string
		var timeoutAt, subscribedAt sql.NullString // SQLite stores datetime as TEXT
		err := rows.Scan(&sub.ID, &sub.InstanceID, &sub.Channel, &modeStr, &timeoutAt, &sub.ActivityID, &subscribedAt)
		if err != nil {
			return nil, err
		}
		sub.Mode = ChannelMode(modeStr)
		if timeoutAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, timeoutAt.String); err == nil {
				sub.TimeoutAt = &t
			}
		}
		if subscribedAt.Valid {
			if t, err := time.Parse("2006-01-02 15:04:05", subscribedAt.String); err == nil {
				sub.SubscribedAt = t
			}
		}
		subs = append(subs, &sub)
	}
	return subs, rows.Err()
}

// ClearChannelWaitingState clears the waiting state for an instance's channel subscription.
// Waiting state is cleared by setting activity_id to NULL.
func (s *SQLiteStorage) ClearChannelWaitingState(ctx context.Context, instanceID, channelName string) error {
	return (fenceStore{s}).withFence(ctx, contextFence(ctx, instanceID), func(conn Executor) error {
		_, err := conn.ExecContext(ctx, `
		UPDATE channel_subscriptions
		SET activity_id = NULL, timeout_at = NULL
		WHERE instance_id = ? AND channel = ?
	`, instanceID, channelName)
		return err
	})
}

// CleanupOldChannelMessages removes old channel messages.
func (s *SQLiteStorage) CleanupOldChannelMessages(ctx context.Context, olderThan time.Duration) error {
	conn := s.getConn(ctx)
	threshold := time.Now().UTC().Add(-olderThan).Format("2006-01-02 15:04:05")
	_, err := conn.ExecContext(ctx, `
		DELETE FROM channel_messages WHERE datetime(published_at) < datetime(?)
	`, threshold)
	return err
}

// FindExpiredChannelSubscriptions finds channel subscriptions that have timed out.
// Waiting state is determined by activity_id IS NOT NULL.
func (s *SQLiteStorage) FindExpiredChannelSubscriptions(ctx context.Context, limit int) ([]*ChannelSubscription, error) {
	if limit <= 0 {
		limit = 100
	}
	conn := s.getConn(ctx)
	rows, err := conn.QueryContext(ctx, `
		SELECT cs.id, cs.instance_id, cs.channel, cs.mode, cs.timeout_at, cs.activity_id, COALESCE(cs.cursor_message_id, 0), cs.subscribed_at
		FROM channel_subscriptions cs
		JOIN workflow_instances w ON cs.instance_id = w.instance_id
		WHERE cs.activity_id IS NOT NULL AND cs.activity_id != '' AND cs.timeout_at IS NOT NULL AND datetime(cs.timeout_at) < datetime('now')

		ORDER BY cs.timeout_at ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var subs []*ChannelSubscription
	for rows.Next() {
		var sub ChannelSubscription
		var modeStr string
		var timeoutAt, subscribedAt sql.NullString // SQLite stores datetime as TEXT
		err := rows.Scan(&sub.ID, &sub.InstanceID, &sub.Channel, &modeStr, &timeoutAt, &sub.ActivityID, &sub.CursorMessageID, &subscribedAt)
		if err != nil {
			return nil, err
		}
		sub.Mode = ChannelMode(modeStr)
		if timeoutAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, timeoutAt.String); err == nil {
				sub.TimeoutAt = &t
			}
		}
		if subscribedAt.Valid {
			if t, err := time.Parse("2006-01-02 15:04:05", subscribedAt.String); err == nil {
				sub.SubscribedAt = t
			}
		}
		subs = append(subs, &sub)
	}
	return subs, rows.Err()
}

// ========================================
// System Lock Manager
// ========================================

// TryAcquireSystemLock attempts to acquire a system lock.
func (s *SQLiteStorage) TryAcquireSystemLock(ctx context.Context, lockName, workerID string, ttl time.Duration) (bool, error) {
	conn := s.getConn(ctx)
	now := time.Now().UTC()
	nowStr := now.Format("2006-01-02 15:04:05.000")
	expiresAt := now.Add(ttl)
	expiresStr := expiresAt.Format("2006-01-02 15:04:05.000")

	// Try to insert or update if expired or same worker
	result, err := conn.ExecContext(ctx, `
		INSERT INTO system_locks (lock_name, locked_by, locked_at, lock_expires_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (lock_name) DO UPDATE SET
			locked_by = excluded.locked_by,
			locked_at = excluded.locked_at,
			lock_expires_at = excluded.lock_expires_at
		WHERE julianday(system_locks.lock_expires_at) < julianday('now') OR system_locks.locked_by = excluded.locked_by
	`, lockName, workerID, nowStr, expiresStr)
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
func (s *SQLiteStorage) ReleaseSystemLock(ctx context.Context, lockName, workerID string) error {
	conn := s.getConn(ctx)
	_, err := conn.ExecContext(ctx, `
		DELETE FROM system_locks WHERE lock_name = ? AND locked_by = ?
	`, lockName, workerID)
	return err
}

// CleanupExpiredSystemLocks removes expired system locks.
func (s *SQLiteStorage) CleanupExpiredSystemLocks(ctx context.Context) error {
	conn := s.getConn(ctx)
	_, err := conn.ExecContext(ctx, `
		DELETE FROM system_locks WHERE datetime(lock_expires_at) < datetime('now')
	`)
	return err
}
