package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrWorkflowNotCancellable indicates that the workflow cannot be cancelled.
// This happens when the workflow is already completed, cancelled, failed, or does not exist.
var ErrWorkflowNotCancellable = errors.New("workflow cannot be cancelled")

// Executor is a database executor interface that can be either *sql.DB or *sql.Tx.
// This allows users to execute custom SQL queries within the same transaction
// as the workflow activity.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Storage defines the interface for workflow persistence.
// Implementations must be safe for concurrent use.
type Storage interface {
	// Initialize sets up the storage (create tables, etc.)
	Initialize(ctx context.Context) error

	// Close closes the storage connection
	Close() error

	// DB returns the underlying database connection.
	// This is primarily used for migrations.
	DB() *sql.DB

	// Transaction Management
	TransactionManager

	// Workflow Instance Operations
	InstanceManager

	// Locking Operations
	LockManager

	// History Operations
	HistoryManager

	// Timer Subscription Operations
	TimerSubscriptionManager

	// Channel Operations
	ChannelManager

	// System Lock Operations
	SystemLockManager
}

// TransactionManager handles transaction operations.
type TransactionManager interface {
	// BeginTransaction starts a new transaction.
	// Returns a context with the transaction attached.
	BeginTransaction(ctx context.Context) (context.Context, error)

	// CommitTransaction commits the current transaction.
	CommitTransaction(ctx context.Context) error

	// RollbackTransaction rolls back the current transaction.
	RollbackTransaction(ctx context.Context) error

	// InTransaction returns true if there is an active transaction.
	InTransaction(ctx context.Context) bool

	// Conn returns the database executor for the current context.
	// If a transaction is active, returns the transaction; otherwise, returns the database.
	// This allows users to execute custom SQL queries within the same transaction.
	Conn(ctx context.Context) Executor

	// RegisterPostCommitCallback registers a callback to be executed after a successful commit.
	// This is used for operations that should only happen if the transaction commits,
	// such as notifying waiting subscribers after a message is published.
	// Returns an error if not currently in a transaction.
	RegisterPostCommitCallback(ctx context.Context, cb func() error) error
}

// InstanceManager handles workflow instance CRUD operations.
type InstanceManager interface {
	// CreateInstance creates a new workflow instance.
	CreateInstance(ctx context.Context, instance *WorkflowInstance) error

	// GetInstance retrieves a workflow instance by ID.
	GetInstance(ctx context.Context, instanceID string) (*WorkflowInstance, error)

	// UpdateInstanceStatus updates the status and related fields.
	UpdateInstanceStatus(ctx context.Context, instanceID string, status WorkflowStatus, errorMsg string) error

	// UpdateInstanceActivity updates the current activity ID.
	UpdateInstanceActivity(ctx context.Context, instanceID string, activityID string) error

	// UpdateInstanceOutput updates the output data for a completed workflow.
	UpdateInstanceOutput(ctx context.Context, instanceID string, outputData []byte) error

	// CancelInstance marks a workflow as cancelled.
	CancelInstance(ctx context.Context, instanceID string, reason string) error

	// ListInstances lists workflow instances with optional filtering and pagination.
	// Returns PaginationResult with cursor-based pagination.
	ListInstances(ctx context.Context, opts ListInstancesOptions) (*PaginationResult, error)

	// FindResumableWorkflows finds workflows that are ready to be resumed.
	// Returns workflows with status='running' that don't have an active lock.
	// These are typically workflows that had a message delivered and are waiting
	// for a worker to pick them up and resume execution.
	// limit specifies the maximum number of workflows to return (0 = default 100).
	FindResumableWorkflows(ctx context.Context, limit int) ([]*ResumableWorkflow, error)
}

// ListInstancesOptions defines options for listing instances.
type ListInstancesOptions struct {
	// Pagination
	Limit     int    // Page size (default: 50)
	PageToken string // Cursor token for pagination (format: "ISO_DATETIME||INSTANCE_ID")

	// Filters
	StatusFilter       WorkflowStatus // Filter by status
	WorkflowNameFilter string         // Filter by workflow name (partial match, case-insensitive)
	InstanceIDFilter   string         // Filter by instance ID (partial match, case-insensitive)
	StartedAfter       *time.Time     // Filter instances started after this time
	StartedBefore      *time.Time     // Filter instances started before this time
	InputFilters       map[string]any // Filter by JSON paths in input data (exact match)

	// Deprecated: Use StatusFilter instead
	Status WorkflowStatus
	// Deprecated: Use WorkflowNameFilter instead
	WorkflowName string
	// Deprecated: Use PageToken instead
	Offset int
}

// LockManager handles distributed locking operations.
type LockManager interface {
	// TryAcquireLock attempts to acquire a lock on a workflow instance.
	// Returns true if the lock was acquired, false if already locked.
	TryAcquireLock(ctx context.Context, instanceID, workerID string, timeoutSec int) (bool, error)

	// ReleaseLock releases the lock on a workflow instance.
	ReleaseLock(ctx context.Context, instanceID, workerID string) error

	// RefreshLock extends the lock timeout.
	RefreshLock(ctx context.Context, instanceID, workerID string, timeoutSec int) error

	// CleanupStaleLocks releases locks that have expired and returns
	// information about running workflows that need to be resumed.
	CleanupStaleLocks(ctx context.Context, timeoutSec int) ([]StaleWorkflowInfo, error)
}

// HistoryManager handles workflow execution history.
type HistoryManager interface {
	// AppendHistory adds a new history event.
	AppendHistory(ctx context.Context, event *HistoryEvent) error

	// GetHistoryPaginated retrieves history events for an instance with pagination.
	// Returns events with id > afterID, up to limit events.
	// Returns (events, hasMore, error).
	GetHistoryPaginated(ctx context.Context, instanceID string, afterID int64, limit int) ([]*HistoryEvent, bool, error)

	// GetHistoryCount returns the total number of history events for an instance.
	GetHistoryCount(ctx context.Context, instanceID string) (int64, error)
}

// TimerSubscriptionManager handles timer subscriptions.
type TimerSubscriptionManager interface {
	// RegisterTimerSubscription registers a workflow to wait for a timer.
	RegisterTimerSubscription(ctx context.Context, sub *TimerSubscription) error

	// RegisterTimerSubscriptionAndReleaseLock atomically registers a timer
	// and releases the workflow lock.
	RegisterTimerSubscriptionAndReleaseLock(
		ctx context.Context,
		sub *TimerSubscription,
		instanceID, workerID string,
	) error

	// FindExpiredTimers finds timers that have expired.
	// limit specifies the maximum number of timers to return (0 = default 100).
	FindExpiredTimers(ctx context.Context, limit int) ([]*TimerSubscription, error)

	// RemoveTimerSubscription removes a timer subscription.
	RemoveTimerSubscription(ctx context.Context, instanceID, timerID string) error
}

// ========================================
// Channel Manager
// ========================================

// ChannelManager handles channel-based messaging operations.
type ChannelManager interface {
	// PublishToChannel publishes a message to a channel.
	PublishToChannel(ctx context.Context, channelName string, dataJSON []byte, metadata []byte) (int64, error)

	// SubscribeToChannel subscribes an instance to a channel.
	SubscribeToChannel(ctx context.Context, instanceID, channelName string, mode ChannelMode) error

	// GetChannelSubscription retrieves a subscription for an instance and channel.
	GetChannelSubscription(ctx context.Context, instanceID, channelName string) (*ChannelSubscription, error)

	// GetChannelMode retrieves the mode for a channel (from any existing subscription).
	// Returns empty string if no subscriptions exist.
	GetChannelMode(ctx context.Context, channelName string) (ChannelMode, error)

	// RegisterChannelReceiveAndReleaseLock atomically registers a channel receive wait
	// and releases the workflow lock.
	RegisterChannelReceiveAndReleaseLock(
		ctx context.Context,
		instanceID, channelName, workerID, activityID string,
		timeoutAt *time.Time,
	) error

	// GetPendingChannelMessagesForInstance gets pending messages for a specific subscriber.
	// For broadcast mode: Returns messages with id > cursor (messages not yet seen by this instance)
	GetPendingChannelMessagesForInstance(ctx context.Context, instanceID, channelName string) ([]*ChannelMessage, error)

	// UpdateDeliveryCursor updates the delivery cursor for broadcast mode.
	UpdateDeliveryCursor(ctx context.Context, instanceID, channelName string, lastMessageID int64) error

	// GetChannelSubscribersWaiting finds subscribers waiting for messages on a channel.
	GetChannelSubscribersWaiting(ctx context.Context, channelName string) ([]*ChannelSubscription, error)

	// ClearChannelWaitingState clears the waiting state for an instance's channel subscription.
	ClearChannelWaitingState(ctx context.Context, instanceID, channelName string) error

	// DeliverChannelMessageWithLock delivers a message using Lock-First pattern.
	// This is used for load balancing in multi-worker environments:
	// 1. Try to acquire lock on the target instance
	// 2. If lock fails, return (nil, nil) - another worker will handle it
	// 3. Record message in history
	// 4. Update status to 'running'
	// 5. Release lock
	// Returns the delivery info if successful, nil if lock was not acquired.
	DeliverChannelMessageWithLock(
		ctx context.Context,
		instanceID string,
		channelName string,
		message *ChannelMessage,
		workerID string,
		lockTimeoutSec int,
	) (*ChannelDeliveryResult, error)

	// CleanupOldChannelMessages removes old channel messages.
	CleanupOldChannelMessages(ctx context.Context, olderThan time.Duration) error

	// FindExpiredChannelSubscriptions finds channel subscriptions that have timed out.
	// limit specifies the maximum number of subscriptions to return (0 = default 100).
	FindExpiredChannelSubscriptions(ctx context.Context, limit int) ([]*ChannelSubscription, error)
}

// ========================================
// System Lock Manager
// ========================================

// SystemLockManager handles system-level locks for background tasks.
type SystemLockManager interface {
	// TryAcquireSystemLock attempts to acquire a system lock.
	// Returns true if the lock was acquired.
	TryAcquireSystemLock(ctx context.Context, lockName, workerID string, timeoutSec int) (bool, error)

	// ReleaseSystemLock releases a system lock.
	ReleaseSystemLock(ctx context.Context, lockName, workerID string) error

	// CleanupExpiredSystemLocks removes expired system locks.
	CleanupExpiredSystemLocks(ctx context.Context) error
}
