package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

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

	// Lease operations
	LeaseManager

	// History Operations
	HistoryManager

	// Timer Subscription Operations
	TimerSubscriptionManager

	// Per-instance signal inbox
	SignalManager

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

	// RequestCancel durably requests cancellation and wakes a running instance.
	RequestCancel(ctx context.Context, instanceID, reason string) (WorkflowStatus, error)

	// IsCancelRequested reports whether cancellation was requested.
	IsCancelRequested(ctx context.Context, instanceID string) (bool, error)

	// CloseInstance atomically closes a running instance under its execution fence.
	CloseInstance(ctx context.Context, f Fence, status WorkflowStatus, output []byte, closeEvent *HistoryEvent) error

	// UpdateInstanceActivity updates the current activity ID.
	UpdateInstanceActivity(ctx context.Context, instanceID string, activityID string) error

	// ListInstances lists workflow instances with optional filtering and pagination.
	// Returns PaginationResult with cursor-based pagination.
	ListInstances(ctx context.Context, opts ListInstancesOptions) (*PaginationResult, error)
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

// Fence identifies one execution lease.
type Fence struct {
	InstanceID, Owner string
	Epoch             int64
}

var ErrHistoryKeyConflict = errors.New("history key holds another command")
var ErrWorkflowAlreadyStarted = errors.New("workflow already started")

// HistoryManager handles workflow execution history.
type HistoryManager interface {
	// ReserveHistory inserts a command with a SQL NULL value; matching repeats return the stored row.
	ReserveHistory(ctx context.Context, f *Fence, instanceID, key, cmd string, seq int64) (*HistoryEvent, error)
	// RecordHistory inserts a command and value. Matching repeats preserve the entire first row.
	RecordHistory(ctx context.Context, f *Fence, event *HistoryEvent) (*HistoryEvent, error)
	// SetHistoryValueIfNull only fills an existing reservation; the first value wins.
	SetHistoryValueIfNull(ctx context.Context, f *Fence, instanceID, key, cmd string, t HistoryEventType, data []byte) (*HistoryEvent, error)
	// GetHistoryEvent returns nil, nil when the key does not exist.
	GetHistoryEvent(ctx context.Context, instanceID, key string) (*HistoryEvent, error)

	// GetHistoryPaginated retrieves history events for an instance with pagination.
	// Returns events with id > afterID, up to limit events.
	// Returns (events, hasMore, error).
	GetHistoryPaginated(ctx context.Context, instanceID string, afterID int64, limit int) ([]*HistoryEvent, bool, error)

	// GetHistoryCount returns the total number of history events for an instance.
	GetHistoryCount(ctx context.Context, instanceID string) (int64, error)
}

// TimerSubscriptionManager handles timer subscriptions.
type TimerSubscriptionManager interface {
	// ReserveTimer atomically reserves a sleep and arms its timer with the first expiry.
	ReserveTimer(context.Context, *Fence, int64, *TimerSubscription) (*HistoryEvent, error)
	// FireTimer atomically records a timer result, wakes its instance, and removes the subscription.
	FireTimer(context.Context, *TimerSubscription) error

	// FindExpiredTimers finds timers that have expired.
	// limit specifies the maximum number of timers to return (0 = default 100).
	FindExpiredTimers(ctx context.Context, limit int) ([]*TimerSubscription, error)

	// RemoveTimerSubscription removes a timer subscription.
	RemoveTimerSubscription(ctx context.Context, instanceID, timerID string) error
}

// ========================================
// System Lock Manager
// ========================================

// SystemLockManager handles system-level locks for background tasks.
type SystemLockManager interface {
	// TryAcquireSystemLock attempts to acquire a system lock.
	// Returns true if the lock was acquired.
	TryAcquireSystemLock(ctx context.Context, lockName, workerID string, ttl time.Duration) (bool, error)

	// ReleaseSystemLock releases a system lock.
	ReleaseSystemLock(ctx context.Context, lockName, workerID string) error

	// CleanupExpiredSystemLocks removes expired system locks.
	CleanupExpiredSystemLocks(ctx context.Context) error
}
