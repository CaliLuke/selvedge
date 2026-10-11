// Package storage provides the storage layer for Selvedge.
package storage

import (
	"time"
)

// WorkflowStatus represents the current state of a workflow instance.
type WorkflowStatus string

const (
	StatusRunning           WorkflowStatus = "running"
	StatusCompleted         WorkflowStatus = "completed"
	StatusFailed            WorkflowStatus = "failed"
	StatusCancelled         WorkflowStatus = "cancelled"
	StatusWaitingForEvent   WorkflowStatus = "waiting_for_event"
	StatusWaitingForTimer   WorkflowStatus = "waiting_for_timer"
	StatusWaitingForMessage WorkflowStatus = "waiting_for_message"
)

// WorkflowInstance represents a single execution of a workflow.
type WorkflowInstance struct {
	InstanceID        string         `json:"instance_id"`
	WorkflowName      string         `json:"workflow_name"`
	Framework         string         `json:"framework"` // "go" or "python"
	Status            WorkflowStatus `json:"status"`
	InputData         []byte         `json:"input_data"`          // JSON-encoded input
	OutputData        []byte         `json:"output_data"`         // JSON-encoded output (if completed)
	CurrentActivityID string         `json:"current_activity_id"` // Last executed activity ID
	SourceHash        string         `json:"source_hash"`         // Source code hash
	OwnerService      string         `json:"owner_service"`       // Service that owns this workflow
	LockedBy          string         `json:"locked_by"`           // Worker ID holding the lock
	LockedAt          *time.Time     `json:"locked_at"`           // When the lock was acquired
	LockTimeoutSec    *int           `json:"lock_timeout_seconds"`
	LockExpiresAt     *time.Time     `json:"lock_expires_at"`
	StartedAt         time.Time      `json:"started_at"` // When the workflow was created
	UpdatedAt         time.Time      `json:"updated_at"`
}

// HistoryEventType represents the type of history event.
type HistoryEventType string

const (
	HistoryReserved HistoryEventType = "Reserved"

	// Activity events
	HistoryActivityCompleted HistoryEventType = "ActivityCompleted"
	HistoryActivityFailed    HistoryEventType = "ActivityFailed"

	// External event received (via WaitEvent)
	HistoryEventReceived HistoryEventType = "EventReceived"

	// Timer expired (via Sleep/SleepUntil)
	HistoryTimerExpired HistoryEventType = "TimerExpired"

	// Internal event delivery record (via WaitEvent)
	HistoryChannelMessageReceived HistoryEventType = "ChannelMessageReceived"

	// Message receive timeout
	HistoryMessageTimeout HistoryEventType = "MessageTimeout"

	// Workflow lifecycle events
	HistoryWorkflowFailed    HistoryEventType = "WorkflowFailed"
	HistoryWorkflowCancelled HistoryEventType = "WorkflowCancelled"
)

// HistoryEvent represents a single event in a workflow's execution history.
type HistoryEvent struct {
	Command         string           `json:"command"`
	ID              int64            `json:"id"`
	InstanceID      string           `json:"instance_id"`
	ActivityID      string           `json:"activity_id"`
	EventType       HistoryEventType `json:"event_type"`
	EventData       []byte           `json:"event_data"`        // JSON-encoded data
	EventDataBinary []byte           `json:"event_data_binary"` // Binary data (alternative)
	DataType        string           `json:"data_type"`         // "json" or "binary"
	CreatedAt       time.Time        `json:"created_at"`
}

// TimerSubscription represents a workflow waiting for a timer.
type TimerSubscription struct {
	ID         int64     `json:"id"`
	InstanceID string    `json:"instance_id"`
	TimerID    string    `json:"timer_id"`
	ExpiresAt  time.Time `json:"expires_at"`
	ActivityID string    `json:"activity_id"` // Activity ID for replay matching
	CreatedAt  time.Time `json:"created_at"`
}

// StaleWorkflowInfo contains information about a workflow with a stale lock.
type StaleWorkflowInfo struct {
	InstanceID   string
	WorkflowName string
}

// ResumableWorkflow contains information about a workflow ready to be resumed.
// These are workflows with status='running' that don't have an active lock.
type ResumableWorkflow struct {
	InstanceID   string
	WorkflowName string
}

// ========================================
// Pagination
// ========================================

// PaginationResult represents a paginated list of workflow instances.
type PaginationResult struct {
	Instances     []*WorkflowInstance `json:"instances"`
	NextPageToken string              `json:"next_page_token,omitempty"`
	HasMore       bool                `json:"has_more"`
}

// ========================================
// Channels
// ========================================

// ChannelMode represents the subscription mode for a channel.
type ChannelMode string

const (
	ChannelModeBroadcast ChannelMode = "broadcast" // All subscribers receive all messages
)

// ChannelMessage represents a message in a channel.
type ChannelMessage struct {
	ID          int64     `json:"id"`
	Channel     string    `json:"channel"`               // Channel name
	MessageID   string    `json:"message_id"`            // UUID for message
	DataType    string    `json:"data_type"`             // "json" or "binary"
	Data        []byte    `json:"data,omitempty"`        // JSON data
	DataBinary  []byte    `json:"data_binary,omitempty"` // Binary data
	Metadata    []byte    `json:"metadata,omitempty"`    // JSON-encoded metadata
	PublishedAt time.Time `json:"published_at"`          // When the message was published
}

// ChannelSubscription represents a workflow's subscription to a channel.
// Waiting state is determined by "activity_id IS NOT NULL" instead of explicit waiting flag.
type ChannelSubscription struct {
	ID              int64       `json:"id"`
	InstanceID      string      `json:"instance_id"`
	Channel         string      `json:"channel"` // Channel name
	Mode            ChannelMode `json:"mode"`
	CursorMessageID int64       `json:"cursor_message_id"` // Last delivered message ID
	TimeoutAt       *time.Time  `json:"timeout_at,omitempty"`
	ActivityID      string      `json:"activity_id,omitempty"` // Activity ID for replay matching; non-null = waiting
	SubscribedAt    time.Time   `json:"subscribed_at"`         // When the subscription was created
}

// ChannelDeliveryResult contains information about a successful message delivery.
type ChannelDeliveryResult struct {
	InstanceID   string `json:"instance_id"`
	WorkflowName string `json:"workflow_name"`
	ActivityID   string `json:"activity_id"`
}

// ========================================
// System Locks
// ========================================

// SystemLock represents a system-level lock for background tasks.
type SystemLock struct {
	LockName  string    `json:"lock_name"`
	LockedBy  string    `json:"locked_by"`
	LockedAt  time.Time `json:"locked_at"`
	ExpiresAt time.Time `json:"expires_at"`
}
