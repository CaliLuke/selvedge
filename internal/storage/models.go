// Package storage provides the storage layer for Selvedge.
package storage

import (
	"time"
)

// WorkflowStatus represents the current state of a workflow instance.
type WorkflowStatus string

const (
	StatusRunning   WorkflowStatus = "running"
	StatusCompleted WorkflowStatus = "completed"
	StatusFailed    WorkflowStatus = "failed"
	StatusCancelled WorkflowStatus = "cancelled"
)

// WorkflowInstance represents a single execution of a workflow.
type WorkflowInstance struct {
	CancelReason      string         `json:"cancel_reason,omitempty"`
	InstanceID        string         `json:"instance_id"`
	WorkflowName      string         `json:"workflow_name"`
	Framework         string         `json:"framework"` // "go" or "python"
	Status            WorkflowStatus `json:"status"`
	InputData         []byte         `json:"input_data"`          // JSON-encoded input
	OutputData        []byte         `json:"output_data"`         // JSON-encoded output (if completed)
	CurrentActivityID string         `json:"current_activity_id"` // Last executed activity ID
	SourceHash        string         `json:"source_hash"`         // Source code hash
	OwnerService      string         `json:"owner_service"`       // Service that owns this workflow
	LeaseOwner        string         `json:"lease_owner"`
	LeaseEpoch        int64          `json:"lease_epoch"`
	LeaseExpiresMS    *int64         `json:"lease_expires_ms"`
	Runnable          bool           `json:"runnable"`

	StartedAt time.Time `json:"started_at"` // When the workflow was created
	UpdatedAt time.Time `json:"updated_at"`
}

// HistoryEventType represents the type of history event.
type HistoryEventType string

const (
	HistoryReserved HistoryEventType = "Reserved"

	// Activity events
	HistoryActivityCancelled HistoryEventType = "ActivityCancelled"
	HistoryActivityCompleted HistoryEventType = "ActivityCompleted"
	HistoryActivityFailed    HistoryEventType = "ActivityFailed"

	// Timer expired (via Sleep/SleepUntil)
	HistoryTimerExpired   HistoryEventType = "TimerExpired"
	HistoryTimerCancelled HistoryEventType = "TimerCancelled"

	// Signal observations contain the payload or a cancellation envelope.
	HistorySignalReceived  HistoryEventType = "SignalReceived"
	HistorySignalCancelled HistoryEventType = "SignalCancelled"

	// Workflow lifecycle events
	HistoryWorkflowFailed    HistoryEventType = "WorkflowFailed"
	HistoryWorkflowCancelled HistoryEventType = "WorkflowCancelled"
)

// HistoryEvent represents a single event in a workflow's execution history.
type HistoryEvent struct {
	Seq             int64            `json:"seq"`                        // Zero represents SQL NULL for lifecycle rows.
	ReservationData []byte           `json:"reservation_data,omitempty"` // Immutable command metadata, separate from its observed value.
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
// System Locks
// ========================================

// SystemLock represents a system-level lock for background tasks.
type SystemLock struct {
	LockName  string    `json:"lock_name"`
	LockedBy  string    `json:"locked_by"`
	LockedAt  time.Time `json:"locked_at"`
	ExpiresAt time.Time `json:"expires_at"`
}
