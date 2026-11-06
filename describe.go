package selvedge

import (
	"context"
	"encoding/json"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// WorkflowStatus is the durable state of a workflow instance.
type WorkflowStatus = storage.WorkflowStatus

const (
	StatusRunning   = storage.StatusRunning
	StatusCompleted = storage.StatusCompleted
	StatusFailed    = storage.StatusFailed
	StatusCancelled = storage.StatusCancelled
)

// WorkflowDescription exposes durable status and parent metadata.
type WorkflowDescription struct {
	InstanceID, WorkflowName string
	Status                   WorkflowStatus
	// InputData is the stored JSON input, including its original encoding.
	InputData                                 json.RawMessage
	CancelRequested                           bool
	CancelReason, ParentInstanceID, ParentKey string
	StartedAt, UpdatedAt                      time.Time
}

// Describe reads status and cancellation metadata from one consistent snapshot.
func Describe(ctx context.Context, app *App, instanceID string) (*WorkflowDescription, error) {
	store := app.Storage()
	if app.closed.Load() || store == nil {
		return nil, ErrAppNotStarted
	}
	snapshot, err := store.ReadSnapshot(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, &WorkflowNotFoundError{InstanceID: instanceID}
	}
	inst := snapshot.Instance
	return &WorkflowDescription{InstanceID: inst.InstanceID, WorkflowName: inst.WorkflowName, Status: inst.Status, InputData: append(json.RawMessage(nil), inst.InputData...),
		CancelRequested: snapshot.CancelRequested, CancelReason: inst.CancelReason, ParentInstanceID: inst.ParentInstanceID,
		ParentKey: inst.ParentKey, StartedAt: inst.StartedAt, UpdatedAt: inst.UpdatedAt}, nil
}
