package selvedge

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// workflowContextKey is the key used to store WorkflowContext in context.Context.
type workflowContextKey struct{}

// ContextWithWorkflowContext returns a new context with the WorkflowContext embedded.
// This is used internally to pass WorkflowContext to activities.
func ContextWithWorkflowContext(ctx context.Context, wfCtx *WorkflowContext) context.Context {
	return context.WithValue(ctx, workflowContextKey{}, wfCtx)
}

// GetWorkflowContext retrieves workflow metadata from an activity context.
// Returns nil if the context does not contain a WorkflowContext.
func GetWorkflowContext(ctx context.Context) *WorkflowContext {
	if v := ctx.Value(workflowContextKey{}); v != nil {
		if wfCtx, ok := v.(*WorkflowContext); ok {
			return wfCtx
		}
	}
	return nil
}

// WorkflowContext provides context for workflow execution.
// It manages activity ID generation, replay state, and recorded history.
type WorkflowContext struct {
	ctx        context.Context
	cancel     context.CancelFunc
	instanceID string
	workerID   string

	// Workflow metadata
	workflowName string

	// Replay state
	isReplaying bool

	// Reference to the replay execution context (when using replay engine)
	execCtx *replay.ExecutionContext

	// Reference to the App for accessing app-level configuration
	app *App
}

// NewWorkflowContext creates a new WorkflowContext.
func NewWorkflowContext(
	ctx context.Context,
	instanceID, workerID, workflowName string,
	isReplaying bool,
) *WorkflowContext {
	ctx, cancel := context.WithCancel(ctx)
	return &WorkflowContext{
		ctx:          ctx,
		cancel:       cancel,
		instanceID:   instanceID,
		workerID:     workerID,
		workflowName: workflowName,
		isReplaying:  isReplaying,
		execCtx:      replay.NewExecutionContext(ctx, instanceID),
	}
}

// NewWorkflowContextFromExecution creates a WorkflowContext from a replay ExecutionContext.
func NewWorkflowContextFromExecution(execCtx *replay.ExecutionContext) *WorkflowContext {
	ctx, cancel := context.WithCancel(execCtx.Context())
	return &WorkflowContext{
		ctx:         ctx,
		cancel:      cancel,
		instanceID:  execCtx.InstanceID(),
		isReplaying: execCtx.IsReplaying(),
		execCtx:     execCtx,
	}
}

// Context returns the underlying context.Context.
func (c *WorkflowContext) Context() context.Context {
	return c.ctx
}

// WithContext returns a copy of the WorkflowContext with a new underlying context.
func (c *WorkflowContext) WithContext(ctx context.Context) *WorkflowContext {
	return &WorkflowContext{
		ctx:          ctx,
		cancel:       c.cancel,
		instanceID:   c.instanceID,
		workerID:     c.workerID,
		workflowName: c.workflowName,
		isReplaying:  c.isReplaying,
		execCtx:      c.execCtx,
		app:          c.app,
	}
}

// InstanceID returns the workflow instance ID.
func (c *WorkflowContext) InstanceID() string {
	return c.instanceID
}

// WorkflowName returns the workflow name.
func (c *WorkflowContext) WorkflowName() string {
	return c.workflowName
}

// WorkerID returns the worker ID that is executing this workflow.
func (c *WorkflowContext) WorkerID() string {
	return c.workerID
}

// IsReplaying returns true if the workflow is being replayed.
func (c *WorkflowContext) IsReplaying() bool {
	return c.isReplaying
}

// GenerateActivityID generates a unique activity ID for the given function name.
// Format: {function_name}:{counter}
// This is used for deterministic replay - the same sequence of calls
// will always generate the same IDs.
func (c *WorkflowContext) GenerateActivityID(functionName string) string {
	return c.execCtx.GenerateActivityID(functionName)
}

// lookupRecorded retrieves a raw row from the replay execution.
func (c *WorkflowContext) lookupRecorded(key string) (*storage.HistoryEvent, bool) {
	if c.execCtx != nil {
		return c.execCtx.Lookup(key)
	}
	return nil, false
}

// RecordActivityResult records an activity result to storage.
func (c *WorkflowContext) RecordActivityResult(activityID string, result any, activityErr error) error {
	_, err := c.recordActivityResult(activityID, result, activityErr)
	return err
}

func (c *WorkflowContext) recordActivityResult(activityID string, result any, activityErr error) (*storage.HistoryEvent, error) {
	event := &storage.HistoryEvent{InstanceID: c.instanceID, ActivityID: activityID, DataType: "json", EventType: storage.HistoryActivityCompleted}
	if activityErr != nil {
		event.EventType = storage.HistoryActivityFailed
		event.EventData = encodeError(activityErr)
	} else {
		data, err := json.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize result: %w", err)
		}
		event.EventData = data
	}
	if c.execCtx != nil {
		return c.execCtx.RecordActivityResultWithContext(c.ctx, activityID, event.EventType, event.EventData)
	}
	// Standalone contexts use the same JSON representation as engine runs.
	return event, nil
}

// RecordActivityID records that an activity ID has been used.
// This is used for tracking the current activity during execution.
func (c *WorkflowContext) RecordActivityID(activityID string) {
	// Record the current activity in execution context
	if c.execCtx != nil {
		c.execCtx.RecordActivityID(activityID)
	}
}

// Cancel cancels the workflow execution.
func (c *WorkflowContext) Cancel() {
	c.cancel()
}

// Err returns any error from the context.
func (c *WorkflowContext) Err() error {
	return c.ctx.Err()
}

// Done returns a channel that's closed when the context is cancelled.
func (c *WorkflowContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Storage returns the storage interface for database operations.
// Returns nil if not using the replay engine.
func (c *WorkflowContext) Storage() storage.Storage {
	if c.execCtx != nil {
		return c.execCtx.Storage()
	}
	return nil
}

// Hooks returns the workflow hooks for observability.
// Returns nil if hooks are not available (e.g., not using replay engine).
func (c *WorkflowContext) Hooks() hooks.WorkflowHooks {
	if c.execCtx != nil {
		return c.execCtx.Hooks()
	}
	return nil
}

// SetApp sets the App reference for this workflow context.
// This is typically called internally when creating the context.
func (c *WorkflowContext) SetApp(app *App) {
	c.app = app
}

// App returns the App reference for this workflow context.
// Returns nil if not set.
func (c *WorkflowContext) App() *App {
	return c.app
}
