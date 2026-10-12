package selvedge

import (
	"context"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// WorkflowContext provides context for workflow execution.
// It provides command positions, replay state, and recorded history.
type WorkflowContext struct {
	ctx        context.Context
	instanceID string
	workerID   string

	// Workflow metadata
	workflowName string

	// Reference to the replay execution context (when using replay engine)
	execCtx *replay.ExecutionContext

	// Reference to the App for accessing app-level configuration
	app *App

	scope     *cancelScope
	timeFloor *workflowTimeFloor
}

// NewWorkflowContext creates a standalone context without recorded history.
// Commands require an engine-managed workflow coroutine.
func NewWorkflowContext(
	ctx context.Context,
	instanceID, workerID, workflowName string,
) *WorkflowContext {
	return &WorkflowContext{
		ctx:          ctx,
		scope:        newCancelScope(nil, true),
		instanceID:   instanceID,
		workerID:     workerID,
		workflowName: workflowName,
		execCtx:      replay.NewExecutionContext(ctx, instanceID),
		timeFloor:    new(workflowTimeFloor),
	}
}

// NewWorkflowContextFromExecution creates a WorkflowContext from a replay ExecutionContext.
func NewWorkflowContextFromExecution(execCtx *replay.ExecutionContext) *WorkflowContext {
	ctx := execCtx.Context()
	return &WorkflowContext{
		ctx:        ctx,
		scope:      newCancelScope(nil, true),
		instanceID: execCtx.InstanceID(),
		execCtx:    execCtx,
		timeFloor:  new(workflowTimeFloor),
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
		scope:        c.scope,
		instanceID:   c.instanceID,
		workerID:     c.workerID,
		workflowName: c.workflowName,
		execCtx:      c.execCtx,
		app:          c.app,
		timeFloor:    c.timeFloor,
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
	return c.execCtx.Replaying()
}

// Err reports lease or shutdown cancellation, never scope cancellation.
func (c *WorkflowContext) Err() error {
	return c.ctx.Err()
}

// Done signals lease or shutdown cancellation, never scope cancellation.
func (c *WorkflowContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

// WorkflowStorage exposes recorded history for diagnostics. Workflow commands
// must use the workflow APIs rather than direct database writes.
type WorkflowStorage interface {
	GetHistoryEvent(context.Context, string, string) (*storage.HistoryEvent, error)
}

type workflowStorageReader struct{ store storage.HistoryManager }

func (s workflowStorageReader) GetHistoryEvent(ctx context.Context, instanceID, key string) (*storage.HistoryEvent, error) {
	return s.store.GetHistoryEvent(ctx, instanceID, key)
}

// Storage returns read-only history access, or nil outside the replay engine.
func (c *WorkflowContext) Storage() WorkflowStorage {
	if c.execCtx != nil && c.execCtx.Storage() != nil {
		return workflowStorageReader{store: c.execCtx.Storage()}
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
