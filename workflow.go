package selvedge

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// Workflow defines the interface for a durable workflow.
// I is the input type, O is the output type.
type Workflow[I, O any] interface {
	// Name returns the unique name of the workflow.
	Name() string

	// Execute runs the workflow logic.
	Execute(ctx *WorkflowContext, input I) (O, error)
}

// WorkflowFunc is a convenience type for workflows defined as functions.
type WorkflowFunc[I, O any] struct {
	name string
	fn   func(ctx *WorkflowContext, input I) (O, error)
}

// Name returns the workflow name.
func (w *WorkflowFunc[I, O]) Name() string {
	return w.name
}

// Execute runs the workflow function.
func (w *WorkflowFunc[I, O]) Execute(ctx *WorkflowContext, input I) (O, error) {
	return w.fn(ctx, input)
}

// DefineWorkflow creates a new workflow from a function.
func DefineWorkflow[I, O any](name string, fn func(ctx *WorkflowContext, input I) (O, error)) *WorkflowFunc[I, O] {
	return &WorkflowFunc[I, O]{
		name: name,
		fn:   fn,
	}
}

// workflowRegistry stores registered workflows.
var (
	workflowRegistry   = make(map[string]workflowEntry)
	workflowRegistryMu sync.RWMutex
)

// workflowEntry holds a registered workflow with its metadata.
type workflowEntry struct {
	name       string
	workflow   any // The actual workflow (type-erased for storage)
	inputType  string
	outputType string
}

// RegisterWorkflow registers a workflow with the application.
// The workflow can later be started by name or by type.
func RegisterWorkflow[I, O any](app *App, workflow Workflow[I, O]) {
	name := workflow.Name()

	workflowRegistryMu.Lock()
	defer workflowRegistryMu.Unlock()

	workflowRegistry[name] = workflowEntry{
		name:       name,
		workflow:   workflow,
		inputType:  fmt.Sprintf("%T", *new(I)),
		outputType: fmt.Sprintf("%T", *new(O)),
	}

	// Create a runner that bridges the generic workflow to the replay engine
	runner := func(execCtx *replay.ExecutionContext) (any, error) {
		// Get the instance to retrieve input data
		instance, err := execCtx.Storage().GetInstance(execCtx.Context(), execCtx.InstanceID())
		if err != nil {
			return nil, fmt.Errorf("failed to get instance: %w", err)
		}

		// Deserialize input
		var input I
		if len(instance.InputData) > 0 {
			if err := json.Unmarshal(instance.InputData, &input); err != nil {
				return nil, fmt.Errorf("failed to deserialize input: %w", err)
			}
		}

		// Create WorkflowContext from ExecutionContext
		wfCtx := NewWorkflowContextFromExecution(execCtx)

		// Set App reference for accessing app-level configuration
		wfCtx.SetApp(app)

		// Execute the workflow
		return workflow.Execute(wfCtx, input)
	}

	app.registerWorkflowWithRunner(name, workflow, runner)
}

// GetWorkflow retrieves a registered workflow by name.
// Returns nil if not found.
func GetWorkflow[I, O any](name string) Workflow[I, O] {
	workflowRegistryMu.RLock()
	defer workflowRegistryMu.RUnlock()

	entry, ok := workflowRegistry[name]
	if !ok {
		return nil
	}

	workflow, ok := entry.workflow.(Workflow[I, O])
	if !ok {
		return nil
	}
	return workflow
}

// StartWorkflow starts a new workflow instance.
// Returns the instance ID.
func StartWorkflow[I, O any](
	ctx context.Context,
	app *App,
	workflow Workflow[I, O],
	input I,
	opts ...StartOption,
) (string, error) {
	options := &startOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return app.startWorkflow(ctx, workflow.Name(), input, options)
}

// StartOption configures workflow start options.
type StartOption func(*startOptions)

type startOptions struct {
	instanceID string
	// Additional options can be added here
}

// WithInstanceID specifies a custom instance ID.
// If not provided, a UUID will be generated.
func WithInstanceID(id string) StartOption {
	return func(o *startOptions) {
		o.instanceID = id
	}
}

// WorkflowResult represents the result of a workflow execution.
type WorkflowResult[O any] struct {
	InstanceID string
	Status     string
	Output     O
	Error      error
}

// GetWorkflowResult retrieves the result of a workflow instance.
func GetWorkflowResult[O any](ctx context.Context, app *App, instanceID string) (*WorkflowResult[O], error) {
	instance, err := app.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}

	result := &WorkflowResult[O]{
		InstanceID: instance.InstanceID,
		Status:     string(instance.Status),
	}

	// Get error from history events if workflow failed
	if instance.Status == storage.StatusFailed || instance.Status == storage.StatusCancelled {
		failure, err := getWorkflowErrorFromHistory(ctx, app.Storage(), instanceID)
		if err != nil {
			return nil, err
		}
		result.Error = failure
	}

	// Deserialize output if present and workflow completed
	if instance.Status == storage.StatusCompleted && len(instance.OutputData) > 0 {
		var output O
		if err := json.Unmarshal(instance.OutputData, &output); err != nil {
			return nil, fmt.Errorf("failed to deserialize output: %w", err)
		}
		result.Output = output
	}

	return result, nil
}

// getWorkflowErrorFromHistory reads the reserved failure key directly.
func getWorkflowErrorFromHistory(ctx context.Context, s storage.Storage, instanceID string) (failure, readErr error) {
	event, err := s.GetHistoryEvent(ctx, instanceID, "$workflow_failed")
	if err != nil {
		return nil, fmt.Errorf("failed to load workflow error: %w", err)
	}
	if event == nil {
		return nil, nil
	}
	failure = decodeError(event.EventData)
	if decodeErr, ok := failure.(*ReplayDecodeError); ok {
		decodeErr.InstanceID = instanceID
		decodeErr.Key = event.ActivityID
	}
	return failure, nil
}

// CancelWorkflow durably requests cancellation and wakes a running instance.
// Sleep and Receive observe the request at their next yield. Activities already
// in progress continue. A workflow that catches cancellation can complete.
func CancelWorkflow(ctx context.Context, app *App, instanceID, reason string) error {
	return app.cancelWorkflow(ctx, instanceID, reason)
}
