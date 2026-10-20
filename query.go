package selvedge

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CaliLuke/selvedge/internal/replay"
)

// ErrQueryCommand matches an attempted workflow command in a query handler.
var ErrQueryCommand = replay.ErrQueryCommand

// QueryNotFoundError means no handler of this name exists at the replay stop.
type QueryNotFoundError struct{ Name string }

func (e *QueryNotFoundError) Error() string { return fmt.Sprintf("query %q not found", e.Name) }

// QueryFailedError wraps a handler error, panic, or JSON conversion failure.
type QueryFailedError struct {
	Name string
	Err  error
}

func (e *QueryFailedError) Error() string { return fmt.Sprintf("query %q failed: %v", e.Name, e.Err) }
func (e *QueryFailedError) Unwrap() error { return e.Err }

// WorkflowNotRegisteredError means the querying App lacks the workflow runner.
type WorkflowNotRegisteredError struct{ WorkflowName string }

func (e *WorkflowNotRegisteredError) Error() string {
	return fmt.Sprintf("workflow %q not registered", e.WorkflowName)
}

// SetQueryHandler replaces an execution-local handler without issuing a command.
// A nil handler removes the name. Call only on the workflow coroutine.
func SetQueryHandler[R any](ctx *WorkflowContext, name string, handler func() (R, error)) error {
	var erased func() (any, error)
	if handler != nil {
		erased = func() (any, error) { return handler() }
	}
	return ctx.execCtx.SetQueryHandler(name, erased)
}

// Query answers from a consistent snapshot replay in the calling App.
// Each call costs one full replay, takes no lease, and writes no workflow state.
// The result always passes through JSON into R, including when R is any.
// In-memory SQLite snapshots (":memory:" with shared cache, without WAL) block
// writers until the snapshot read transaction ends.
func Query[R any](ctx context.Context, app *App, instanceID, name string) (R, error) {
	var zero R
	store := app.Storage()
	if app.closed.Load() || store == nil {
		return zero, ErrAppNotStarted
	}
	snapshot, err := store.ReadSnapshot(ctx, instanceID)
	if err != nil {
		return zero, err
	}
	if snapshot == nil {
		return zero, &WorkflowNotFoundError{InstanceID: instanceID}
	}
	app.workflowRunnersMu.RLock()
	runner, ok := app.workflowRunners[snapshot.Instance.WorkflowName]
	app.workflowRunnersMu.RUnlock()
	if !ok {
		return zero, &WorkflowNotRegisteredError{WorkflowName: snapshot.Instance.WorkflowName}
	}
	execution, err := new(replay.Engine).Query(ctx, instanceID, runner, snapshot)
	if err != nil {
		return zero, err
	}
	defer execution.Close()
	value, found, err := execution.Answer(name)
	if err != nil {
		return zero, &QueryFailedError{Name: name, Err: err}
	}
	if !found {
		return zero, &QueryNotFoundError{Name: name}
	}
	data, err := json.Marshal(value)
	if err != nil {
		return zero, &QueryFailedError{Name: name, Err: err}
	}
	var result R
	if err = json.Unmarshal(data, &result); err != nil {
		return zero, &QueryFailedError{Name: name, Err: err}
	}
	return result, nil
}
