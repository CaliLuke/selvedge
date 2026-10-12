package selvedge

import (
	"context"
	"errors"
	"time"
)

// ErrWorkflowStartRejected means no instance insert was attempted.
// Duplicate IDs and uncertain storage failures do not match this error.
var ErrWorkflowStartRejected = errors.New("workflow start rejected")

// ErrAppNotStarted means the App has no initialized storage or has shut down.
var ErrAppNotStarted = errors.New("app not started")

// ErrInvalidSignalPayload means a signal payload could not be encoded as JSON.
var ErrInvalidSignalPayload = errors.New("invalid signal payload")

// StartWorkflowByName starts a workflow without requiring local registration.
// The returned instance ID also identifies the run; IDs cannot be reused.
func StartWorkflowByName(ctx context.Context, app *App, workflow string, input any, opts ...StartOption) (string, error) {
	options := &startOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return app.startWorkflow(ctx, workflow, input, options)
}

// WaitWorkflow waits for a terminal result. Workflow failures are in Result.Error.
// Cancellation of ctx stops only the wait. GetWorkflowResult is the nonblocking form.
func WaitWorkflow[O any](ctx context.Context, app *App, instanceID string) (*WorkflowResult[O], error) {
	if app.closed.Load() || app.storage == nil {
		return nil, ErrAppNotStarted
	}
	wake, unsubscribe := app.subscribeClosed(instanceID)
	defer unsubscribe()
	ticker := time.NewTicker(app.config.waitPollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := readWorkflowResult[O](ctx, app, instanceID)
		if err != nil {
			return nil, err
		}
		if result.Status != "running" {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		case <-ticker.C:
		}
	}
}

func (a *App) subscribeClosed(id string) (notification <-chan struct{}, unsubscribe func()) {
	wake := make(chan struct{}, 1)
	a.waitMu.Lock()
	if a.waiters == nil {
		a.waiters = make(map[string]map[chan struct{}]struct{})
	}
	if a.waiters[id] == nil {
		a.waiters[id] = make(map[chan struct{}]struct{})
	}
	a.waiters[id][wake] = struct{}{}
	a.waitMu.Unlock()
	return wake, func() {
		a.waitMu.Lock()
		delete(a.waiters[id], wake)
		if len(a.waiters[id]) == 0 {
			delete(a.waiters, id)
		}
		a.waitMu.Unlock()
	}
}

func (a *App) wakeWaiters(id string) {
	a.waitMu.Lock()
	defer a.waitMu.Unlock()
	for wake := range a.waiters[id] {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}
