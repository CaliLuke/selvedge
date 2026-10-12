package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

// Activity represents a single unit of work within a workflow.
// Activities are the only way to perform I/O or side effects in workflows.
// I is the input type, O is the output type.
type Activity[I, O any] struct {
	name string
	fn   func(ctx context.Context, input I) (O, error)

	// Configuration
	retryPolicy *retry.Policy
	timeout     time.Duration
}

// ActivityOption configures an activity.
type ActivityOption[I, O any] func(*Activity[I, O])

// DefineActivity creates a new activity.
func DefineActivity[I, O any](
	name string,
	fn func(ctx context.Context, input I) (O, error),
	opts ...ActivityOption[I, O],
) *Activity[I, O] {
	a := &Activity[I, O]{
		name: name,
		fn:   fn,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Name returns the activity name.
func (a *Activity[I, O]) Name() string {
	return a.name
}

// WithRetryPolicy sets the retry policy for the activity.
func WithRetryPolicy[I, O any](policy *retry.Policy) ActivityOption[I, O] {
	return func(a *Activity[I, O]) {
		a.retryPolicy = policy
	}
}

// WithTimeout sets the timeout for each activity execution attempt.
func WithTimeout[I, O any](d time.Duration) ActivityOption[I, O] {
	return func(a *Activity[I, O]) {
		a.timeout = d
	}
}

// Execute issues the activity and waits for its durable outcome.
func (a *Activity[I, O]) Execute(ctx *WorkflowContext, input I, opts ...ExecuteOption) (O, error) {
	future, err := a.ExecuteAsync(ctx, input, opts...)
	if err != nil {
		var zero O
		return zero, err
	}
	return future.Get(ctx)
}

// ExecuteAsync reserves an activity in issue order without waiting for its result.
// Get reads the durable outcome. Un-awaited tasks are cancelled when the workflow closes.
func (a *Activity[I, O]) ExecuteAsync(ctx *WorkflowContext, input I, opts ...ExecuteOption) (*Future[O], error) {
	if err := ctx.execCtx.Check(); err != nil {
		return nil, err
	}
	options := &executeOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.err != nil {
		ctx.execCtx.Fail(options.err)
		return nil, options.err
	}
	payload, err := json.Marshal(input)
	if err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	cmd, err := ctx.execCtx.Issue("activity:"+a.name, options.activityID)
	if err != nil {
		return nil, err
	}
	defer cmd.End()
	ctx.scope.track(cmd.Key, "activity:"+a.name, "activity")
	future := &Future[O]{exec: ctx.execCtx, scope: ctx.scope, key: cmd.Key, command: "activity:" + a.name}
	if ctx.scope.locallyCancelled() && cmd.Recorded == nil {
		row, err := ctx.execCtx.Storage().RecordHistory(ctx.Context(), ctx.execCtx.Fence(), scopeCancelledEvent(ctx, cmd.Key, future.command, cmd.Seq, storage.HistoryActivityCancelled))
		if err != nil {
			ctx.execCtx.Fail(err)
			return nil, err
		}
		ctx.execCtx.ApplyDelivery(row.DeliveredSeq, storage.DeliveryBatch{HistoryKeys: []string{cmd.Key}})
		future.readOutcome(row)
		return future, nil
	}
	if cmd.Recorded != nil && cmd.Recorded.EventType == storage.HistoryActivityCancelled && ctx.scope.locallyCancelled() {
		future.readOutcome(cmd.Recorded)
		return future, nil
	}
	if err := RegisterActivity(ctx.app, a); err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	if h := ctx.Hooks(); h != nil {
		h.OnActivityStart(ctx.Context(), hooks.ActivityStartInfo{
			InstanceID: ctx.InstanceID(), WorkflowName: ctx.WorkflowName(), ActivityID: cmd.Key,
			ActivityName: a.name, InputData: input, IsReplay: ctx.IsReplaying(),
		})
	}
	if event := cmd.Recorded; event != nil && event.EventData != nil {
		future.readOutcome(event)
		if h := ctx.Hooks(); h != nil {
			h.OnActivityCacheHit(ctx.Context(), hooks.ActivityCacheHitInfo{
				InstanceID: ctx.InstanceID(), WorkflowName: ctx.WorkflowName(), ActivityID: cmd.Key,
				ActivityName: a.name, CachedResult: future.value,
			})
		}
	} else if event == nil {
		if err := cmd.ReserveActivity(ctx.Context(), &storage.ActivityTask{InstanceID: ctx.InstanceID(), ActivityName: a.name, InputData: payload}); err != nil {
			return nil, err
		}
		ctx.app.wakeActivityWorkers()
	}
	return future, nil
}

// executeWithRetry executes the activity with retry logic.
func (a *Activity[I, O]) executeWithRetry(ctx context.Context, info ActivityInfo, h hooks.WorkflowHooks, input I) (O, error) {
	policy := a.retryPolicy
	if policy == nil {
		policy = retry.DefaultPolicy()
	}

	var lastErr error
	attempts := 0
	startTime := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			var zero O
			return zero, err
		}
		attempts++

		// Create execution context with timeout
		execCtx := ctx
		var cancel context.CancelFunc
		if a.timeout > 0 {
			execCtx, cancel = context.WithTimeout(execCtx, a.timeout)
		}

		// Execute the activity
		result, err := a.invoke(execCtx, input)

		if cancel != nil {
			cancel()
		}

		if err == nil {
			// Call OnActivityComplete hook
			if h != nil {
				h.OnActivityComplete(ctx, hooks.ActivityCompleteInfo{
					InstanceID:   info.InstanceID,
					WorkflowName: info.WorkflowName,
					ActivityID:   info.ActivityID,
					ActivityName: a.name,
					OutputData:   result,
					Duration:     time.Since(startTime),
					IsReplay:     false,
				})
			}
			return result, nil
		}

		lastErr = err

		// Check if error is terminal (should not retry)
		if IsTerminalError(err) || nonRetryableApplication(err) || isActivityPanic(err) {
			// Call OnActivityFailed hook
			if h != nil {
				h.OnActivityFailed(ctx, hooks.ActivityFailedInfo{
					InstanceID:   info.InstanceID,
					WorkflowName: info.WorkflowName,
					ActivityID:   info.ActivityID,
					ActivityName: a.name,
					Error:        err,
					Duration:     time.Since(startTime),
					Attempt:      attempts,
				})
			}
			var zero O
			return zero, err
		}

		// Check if we should retry
		if !policy.ShouldRetry(attempts, err) {
			retryErr := &RetryExhaustedError{
				ActivityName: a.name,
				Attempts:     attempts,
				LastErr:      lastErr,
			}
			// Call OnActivityFailed hook
			if h != nil {
				h.OnActivityFailed(ctx, hooks.ActivityFailedInfo{
					InstanceID:   info.InstanceID,
					WorkflowName: info.WorkflowName,
					ActivityID:   info.ActivityID,
					ActivityName: a.name,
					Error:        retryErr,
					Duration:     time.Since(startTime),
					Attempt:      attempts,
				})
			}
			var zero O
			return zero, retryErr
		}

		// Wait before retrying
		delay := policy.GetDelay(attempts)

		// Call OnActivityRetry hook
		if h != nil {
			h.OnActivityRetry(ctx, hooks.ActivityRetryInfo{
				InstanceID:   info.InstanceID,
				WorkflowName: info.WorkflowName,
				ActivityID:   info.ActivityID,
				ActivityName: a.name,
				Attempt:      attempts,
				MaxAttempts:  policy.MaxAttempts,
				NextDelay:    delay,
				Error:        lastErr,
			})
		}

		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			var zero O
			return zero, ctx.Err()
		}
	}
}

// ExecuteOption configures activity execution.
type ExecuteOption func(*executeOptions)

type executeOptions struct {
	err        error
	activityID string
}

// WithActivityID specifies a manual activity ID.
// Commands must be issued serially from workflow code.
// IDs starting with $ or # are reserved.
// Execute rejects these IDs with ErrHistoryKeyConflict before running the activity.
func WithActivityID(id string) ExecuteOption {
	return func(o *executeOptions) {
		o.activityID = id
		if err := reservedHistoryIDError("activity", id); err != nil {
			o.err = err
		}
	}
}

// Runtime-generated keys and the lifecycle namespace cannot be explicit history IDs.
func reservedHistoryIDError(kind, id string) error {
	for _, prefix := range []string{"$", "#"} {
		if strings.HasPrefix(id, prefix) {
			return fmt.Errorf("%s ID %q uses reserved prefix %q: %w", kind, id, prefix, ErrHistoryKeyConflict)
		}
	}
	return nil
}

func (a *Activity[I, O]) invoke(ctx context.Context, input I) (out O, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &WorkflowPanicError{Value: fmt.Sprint(value), Stack: string(debug.Stack())}
		}
	}()
	return a.fn(ctx, input)
}
func isActivityPanic(err error) bool { var p *WorkflowPanicError; return errors.As(err, &p) }
