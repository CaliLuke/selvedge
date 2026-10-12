package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/CaliLuke/selvedge/internal/replay"

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

// Execute runs the activity within a workflow context.
// If activityID is empty, it will be auto-generated.
func (a *Activity[I, O]) Execute(
	ctx *WorkflowContext,
	input I,
	opts ...ExecuteOption,
) (O, error) {
	if ctx.execCtx != nil {
		if err := ctx.execCtx.Check(); err != nil {
			var zero O
			return zero, err
		}
	}
	options := &executeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.err != nil {
		ctx.execCtx.Fail(options.err)
		var zero O
		return zero, options.err
	}

	payload, err := json.Marshal(input)
	if err != nil {
		ctx.execCtx.Fail(err)
		var zero O
		return zero, err
	}

	cmd, claimErr := ctx.execCtx.Issue("activity:"+a.name, options.activityID)
	if claimErr != nil {
		var zero O
		return zero, claimErr
	}
	defer cmd.End()
	if err := RegisterActivity(ctx.app, a); err != nil {
		ctx.execCtx.Fail(err)
		var zero O
		return zero, err
	}

	activityID := cmd.Key
	event := cmd.Recorded

	// Call OnActivityStart hook
	if h := ctx.Hooks(); h != nil {
		h.OnActivityStart(ctx.Context(), hooks.ActivityStartInfo{
			InstanceID:   ctx.InstanceID(),
			WorkflowName: ctx.WorkflowName(),
			ActivityID:   activityID,
			ActivityName: a.name,
			InputData:    input,
			IsReplay:     ctx.IsReplaying(),
		})
	}

	// Replay uses recorded bytes decoded directly into the activity's output type.
	if event != nil && event.EventData != nil {
		if event.EventType == storage.HistoryActivityFailed {
			var zero O
			return zero, replayRecordedError(ctx, event)
		}
		result, err := decodeActivityRecorded[O](ctx, event)
		if err != nil {
			return result, err
		}
		if h := ctx.Hooks(); h != nil {
			h.OnActivityCacheHit(ctx.Context(), hooks.ActivityCacheHitInfo{
				InstanceID:   ctx.InstanceID(),
				WorkflowName: ctx.WorkflowName(),
				ActivityID:   activityID,
				ActivityName: a.name,
				CachedResult: result,
			})
		}
		return result, nil
	}

	if event == nil {
		if err := cmd.ReserveActivity(ctx.Context(), &storage.ActivityTask{InstanceID: ctx.InstanceID(), ActivityName: a.name, InputData: payload}); err != nil {
			var zero O
			return zero, err
		}
		ctx.app.wakeActivityWorkers()
	}
	return a.awaitActivity(ctx, cmd)
}

// awaitActivity always reads the winning bytes from history, including after an inline attempt.
func (a *Activity[I, O]) awaitActivity(ctx *WorkflowContext, cmd *replay.Command) (O, error) {
	attempted := false
	for {
		var zero O
		if err := ctx.execCtx.Check(); err != nil {
			return zero, err
		}
		row, err := ctx.Storage().GetHistoryEvent(ctx.Context(), ctx.InstanceID(), cmd.Key)
		if err != nil {
			ctx.execCtx.Fail(err)
			return zero, err
		}
		if row != nil && row.EventData != nil {
			return decodeActivityRecorded[O](ctx, row)
		}
		if !attempted {
			task, err := ctx.execCtx.Storage().ClaimActivityTask(ctx.Context(), ctx.InstanceID(), cmd.Key, ctx.app.config.workerID+"/"+uuid.NewString(), ctx.app.config.lease.TTL)
			if err != nil {
				ctx.execCtx.Fail(err)
				return zero, err
			}
			attempted = true
			if task != nil {
				err = ctx.app.runActivityTask(ctx.Context(), ctx.execCtx.Storage(), task, ctx.execCtx.CheckLease)
				if err != nil && !errors.Is(err, storage.ErrFenced) && !errors.Is(err, errClaimLost) {
					ctx.execCtx.Fail(err)
					return zero, err
				}
				continue
			}
		}
		if err := ctx.execCtx.Yield(replay.Wait{Seq: ctx.execCtx.Position(), Key: cmd.Key, Kind: replay.WaitActivity}); err != nil {
			return zero, err
		}
	}
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
