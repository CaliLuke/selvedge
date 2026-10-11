package selvedge

import (
	"context"
	"fmt"
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

// Execute runs the activity within a workflow context.
// If activityID is empty, it will be auto-generated.
func (a *Activity[I, O]) Execute(
	ctx *WorkflowContext,
	input I,
	opts ...ExecuteOption,
) (O, error) {
	if ctx.execCtx != nil {
		if err := ctx.execCtx.Fatal(); err != nil {
			var zero O
			return zero, err
		}
	}
	options := &executeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Generate or use provided activity ID
	activityID := options.activityID
	if activityID == "" {
		activityID = ctx.GenerateActivityID(a.name)
	}

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
	if event, ok := ctx.lookupRecorded(activityID); ok {
		if event.EventType == storage.HistoryActivityFailed || event.EventType == storage.HistoryMessageTimeout {
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

	// Execute the activity
	result, err := a.executeWithRetry(ctx, input, activityID)

	// Return the serialized representation on first execution, too.
	stored, recordErr := ctx.recordActivityResult(activityID, result, err)
	if recordErr != nil {
		var zero O
		return zero, fmt.Errorf("failed to record activity result: %w", recordErr)
	}

	// Record activity ID for tracking
	ctx.RecordActivityID(activityID)

	return decodeActivityRecorded[O](ctx, stored)
}

// executeWithRetry executes the activity with retry logic.
func (a *Activity[I, O]) executeWithRetry(ctx *WorkflowContext, input I, activityID string) (O, error) {
	policy := a.retryPolicy
	if policy == nil {
		policy = retry.DefaultPolicy()
	}

	var lastErr error
	attempts := 0
	startTime := time.Now()

	for {
		attempts++

		// Create execution context with timeout
		execCtx := ctx.Context()
		var cancel context.CancelFunc
		if a.timeout > 0 {
			execCtx, cancel = context.WithTimeout(execCtx, a.timeout)
		}

		// Embed WorkflowContext in the execution context
		// This allows activities to access workflow features via GetWorkflowContext()
		execCtx = ContextWithWorkflowContext(execCtx, ctx)

		// Execute the activity
		result, err := a.fn(execCtx, input)

		if cancel != nil {
			cancel()
		}

		if err == nil {
			// Call OnActivityComplete hook
			if h := ctx.Hooks(); h != nil {
				h.OnActivityComplete(ctx.Context(), hooks.ActivityCompleteInfo{
					InstanceID:   ctx.InstanceID(),
					WorkflowName: ctx.WorkflowName(),
					ActivityID:   activityID,
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
		if IsTerminalError(err) || nonRetryableApplication(err) {
			// Call OnActivityFailed hook
			if h := ctx.Hooks(); h != nil {
				h.OnActivityFailed(ctx.Context(), hooks.ActivityFailedInfo{
					InstanceID:   ctx.InstanceID(),
					WorkflowName: ctx.WorkflowName(),
					ActivityID:   activityID,
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
			if h := ctx.Hooks(); h != nil {
				h.OnActivityFailed(ctx.Context(), hooks.ActivityFailedInfo{
					InstanceID:   ctx.InstanceID(),
					WorkflowName: ctx.WorkflowName(),
					ActivityID:   activityID,
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
		if h := ctx.Hooks(); h != nil {
			h.OnActivityRetry(ctx.Context(), hooks.ActivityRetryInfo{
				InstanceID:   ctx.InstanceID(),
				WorkflowName: ctx.WorkflowName(),
				ActivityID:   activityID,
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
	activityID string
}

// WithActivityID specifies a manual activity ID.
// Required for concurrent activity execution to maintain determinism.
func WithActivityID(id string) ExecuteOption {
	return func(o *executeOptions) {
		o.activityID = id
	}
}
