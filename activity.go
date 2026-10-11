package selvedge

import (
	"context"
	"fmt"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/retry"
)

// Activity represents a single unit of work within a workflow.
// Activities are the only way to perform I/O or side effects in workflows.
// I is the input type, O is the output type.
type Activity[I, O any] struct {
	name string
	fn   func(ctx context.Context, input I) (O, error)

	// Configuration
	retryPolicy   *retry.Policy
	timeout       time.Duration
	transactional bool // Wrap execution in a transaction (default: true)
}

// ActivityOption configures an activity.
type ActivityOption[I, O any] func(*Activity[I, O])

// DefineActivity creates a new activity.
// By default, activities are wrapped in a transaction for atomicity with
// history recording.
func DefineActivity[I, O any](
	name string,
	fn func(ctx context.Context, input I) (O, error),
	opts ...ActivityOption[I, O],
) *Activity[I, O] {
	a := &Activity[I, O]{
		name:          name,
		fn:            fn,
		transactional: true, // Default: wrap in transaction
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

// WithTransactional sets whether the activity execution should be wrapped
// in a database transaction. Default is true.
//
// When transactional=true (default):
//   - Activity execution and history recording are atomic
//   - Rollback occurs on failure
//   - Use ctx.Storage() for database operations within the same transaction
//
// When transactional=false:
//   - Useful for activities that call external APIs or don't need atomicity
//   - History is still recorded, but not atomically
func WithTransactional[I, O any](transactional bool) ActivityOption[I, O] {
	return func(a *Activity[I, O]) {
		a.transactional = transactional
	}
}

// Execute runs the activity within a workflow context.
// If activityID is empty, it will be auto-generated.
//
// When transactional=true (default), the activity execution and history recording
// are wrapped in a database transaction for atomicity.
func (a *Activity[I, O]) Execute(
	ctx *WorkflowContext,
	input I,
	opts ...ExecuteOption,
) (O, error) {
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

	// Check if result is cached (replay)
	if result, ok := ctx.GetCachedResult(activityID); ok {
		// Call OnActivityCacheHit hook
		if h := ctx.Hooks(); h != nil {
			h.OnActivityCacheHit(ctx.Context(), hooks.ActivityCacheHitInfo{
				InstanceID:   ctx.InstanceID(),
				WorkflowName: ctx.WorkflowName(),
				ActivityID:   activityID,
				ActivityName: a.name,
				CachedResult: result,
			})
		}
		// Type assertion with proper handling
		if typedResult, ok := result.(O); ok {
			return typedResult, nil
		}
		// Handle error results stored in cache
		if err, ok := result.(error); ok {
			var zero O
			return zero, err
		}
		var zero O
		return zero, fmt.Errorf("cached result type mismatch for activity %s", activityID)
	}

	// Execute with or without transaction based on configuration
	if a.transactional && ctx.Storage() != nil {
		return a.executeInTransaction(ctx, input, activityID)
	}

	return a.executeWithoutTransaction(ctx, input, activityID)
}

// executeInTransaction runs the activity within a database transaction.
func (a *Activity[I, O]) executeInTransaction(
	ctx *WorkflowContext,
	input I,
	activityID string,
) (O, error) {
	var zero O

	// Begin transaction
	txCtx, err := ctx.Storage().BeginTransaction(ctx.Context())
	if err != nil {
		return zero, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Create a new context with the transaction
	txWorkflowCtx := ctx.WithContext(txCtx)

	// Execute the activity
	result, execErr := a.executeWithRetry(txWorkflowCtx, input, activityID)

	// Record result to storage and cache
	if recordErr := txWorkflowCtx.RecordActivityResult(activityID, result, execErr); recordErr != nil {
		// Rollback on record failure
		_ = ctx.Storage().RollbackTransaction(txCtx)
		return zero, fmt.Errorf("failed to record activity result: %w", recordErr)
	}

	if execErr != nil {
		// Rollback on execution failure
		_ = ctx.Storage().RollbackTransaction(txCtx)
		return zero, execErr
	}

	// Record activity ID for tracking
	txWorkflowCtx.RecordActivityID(activityID)

	// Commit the transaction
	if commitErr := ctx.Storage().CommitTransaction(txCtx); commitErr != nil {
		return zero, fmt.Errorf("failed to commit transaction: %w", commitErr)
	}

	return result, nil
}

// executeWithoutTransaction runs the activity without a transaction wrapper.
func (a *Activity[I, O]) executeWithoutTransaction(
	ctx *WorkflowContext,
	input I,
	activityID string,
) (O, error) {
	// Execute the activity
	result, err := a.executeWithRetry(ctx, input, activityID)

	// Record result to storage and cache (only when not replaying)
	if recordErr := ctx.RecordActivityResult(activityID, result, err); recordErr != nil {
		var zero O
		return zero, fmt.Errorf("failed to record activity result: %w", recordErr)
	}

	if err != nil {
		var zero O
		return zero, err
	}

	// Record activity ID for tracking
	ctx.RecordActivityID(activityID)

	return result, nil
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
		if IsTerminalError(err) {
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
