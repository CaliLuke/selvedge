package selvedge

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/retry"
)

func TestDefineActivity(t *testing.T) {
	tests := []struct {
		name           string
		activityName   string
		opts           []ActivityOption[string, string]
		wantHasRetry   bool
		wantHasTimeout bool
	}{
		{
			name:           "default options",
			activityName:   "test_activity",
			opts:           nil,
			wantHasRetry:   false,
			wantHasTimeout: false,
		},
		{
			name:         "with retry policy",
			activityName: "retry_activity",
			opts: []ActivityOption[string, string]{
				WithRetryPolicy[string, string](retry.DefaultPolicy()),
			},
			wantHasRetry:   true,
			wantHasTimeout: false,
		},
		{
			name:         "with timeout",
			activityName: "timeout_activity",
			opts: []ActivityOption[string, string]{
				WithTimeout[string, string](5 * time.Second),
			},
			wantHasRetry:   false,
			wantHasTimeout: true,
		},
		{
			name:         "all options combined",
			activityName: "full_activity",
			opts: []ActivityOption[string, string]{
				WithRetryPolicy[string, string](retry.Fixed(3, 100*time.Millisecond)),
				WithTimeout[string, string](10 * time.Second),
			},
			wantHasRetry:   true,
			wantHasTimeout: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activity := DefineActivity(tt.activityName,
				func(ctx context.Context, input string) (string, error) {
					return input, nil
				},
				tt.opts...,
			)

			assert.Equal(t, tt.activityName, activity.Name())
			assert.Equal(t, tt.wantHasRetry, activity.retryPolicy != nil)
			assert.Equal(t, tt.wantHasTimeout, activity.timeout > 0)
		})
	}
}

func TestActivity_Name(t *testing.T) {
	tests := []struct {
		name         string
		activityName string
	}{
		{
			name:         "simple name",
			activityName: "send_email",
		},
		{
			name:         "namespaced name",
			activityName: "order.process",
		},
		{
			name:         "empty name",
			activityName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activity := DefineActivity(tt.activityName,
				func(ctx context.Context, input string) (string, error) {
					return input, nil
				},
			)
			assert.Equal(t, tt.activityName, activity.Name())
		})
	}
}

func TestActivity_Execute_CacheHit(t *testing.T) {
	tests := []struct {
		name         string
		cachedResult any
		wantResult   string
		wantErr      bool
	}{
		{
			name:         "cached success result",
			cachedResult: "cached_value",
			wantResult:   "cached_value",
			wantErr:      false,
		},
		{
			name:         "cached error result",
			cachedResult: errors.New("cached error"),
			wantResult:   "",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fnCalled bool
			activity := DefineActivity("test_activity",
				func(ctx context.Context, input string) (string, error) {
					fnCalled = true
					return "fresh_result", nil
				},
			)

			historyCache := map[string]any{
				"test_activity:1": tt.cachedResult,
			}
			workflowContextWithRecordedHistory(t, historyCache, func(wfCtx *WorkflowContext) {
				result, err := activity.Execute(wfCtx, "input")

				assert.False(t, fnCalled, "activity function should not be called on cache hit")

				if tt.wantErr {
					require.Error(t, err)
					assert.Equal(t, tt.wantResult, result)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.wantResult, result)
				}
			})
		})
	}
}

func TestActivity_Execute_WithActivityID(t *testing.T) {
	tests := []struct {
		name         string
		providedID   string
		expectID     string
		useAutoGenID bool
	}{
		{
			name:         "auto-generated ID",
			providedID:   "",
			expectID:     "#1",
			useAutoGenID: true,
		},
		{
			name:         "manual ID",
			providedID:   "custom_id_123",
			expectID:     "custom_id_123",
			useAutoGenID: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activity := DefineActivity("my_activity",
				func(ctx context.Context, input string) (string, error) {
					return "result", nil
				},
			)

			workflowContextWithRecordedHistory(t, nil, func(wfCtx *WorkflowContext) {
				var opts []ExecuteOption
				if tt.providedID != "" {
					opts = append(opts, WithActivityID(tt.providedID))
				}

				result, err := activity.Execute(wfCtx, "input", opts...)
				require.NoError(t, err)
				assert.Equal(t, "result", result)

				cachedResult, err := wfCtx.Storage().GetHistoryEvent(wfCtx.Context(), wfCtx.InstanceID(), tt.expectID)
				require.NoError(t, err)
				require.NotNil(t, cachedResult)
				assert.JSONEq(t, `"result"`, string(cachedResult.EventData))
			})
		})
	}
}

func TestActivity_executeWithRetry_SuccessOnFirstTry(t *testing.T) {
	var attempts int32
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			atomic.AddInt32(&attempts, 1)
			return "success", nil
		},
		WithRetryPolicy[string, string](retry.Fixed(3, time.Millisecond)),
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	result, err := activity.executeWithRetry(wfCtx, "input", "test:1")

	require.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, int32(1), atomic.LoadInt32(&attempts))
}

func TestActivity_executeWithRetry_SuccessAfterRetries(t *testing.T) {
	var attempts int32
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			count := atomic.AddInt32(&attempts, 1)
			if count < 3 {
				return "", errors.New("transient error")
			}
			return "success", nil
		},
		WithRetryPolicy[string, string](retry.Fixed(5, time.Millisecond)),
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	result, err := activity.executeWithRetry(wfCtx, "input", "test:1")

	require.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
}

func TestActivity_executeWithRetry_ExhaustedRetries(t *testing.T) {
	var attempts int32
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			atomic.AddInt32(&attempts, 1)
			return "", errors.New("persistent error")
		},
		WithRetryPolicy[string, string](retry.Fixed(3, time.Millisecond)),
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	result, err := activity.executeWithRetry(wfCtx, "input", "test:1")

	require.Error(t, err)
	assert.Empty(t, result)
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))

	var retryErr *RetryExhaustedError
	require.True(t, errors.As(err, &retryErr))
	assert.Equal(t, "test", retryErr.ActivityName)
	assert.Equal(t, 3, retryErr.Attempts)
}

func TestActivity_executeWithRetry_TerminalErrorNoRetry(t *testing.T) {
	var attempts int32
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			atomic.AddInt32(&attempts, 1)
			return "", NewTerminalError(errors.New("terminal"))
		},
		WithRetryPolicy[string, string](retry.Fixed(5, time.Millisecond)),
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	result, err := activity.executeWithRetry(wfCtx, "input", "test:1")

	require.Error(t, err)
	assert.Empty(t, result)
	assert.Equal(t, int32(1), atomic.LoadInt32(&attempts), "should not retry terminal error")
	assert.True(t, IsTerminalError(err))
}

func TestActivity_executeWithRetry_ContextCancellation(t *testing.T) {
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			return "", errors.New("error")
		},
		WithRetryPolicy[string, string](retry.Fixed(100, 100*time.Millisecond)),
	)

	ctx, cancel := context.WithCancel(context.Background())
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := activity.executeWithRetry(wfCtx, "input", "test:1")

	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestActivity_executeWithRetry_Timeout(t *testing.T) {
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			select {
			case <-time.After(1 * time.Second):
				return "done", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
		WithTimeout[string, string](10*time.Millisecond),
		WithRetryPolicy[string, string](retry.Fixed(1, time.Millisecond)),
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	_, err := activity.executeWithRetry(wfCtx, "input", "test:1")

	require.Error(t, err)
}

func TestWithActivityID_Option(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{
			name: "simple ID",
			id:   "my-activity-1",
		},
		{
			name: "UUID style ID",
			id:   "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "empty ID",
			id:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &executeOptions{}
			WithActivityID(tt.id)(opts)
			assert.Equal(t, tt.id, opts.activityID)
		})
	}
}

func TestActivity_Execute_NonTransactional_NoStorage(t *testing.T) {
	var fnCalled bool
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			fnCalled = true
			return "result", nil
		},
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	result, err := activity.Execute(wfCtx, "input")

	require.NoError(t, err)
	assert.Equal(t, "result", result)
	assert.True(t, fnCalled)

	assert.Nil(t, wfCtx.Storage())
}

func TestActivity_Execute_Error_NoStorage(t *testing.T) {
	expectedErr := NewTerminalError(errors.New("activity error"))
	activity := DefineActivity("test",
		func(ctx context.Context, input string) (string, error) {
			return "", expectedErr
		},
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	result, err := activity.Execute(wfCtx, "input")

	require.Error(t, err)
	assert.Empty(t, result)
	assert.True(t, IsTerminalError(err))

	assert.Nil(t, wfCtx.Storage())
}

func TestActivity_TypedInputOutput(t *testing.T) {
	type Input struct {
		Name  string
		Value int
	}
	type Output struct {
		Result string
		Sum    int
	}

	activity := DefineActivity("typed_activity",
		func(ctx context.Context, input Input) (Output, error) {
			return Output{
				Result: "processed: " + input.Name,
				Sum:    input.Value * 2,
			}, nil
		},
	)

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	result, err := activity.Execute(wfCtx, Input{Name: "test", Value: 21})

	require.NoError(t, err)
	assert.Equal(t, "processed: test", result.Result)
	assert.Equal(t, 42, result.Sum)
}
