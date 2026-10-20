package selvedge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/retry"
)

func TestShutdownCancellationStillClosesStorage(t *testing.T) {
	app, cleanup := createTestApp(t)
	t.Cleanup(cleanup)
	require.NoError(t, app.Start(context.Background()))
	app.wg.Add(1) // Hold one worker until shutdown has returned to the caller.
	var released sync.Once
	release := func() { released.Do(app.wg.Done) }
	t.Cleanup(release)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, app.Shutdown(ctx), context.Canceled)
	restartErr := app.Start(context.Background())
	secondShutdownErr := app.Shutdown(ctx)
	release()
	require.ErrorContains(t, restartErr, "shutting down")
	require.ErrorIs(t, secondShutdownErr, context.Canceled)
	require.NoError(t, app.Shutdown(context.Background()))
	require.Error(t, app.Storage().DB().PingContext(context.Background()), "cleanup must close storage after caller cancellation")

	// Completed shutdown still permits a fresh run on the same App.
	require.NoError(t, app.Start(context.Background()))
	require.NoError(t, app.Shutdown(context.Background()))
}

func TestWaitWorkflowReturnsWhenAppShutsDown(t *testing.T) {
	app, cleanup := createTestApp(t, WithWaitPollInterval(time.Hour))
	t.Cleanup(cleanup)
	require.NoError(t, app.Start(context.Background()))
	id, err := StartWorkflowByName(context.Background(), app, "remote-worker", nil)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, err := WaitWorkflow[string](context.Background(), app, id)
		result <- err
	}()
	require.Eventually(t, func() bool {
		app.waitMu.Lock()
		defer app.waitMu.Unlock()
		return len(app.waiters[id]) == 1
	}, time.Second, time.Millisecond)

	require.NoError(t, app.Shutdown(context.Background()))
	select {
	case err := <-result:
		require.ErrorIs(t, err, ErrAppNotStarted)
	case <-time.After(time.Second):
		t.Fatal("WaitWorkflow did not observe application shutdown")
	}
}

func TestStartRejectsInvalidRuntimeIntervals(t *testing.T) {
	for _, option := range []Option{
		WithTimerCheckInterval(0),
		WithWorkflowResumptionInterval(-time.Second),
		WithShutdownTimeout(0),
		WithNotifyReconnectDelay(-time.Second),
	} {
		app := NewApp(option)
		require.Error(t, app.Start(context.Background()))
		require.Nil(t, app.Storage(), "invalid configuration must fail before opening storage")
	}
}

func TestStartRejectsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	app := NewApp()
	require.True(t, errors.Is(app.Start(ctx), context.Canceled))
	require.Nil(t, app.Storage())
}

func TestTimerJitterNeverProducesNonpositiveDuration(t *testing.T) {
	for range 100 {
		require.Positive(t, addJitter(time.Nanosecond))
		require.Positive(t, addJitter(time.Duration(1<<63-1)))
	}
}

func TestPollingBackoffSaturatesBeforeDurationOverflow(t *testing.T) {
	require.Equal(t, time.Minute, calculateBackoff(time.Duration(1<<63-1), 5, time.Minute))
}

func TestActivityAttemptDeadlineDoesNotWaitForUncooperativeFunction(t *testing.T) {
	h, _, _ := leaseHarness(t, createTestApp)
	release := make(chan struct{})
	defer close(release)
	activity := DefineActivity("uncooperative", func(context.Context, string) (string, error) {
		<-release
		return "late", nil
	}, WithActivityDefaults[string, string](ActivityOptions{
		StartToCloseTimeout: 10 * time.Millisecond,
		RetryPolicy:         retry.NoRetry(),
	}))
	workflow := DefineWorkflow("deadline", func(ctx *WorkflowContext, input string) (string, error) {
		return activity.Execute(ctx, input)
	})
	id := seedErrorWorkflow(t, h, workflow)
	result := make(chan error, 1)
	go func() { result <- h.RunOnce(id) }()

	// The store clock remains fixed, so neither lease expiry nor a background
	// timeout sweep can release this workflow's inline activity attempt.
	select {
	case err := <-result:
		var timeout *ActivityTimeoutError
		require.ErrorAs(t, err, &timeout)
		require.Equal(t, TimeoutStartToClose, timeout.Kind)
	case <-time.After(time.Second):
		t.Fatal("attempt deadline failed to release workflow execution")
	}
}

// Counting callbacks verifies option replacement through actual dispatch.
type lifecycleStartHooks struct {
	hooks.NoOpHooks
	starts atomic.Int32
}

func (h *lifecycleStartHooks) OnWorkflowStart(context.Context, hooks.WorkflowStartInfo) {
	h.starts.Add(1)
}

func TestNilHooksResetCustomHooksAndRunWorkflow(t *testing.T) {
	custom := &lifecycleStartHooks{}
	app, cleanup := createTestApp(t, WithHooks(custom), WithHooks(nil))
	t.Cleanup(cleanup)
	workflow := DefineWorkflow("no-hooks", func(_ *WorkflowContext, input string) (string, error) {
		return input, nil
	})
	RegisterWorkflow(app, workflow)
	require.NoError(t, app.Start(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id, err := StartWorkflow(ctx, app, workflow, "completed")
	require.NoError(t, err)
	result, err := WaitWorkflow[string](ctx, app, id)
	require.NoError(t, err)
	require.NoError(t, result.Error)
	require.Equal(t, "completed", result.Output)
	require.Zero(t, custom.starts.Load(), "nil must replace previously configured hooks")
	require.NoError(t, app.Shutdown(ctx))
}
