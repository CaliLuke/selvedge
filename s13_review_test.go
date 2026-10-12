package selvedge

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func TestWaitWorkflowWakeBetweenReadAndSelect(t *testing.T) {
	s13WaitWakeBetweenReadAndSelect(t, createTestApp)
}

func s13WaitWakeBetweenReadAndSelect(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory, WithWaitPollInterval(time.Hour))
	inner := h.app.storage
	require.NoError(t, inner.CreateInstance(context.Background(), &storage.WorkflowInstance{
		InstanceID: "s13-wake-order", WorkflowName: "unregistered", Status: storage.StatusRunning, InputData: []byte(`null`),
	}))
	fs := faultstore.New(inner)
	var once sync.Once
	fs.After = func(_ context.Context, method string) error {
		if method == "GetInstance" {
			once.Do(func() {
				// GetInstance has already read running. Commit and wake before
				// returning that stale row to WaitWorkflow's select.
				_, err := inner.DB().Exec(`UPDATE workflow_instances SET status='completed', output_data='"done"' WHERE instance_id='s13-wake-order'`)
				require.NoError(t, err)
				h.app.wakeWaiters("s13-wake-order")
			})
		}
		return nil
	}
	h.app.storage = fs // No background loops use this deterministic harness.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := WaitWorkflow[string](ctx, h.app, "s13-wake-order")
	require.NoError(t, err)
	require.Equal(t, "done", result.Output)
	require.Empty(t, h.app.waiters, "Wait must unsubscribe on return")
}

func TestClientAfterShutdown(t *testing.T) { s13ClientAfterShutdown(t, createTestApp) }

func s13ClientAfterShutdown(t *testing.T, factory replayAppFactory) {
	app, cleanup := factory(t)
	t.Cleanup(cleanup)
	ctx := context.Background()
	require.NoError(t, app.Start(ctx))
	id, err := StartWorkflowByName(ctx, app, "unregistered", nil)
	require.NoError(t, err)
	require.NoError(t, app.Shutdown(ctx))
	t.Run("start_by_name", func(t *testing.T) {
		id, err := StartWorkflowByName(ctx, app, "unregistered", nil)
		require.Empty(t, id)
		require.ErrorIs(t, err, ErrAppNotStarted)
		require.ErrorIs(t, err, ErrWorkflowStartRejected)
	})
	t.Run("start_generic", func(t *testing.T) {
		workflow := DefineWorkflow("unregistered", func(*WorkflowContext, string) (string, error) { return "", nil })
		id, err := StartWorkflow(ctx, app, workflow, "")
		require.Empty(t, id)
		require.ErrorIs(t, err, ErrAppNotStarted)
		require.ErrorIs(t, err, ErrWorkflowStartRejected)
	})
	t.Run("signal", func(t *testing.T) { require.ErrorIs(t, app.Signal(ctx, id, "input", nil), ErrAppNotStarted) })
	t.Run("cancel", func(t *testing.T) { require.ErrorIs(t, CancelWorkflow(ctx, app, id, "stop"), ErrAppNotStarted) })
	t.Run("wait", func(t *testing.T) {
		result, err := WaitWorkflow[string](ctx, app, id)
		require.Nil(t, result)
		require.ErrorIs(t, err, ErrAppNotStarted)
	})
}

func TestClientStartRejectionSingleLine(t *testing.T) {
	s13StartRejectionSingleLine(t, createTestApp)
}

func s13StartRejectionSingleLine(t *testing.T, factory replayAppFactory) {
	t.Run("not_started", func(t *testing.T) {
		_, err := StartWorkflowByName(context.Background(), NewApp(), "unregistered", nil)
		require.ErrorIs(t, err, ErrAppNotStarted)
		require.ErrorIs(t, err, ErrWorkflowStartRejected)
		require.NotContains(t, err.Error(), "\n")
	})
	t.Run("cancelled", func(t *testing.T) {
		h := newTypedReplayHarness(t, factory)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := StartWorkflowByName(ctx, h.app, "unregistered", nil)
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, ErrWorkflowStartRejected)
		require.NotContains(t, err.Error(), "\n")
	})
}
