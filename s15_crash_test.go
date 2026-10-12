package selvedge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

func TestReplayDeterministicAfterCrashRetry(t *testing.T) {
	testReplayDeterministicAfterCrashRetry(t, createTestApp)
}
func testReplayDeterministicAfterCrashRetry(t *testing.T, factory replayAppFactory) {
	baseline, writes := s15CrashScenario(t, factory, 0, false)
	for cut := 1; cut <= writes; cut++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) {
				got, _ := s15CrashScenario(t, factory, cut, after)
				require.Equal(t, baseline, got)
			})
		}
	}
}
func s15CrashScenario(t *testing.T, factory replayAppFactory, cut int, after bool) ([]historyValue, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	a := DefineActivity("crash_retry", func(ctx context.Context, _ string) (string, error) {
		if GetActivityInfo(ctx).Attempt == 1 {
			return "", errors.New("retry")
		}
		return "done", nil
	}, WithRetryPolicy[string, string](retry.Fixed(10, time.Second)))
	w := DefineWorkflow("crash_retry", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	var mu sync.Mutex
	writes := 0
	crashed := false
	inject := func(_ context.Context, method string) error {
		mu.Lock()
		defer mu.Unlock()
		switch method {
		case "AcquireLease", "ReserveActivity", "ClaimActivityTask", "ClaimActivityTasks", "RetryActivityTask", "CompleteActivityTask", "ReleaseActivityTask", "ReleaseLease", "CloseInstance":
			writes++
		default:
			return nil
		}
		if cut > 0 && !crashed && writes == cut {
			crashed = true
			return storage.ErrFenced
		}
		return nil
	}
	if after {
		fs.After = inject
	} else {
		fs.Before = inject
	}
	for turn := 0; turn < 12; turn++ {
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "dispatch: %v", err)
		inst, err := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		if inst.Status == storage.StatusCompleted {
			break
		}
		setStoreClock(t, h, int64(4000+turn*3000))
		err = h.RunTasks(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "attempt: %v", err)
	}
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
	require.JSONEq(t, `"done"`, string(inst.OutputData))
	if cut > 0 {
		require.True(t, crashed)
	} else {
		require.Contains(t, fs.Calls(), "RetryActivityTask")
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	values := make([]historyValue, 0, len(rows))
	for _, row := range rows {
		values = append(values, historyValue{row.Seq, row.DeliveredSeq, row.ActivityID, row.Command, row.EventType, string(row.EventData)})
	}
	baselineWrites := writes
	before := len(fs.Calls())
	_, err = fs.DB().Exec("UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1", id)
	require.NoError(t, err)
	require.NoError(t, h.RunOnce(id))
	require.NotContains(t, fs.Calls()[before:], "ReserveActivity")
	require.NotContains(t, fs.Calls()[before:], "ClaimActivityTask")
	return values, baselineWrites
}
