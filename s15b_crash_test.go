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
)

func TestReplayDeterministicAfterCrashActivityTimeout(t *testing.T) {
	testReplayDeterministicAfterCrashActivityTimeout(t, createTestApp)
}
func testReplayDeterministicAfterCrashActivityTimeout(t *testing.T, factory replayAppFactory) {
	baseline, writes := s15bCrash(t, factory, 0, false)
	for cut := 1; cut <= writes; cut++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) { got, _ := s15bCrash(t, factory, cut, after); require.Equal(t, baseline, got) })
		}
	}
}
func s15bCrash(t *testing.T, factory replayAppFactory, cut int, after bool) ([]historyValue, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	h.app.config.activityQueues = []string{"unserved"}
	a := DefineActivity("crash_timeout", func(context.Context, string) (string, error) { t.Error("expired activity ran"); return "", nil }, WithActivityDefaults[string, string](ActivityOptions{ScheduleToStartTimeout: time.Millisecond}))
	w := DefineWorkflow("crash_timeout", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	var mu sync.Mutex
	writes := 0
	crashed := false
	inject := func(_ context.Context, method string) error {
		mu.Lock()
		defer mu.Unlock()
		switch method {
		case "AcquireLease", "ReserveActivity", "ClaimActivityTask", "TimeoutActivityTask", "ReleaseLease", "CloseInstance":
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
	for turn := 0; turn < 8; turn++ {
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced) || errors.Is(err, context.DeadlineExceeded), "run: %v", err)
		inst, err := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		if inst.Status == storage.StatusFailed {
			break
		}
		setStoreClock(t, h, int64(4000+turn*3000))
		err = h.app.checkActivityTimeouts(h.app.ctx)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "sweep: %v", err)
	}
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusFailed, inst.Status)
	if cut > 0 {
		require.True(t, crashed)
	} else {
		require.Contains(t, fs.Calls(), "TimeoutActivityTask")
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	values := make([]historyValue, 0, len(rows))
	for _, row := range rows {
		values = append(values, historyValue{row.Seq, row.DeliveredSeq, row.ActivityID, row.Command, row.EventType, string(row.EventData)})
	}
	return values, writes
}
func TestActivityTimeoutLeaderSweep(t *testing.T) { testActivityTimeoutLeaderSweep(t, createTestApp) }
func testActivityTimeoutLeaderSweep(t *testing.T, factory replayAppFactory) {
	app, cleanup := factory(t, WithActivityQueues("unserved"), WithTimerCheckInterval(5*time.Millisecond))
	t.Cleanup(cleanup)
	a := DefineActivity("leader_timeout", func(context.Context, string) (string, error) { t.Error("unserved activity ran"); return "", nil }, WithActivityDefaults[string, string](ActivityOptions{ScheduleToStartTimeout: 5 * time.Millisecond}))
	w := DefineWorkflow("leader_timeout", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	RegisterWorkflow(app, w)
	require.NoError(t, app.Start(context.Background()))
	id, err := StartWorkflow(context.Background(), app, w, "")
	require.NoError(t, err)
	result := s13Wait[string](t, app, id)
	require.Equal(t, "failed", result.Status)
	require.ErrorIs(t, result.Error, context.DeadlineExceeded)
}

func TestReplayHeartbeatLapseDifferentResults(t *testing.T) {
	testFutureGetReadsHistoryWithHeartbeat(t, createTestApp, true)
}
