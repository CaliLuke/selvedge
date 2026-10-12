package selvedge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReplayDeterministicAfterCrashAsync(t *testing.T) {
	testReplayDeterministicAfterCrashAsync(t, createTestApp)
}
func testReplayDeterministicAfterCrashAsync(t *testing.T, factory replayAppFactory) {
	for _, order := range []string{"AB", "BA", "cancel"} {
		t.Run(order, func(t *testing.T) {
			baseline, writes := runAsyncCrashScenario(t, factory, order, 0, false)
			for cut := 1; cut <= writes; cut++ {
				for _, after := range []bool{false, true} {
					t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) {
						got, _ := runAsyncCrashScenario(t, factory, order, cut, after)
						require.Equal(t, baseline, got)
					})
				}
			}
		})
	}
}

func runAsyncCrashScenario(t *testing.T, factory replayAppFactory, order string, cut int, after bool) ([]historyValue, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	var waits []string
	h.app.ctx = replay.WithTestYieldObserver(h.app.ctx, func(w replay.Wait) { waits = append(waits, w.Key) })
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	b := DefineActivity("B", func(context.Context, string) (string, error) { return "B", nil })
	w := DefineWorkflow("async_crash", func(ctx *WorkflowContext, in string) (string, error) {
		fa, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		fb, err := b.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		if order != "cancel" {
			_, e := fs.ClaimActivityTasks(ctx.Context(), "controlled-pool", time.Second, 10, "A", "B")
			if e != nil {
				return "", e
			}
		}
		vb, err := fb.Get(ctx)
		if err != nil {
			return "", err
		}
		va, err := fa.Get(ctx)
		return va + vb, err
	})
	id := seedErrorWorkflow(t, h, w)
	if order == "cancel" {
		require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	}
	var mu sync.Mutex
	writes := 0
	crashed := false
	inject := func(_ context.Context, method string) error {
		mu.Lock()
		defer mu.Unlock()
		switch method {
		case "AcquireLease", "ReserveHistory", "ReserveActivity", "ClaimActivityTask", "ClaimActivityTasks", "RenewActivityTask", "CompleteActivityTask", "ReleaseActivityTask", "CancelActivityAtYield", "ConsumeSignal", "SetHistoryValueIfNull", "ReserveTimer", "ReleaseLease", "FireTimer", "CloseInstance":
			writes++
		default:
			return nil
		}
		if !crashed && cut > 0 && writes == cut {
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
	for attempt := 0; attempt < 12; attempt++ {
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced) || errors.Is(err, context.Canceled), "dispatch: %v", err)
		inst, e := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, e)
		if inst.Status == storage.StatusCompleted || inst.Status == storage.StatusCancelled {
			break
		}
		setStoreClock(t, h, int64(4000+attempt*3000))
		if order != "cancel" {
			// Recover held claims, then run exactly one activity before redispatch.
			// The first result must be durable before we allow the other to run.
			firstName, firstKey, secondName := "A", "#1", "B"
			if order == "BA" {
				firstName, firstKey, secondName = "B", "#2", "A"
			}
			row, err := fs.GetHistoryEvent(h.app.ctx, id, firstKey)
			require.NoError(t, err)
			name := firstName
			if row != nil && row.EventData != nil {
				name = secondName
			}
			err = h.RunTasks(id, name)
			require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "attempt: %v", err)
		}
	}
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	if order == "cancel" {
		require.Equal(t, storage.StatusCancelled, inst.Status)
	} else {
		require.Equal(t, storage.StatusCompleted, inst.Status)
		require.JSONEq(t, `"AB"`, string(inst.OutputData))
	}
	if cut == 0 && order != "cancel" {
		expected := []string{"#2", "#2"}
		if order == "BA" {
			expected[1] = "#1"
		}
		require.Equal(t, expected, waits, "dispatch must observe the gap between completions")
	}
	if cut > 0 {
		require.True(t, crashed, "cut must be reached")
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	values := make([]historyValue, 0, len(rows))
	for _, row := range rows {
		values = append(values, historyValue{row.Seq, row.ActivityID, row.Command, row.EventType, string(row.EventData)})
	}
	return values, writes
}
