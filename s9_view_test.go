package selvedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestIsReadyReadsDeliveredView(t *testing.T) { testIsReadyReadsDeliveredView(t, createTestApp) }
func testIsReadyReadsDeliveredView(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var ready []bool
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	w := DefineWorkflow("ready", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		ready = append(ready, f.IsReady())
		if err := Await(ctx, f.IsReady); err != nil {
			return "", err
		}
		ready = append(ready, f.IsReady())
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NoError(t, claim.Complete(`"A"`))
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []bool{false, true, false, true}, ready)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.EqualValues(t, 2, row.DeliveredSeq)
}
func TestPendingReadsDeliveredView(t *testing.T) { testPendingReadsDeliveredView(t, createTestApp) }
func testPendingReadsDeliveredView(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	var counts []int
	w := DefineWorkflow("pending", func(ctx *WorkflowContext, _ string) (string, error) {
		counts = append(counts, Pending(ctx, "c"), Pending(ctx, "other"))
		if err := Await(ctx, func() bool { return Pending(ctx, "c") > 0 }); err != nil {
			return "", err
		}
		counts = append(counts, Pending(ctx, "c"), Pending(ctx, "other"))
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 1))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "other", 2))
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []int{0, 0, 1, 1, 0, 0, 1, 1}, counts)
}
func TestPendingUsesConsumptionPositions(t *testing.T) {
	testPendingUsesConsumptionPositions(t, createTestApp)
}
func testPendingUsesConsumptionPositions(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	var counts, values []int
	w := DefineWorkflow("consume", func(ctx *WorkflowContext, _ string) (string, error) {
		if err := Await(ctx, func() bool { return Pending(ctx, "c") > 0 }); err != nil {
			return "", err
		}
		counts = append(counts, Pending(ctx, "c"))
		v, ok, err := ReceiveAsync[int](ctx, "c")
		if err != nil {
			return "", err
		}
		if !ok {
			return "", errors.New("missing signal")
		}
		values = append(values, v)
		counts = append(counts, Pending(ctx, "c"))
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 7))
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []int{1, 0, 1, 0}, counts)
	require.Equal(t, []int{7, 7}, values)
	positions, err := h.app.storage.DeliveredSignals(h.app.ctx, id)
	require.NoError(t, err)
	require.Len(t, positions, 1)
	require.EqualValues(t, 1, positions[0].DeliveredSeq)
	require.EqualValues(t, 2, positions[0].ConsumedSeq)
}
func TestIsReadyAfterReplayedWake(t *testing.T) { testIsReadyAfterReplayedWake(t, createTestApp) }
func testIsReadyAfterReplayedWake(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var ready []bool
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	w := DefineWorkflow("wake", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		if err := Sleep(ctx, time.Hour, WithSleepID("wake")); err != nil {
			return "", err
		}
		ready = append(ready, f.IsReady())
		if err := Await(ctx, f.IsReady); err != nil {
			return "", err
		}
		ready = append(ready, f.IsReady())
		return "", Sleep(ctx, time.Hour, WithSleepID("park"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.RunTasks(id))
	require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "wake"}))
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []bool{false, true, false, true}, ready)
}
func TestGetDoesNotDeliver(t *testing.T) { testGetDoesNotDeliver(t, createTestApp) }
func testGetDoesNotDeliver(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var ready []bool
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	w := DefineWorkflow("get", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		ready = append(ready, f.IsReady())
		_, err = f.Get(ctx)
		if err != nil {
			return "", err
		}
		ready = append(ready, f.IsReady())
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []bool{false, true, false, true}, ready)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Zero(t, row.DeliveredSeq)
	require.NotContains(t, fs.Calls(), "CommitObservation")
}
func TestReceiveDeliversBatch(t *testing.T) { testReceiveDeliversBatch(t, createTestApp) }
func testReceiveDeliversBatch(t *testing.T, factory replayAppFactory) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "receive", true: "cancel"}[cancel], func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var ready []bool
			var counts []int
			a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
			w := DefineWorkflow("receive", func(ctx *WorkflowContext, in string) (string, error) {
				f, err := a.ExecuteAsync(ctx, in)
				if err != nil {
					return "", err
				}
				_, err = Receive[int](ctx, "c")
				ready = append(ready, f.IsReady())
				counts = append(counts, Pending(ctx, "other"))
				return "", err
			})
			id := seedErrorWorkflow(t, h, w)
			seedTask(t, h, id)
			claim, err := h.ClaimTask(id, "#1")
			require.NoError(t, err)
			require.NoError(t, claim.Complete(`"A"`))
			require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 1))
			require.NoError(t, h.app.Signal(h.app.ctx, id, "other", 2))
			if cancel {
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
				require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			} else {
				require.NoError(t, h.RunOnce(id))
			}
			require.Equal(t, []bool{true}, ready)
			require.Equal(t, []int{1}, counts)
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			require.EqualValues(t, 2, row.DeliveredSeq)
		})
	}
	t.Run("sleep_cancel", func(t *testing.T) {
		h, _, fs := leaseHarness(t, factory)
		a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
		w := DefineWorkflow("sleep-cancel", func(ctx *WorkflowContext, in string) (string, error) {
			if _, err := a.ExecuteAsync(ctx, in); err != nil {
				return "", err
			}
			return "", Sleep(ctx, time.Hour)
		})
		id := seedErrorWorkflow(t, h, w)
		require.NoError(t, h.RunOnce(id))
		claim, err := h.ClaimTask(id, "#1")
		require.NoError(t, err)
		require.NoError(t, claim.Complete(`"A"`))
		activity, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
		require.NoError(t, err)
		require.Zero(t, activity.DeliveredSeq)
		sleep, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
		require.NoError(t, err)
		require.Nil(t, sleep.EventData)
		require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
		require.ErrorIs(t, h.RunOnce(id), context.Canceled)
		activity, err = fs.GetHistoryEvent(h.app.ctx, id, "#1")
		require.NoError(t, err)
		sleep, err = fs.GetHistoryEvent(h.app.ctx, id, "#2")
		require.NoError(t, err)
		require.Equal(t, storage.HistoryTimerCancelled, sleep.EventType)
		require.EqualValues(t, 2, sleep.DeliveredSeq)
		require.Equal(t, sleep.Seq, activity.DeliveredSeq)
	})
}
