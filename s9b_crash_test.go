package selvedge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReplayDeterministicAfterCrashTimeout(t *testing.T) {
	testReplayDeterministicAfterCrashTimeout(t, createTestApp)
}
func testReplayDeterministicAfterCrashTimeout(t *testing.T, factory replayAppFactory) {
	for _, order := range []string{"signal-first", "fire-first"} {
		t.Run(order, func(t *testing.T) {
			baseline, writes := runTimeoutCrash(t, factory, order, 0, false)
			for cut := 1; cut <= writes; cut++ {
				for _, after := range []bool{false, true} {
					t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) {
						got, _ := runTimeoutCrash(t, factory, order, cut, after)
						require.Equal(t, baseline, got)
					})
				}
			}
		})
	}
}
func runTimeoutCrash(t *testing.T, factory replayAppFactory, order string, cut int, after bool) (deliverySnapshot, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	// Keep reservation-sourced fire values identical across crash runs.
	WithClock(func() time.Time { return time.Unix(123, 0).UTC() })(h.app.config)
	h.app.storage = fs
	w := DefineWorkflow("timeout-crash", func(ctx *WorkflowContext, _ string) (string, error) {
		v, err := ReceiveWithTimeout[int](ctx, "c", time.Hour)
		outcome := fmt.Sprint(v)
		if errors.Is(err, context.DeadlineExceeded) {
			outcome = "timeout"
		} else if err != nil {
			return "", err
		}
		if _, err := Receive[int](ctx, "finish"); err != nil {
			return "", err
		}
		return outcome, nil
	})
	id := seedErrorWorkflow(t, h, w)
	writes := 0
	crashed := false
	inject := func(_ context.Context, method string) error {
		switch method {
		case "AcquireLease", "ReserveTimer", "ReleaseLease", "FireTimer", "AppendSignal", "CommitObservation", "CloseInstance":
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
	for attempt := 0; attempt < 30; attempt++ {
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "dispatch: %v", err)
		inst, err := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		if inst.Status == storage.StatusCompleted {
			break
		}
		setStoreClock(t, h, int64(4000+attempt*4000000))
		row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
		require.NoError(t, err)
		if row == nil {
			continue
		}
		observation, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
		require.NoError(t, err)
		fire := func() error {
			return fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1"})
		}
		appendOnce := func(channel string) error {
			var n int
			require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_signals WHERE instance_id=$1 AND channel=$2`, id, channel).Scan(&n))
			if n > 0 {
				return nil
			}
			_, err := fs.AppendSignal(h.app.ctx, id, channel, []byte(`42`))
			return err
		}
		if observation == nil {
			if order == "signal-first" {
				err = appendOnce("c")
			} else {
				err = fire()
			}
		} else {
			if order == "signal-first" {
				err = fire()
			} else {
				err = appendOnce("c")
			}
			if err == nil {
				err = appendOnce("finish")
			}
		}
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "input: %v", err)
	}
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
	// A poller snapshot remains harmless even after the winning observation closed the instance.
	require.NoError(t, fs.Storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1"}))
	if cut > 0 {
		require.True(t, crashed)
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	result := deliverySnapshot{Output: string(inst.OutputData)}
	for _, row := range rows {
		result.History = append(result.History, deliveryHistoryValue{row.Seq, row.DeliveredSeq, row.ActivityID, row.Command, row.EventType, string(row.EventData)})
	}
	result.Signals, err = h.Deliveries(id)
	require.NoError(t, err)
	return result, writes
}
