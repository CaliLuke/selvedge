package selvedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReceiveWithTimeoutNonPositiveIsRecorded(t *testing.T) {
	testReceiveWithTimeoutNonPositiveIsRecorded(t, createTestApp)
}
func testReceiveWithTimeoutNonPositiveIsRecorded(t *testing.T, factory replayAppFactory) {
	for _, d := range []time.Duration{0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var outcomes []error
			w := DefineWorkflow("immediate", func(ctx *WorkflowContext, _ string) (string, error) {
				_, err := ReceiveWithTimeout[int](ctx, "c", d)
				outcomes = append(outcomes, err)
				if !errors.Is(err, context.DeadlineExceeded) {
					return "", err
				}
				return "", Sleep(ctx, time.Hour)
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 42))
			require.NoError(t, h.RunOnce(id))
			require.Len(t, outcomes, 1)
			require.ErrorIs(t, outcomes[0], context.DeadlineExceeded)
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			require.NoError(t, h.RunOnce(id))
			require.Len(t, outcomes, 2)
			require.ErrorIs(t, outcomes[1], context.DeadlineExceeded)
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			require.Equal(t, "receive_timeout:c", row.Command)
			require.Equal(t, storage.HistoryEventType("SignalTimeout"), row.EventType)
			require.EqualValues(t, 1, row.DeliveredSeq)
			signals, err := h.Deliveries(id)
			require.NoError(t, err)
			require.Len(t, signals, 1)
			require.Zero(t, signals[0].ConsumedSeq)
			require.Zero(t, signals[0].DeliveredSeq)
			require.NotContains(t, fs.Calls(), "CommitObservation")
		})
	}
}

func TestReceiveWithTimeoutLosingTimerDoesNotWake(t *testing.T) {
	testReceiveWithTimeoutLosingTimerDoesNotWake(t, createTestApp)
}
func testReceiveWithTimeoutLosingTimerDoesNotWake(t *testing.T, factory replayAppFactory) {
	for _, fireFirst := range []bool{false, true} {
		name := "signal-first"
		if fireFirst {
			name = "fire-first"
		}
		t.Run(name, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var values []int
			w := DefineWorkflow("losing-timer", func(ctx *WorkflowContext, _ string) (string, error) {
				v, err := ReceiveWithTimeout[int](ctx, "c", time.Hour)
				if err != nil {
					return "", err
				}
				values = append(values, v)
				return "", Sleep(ctx, time.Hour, WithSleepID("park"))
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			stale := &storage.TimerSubscription{InstanceID: id, TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()}
			if fireFirst {
				require.NoError(t, fs.FireTimer(h.app.ctx, stale))
			}
			require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 42))
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, []int{42}, values)
			before, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			if fireFirst {
				require.Equal(t, storage.HistoryTimerExpired, before.EventType)
			} else {
				require.Equal(t, storage.HistoryEventType("TimerSuperseded"), before.EventType)
			}
			require.EqualValues(t, 2, before.DeliveredSeq)
			require.NoError(t, fs.FireTimer(h.app.ctx, stale))
			after, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			require.Equal(t, before, after)
			inst, err := fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.False(t, inst.Runnable)
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, []int{42, 42}, values)
			var n int
			require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id='#1'`, id).Scan(&n))
			require.Zero(t, n)
		})
	}
}

func TestReceiveWithTimeoutTimesOut(t *testing.T) { testReceiveWithTimeoutTimesOut(t, createTestApp) }
func testReceiveWithTimeoutTimesOut(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var outcomes []error
	var pending []int
	w := DefineWorkflow("timeout", func(ctx *WorkflowContext, _ string) (string, error) {
		_, err := ReceiveWithTimeout[int](ctx, "c", time.Hour)
		outcomes = append(outcomes, err)
		if !errors.Is(err, context.DeadlineExceeded) {
			return "", err
		}
		pending = append(pending, Pending(ctx, "other"))
		return "", Sleep(ctx, time.Hour, WithSleepID("park"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Empty(t, outcomes)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "other", 7))
	require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()}))
	require.NoError(t, h.RunOnce(id))
	require.Len(t, outcomes, 1)
	require.ErrorIs(t, outcomes[0], context.DeadlineExceeded)
	require.Equal(t, []int{1}, pending)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 42))
	require.NoError(t, h.RunOnce(id))
	require.Len(t, outcomes, 2)
	require.ErrorIs(t, outcomes[1], context.DeadlineExceeded)
	require.Equal(t, []int{1, 1}, pending)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryEventType("SignalTimeout"), row.EventType)
	require.EqualValues(t, 2, row.DeliveredSeq)
	timer, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.EqualValues(t, 2, timer.DeliveredSeq)
	sig, err := fs.NextSignal(h.app.ctx, id, "c")
	require.NoError(t, err)
	require.NotNil(t, sig)
	require.Zero(t, sig.DeliveredSeq)
}

func TestReplayCommandMismatchFailsFastTimeout(t *testing.T) {
	testReplayCommandMismatchFailsFastTimeout(t, createTestApp)
}
func testReplayCommandMismatchFailsFastTimeout(t *testing.T, factory replayAppFactory) {
	for _, d := range []time.Duration{0, time.Hour} {
		t.Run(d.String(), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			changed := false
			w := DefineWorkflow("mismatch-timeout", func(ctx *WorkflowContext, _ string) (string, error) {
				if changed {
					_, err := Receive[int](ctx, "c")
					return "", err
				}
				_, err := ReceiveWithTimeout[int](ctx, "c", d)
				if err != nil && !errors.Is(err, context.DeadlineExceeded) {
					return "", err
				}
				return "", Sleep(ctx, time.Hour, WithSleepID("park"))
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			changed = true
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			var nondet *NondeterminismError
			require.ErrorAs(t, h.RunOnce(id), &nondet)
			require.EqualValues(t, 1, nondet.Seq)
			require.Equal(t, "receive:c", nondet.Issued)
		})
	}
}
