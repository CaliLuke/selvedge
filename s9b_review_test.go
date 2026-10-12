package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReceiveWithTimeoutCancelPriority(t *testing.T) {
	testReceiveWithTimeoutCancelPriority(t, createTestApp)
}
func testReceiveWithTimeoutCancelPriority(t *testing.T, factory replayAppFactory) {
	for _, fired := range []bool{true, false} {
		name := "cancel_signal"
		if fired {
			name = "cancel_signal_fire"
		}
		t.Run(name, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			w := DefineWorkflow("timeout-cancel-priority", func(ctx *WorkflowContext, _ string) (string, error) {
				_, err := ReceiveWithTimeout[int](ctx, "c", time.Hour)
				return "done", err
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			parked, err := fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.False(t, parked.Runnable)
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 42))
			if fired {
				require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()}))
			}

			runErr := h.RunOnce(id)
			observation, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
			require.NoError(t, err)
			require.NotNil(t, observation)
			require.Equal(t, storage.HistorySignalCancelled, observation.EventType)
			require.ErrorIs(t, runErr, context.Canceled)
			signal, err := fs.NextSignal(h.app.ctx, id, "c")
			require.NoError(t, err)
			require.NotNil(t, signal)
			require.Empty(t, signal.ConsumedKey)
			require.Zero(t, signal.ConsumedSeq)
			require.EqualValues(t, 2, signal.DeliveredSeq)
			timer, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			expected := storage.HistoryTimerSuperseded
			if fired {
				expected = storage.HistoryTimerExpired
			}
			require.Equal(t, expected, timer.EventType)
			var subscriptions int
			require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id='#1'`, id).Scan(&subscriptions))
			require.Zero(t, subscriptions)
		})
	}
}

func TestSleepRejectsSupersededTimeout(t *testing.T) {
	testSleepRejectsSupersededTimeout(t, createTestApp)
}
func testSleepRejectsSupersededTimeout(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	changed := false
	var sleepErr error
	w := DefineWorkflow("timeout-to-sleep", func(ctx *WorkflowContext, _ string) (string, error) {
		duration := time.Hour
		if changed {
			// Keep the same sleep / receive_timeout:c command sequence and ignore the
			// Sleep error: the invalid recorded outcome must still fail the execution.
			sleepErr = Sleep(ctx, time.Hour)
			duration = 0
		}
		_, err := ReceiveWithTimeout[int](ctx, "c", duration)
		if err != nil {
			return "ignored", nil
		}
		return "", Sleep(ctx, time.Hour, WithSleepID("park"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 42))
	require.NoError(t, h.RunOnce(id))
	timer, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryTimerSuperseded, timer.EventType)
	observation, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
	require.NoError(t, err)
	require.Equal(t, "receive_timeout:c", observation.Command)
	require.Equal(t, storage.HistorySignalReceived, observation.EventType)
	changed = true
	require.NoError(t, fs.MarkRunnable(h.app.ctx, id))

	err = h.RunOnce(id)
	require.ErrorContains(t, err, "TimerSuperseded")
	require.ErrorContains(t, sleepErr, "TimerSuperseded")
	instance, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusFailed, instance.Status)
}

func TestReceiveWithTimeoutConsistentReads(t *testing.T) {
	testReceiveWithTimeoutConsistentReads(t, createTestApp)
}
func testReceiveWithTimeoutConsistentReads(t *testing.T, factory replayAppFactory) {
	for _, race := range []string{"signal_before_fire", "cancel_before_signal"} {
		t.Run(race, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var value int
			w := DefineWorkflow("timeout-read-race", func(ctx *WorkflowContext, _ string) (string, error) {
				var err error
				value, err = ReceiveWithTimeout[int](ctx, "c", time.Hour)
				return "done", err
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			injected := false
			methodToRace := "NextSignal"
			if race == "cancel_before_signal" {
				methodToRace = "IsCancelRequested"
			}
			fs.After = func(ctx context.Context, method string) error {
				if method != methodToRace || injected {
					return nil
				}
				injected = true
				if race == "cancel_before_signal" {
					if _, err := fs.Storage.RequestCancel(ctx, id, "stop"); err != nil {
						return err
					}
				}
				if _, err := fs.Storage.AppendSignal(ctx, id, "c", []byte(`42`)); err != nil {
					return err
				}
				if race == "signal_before_fire" {
					return fs.Storage.FireTimer(ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()})
				}
				return nil
			}
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))

			firstErr := h.RunOnce(id)
			require.True(t, injected)
			observation, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
			require.NoError(t, err)
			require.Nil(t, observation, "a lower-priority arrival must not win using an earlier false read")
			require.NoError(t, firstErr)
			fs.After = nil
			runErr := h.RunOnce(id)
			observation, err = fs.GetHistoryEvent(h.app.ctx, id, "#2")
			require.NoError(t, err)
			require.NotNil(t, observation)
			if race == "signal_before_fire" {
				require.NoError(t, runErr)
				require.Equal(t, 42, value)
				require.Equal(t, storage.HistorySignalReceived, observation.EventType)
			} else {
				require.ErrorIs(t, runErr, context.Canceled)
				require.Equal(t, storage.HistorySignalCancelled, observation.EventType)
				signal, err := fs.NextSignal(h.app.ctx, id, "c")
				require.NoError(t, err)
				require.NotNil(t, signal)
				require.Zero(t, signal.ConsumedSeq)
			}
		})
	}
}
