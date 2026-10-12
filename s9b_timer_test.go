package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestNewTimerFuture(t *testing.T) { testNewTimerFuture(t, createTestApp) }
func testNewTimerFuture(t *testing.T, factory replayAppFactory) {
	for _, d := range []time.Duration{0, -time.Second, time.Hour} {
		t.Run(d.String(), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var ready []bool
			var values []time.Time
			var waits []replay.Wait
			h.app.ctx = replay.WithTestYieldObserver(h.app.ctx, func(w replay.Wait) { waits = append(waits, w) })
			w := DefineWorkflow("timer", func(ctx *WorkflowContext, _ string) (string, error) {
				timer, err := NewTimer(ctx, d, WithSleepID("timer"))
				if err != nil {
					return "", err
				}
				if timer.Key() != "timer" {
					return "", errors.New("timer key changed")
				}
				ready = append(ready, timer.IsReady())
				v, err := timer.Get(ctx)
				if err != nil {
					return "", err
				}
				v2, err := timer.Get(ctx)
				if err != nil {
					return "", err
				}
				if v != v2 || !timer.IsReady() {
					return "", errors.New("cached timer outcome changed")
				}
				values = append(values, v)
				return "", Sleep(ctx, time.Hour, WithSleepID("park"))
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "timer")
			require.NoError(t, err)
			require.NotNil(t, row)
			if d > 0 {
				require.Equal(t, []replay.Wait{{Seq: 1, Key: "timer", Kind: replay.WaitTimer}}, waits)
				require.Nil(t, row.EventData)
				var reservation storage.SleepReservation
				require.NoError(t, json.Unmarshal(row.ReservationData, &reservation))
				// An unfilled reservation re-arms with its original deadline.
				_, err = fs.DB().Exec(`DELETE FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id='timer'`, id)
				require.NoError(t, err)
				require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
				require.NoError(t, h.RunOnce(id))
				var n int
				require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id='timer'`, id).Scan(&n))
				require.Equal(t, 1, n)
				require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "timer", ExpiresAt: reservation.ExpiresAt}))
				require.NoError(t, h.RunOnce(id))
			}
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			require.NoError(t, h.RunOnce(id))
			require.Len(t, values, 2)
			require.False(t, values[0].IsZero())
			require.Equal(t, values[0], values[1])
			row, err = fs.GetHistoryEvent(h.app.ctx, id, "timer")
			require.NoError(t, err)
			if d > 0 {
				require.Equal(t, []bool{false, false, false, false}, ready)
				require.Zero(t, row.DeliveredSeq)
			} else {
				require.Equal(t, []bool{true, true}, ready)
				require.EqualValues(t, 1, row.DeliveredSeq)
			}
			require.NotContains(t, fs.Calls(), "CommitObservation")
		})
	}
	t.Run("await-delivery", func(t *testing.T) {
		h, _, fs := leaseHarness(t, factory)
		var ready []bool
		w := DefineWorkflow("await-timer", func(ctx *WorkflowContext, _ string) (string, error) {
			timer, err := NewTimer(ctx, time.Hour)
			if err != nil {
				return "", err
			}
			ready = append(ready, timer.IsReady())
			if err := Await(ctx, timer.IsReady); err != nil {
				return "", err
			}
			ready = append(ready, timer.IsReady())
			_, err = timer.Get(ctx)
			if err != nil {
				return "", err
			}
			return "", Sleep(ctx, time.Hour)
		})
		id := seedErrorWorkflow(t, h, w)
		require.NoError(t, h.RunOnce(id))
		require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()}))
		require.NoError(t, h.RunOnce(id))
		require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
		require.NoError(t, h.RunOnce(id))
		require.Equal(t, []bool{false, false, true, false, true}, ready)
		row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
		require.NoError(t, err)
		require.EqualValues(t, 2, row.DeliveredSeq)
	})
}

func TestCancelEndsBlockedWaitsTimers(t *testing.T) {
	testCancelEndsBlockedWaitsTimers(t, createTestApp)
}
func testCancelEndsBlockedWaitsTimers(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"get", "receive"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var reasons []string
			w := DefineWorkflow("cancel-timer", func(ctx *WorkflowContext, _ string) (string, error) {
				var err error
				if kind == "get" {
					var timer *Timer
					timer, err = NewTimer(ctx, time.Hour)
					if err == nil {
						_, err = timer.Get(ctx)
					}
				} else {
					_, err = ReceiveWithTimeout[int](ctx, "c", time.Hour)
				}
				var cancellation *WorkflowCancelledError
				if errors.As(err, &cancellation) {
					reasons = append(reasons, cancellation.Reason)
				}
				return "", err
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			inst, err := fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, storage.StatusCancelled, inst.Status)
			require.NoError(t, h.RunOnce(id))
			// Force replay of the recorded cancellation, bypassing only terminal dispatch.
			_, err = fs.DB().Exec(`UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1`, id)
			require.NoError(t, err)
			require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			require.Equal(t, []string{"stop", "stop"}, reasons)
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			if kind == "get" {
				require.Equal(t, storage.HistoryTimerCancelled, row.EventType)
				require.Zero(t, row.DeliveredSeq)
			} else {
				require.Equal(t, storage.HistoryEventType("TimerSuperseded"), row.EventType)
				require.EqualValues(t, 2, row.DeliveredSeq)
			}
		})
	}
}

func TestTimerReplayCompatibility(t *testing.T) { testTimerReplayCompatibility(t, createTestApp) }
func testTimerReplayCompatibility(t *testing.T, factory replayAppFactory) {
	for _, d := range []time.Duration{0, time.Hour} {
		for _, firstSleep := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/sleep_%t", d, firstSleep), func(t *testing.T) {
				h, _, fs := leaseHarness(t, factory)
				useSleep := firstSleep
				w := DefineWorkflow("compatible", func(ctx *WorkflowContext, _ string) (string, error) {
					if useSleep {
						if err := Sleep(ctx, d); err != nil {
							return "", err
						}
					} else {
						timer, err := NewTimer(ctx, d)
						if err != nil {
							return "", err
						}
						if _, err = timer.Get(ctx); err != nil {
							return "", err
						}
					}
					return "", Sleep(ctx, time.Hour, WithSleepID("park"))
				})
				id := seedErrorWorkflow(t, h, w)
				require.NoError(t, h.RunOnce(id))
				if d > 0 {
					require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()}))
					require.NoError(t, h.RunOnce(id))
				}
				useSleep = !useSleep
				require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
				require.NoError(t, h.RunOnce(id))
			})
		}
	}
}
