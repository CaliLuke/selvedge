package selvedge

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestCancelWhileSleepingStaysCancelled(t *testing.T) {
	testCancelWhileSleepingStaysCancelled(t, createTestApp)
}
func testCancelWhileSleepingStaysCancelled(t *testing.T, factory replayAppFactory) {
	for _, fireFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(fireFirst), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			w := DefineWorkflow("cancel_sleep", func(ctx *WorkflowContext, _ string) (string, error) {
				if err := Sleep(ctx, time.Hour, WithSleepID("pause")); err != nil {
					return "", err
				}
				return "", Sleep(ctx, time.Hour, WithSleepID("next"))
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			timer := &storage.TimerSubscription{InstanceID: id, TimerID: "pause"}
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			if fireFirst {
				require.NoError(t, fs.FireTimer(h.app.ctx, timer))
			}
			require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			assertCancelledRun(t, h, id)
			rows, err := fs.GetHistoryCount(h.app.ctx, id)
			require.NoError(t, err)
			require.NoError(t, fs.FireTimer(h.app.ctx, timer))
			require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "late"}))
			after, err := fs.GetHistoryCount(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, rows, after)
			require.NoError(t, h.RunOnce(id))
			assertCancelledRun(t, h, id)
		})
	}
}
func assertCancelledRun(t *testing.T, h *typedReplayHarness, id string) {
	t.Helper()
	inst, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCancelled, inst.Status)
	var timers int
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1", id).Scan(&timers))
	require.Zero(t, timers)
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.ErrorIs(t, result.Error, context.Canceled)
}
func TestWakeCancelRacesTimerFire(t *testing.T) { testWakeCancelRacesTimerFire(t, createTestApp) }
func testWakeCancelRacesTimerFire(t *testing.T, factory replayAppFactory) {
	for _, fireFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(fireFirst), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			continued := false
			w := DefineWorkflow("cancel_race", func(ctx *WorkflowContext, _ string) (string, error) {
				if err := Sleep(ctx, time.Hour, WithSleepID("pause")); err != nil {
					return "", err
				}
				continued = true
				return "", Sleep(ctx, time.Hour, WithSleepID("next"))
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			var once sync.Once
			inject := func(ctx context.Context, method string) error {
				if method == "CommitObservation" {
					once.Do(func() {
						require.NoError(t, fs.Storage.FireTimer(ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "pause"}))
					})
				}
				return nil
			}
			if fireFirst {
				fs.Before = inject
			} else {
				fs.After = inject
			}
			require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			require.Equal(t, fireFirst, continued)
			ev, err := fs.GetHistoryEvent(h.app.ctx, id, "pause")
			require.NoError(t, err)
			expected := storage.HistoryTimerCancelled
			if fireFirst {
				expected = storage.HistoryTimerExpired
			}
			require.Equal(t, expected, ev.EventType)
			assertCancelledRun(t, h, id)
		})
	}
}
func TestTimerFireRecordedOnce(t *testing.T) { testTimerFireRecordedOnce(t, createTestApp) }
func testTimerFireRecordedOnce(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("timer_once", func(ctx *WorkflowContext, _ string) (string, error) {
		return "done", Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	timer := &storage.TimerSubscription{InstanceID: id, TimerID: "pause", ExpiresAt: time.Unix(10, 0)}
	require.NoError(t, fs.FireTimer(h.app.ctx, timer))
	first, err := fs.GetHistoryEvent(h.app.ctx, id, "pause")
	require.NoError(t, err)
	// Two other pollers concurrently retry the same timer with different payloads.
	outcomes := make(chan error, 2)
	start := make(chan struct{})
	for _, expiry := range []int64{20, 30} {
		go func() {
			<-start
			outcomes <- fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "pause", ExpiresAt: time.Unix(expiry, 0)})
		}()
	}
	close(start)
	require.NoError(t, <-outcomes)
	require.NoError(t, <-outcomes)
	second, err := fs.GetHistoryEvent(h.app.ctx, id, "pause")
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, fs.FireTimer(h.app.ctx, timer))
	third, err := fs.GetHistoryEvent(h.app.ctx, id, "pause")
	require.NoError(t, err)
	require.Equal(t, first, third)
	assertCompletedLeaseRun(t, h, id)
}
func TestStaleExecutionCannotOverwriteClosedStatus(t *testing.T) {
	testStaleExecutionCannotOverwriteClosedStatus(t, createTestApp)
}
func testStaleExecutionCannotOverwriteClosedStatus(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("stale_close", func(ctx *WorkflowContext, _ string) (string, error) { return "", Sleep(ctx, time.Hour) })
	id := seedErrorWorkflow(t, h, w)
	old, ok, err := fs.AcquireLease(h.app.ctx, id, "old", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	setStoreClock(t, h, 2001)
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	err = fs.CloseInstance(h.app.ctx, old, storage.StatusCompleted, []byte(`"bad"`), nil)
	require.ErrorIs(t, err, storage.ErrFenced)
	assertCancelledRun(t, h, id)
}
func TestCancelLeasedInstanceSucceeds(t *testing.T) {
	testCancelLeasedInstanceSucceeds(t, createTestApp)
}
func testCancelLeasedInstanceSucceeds(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	entered := make(chan struct{})
	release := make(chan struct{})
	activityErr := make(chan error, 1)
	a := DefineActivity("in_flight", func(ctx context.Context, _ string) (string, error) {
		close(entered)
		<-release
		activityErr <- ctx.Err()
		return "done", nil
	})
	w := DefineWorkflow("leased_cancel", func(ctx *WorkflowContext, input string) (string, error) {
		if _, err := a.Execute(ctx, input); err != nil {
			return "", err
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	<-entered
	err := CancelWorkflow(h.app.ctx, h.app, id, "stop")
	close(release)
	require.NoError(t, err)
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, <-activityErr)
	assertCancelledRun(t, h, id)
}
func TestCancelOutcomesTyped(t *testing.T) { testCancelOutcomesTyped(t, createTestApp) }
func testCancelOutcomesTyped(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	for _, tc := range []struct {
		status storage.WorkflowStatus
		err    error
	}{{storage.StatusCompleted, ErrWorkflowAlreadyCompleted}, {storage.StatusFailed, ErrWorkflowAlreadyFailed}, {storage.StatusCancelled, nil}, {storage.StatusRunning, nil}} {
		id := string(tc.status)
		require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: id, WorkflowName: "outcomes", Status: tc.status, InputData: []byte(`{}`)}))
		require.ErrorIs(t, CancelWorkflow(h.app.ctx, h.app, id, "first"), tc.err)
		require.ErrorIs(t, CancelWorkflow(h.app.ctx, h.app, id, "second"), tc.err)
	}
	var missing *WorkflowNotFoundError
	require.ErrorAs(t, CancelWorkflow(h.app.ctx, h.app, "unknown", ""), &missing)
	var reason string
	var requested bool
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT cancel_reason,cancel_requested_at IS NOT NULL FROM workflow_instances WHERE instance_id='running'").Scan(&reason, &requested))
	require.Equal(t, "first", reason)
	require.True(t, requested)
}
func TestAbandonedRunIsRedispatchedPendingCancel(t *testing.T) {
	testAbandonedRunIsRedispatchedPendingCancel(t, createTestApp)
}
func testAbandonedRunIsRedispatchedPendingCancel(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	first := true
	w := DefineWorkflow("abandon_cancel", func(ctx *WorkflowContext, _ string) (string, error) {
		if first {
			first = false
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, ctx.InstanceID(), "stop"))
			clk.Advance(time.Second)
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	assertCancelledRun(t, h, id)
}
func TestCaughtCancelCompletes(t *testing.T) { testCaughtCancelCompletes(t, createTestApp) }
func testCaughtCancelCompletes(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	w := DefineWorkflow("caught", func(ctx *WorkflowContext, _ string) (string, error) {
		err := Sleep(ctx, time.Hour)
		if errors.Is(err, context.Canceled) {
			return "caught", nil
		}
		return "", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
}
func TestReceiveObservesCancel(t *testing.T) { testReceiveObservesCancel(t, createTestApp) }
func testReceiveObservesCancel(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	w := DefineWorkflow("receive_cancel", func(ctx *WorkflowContext, _ string) (string, error) {
		_, err := Receive[string](ctx, "inbox")
		return "", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	assertCancelledRun(t, h, id)
}
func TestStickyFatalStopsYields(t *testing.T) { testStickyFatalStopsYields(t, createTestApp) }
func testStickyFatalStopsYields(t *testing.T, factory replayAppFactory) {
	for _, receive := range []bool{false, true} {
		t.Run(fmt.Sprint(receive), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			w := DefineWorkflow("fatal_yield", func(ctx *WorkflowContext, _ string) (string, error) {
				ctx.execCtx.Fail(ErrHistoryKeyConflict)
				var err error
				if receive {
					_, err = Receive[string](ctx, "inbox")
				} else {
					err = Sleep(ctx, time.Hour)
				}
				require.ErrorIs(t, err, ErrHistoryKeyConflict)
				return "caught", nil
			})
			id := seedErrorWorkflow(t, h, w)
			require.ErrorIs(t, h.RunOnce(id), ErrHistoryKeyConflict)
			rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, "$workflow_failed", rows[0].ActivityID)
			require.NotContains(t, fs.Calls(), "NextSignal")
		})
	}
}
func TestSleepReservationAtomicAndExpiryStable(t *testing.T) {
	testSleepReservationAtomicAndExpiryStable(t, createTestApp)
}
func testSleepReservationAtomicAndExpiryStable(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	duration := time.Hour
	w := DefineWorkflow("expiry", func(ctx *WorkflowContext, _ string) (string, error) {
		return "", Sleep(ctx, duration, WithSleepID("pause"))
	})
	id := seedErrorWorkflow(t, h, w)
	// A database failure at subscription insertion must roll back the reservation.
	_, err := fs.DB().Exec("ALTER TABLE workflow_timer_subscriptions RENAME TO hidden_timers")
	require.NoError(t, err)
	require.Error(t, h.RunOnce(id))
	ev, err := fs.GetHistoryEvent(h.app.ctx, id, "pause")
	require.NoError(t, err)
	require.Nil(t, ev)
	_, err = fs.DB().Exec("ALTER TABLE hidden_timers RENAME TO workflow_timer_subscriptions")
	require.NoError(t, err)
	// Start another run because storage errors may close the first run as failed.
	id = "expiry-second"
	require.NoError(t, fs.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: id, WorkflowName: w.Name(), Status: storage.StatusRunning, InputData: []byte(`"input"`)}))
	require.NoError(t, h.RunOnce(id))
	var expiry string
	require.NoError(t, fs.DB().QueryRow("SELECT CAST(expires_at AS TEXT) FROM workflow_timer_subscriptions WHERE instance_id=$1", id).Scan(&expiry))
	ev, err = fs.GetHistoryEvent(h.app.ctx, id, "pause")
	require.NoError(t, err)
	require.Nil(t, ev.EventData)
	var reservation string
	require.NoError(t, fs.DB().QueryRow("SELECT reservation_data FROM workflow_history WHERE instance_id=$1 AND activity_id='pause'", id).Scan(&reservation))
	require.True(t, strings.Contains(reservation, "expires_at"))
	duration = 24 * time.Hour
	require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	var replayExpiry string
	require.NoError(t, fs.DB().QueryRow("SELECT CAST(expires_at AS TEXT) FROM workflow_timer_subscriptions WHERE instance_id=$1", id).Scan(&replayExpiry))
	require.Equal(t, expiry, replayExpiry)
}

func TestCancelObservationReplays(t *testing.T) { testCancelObservationReplays(t, createTestApp) }
func testCancelObservationReplays(t *testing.T, factory replayAppFactory) {
	for _, receive := range []bool{false, true} {
		t.Run(fmt.Sprint(receive), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			observations := 0
			w := DefineWorkflow("cancel_replay", func(ctx *WorkflowContext, _ string) (string, error) {
				var err error
				if receive {
					_, err = Receive[string](ctx, "inbox")
				} else {
					err = Sleep(ctx, time.Hour, WithSleepID("pause"))
				}
				if !errors.Is(err, context.Canceled) {
					return "", err
				}
				var cancelled *WorkflowCancelledError
				require.ErrorAs(t, err, &cancelled)
				require.Equal(t, "original reason", cancelled.Reason)
				observations++
				return "caught", nil
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "original reason"))
			fs.Before = func(_ context.Context, method string) error {
				if method == "CloseInstance" && observations == 1 {
					return replay.ErrLeaseLost
				}
				return nil
			}
			require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, 2, observations)
			assertCompletedLeaseRun(t, h, id)
		})
	}
}

func TestCloseInstanceAtomic(t *testing.T) { testCloseInstanceAtomic(t, createTestApp) }
func testCloseInstanceAtomic(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "closer", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, fixtureTimer(h.app.ctx, h.app.storage, &storage.TimerSubscription{InstanceID: "lease", TimerID: "pause", ExpiresAt: time.Now()}))
	ev := &storage.HistoryEvent{InstanceID: "lease", ActivityID: "$workflow_failed", Command: "close", EventType: storage.HistoryWorkflowCancelled, EventData: encodeError(&WorkflowCancelledError{InstanceID: "lease"}), DataType: "json"}
	_, err = h.app.storage.DB().Exec("ALTER TABLE workflow_timer_subscriptions RENAME TO hidden_timers")
	require.NoError(t, err)
	require.Error(t, h.app.storage.CloseInstance(h.app.ctx, f, storage.StatusCancelled, nil, ev))
	inst, err := h.app.storage.GetInstance(h.app.ctx, "lease")
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, inst.Status)
	require.Equal(t, f.Owner, inst.LeaseOwner)
	recorded, err := h.app.storage.GetHistoryEvent(h.app.ctx, "lease", ev.ActivityID)
	require.NoError(t, err)
	require.Nil(t, recorded)
	_, err = h.app.storage.DB().Exec("ALTER TABLE hidden_timers RENAME TO workflow_timer_subscriptions")
	require.NoError(t, err)
	require.NoError(t, h.app.storage.CloseInstance(h.app.ctx, f, storage.StatusCancelled, nil, ev))
	assertCancelledRun(t, h, "lease")
	require.ErrorIs(t, h.app.storage.CloseInstance(h.app.ctx, f, storage.StatusCompleted, []byte(`"wrong"`), nil), storage.ErrFenced)
}

type cancelCommitHooks struct {
	hooks.NoOpHooks
	cancelled func(hooks.WorkflowCancelledInfo)
}

func (h *cancelCommitHooks) OnWorkflowCancelled(_ context.Context, info hooks.WorkflowCancelledInfo) {
	h.cancelled(info)
}
func TestCancelHookAfterClose(t *testing.T) { testCancelHookAfterClose(t, createTestApp) }
func testCancelHookAfterClose(t *testing.T, factory replayAppFactory) {
	calls := 0
	observed := &cancelCommitHooks{}
	h := newTypedReplayHarness(t, factory, WithHooks(observed))
	w := DefineWorkflow("cancel_hook", func(ctx *WorkflowContext, _ string) (string, error) { return "", Sleep(ctx, time.Hour) })
	id := seedErrorWorkflow(t, h, w)
	observed.cancelled = func(info hooks.WorkflowCancelledInfo) {
		calls++
		require.Equal(t, w.Name(), info.WorkflowName)
		require.Equal(t, "stop", info.Reason)
		assertCancelledRun(t, h, id)
	}
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	require.Zero(t, calls)
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	require.Equal(t, 1, calls)
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "again"))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 1, calls)
}
