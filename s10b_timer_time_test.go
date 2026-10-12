package selvedge

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestTimerFiresAfterNowAndNowAfterFire(t *testing.T) {
	testTimerFiresAfterNowAndNowAfterFire(t, createTestApp)
}

func testTimerFiresAfterNowAndNowAfterFire(t *testing.T, factory replayAppFactory) {
	t.Run("poller_precision", func(t *testing.T) { testTimerPollerPrecision(t, factory) })
	for _, kind := range []string{"sleep", "get", "until", "wall_get"} {
		for _, duration := range []time.Duration{time.Hour, 0, -time.Hour} {
			t.Run(fmt.Sprintf("expiry/%s/%s", kind, duration), func(t *testing.T) {
				testTimerWorkflowExpiry(t, factory, kind, duration)
			})
		}
	}
	for _, kind := range []string{"sleep", "get"} {
		t.Run("observed_floor/"+kind, func(t *testing.T) {
			testObservedTimerFloor(t, factory, kind)
		})
	}
}

func testTimerWorkflowExpiry(t *testing.T, factory replayAppFactory, kind string, duration time.Duration) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	first := time.Date(2040, 1, 2, 3, 4, 5, 123456789, time.UTC)
	clock := first
	WithClock(func() time.Time {
		value := clock
		clock = clock.Add(time.Second) // SleepUntil must use the same sample for base and duration.
		return value
	})(h.app.config)
	base := first
	if kind == "wall_get" {
		base = first.Add(time.Hour)
	}
	expected := base.Add(duration)
	var values []time.Time
	w := DefineWorkflow("timer_expiry", func(ctx *WorkflowContext, _ string) (string, error) {
		if _, err := Now(ctx); err != nil {
			return "", err
		}
		clock = first.Add(-time.Hour)
		if kind == "wall_get" {
			clock = base
		}
		copyCtx := ctx.WithContext(ctx.Context())
		var err error
		switch kind {
		case "sleep":
			err = Sleep(copyCtx, duration, WithSleepID("timer"))
		case "until":
			err = SleepUntil(copyCtx, expected, WithSleepID("timer"))
		default:
			var timer *Timer
			timer, err = NewTimer(copyCtx, duration, WithSleepID("timer"))
			if err == nil {
				var fired time.Time
				fired, err = timer.Get(ctx)
				values = append(values, fired)
			}
		}
		if err != nil {
			return "", err
		}
		return "", Sleep(ctx, time.Hour, WithSleepID("park"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "timer")
	require.NoError(t, err)
	require.NotNil(t, row)
	var expiry storage.SleepReservation
	if duration > 0 {
		require.Nil(t, row.EventData)
		require.NoError(t, json.Unmarshal(row.ReservationData, &expiry))
	} else {
		require.Equal(t, storage.HistoryTimerExpired, row.EventType)
		require.NoError(t, json.Unmarshal(row.EventData, &expiry))
	}
	require.Equal(t, expected, expiry.ExpiresAt, "expiry must use workflow time plus duration")
	originalReservation := append([]byte(nil), row.ReservationData...)
	clock = first.Add(-2 * time.Hour)
	if duration > 0 {
		// Re-arm the reservation on replay without changing its deadline.
		_, err = fs.DB().Exec(`DELETE FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id='timer'`, id)
		require.NoError(t, err)
		require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
		require.NoError(t, h.RunOnce(id))
		require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "timer", ExpiresAt: expiry.ExpiresAt}))
	}
	require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	row, err = fs.GetHistoryEvent(h.app.ctx, id, "timer")
	require.NoError(t, err)
	require.Equal(t, originalReservation, row.ReservationData)
	require.NoError(t, json.Unmarshal(row.EventData, &expiry))
	require.Equal(t, expected, expiry.ExpiresAt)
	if kind == "get" || kind == "wall_get" {
		require.NotEmpty(t, values)
		require.Equal(t, expected, values[len(values)-1])
	}
}

func testObservedTimerFloor(t *testing.T, factory replayAppFactory, kind string) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	before := time.Date(2040, 1, 2, 3, 4, 5, 0, time.UTC)
	fired := before.Add(time.Hour)
	clock := before
	WithClock(func() time.Time { return clock })(h.app.config)
	var afterFire, afterReplay []time.Time
	var cached []time.Time
	w := DefineWorkflow("timer_floor", func(ctx *WorkflowContext, _ string) (string, error) {
		if _, err := Now(ctx); err != nil {
			return "", err
		}
		copyCtx := ctx.WithContext(ctx.Context())
		if kind == "sleep" {
			if err := Sleep(copyCtx, time.Hour, WithSleepID("timer")); err != nil {
				return "", err
			}
		} else {
			timer, err := NewTimer(copyCtx, time.Hour, WithSleepID("timer"))
			if err != nil {
				return "", err
			}
			value, err := timer.Get(copyCtx)
			if err != nil {
				return "", err
			}
			cached = append(cached, value)
			value, err = timer.Get(ctx)
			if err != nil {
				return "", err
			}
			cached = append(cached, value)
		}
		value, err := Now(ctx)
		if err != nil {
			return "", err
		}
		afterFire = append(afterFire, value)
		if _, err := Receive[string](ctx, "go"); err != nil {
			return "", err
		}
		value, err = Now(copyCtx)
		afterReplay = append(afterReplay, value)
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	// Fire the reserved timer before replaying with a backward clock.
	require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "timer", ExpiresAt: fired}))
	clock = before.Add(-time.Hour)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []time.Time{fired}, afterFire, "Now must include a fire observed by Sleep or Get")
	require.NoError(t, h.app.Signal(h.app.ctx, id, "go", "go"))
	clock = before.Add(-2 * time.Hour)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []time.Time{fired, fired}, afterFire)
	require.Equal(t, []time.Time{fired}, afterReplay)
	if kind == "get" {
		require.Equal(t, []time.Time{fired, fired, fired, fired}, cached)
	}
	assertCompletedLeaseRun(t, h, id)
}

func testTimerPollerPrecision(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	before := time.Date(2000, 1, 2, 3, 4, 5, 123456789, time.UTC)
	clock := before
	WithClock(func() time.Time { return clock })(h.app.config)
	var fires, times []time.Time
	w := DefineWorkflow("timer_precision", func(ctx *WorkflowContext, _ string) (string, error) {
		if _, err := Now(ctx); err != nil {
			return "", err
		}
		timer, err := NewTimer(ctx, time.Hour)
		if err != nil {
			return "", err
		}
		fired, err := timer.Get(ctx)
		if err != nil {
			return "", err
		}
		fires = append(fires, fired)
		now, err := Now(ctx)
		if err != nil {
			return "", err
		}
		times = append(times, now)
		_, err = Receive[string](ctx, "go")
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	timers, err := fs.FindExpiredTimers(h.app.ctx, 10)
	require.NoError(t, err)
	require.Len(t, timers, 1)
	require.Equal(t, before.Add(time.Hour).Truncate(time.Microsecond), timers[0].ExpiresAt.UTC())
	require.NoError(t, fs.FireTimer(h.app.ctx, timers[0]))
	clock = before.Add(-time.Hour)
	require.NoError(t, h.RunOnce(id))
	require.Len(t, fires, 1)
	require.False(t, fires[0].Before(before.Add(time.Hour)), "fire lost nanosecond precision: fired=%s, before+d=%s", fires[0].Format(time.RFC3339Nano), before.Add(time.Hour).Format(time.RFC3339Nano))
	require.Equal(t, []time.Time{before.Add(time.Hour)}, fires)
	require.Equal(t, fires, times)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "go", "go"))
	clock = before.Add(-2 * time.Hour)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []time.Time{before.Add(time.Hour), before.Add(time.Hour)}, fires)
	require.Equal(t, fires, times)
	assertCompletedLeaseRun(t, h, id)
}
