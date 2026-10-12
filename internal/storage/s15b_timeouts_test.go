package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func s15bTask(t *testing.T, factory historyFactory) (Storage, *Fence, *ActivityTask) {
	t.Helper()
	s, f, task := taskFixture(t, factory)
	_, err := s.DB().Exec("CREATE TABLE selvedge_test_clock(now_ms BIGINT NOT NULL)")
	require.NoError(t, err)
	_, err = s.DB().Exec("INSERT INTO selvedge_test_clock VALUES(1000)")
	require.NoError(t, err)
	WithTestStoreClock()(s)
	return s, f, task
}
func s15bClock(t *testing.T, s Storage, now int64) {
	t.Helper()
	_, err := s.DB().Exec("UPDATE selvedge_test_clock SET now_ms=$1", now)
	require.NoError(t, err)
}
func TestScheduleToStartDeadlineStrict(t *testing.T) {
	testScheduleToStartDeadlineStrict(t, sqliteHistoryFactory)
}
func testScheduleToStartDeadlineStrict(t *testing.T, factory historyFactory) {
	for _, now := range []int64{1100, 1101} {
		t.Run(time.Duration(now).String(), func(t *testing.T) {
			s, f, task := s15bTask(t, factory)
			ctx := context.Background()
			task.ScheduleToStartMS = 100
			_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
			require.NoError(t, err)
			s15bClock(t, s, now)
			timed, err := s.FindActivityTimeouts(ctx, 10)
			require.NoError(t, err)
			claimed, err := s.ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Second, nil)
			require.NoError(t, err)
			if now == 1100 {
				require.Empty(t, timed)
				require.NotNil(t, claimed)
			} else {
				require.Len(t, timed, 1)
				require.Equal(t, "schedule_to_start", timed[0].Kind)
				require.Nil(t, claimed)
			}
		})
	}
}
func TestActivityDeadlineFencesWrites(t *testing.T) {
	testActivityDeadlineFencesWrites(t, sqliteHistoryFactory)
}
func testActivityDeadlineFencesWrites(t *testing.T, factory historyFactory) {
	for _, kind := range []string{"complete", "retry", "release", "renew"} {
		t.Run(kind, func(t *testing.T) {
			s, f, task := s15bTask(t, factory)
			ctx := context.Background()
			task.StartToCloseMS = 100
			task.HeartbeatTimeoutMS = 30
			_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
			require.NoError(t, err)
			claimed, err := s.ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Second, nil)
			require.NoError(t, err)
			require.Equal(t, int64(1030), *claimed.ClaimExpiresMS)
			require.Equal(t, int64(1100), *claimed.AttemptDeadlineMS)
			s15bClock(t, s, 1030)
			require.NoError(t, s.RenewActivityTask(ctx, claimed.Claim(), time.Second, false))
			rows, err := s.ListActivityTasks(ctx, "tasks")
			require.NoError(t, err)
			require.Equal(t, int64(1030), *rows[0].ClaimExpiresMS)
			require.NoError(t, s.RenewActivityTask(ctx, claimed.Claim(), time.Second, true))
			s15bClock(t, s, 1060)
			require.NoError(t, s.RenewActivityTask(ctx, claimed.Claim(), time.Second, true))
			s15bClock(t, s, 1090)
			require.NoError(t, s.RenewActivityTask(ctx, claimed.Claim(), time.Second, true))
			rows, err = s.ListActivityTasks(ctx, "tasks")
			require.NoError(t, err)
			require.Equal(t, int64(1100), *rows[0].ClaimExpiresMS)
			s15bClock(t, s, 1101)
			switch kind {
			case "complete":
				err = s.CompleteActivityTask(ctx, claimed.Claim(), HistoryActivityCompleted, []byte(`"late"`))
			case "retry":
				err = s.RetryActivityTask(ctx, claimed.Claim(), []byte(`"late"`), time.Second)
			case "release":
				err = s.ReleaseActivityTask(ctx, claimed.Claim())
			case "renew":
				err = s.RenewActivityTask(ctx, claimed.Claim(), time.Second, true)
			}
			require.ErrorIs(t, err, ErrFenced)
			row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
			require.NoError(t, err)
			require.Nil(t, row.EventData)
		})
	}
}
func TestTimeoutActivityTaskAtomic(t *testing.T) {
	testTimeoutActivityTaskAtomic(t, sqliteHistoryFactory)
}
func testTimeoutActivityTaskAtomic(t *testing.T, factory historyFactory) {
	for _, kind := range []string{"queued", "exhausted", "lapse_schedule", "stale_epoch", "cancelled", "clock_back"} {
		t.Run(kind, func(t *testing.T) {
			s, f, task := s15bTask(t, factory)
			ctx := context.Background()
			task.ScheduleToStartMS = 100
			task.StartToCloseMS = 50
			task.MaxAttempts = 1
			_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
			require.NoError(t, err)
			if kind == "exhausted" || kind == "lapse_schedule" || kind == "stale_epoch" {
				if kind != "exhausted" {
					_, err = s.DB().Exec("UPDATE workflow_activity_tasks SET max_attempts=0")
					require.NoError(t, err)
				}
				_, err = s.ClaimActivityTask(ctx, "tasks", "#1", "first", time.Second, nil)
				require.NoError(t, err)
			}
			s15bClock(t, s, 1200)
			timed, err := s.FindActivityTimeouts(ctx, 10)
			require.NoError(t, err)
			require.Len(t, timed, 1)
			candidate := timed[0]
			switch kind {
			case "stale_epoch":
				s15bClock(t, s, 1100)
				claimed, err := s.ClaimActivityTask(ctx, "tasks", "#1", "second", time.Second, nil)
				require.NoError(t, err)
				require.NotNil(t, claimed)
				s15bClock(t, s, 1300)
			case "cancelled":
				_, err = s.DB().Exec("UPDATE workflow_activity_tasks SET status='cancelled'")
				require.NoError(t, err)
			case "clock_back":
				s15bClock(t, s, 1100)
			}
			err = s.TimeoutActivityTask(ctx, candidate, []byte(`"timeout"`))
			if kind == "stale_epoch" || kind == "cancelled" || kind == "clock_back" {
				require.ErrorIs(t, err, ErrFenced)
				inst, e := s.GetInstance(ctx, "tasks")
				require.NoError(t, e)
				require.False(t, inst.Runnable)
				return
			}
			require.NoError(t, err)
			require.ErrorIs(t, s.TimeoutActivityTask(ctx, candidate, []byte(`"second"`)), ErrFenced)
			hist, err := s.GetHistoryEvent(ctx, "tasks", "#1")
			require.NoError(t, err)
			require.Equal(t, HistoryActivityFailed, hist.EventType)
			require.JSONEq(t, `"timeout"`, string(hist.EventData))
			inst, err := s.GetInstance(ctx, "tasks")
			require.NoError(t, err)
			require.Equal(t, StatusRunning, inst.Status)
			require.True(t, inst.Runnable)
			if kind == "exhausted" {
				require.True(t, candidate.Exhausted)
				require.Equal(t, "start_to_close", candidate.Kind)
				rows, err := s.ListActivityTasks(ctx, "tasks")
				require.NoError(t, err)
				require.Equal(t, 1, rows[0].Failures)
			}
		})
	}
}

func TestActivityScheduleDeadlineReset(t *testing.T) {
	testActivityScheduleDeadlineReset(t, sqliteHistoryFactory)
}
func testActivityScheduleDeadlineReset(t *testing.T, factory historyFactory) {
	s, f, task := s15bTask(t, factory)
	ctx := context.Background()
	task.ScheduleToStartMS = 100
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	claim, err := s.ClaimActivityTask(ctx, "tasks", "#1", "one", time.Second, nil)
	require.NoError(t, err)
	s15bClock(t, s, 1050)
	require.NoError(t, s.RetryActivityTask(ctx, claim.Claim(), []byte(`"retry"`), time.Second))
	rows, err := s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, int64(2050), rows[0].NotBeforeMS)
	require.Equal(t, int64(2150), *rows[0].ScheduleDeadlineMS)
	s15bClock(t, s, 2100)
	claim, err = s.ClaimActivityTask(ctx, "tasks", "#1", "two", time.Second, nil)
	require.NoError(t, err)
	require.NotNil(t, claim)
	s15bClock(t, s, 2125)
	require.NoError(t, s.ReleaseActivityTask(ctx, claim.Claim()))
	rows, err = s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, int64(2225), *rows[0].ScheduleDeadlineMS)
	s15bClock(t, s, 2226)
	_, due, err := s.NextActivityDue(ctx, TaskFilter{Names: []string{"A"}})
	require.NoError(t, err)
	require.False(t, due, "expired queued tasks must not cause a zero-delay pool spin")
}
