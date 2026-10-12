package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryActivityTaskFencing(t *testing.T) {
	testRetryActivityTaskFencing(t, sqliteHistoryFactory)
}
func testRetryActivityTaskFencing(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.DB().Exec("CREATE TABLE selvedge_test_clock(now_ms BIGINT NOT NULL)")
	require.NoError(t, err)
	_, err = s.DB().Exec("INSERT INTO selvedge_test_clock VALUES(1000)")
	require.NoError(t, err)
	WithTestStoreClock()(s)
	task.Queue = "a"
	task.Options = []byte(`{"hello":"recorded"}`)
	task.MaxAttempts = 2
	_, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	first, err := s.ClaimActivityTask(ctx, "tasks", "#1", "one", time.Second, []string{"a"})
	require.NoError(t, err)
	require.NotNil(t, first)
	stale := first.Claim()
	stale.Epoch++
	require.ErrorIs(t, s.RetryActivityTask(ctx, stale, []byte(`"bad"`), time.Second), ErrFenced)
	require.NoError(t, s.RetryActivityTask(ctx, first.Claim(), []byte(`"transient"`), time.Second))
	require.ErrorIs(t, s.RetryActivityTask(ctx, first.Claim(), nil, time.Second), ErrFenced)
	rows, err := s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "queued", rows[0].Status)
	require.Equal(t, 1, rows[0].Failures)
	require.Equal(t, int64(2000), rows[0].NotBeforeMS)
	require.JSONEq(t, `"transient"`, string(rows[0].LastError))
	require.Empty(t, rows[0].ClaimOwner)
	hist, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Nil(t, hist.EventData)
	inst, err := s.GetInstance(ctx, "tasks")
	require.NoError(t, err)
	require.False(t, inst.Runnable)
	delay, ok, err := s.NextActivityDue(ctx, TaskFilter{Names: []string{"A"}, Queues: []string{"a"}})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, time.Second, delay)
	_, err = s.DB().Exec("UPDATE selvedge_test_clock SET now_ms=1999")
	require.NoError(t, err)
	next, err := s.ClaimActivityTask(ctx, "tasks", "#1", "two", time.Second, nil)
	require.NoError(t, err)
	require.Nil(t, next)
	_, err = s.DB().Exec("UPDATE selvedge_test_clock SET now_ms=2000")
	require.NoError(t, err)
	next, err = s.ClaimActivityTask(ctx, "tasks", "#1", "two", time.Second, []string{"b"})
	require.NoError(t, err)
	require.Nil(t, next)
	next, err = s.ClaimActivityTask(ctx, "tasks", "#1", "two", time.Second, []string{"a"})
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Equal(t, 2, next.Attempts)
	require.Equal(t, 1, next.Failures)
	require.NoError(t, s.ReleaseActivityTask(ctx, next.Claim()))
	next, err = s.ClaimActivityTask(ctx, "tasks", "#1", "three", time.Second, nil)
	require.NoError(t, err)
	require.Equal(t, 3, next.Attempts)
	require.Equal(t, 1, next.Failures)
	_, err = s.DB().Exec("UPDATE selvedge_test_clock SET now_ms=3001")
	require.NoError(t, err)
	next, err = s.ClaimActivityTask(ctx, "tasks", "#1", "four", time.Second, nil)
	require.NoError(t, err)
	require.Nil(t, next, "exhausted lapse cannot be reclaimed")
}
func TestClaimEligibility(t *testing.T) { testClaimEligibility(t, sqliteHistoryFactory) }
func testClaimEligibility(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	task.Queue = "a"
	task.Options = []byte(`{}`)
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	for _, filter := range []TaskFilter{{}, {Names: []string{"B"}}, {Names: []string{"A"}, Queues: []string{}}, {Names: []string{"A"}, Queues: []string{"b"}}} {
		got, err := s.ClaimActivityTasks(ctx, "worker", time.Second, 1, filter)
		require.NoError(t, err)
		require.Empty(t, got)
	}
	got, err := s.ClaimActivityTasks(ctx, "worker", time.Second, 1, TaskFilter{Names: []string{"A"}, Queues: []string{"a"}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	task.Queue = "b"
	task.Options = []byte(`{"changed":true}`)
	_, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	rows, err := s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, "a", rows[0].Queue)
	require.JSONEq(t, `{}`, string(rows[0].Options))
}

func TestRetryBackoffRoundsUp(t *testing.T) { testRetryBackoffRoundsUp(t, sqliteHistoryFactory) }
func testRetryBackoffRoundsUp(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.DB().Exec("CREATE TABLE selvedge_test_clock(now_ms BIGINT NOT NULL)")
	require.NoError(t, err)
	_, err = s.DB().Exec("INSERT INTO selvedge_test_clock VALUES(1000)")
	require.NoError(t, err)
	WithTestStoreClock()(s)
	_, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	first, err := s.ClaimActivityTask(ctx, "tasks", "#1", "one", time.Second, nil)
	require.NoError(t, err)
	require.NoError(t, s.RetryActivityTask(ctx, first.Claim(), []byte(`"retry"`), time.Nanosecond))
	next, err := s.ClaimActivityTask(ctx, "tasks", "#1", "two", time.Second, nil)
	require.NoError(t, err)
	require.Nil(t, next, "positive backoff must not round down to an immediate retry")
}
