package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func taskFixture(t *testing.T, factory historyFactory) (Storage, *Fence, *ActivityTask) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "tasks", WorkflowName: "test", Status: StatusRunning}))
	f := fixtureFence(t, s, "tasks")
	return s, f, &ActivityTask{InstanceID: "tasks", ActivityID: "#1", Seq: 1, ActivityName: "A", InputData: []byte(`"input"`)}
}
func TestReserveActivityAtomic(t *testing.T) { testReserveActivityAtomic(t, sqliteHistoryFactory) }
func testReserveActivityAtomic(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	tx, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = s.ReserveActivity(tx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	require.NoError(t, s.RollbackTransaction(tx))
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Nil(t, row)
	var n int
	require.NoError(t, s.DB().QueryRow("SELECT COUNT(*) FROM workflow_activity_tasks").Scan(&n))
	require.Zero(t, n)
	row, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	require.Nil(t, row.EventData)
	again, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	require.Equal(t, row.ID, again.ID)
	require.NoError(t, s.DB().QueryRow("SELECT COUNT(*) FROM workflow_activity_tasks").Scan(&n))
	require.Equal(t, 1, n)
	_, err = s.ReserveActivity(ctx, f, 1, "conflict", "activity:B", &ActivityTask{InstanceID: "tasks", ActivityName: "B", InputData: []byte(`null`)})
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	require.NoError(t, s.DB().QueryRow("SELECT COUNT(*) FROM workflow_activity_tasks").Scan(&n))
	require.Equal(t, 1, n)
	bad := *f
	bad.Epoch++
	_, err = s.ReserveActivity(ctx, &bad, 2, "#2", "activity:A", task)
	require.ErrorIs(t, err, ErrFenced)
}
func TestActivityTaskClaimFencing(t *testing.T) {
	testActivityTaskClaimFencing(t, sqliteHistoryFactory)
}
func testActivityTaskClaimFencing(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	rows, err := s.ClaimActivityTasks(ctx, "worker", time.Hour, 10, "B")
	require.NoError(t, err)
	require.Empty(t, rows)
	rows, err = s.ClaimActivityTasks(ctx, "worker", time.Hour, 10, "A")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	claim := rows[0].Claim()
	require.Equal(t, 1, rows[0].Attempts)
	stale := claim
	stale.Epoch++
	require.ErrorIs(t, s.RenewActivityTask(ctx, stale, time.Hour), ErrFenced)
	require.ErrorIs(t, s.CompleteActivityTask(ctx, stale, HistoryActivityCompleted, []byte(`"stale"`)), ErrFenced)
	require.ErrorIs(t, s.ReleaseActivityTask(ctx, stale), ErrFenced)
	require.NoError(t, s.RenewActivityTask(ctx, claim, time.Hour))
	require.NoError(t, s.CompleteActivityTask(ctx, claim, HistoryActivityCompleted, []byte(`"winner"`)))
	require.ErrorIs(t, s.CompleteActivityTask(ctx, claim, HistoryActivityCompleted, []byte(`"late"`)), ErrFenced)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.JSONEq(t, `"winner"`, string(row.EventData))
	inst, err := s.GetInstance(ctx, "tasks")
	require.NoError(t, err)
	require.True(t, inst.Runnable)
	claimed, err := s.ClaimActivityTask(ctx, "tasks", "#1", "again", time.Hour)
	require.NoError(t, err)
	require.Nil(t, claimed)
	_, err = s.DB().Exec("UPDATE workflow_activity_tasks SET status='cancelled'")
	require.NoError(t, err)
	claimed, err = s.ClaimActivityTask(ctx, "tasks", "#1", "again", time.Hour)
	require.NoError(t, err)
	require.Nil(t, claimed)
}
func TestActivityClaimExpiryIsStrict(t *testing.T) {
	testActivityClaimExpiryIsStrict(t, sqliteHistoryFactory)
}
func testActivityClaimExpiryIsStrict(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.DB().Exec("CREATE TABLE selvedge_test_clock (now_ms BIGINT NOT NULL)")
	require.NoError(t, err)
	_, err = s.DB().Exec("INSERT INTO selvedge_test_clock VALUES(1000)")
	require.NoError(t, err)
	WithTestStoreClock()(s)
	_, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	first, err := s.ClaimActivityTask(ctx, "tasks", "#1", "one", time.Second)
	require.NoError(t, err)
	require.NotNil(t, first)
	_, err = s.DB().Exec("UPDATE selvedge_test_clock SET now_ms=2000")
	require.NoError(t, err)
	second, err := s.ClaimActivityTask(ctx, "tasks", "#1", "two", time.Second)
	require.NoError(t, err)
	require.Nil(t, second)
	_, err = s.DB().Exec("UPDATE selvedge_test_clock SET now_ms=2001")
	require.NoError(t, err)
	require.ErrorIs(t, s.RenewActivityTask(ctx, first.Claim(), time.Second), ErrFenced)
	second, err = s.ClaimActivityTask(ctx, "tasks", "#1", "two", time.Second)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, 2, second.Attempts)
	require.ErrorIs(t, s.CompleteActivityTask(ctx, first.Claim(), HistoryActivityCompleted, []byte(`1`)), ErrFenced)
	require.NoError(t, s.ReleaseActivityTask(ctx, second.Claim()))
	third, err := s.ClaimActivityTask(ctx, "tasks", "#1", "three", time.Second)
	require.NoError(t, err)
	require.Equal(t, 3, third.Attempts)
}
func TestActivityFillRequiresClaim(t *testing.T) {
	testActivityFillRequiresClaim(t, sqliteHistoryFactory)
}
func testActivityFillRequiresClaim(t *testing.T, factory historyFactory) {
	s, f, _ := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.ReserveHistory(ctx, f, "tasks", "#1", "activity:A", 1)
	require.NoError(t, err)
	_, err = s.SetHistoryValueIfNull(ctx, nil, "tasks", "#1", "activity:A", HistoryActivityCompleted, []byte(`true`))
	require.ErrorIs(t, err, ErrFenced)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Nil(t, row.EventData)
}
func TestCloseCancelsInFlightTasks(t *testing.T) {
	testCloseCancelsInFlightTasks(t, sqliteHistoryFactory)
}
func testCloseCancelsInFlightTasks(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	running, err := s.ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Hour)
	require.NoError(t, err)
	_, err = s.ReserveActivity(ctx, f, 2, "#2", "activity:A", task)
	require.NoError(t, err)
	require.NoError(t, s.CloseInstance(ctx, *f, StatusCompleted, []byte(`null`), nil))
	var n int
	require.NoError(t, s.DB().QueryRow("SELECT COUNT(*) FROM workflow_activity_tasks WHERE status='cancelled'").Scan(&n))
	require.Equal(t, 2, n)
	require.ErrorIs(t, s.RenewActivityTask(ctx, running.Claim(), time.Hour), ErrFenced)
	require.ErrorIs(t, s.CompleteActivityTask(ctx, running.Claim(), HistoryActivityCompleted, []byte(`true`)), ErrFenced)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Nil(t, row.EventData)
}
