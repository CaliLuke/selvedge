package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitObservationScopeCancelAtomic(t *testing.T) {
	testCommitObservationScopeCancelAtomic(t, sqliteHistoryFactory)
}
func testCommitObservationScopeCancelAtomic(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	_, err = s.ReserveTimer(ctx, f, 2, &TimerSubscription{InstanceID: "tasks", TimerID: "#2", ActivityID: "#2", ExpiresAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	_, err = s.ReserveActivity(ctx, f, 3, "#3", "activity:A", task)
	require.NoError(t, err)
	claim, err := s.ClaimActivityTask(ctx, "tasks", "#3", "worker", time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.CompleteActivityTask(ctx, claim.Claim(), HistoryActivityCompleted, []byte(`42`)))
	_, err = s.ReserveTimer(ctx, f, 4, &TimerSubscription{InstanceID: "tasks", TimerID: "#4", ActivityID: "#4", ExpiresAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	const seq int64 = 1 << 32
	c := &ObservationCommit{Seq: seq, Record: &HistoryEvent{InstanceID: "tasks", ActivityID: "cancel", Seq: seq, Command: "cancel_scope", EventType: HistoryScopeCancelled, EventData: []byte(`{"targets":3}`)}, Cancel: &ScopeCancel{Targets: []ScopeTarget{{Key: "#1", Command: "activity:A", Kind: "activity"}, {Key: "#2", Command: "sleep", Kind: "timer"}, {Key: "#3", Command: "activity:A", Kind: "activity"}}}}
	assertUntouched := func() {
		t.Helper()
		row, e := s.GetHistoryEvent(ctx, "tasks", "#1")
		require.NoError(t, e)
		require.Nil(t, row.EventData)
		var status string
		require.NoError(t, s.DB().QueryRow(`SELECT status FROM workflow_activity_tasks WHERE activity_id='#1'`).Scan(&status))
		require.Equal(t, "queued", status)
		var n int
		require.NoError(t, s.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions`).Scan(&n))
		require.Equal(t, 2, n)
	}
	tx, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = s.CommitObservation(tx, f, c)
	require.NoError(t, err)
	require.NoError(t, s.RollbackTransaction(tx))
	assertUntouched()
	conflict := *c
	record := *c.Record
	record.Seq = 4
	conflict.Seq = 4
	conflict.Record = &record
	_, err = s.CommitObservation(ctx, f, &conflict)
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	assertUntouched()
	stale := *f
	stale.Epoch++
	_, err = s.CommitObservation(ctx, &stale, c)
	require.ErrorIs(t, err, ErrFenced)
	assertUntouched()
	result, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, []string{"#1", "#2", "cancel"}, result.Delivered.HistoryKeys)
	again, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, result, again)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#3")
	require.NoError(t, err)
	require.Equal(t, []byte(`42`), row.EventData)
	require.Zero(t, row.DeliveredSeq)
	row, err = s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Equal(t, HistoryActivityCancelled, row.EventType)
	require.Equal(t, seq, row.DeliveredSeq)
	var status string
	require.NoError(t, s.DB().QueryRow(`SELECT status FROM workflow_activity_tasks WHERE activity_id='#1'`).Scan(&status))
	require.Equal(t, "cancelled", status)
	var key string
	require.NoError(t, s.DB().QueryRow(`SELECT timer_id FROM workflow_timer_subscriptions`).Scan(&key))
	require.Equal(t, "#4", key)
}

func TestScopeCancelUpdatesTaskTimestamp(t *testing.T) {
	testScopeCancelUpdatesTaskTimestamp(t, sqliteHistoryFactory)
}
func testScopeCancelUpdatesTaskTimestamp(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	_, err = s.DB().Exec(`UPDATE workflow_activity_tasks SET updated_at='2000-01-01 00:00:00' WHERE instance_id='tasks'`)
	require.NoError(t, err)
	_, err = s.CommitObservation(ctx, f, &ObservationCommit{Seq: 2, Record: &HistoryEvent{InstanceID: "tasks", ActivityID: "#2", Seq: 2, Command: "cancel_scope", EventType: HistoryScopeCancelled, EventData: []byte(`{"targets":1}`)}, Cancel: &ScopeCancel{Targets: []ScopeTarget{{Key: "#1", Command: "activity:A", Kind: "activity"}}}})
	require.NoError(t, err)
	var refreshed bool
	require.NoError(t, s.DB().QueryRow(`SELECT updated_at > '2000-01-01 00:00:00' FROM workflow_activity_tasks WHERE instance_id='tasks' AND activity_id='#1'`).Scan(&refreshed))
	require.True(t, refreshed, "scope cancellation must refresh the task timestamp")
}
