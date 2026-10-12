package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCancelActivityAtYieldAtomic(t *testing.T) {
	testCancelActivityAtYieldAtomic(t, sqliteHistoryFactory)
}
func testCancelActivityAtYieldAtomic(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	claim, err := s.ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Hour, nil)
	require.NoError(t, err)
	require.NotNil(t, claim)
	stale := *f
	stale.Epoch++
	_, err = s.CancelActivityAtYield(ctx, &stale, "tasks", "#1", "activity:A", []byte(`"cancel"`))
	require.ErrorIs(t, err, ErrFenced)
	_, err = s.CancelActivityAtYield(ctx, nil, "tasks", "#1", "activity:A", []byte(`"cancel"`))
	require.ErrorIs(t, err, ErrFenced)
	_, err = s.CancelActivityAtYield(ctx, f, "tasks", "#1", "activity:B", []byte(`"cancel"`))
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	tx, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = s.CancelActivityAtYield(tx, f, "tasks", "#1", "activity:A", []byte(`"cancel"`))
	require.NoError(t, err)
	require.NoError(t, s.RollbackTransaction(tx))
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Nil(t, row.EventData)
	require.NoError(t, s.RenewActivityTask(ctx, claim.Claim(), time.Hour, true))
	row, err = s.CancelActivityAtYield(ctx, f, "tasks", "#1", "activity:A", []byte(`"cancel"`))
	require.NoError(t, err)
	require.Equal(t, HistoryActivityCancelled, row.EventType)
	require.ErrorIs(t, s.CompleteActivityTask(ctx, claim.Claim(), HistoryActivityCompleted, []byte(`"late"`)), ErrFenced)
	require.ErrorIs(t, s.RenewActivityTask(ctx, claim.Claim(), time.Hour, true), ErrFenced)
	var status string
	require.NoError(t, s.DB().QueryRow("SELECT status FROM workflow_activity_tasks WHERE instance_id='tasks'").Scan(&status))
	require.Equal(t, "cancelled", status)
}

func TestCancelActivityAtYieldVersusCompletion(t *testing.T) {
	testCancelActivityAtYieldVersusCompletion(t, sqliteHistoryFactory)
}
func testCancelActivityAtYieldVersusCompletion(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	for i := range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		key := fmt.Sprintf("#%d", i+1)
		_, err := s.ReserveActivity(ctx, f, int64(i+1), key, "activity:A", task)
		require.NoError(t, err)
		claim, err := s.ClaimActivityTask(ctx, "tasks", key, "worker", time.Hour, nil)
		require.NoError(t, err)
		require.NotNil(t, claim)
		start := make(chan struct{})
		done := make(chan error, 2)
		go func() {
			<-start
			_, e := s.CancelActivityAtYield(ctx, f, "tasks", key, "activity:A", []byte(`"cancel"`))
			done <- e
		}()
		go func() {
			<-start
			done <- s.CompleteActivityTask(ctx, claim.Claim(), HistoryActivityCompleted, []byte(`"result"`))
		}()
		close(start)
		for range 2 {
			err = <-done
			require.True(t, err == nil || errors.Is(err, ErrFenced), "unexpected error: %v", err)
		}
		row, err := s.GetHistoryEvent(ctx, "tasks", key)
		require.NoError(t, err)
		var status string
		require.NoError(t, s.DB().QueryRow("SELECT status FROM workflow_activity_tasks WHERE instance_id='tasks' AND activity_id=$1", key).Scan(&status))
		switch row.EventType {
		case HistoryActivityCompleted:
			require.Equal(t, "completed", status)
			require.JSONEq(t, `"result"`, string(row.EventData))
		case HistoryActivityCancelled:
			require.Equal(t, "cancelled", status)
			require.JSONEq(t, `"cancel"`, string(row.EventData))
		default:
			t.Fatalf("no outcome: %v", row.EventType)
		}
		cancel()
	}
}
