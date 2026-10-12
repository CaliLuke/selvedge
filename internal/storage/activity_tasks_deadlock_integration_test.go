//go:build integration

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Hold the close transaction's instance lock, then wait until completion is
// blocked by that transaction. With task-first completion, closing now forms
// instance -> task -> instance; with instance-first completion it can finish.
func TestPostgresActivityCompleteConcurrentClose(t *testing.T) {
	s, f, task := taskFixture(t, postgresHistoryFactory)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	claimed, err := s.ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	closing, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(closing) }()
	var closingPID int
	require.NoError(t, s.Conn(closing).QueryRowContext(closing, "SELECT pg_backend_pid()").Scan(&closingPID))
	// This is exactly the guard that CloseInstance takes before its writes.
	require.NoError(t, (fenceStore{s}).withFence(closing, f, func(Executor) error { return nil }))

	completing, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(completing) }()
	var completingPID int
	require.NoError(t, s.Conn(completing).QueryRowContext(completing, "SELECT pg_backend_pid()").Scan(&completingPID))
	done := make(chan error, 1)
	go func() {
		err := s.CompleteActivityTask(completing, claimed.Claim(), HistoryActivityCompleted, []byte(`"late"`))
		// The caller owns this transaction and must roll back ErrFenced writes.
		if err != nil {
			_ = s.RollbackTransaction(completing)
		} else {
			err = s.CommitTransaction(completing)
		}
		done <- err
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := s.DB().QueryRowContext(ctx, "SELECT $1 = ANY(pg_blocking_pids($2))", closingPID, completingPID).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond, "completion must reach the close transaction's instance lock")

	require.NoError(t, s.CloseInstance(closing, *f, StatusFailed, nil, nil))
	require.NoError(t, s.CommitTransaction(closing))
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrFenced)
	case <-ctx.Done():
		t.Fatal("completion did not finish after close committed")
	}
	var status string
	require.NoError(t, s.DB().QueryRowContext(ctx, "SELECT status FROM workflow_activity_tasks WHERE instance_id='tasks'").Scan(&status))
	require.Equal(t, "cancelled", status)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Nil(t, row.EventData)
	instance, err := s.GetInstance(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, StatusFailed, instance.Status)
	require.False(t, instance.Runnable)
}
