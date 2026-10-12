package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// claimResultStore interrupts the real database result at the acquisition boundary.
type claimResultStore struct {
	Storage
	mode   string
	cancel context.CancelFunc
	read   int
}
type claimResultExecutor struct {
	Executor
	store *claimResultStore
}

func TestActivityClaimAcquisitionAtomic(t *testing.T) {
	testActivityClaimAcquisitionAtomic(t, sqliteHistoryFactory)
}
func testActivityClaimAcquisitionAtomic(t *testing.T, factory historyFactory) {
	for _, single := range []bool{true, false} {
		for _, mode := range []string{"cancel_before_read", "cancel_after_read", "query_error", "scan_error"} {
			t.Run(fmt.Sprintf("single=%t/%s", single, mode), func(t *testing.T) {
				s, f, task := taskFixture(t, factory)
				task.MaxAttempts = 1
				task.StartToCloseMS = 10000
				count := 1
				if !single {
					count = 2
				}
				for i := 1; i <= count; i++ {
					_, err := s.ReserveActivity(t.Context(), f, int64(i), fmt.Sprintf("#%d", i), "activity:A", task)
					require.NoError(t, err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				wrapped := &claimResultStore{Storage: s, mode: mode, cancel: cancel}
				store := taskStore{wrapped, claimTestDriver(s)}
				if single {
					got, err := store.ClaimActivityTask(ctx, "tasks", "#1", "interrupted", time.Minute, nil)
					require.Error(t, err)
					require.Nil(t, got)
				} else {
					got, err := store.ClaimActivityTasks(ctx, "interrupted", time.Minute, 2, TaskFilter{Names: []string{"A"}})
					require.Error(t, err)
					require.Empty(t, got, "no partial claim batch may escape on error")
				}
				if mode == "cancel_after_read" {
					require.Equal(t, 1, wrapped.read)
				}
				assertUnclaimedTasks(t, s, count)
				got, err := s.ClaimActivityTasks(t.Context(), "replacement", time.Minute, count, TaskFilter{Names: []string{"A"}})
				require.NoError(t, err)
				require.Len(t, got, count)
				for _, claimed := range got {
					require.Equal(t, 1, claimed.Attempts)
					require.Zero(t, claimed.Failures)
					require.NoError(t, s.CompleteActivityTask(t.Context(), claimed.Claim(), HistoryActivityCompleted, []byte(`"done"`)))
				}
			})
		}
	}
}
func TestActivityClaimCallerTransaction(t *testing.T) {
	testActivityClaimCallerTransaction(t, sqliteHistoryFactory)
}
func testActivityClaimCallerTransaction(t *testing.T, factory historyFactory) {
	for _, mode := range []string{"query_error", "scan_error", "cancel_before_read", "success"} {
		t.Run(mode, func(t *testing.T) {
			s, f, task := taskFixture(t, factory)
			_, err := s.ReserveActivity(t.Context(), f, 1, "#1", "activity:A", task)
			require.NoError(t, err)
			tx, err := s.BeginTransaction(t.Context())
			require.NoError(t, err)
			defer func() {
				err := s.RollbackTransaction(tx)
				require.True(t, err == nil || errors.Is(err, sql.ErrTxDone))
			}()
			_, err = s.Conn(tx).ExecContext(tx, "UPDATE workflow_instances SET cancel_reason='outer change' WHERE instance_id='tasks'")
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(tx)
			defer cancel()
			wrapped := &claimResultStore{Storage: s, mode: mode, cancel: cancel}
			got, err := (taskStore{wrapped, claimTestDriver(s)}).ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Minute, nil)
			if mode == "success" {
				require.NoError(t, err)
				require.NotNil(t, got)
				require.NoError(t, s.RollbackTransaction(tx))
			} else {
				require.Error(t, err)
				require.Nil(t, got)
				require.NoError(t, s.CommitTransaction(tx), "failed claim must not own or poison caller transaction")
				inst, err := s.GetInstance(t.Context(), "tasks")
				require.NoError(t, err)
				require.Equal(t, "outer change", inst.CancelReason)
			}
			assertUnclaimedTasks(t, s, 1)
		})
	}
}
func assertUnclaimedTasks(t *testing.T, s Storage, count int) {
	t.Helper()
	tasks, err := s.ListActivityTasks(t.Context(), "tasks")
	require.NoError(t, err)
	require.Len(t, tasks, count)
	for _, task := range tasks {
		require.Equal(t, "queued", task.Status)
		require.Empty(t, task.ClaimOwner)
		require.Zero(t, task.ClaimEpoch)
		require.Zero(t, task.Attempts)
		require.Zero(t, task.Failures)
		require.Nil(t, task.ClaimExpiresMS)
		require.Nil(t, task.AttemptDeadlineMS)
	}
}
func claimTestDriver(s Storage) Driver {
	switch s := s.(type) {
	case *SQLiteStorage:
		return s.driver
	case *PostgresStorage:
		return s.driver
	default:
		panic("unsupported test storage")
	}
}
func (s *claimResultStore) Conn(ctx context.Context) Executor {
	return claimResultExecutor{s.Storage.Conn(ctx), s}
}
func (e claimResultExecutor) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	if e.store.mode == "scan_error" {
		// Real UPDATE, with an undecodable final returned count. Earlier rows remain valid.
		at := strings.LastIndex(q, " RETURNING ")
		q = q[:at] + strings.Replace(q[at:], "attempts,", "CASE WHEN id=(SELECT MAX(id) FROM workflow_activity_tasks) THEN 'invalid-count' ELSE CAST(attempts AS TEXT) END,", 1)
	}
	rows, err := e.Executor.QueryContext(ctx, q, args...)
	if err != nil {
		return rows, err
	}
	switch e.store.mode {
	case "query_error", "rollback_error":
		for rows.Next() {
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		return nil, errors.New("claim result unavailable")
	case "cancel_before_read", "cancel_after_read":
		if e.store.mode == "cancel_after_read" && rows.Next() {
			if _, err := scanTask(rows); err != nil {
				return nil, errors.Join(err, rows.Close())
			}
			e.store.read++
		}
		e.store.cancel()
		deadline := time.Now().Add(2 * time.Second)
		for rows.Err() == nil && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func TestActivityClaimCommittedHandoff(t *testing.T) {
	testActivityClaimCommittedHandoff(t, sqliteHistoryFactory)
}
func testActivityClaimCommittedHandoff(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	_, err := s.ReserveActivity(t.Context(), f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wrapped := &claimResultStore{Storage: s, mode: "cancel_after_commit", cancel: cancel}
	got, err := (taskStore{wrapped, claimTestDriver(s)}).ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Minute, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NoError(t, s.ReleaseActivityTask(t.Context(), got.Claim()))
	next, err := s.ClaimActivityTask(t.Context(), "tasks", "#1", "replacement", time.Minute, nil)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Equal(t, 2, next.Attempts)
	require.Zero(t, next.Failures)
}
func TestActivityClaimRollbackFailure(t *testing.T) {
	testActivityClaimRollbackFailure(t, sqliteHistoryFactory)
}
func testActivityClaimRollbackFailure(t *testing.T, factory historyFactory) {
	s, f, task := taskFixture(t, factory)
	_, err := s.ReserveActivity(t.Context(), f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	tx, err := s.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer func() {
		err := s.RollbackTransaction(tx)
		require.True(t, err == nil || errors.Is(err, sql.ErrTxDone))
	}()
	wrapped := &claimResultStore{Storage: s, mode: "rollback_error"}
	got, err := (taskStore{wrapped, claimTestDriver(s)}).ClaimActivityTask(tx, "tasks", "#1", "worker", time.Minute, nil)
	require.Error(t, err)
	require.Nil(t, got)
	require.ErrorIs(t, s.CommitTransaction(tx), sql.ErrTxDone, "uncertain savepoint rollback must abort the caller transaction")
	assertUnclaimedTasks(t, s, 1)
}
func (s *claimResultStore) CommitTransaction(ctx context.Context) error {
	err := s.Storage.CommitTransaction(ctx)
	if err == nil && s.mode == "cancel_after_commit" {
		s.cancel()
	}
	return err
}
func (e claimResultExecutor) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	if e.store.mode == "rollback_error" && strings.HasPrefix(q, "ROLLBACK TO") {
		return nil, errors.New("savepoint rollback unavailable")
	}
	return e.Executor.ExecContext(ctx, q, args...)
}
