//go:build integration

package storage

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPostgresScheduleToStartDeadlineStrict(t *testing.T) {
	testScheduleToStartDeadlineStrict(t, postgresHistoryFactory)
}
func TestPostgresActivityDeadlineFencesWrites(t *testing.T) {
	testActivityDeadlineFencesWrites(t, postgresHistoryFactory)
}
func TestPostgresTimeoutActivityTaskAtomic(t *testing.T) {
	testTimeoutActivityTaskAtomic(t, postgresHistoryFactory)
}
func TestPostgresActivityScheduleDeadlineReset(t *testing.T) {
	testActivityScheduleDeadlineReset(t, postgresHistoryFactory)
}

// A volatile sequence makes multiple evaluations observable without relying on a real clock crossing a millisecond.
type s15bSequenceClock struct{ Driver }

func (s15bSequenceClock) NowMillisExpr() string { return "nextval('s15b_clock')" }
func TestPostgresActivityTimestampPairs(t *testing.T) {
	s, f, task := s15bTask(t, postgresHistoryFactory)
	_, err := s.DB().Exec("CREATE SEQUENCE s15b_clock START 1000")
	require.NoError(t, err)
	pg := s.(*PostgresStorage)
	pg.driver = s15bSequenceClock{pg.driver}
	ctx := context.Background()
	task.StartToCloseMS = 1000
	task.ScheduleToStartMS = 10000
	_, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	rows, err := s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, rows[0].NotBeforeMS+10000, *rows[0].ScheduleDeadlineMS)
	claim, err := s.ClaimActivityTask(ctx, "tasks", "#1", "first", time.Hour, nil)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.Equal(t, *claim.AttemptDeadlineMS, *claim.ClaimExpiresMS)
	require.NoError(t, s.RenewActivityTask(ctx, claim.Claim(), time.Hour, true))
	rows, err = s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, *rows[0].AttemptDeadlineMS, *rows[0].ClaimExpiresMS)
	require.NoError(t, s.RetryActivityTask(ctx, claim.Claim(), []byte(`"retry"`), 0))
	rows, err = s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, rows[0].NotBeforeMS+10000, *rows[0].ScheduleDeadlineMS)
	claim, err = s.ClaimActivityTask(ctx, "tasks", "#1", "second", time.Hour, nil)
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, s.ReleaseActivityTask(ctx, claim.Claim()))
	rows, err = s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, rows[0].NotBeforeMS+10000, *rows[0].ScheduleDeadlineMS)
}

func TestPostgresClaimDeadlineAfterRowLock(t *testing.T) {
	s, f, task := taskFixture(t, postgresHistoryFactory)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	task.StartToCloseMS = 200
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	blocking, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(blocking) }()
	var blockingPID, claimPID int
	require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT pg_backend_pid()").Scan(&blockingPID))
	var id int64
	require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT id FROM workflow_activity_tasks WHERE instance_id='tasks' FOR UPDATE").Scan(&id))
	claiming, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(claiming) }()
	require.NoError(t, s.Conn(claiming).QueryRowContext(claiming, "SELECT pg_backend_pid()").Scan(&claimPID))
	type result struct {
		task *ActivityTask
		err  error
	}
	done := make(chan result, 1)
	go func() {
		v, e := s.ClaimActivityTask(claiming, "tasks", "#1", "worker", time.Second, nil)
		done <- result{v, e}
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		e := s.DB().QueryRowContext(ctx, "SELECT $1 = ANY(pg_blocking_pids($2))", blockingPID, claimPID).Scan(&blocked)
		return e == nil && blocked
	}, 3*time.Second, 5*time.Millisecond)
	time.Sleep(600 * time.Millisecond)
	var releaseStoreTime int64
	require.NoError(t, s.DB().QueryRowContext(ctx, "SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)").Scan(&releaseStoreTime))
	require.NoError(t, s.CommitTransaction(blocking))
	got := <-done
	require.NoError(t, got.err)
	require.NotNil(t, got.task)
	require.NoError(t, s.CommitTransaction(claiming))
	t.Logf("store time before releasing blocker=%d; returned claim deadline=%d; expiry=%d", releaseStoreTime, *got.task.AttemptDeadlineMS, *got.task.ClaimExpiresMS)
	require.Greater(t, *got.task.AttemptDeadlineMS, releaseStoreTime, "SQL grants an already-expired claim after the row-lock wait")
}

func TestPostgresBlockedRenewalDoesNotRegressExpiry(t *testing.T) {
	s, f, task := taskFixture(t, postgresHistoryFactory)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	initial, err := s.ClaimActivityTask(ctx, "tasks", "#1", "worker", 2*time.Second, nil)
	require.NoError(t, err)
	require.NotNil(t, initial)
	blocking, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(blocking) }()
	var blockingPID, renewPID int
	require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT pg_backend_pid()").Scan(&blockingPID))
	var id int64
	require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT id FROM workflow_activity_tasks WHERE instance_id='tasks' FOR UPDATE").Scan(&id))
	renewing, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(renewing) }()
	require.NoError(t, s.Conn(renewing).QueryRowContext(renewing, "SELECT pg_backend_pid()").Scan(&renewPID))
	done := make(chan error, 1)
	go func() { done <- s.RenewActivityTask(renewing, initial.Claim(), 2*time.Second, true) }()
	require.Eventually(t, func() bool {
		var blocked bool
		e := s.DB().QueryRowContext(ctx, "SELECT $1 = ANY(pg_blocking_pids($2))", blockingPID, renewPID).Scan(&blocked)
		return e == nil && blocked
	}, 3*time.Second, 5*time.Millisecond)
	time.Sleep(600 * time.Millisecond)
	require.NoError(t, s.RenewActivityTask(blocking, initial.Claim(), 2*time.Second, true))
	before, err := s.ListActivityTasks(blocking, "tasks")
	require.NoError(t, err)
	require.NoError(t, s.CommitTransaction(blocking))
	require.NoError(t, <-done)
	require.NoError(t, s.CommitTransaction(renewing))
	after, err := s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	t.Logf("expiry after newer renewal=%d; expiry after older blocked renewal=%d", *before[0].ClaimExpiresMS, *after[0].ClaimExpiresMS)
	require.GreaterOrEqual(t, *after[0].ClaimExpiresMS, *before[0].ClaimExpiresMS, "a blocked renewal regressed the active claim expiry")
}

func TestPostgresActivityWritesAfterRowLock(t *testing.T) {
	for _, operation := range []string{"renew", "check", "retry", "release", "complete"} {
		for _, expired := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/expired_%t", operation, expired), func(t *testing.T) {
				s, f, task := taskFixture(t, postgresHistoryFactory)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				_, err := s.DB().Exec("CREATE SEQUENCE s15b_clock START 1000")
				require.NoError(t, err)
				pg := s.(*PostgresStorage)
				pg.driver = s15bSequenceClock{pg.driver}
				task.ScheduleToStartMS = 100
				_, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
				require.NoError(t, err)
				claim, err := s.ClaimActivityTask(ctx, "tasks", "#1", "first", 10*time.Second, nil)
				require.NoError(t, err)
				require.NotNil(t, claim)
				blocking, err := s.BeginTransaction(ctx)
				require.NoError(t, err)
				defer func() { _ = s.RollbackTransaction(blocking) }()
				writing, err := s.BeginTransaction(ctx)
				require.NoError(t, err)
				defer func() { _ = s.RollbackTransaction(writing) }()
				var blockerPID, writerPID int
				require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT pg_backend_pid()").Scan(&blockerPID))
				require.NoError(t, s.Conn(writing).QueryRowContext(writing, "SELECT pg_backend_pid()").Scan(&writerPID))
				var id int64
				require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT id FROM workflow_activity_tasks WHERE instance_id='tasks' FOR UPDATE").Scan(&id))
				done := make(chan error, 1)
				go func() {
					var e error
					switch operation {
					case "renew":
						e = s.RenewActivityTask(writing, claim.Claim(), 10*time.Second, true)
					case "check":
						e = s.RenewActivityTask(writing, claim.Claim(), 10*time.Second, false)
					case "retry":
						e = s.RetryActivityTask(writing, claim.Claim(), []byte(`"retry"`), time.Second)
					case "release":
						e = s.ReleaseActivityTask(writing, claim.Claim())
					case "complete":
						e = s.CompleteActivityTask(writing, claim.Claim(), HistoryActivityCompleted, []byte(`"done"`))
					}
					done <- e
				}()
				require.Eventually(t, func() bool {
					var blocked bool
					e := s.DB().QueryRowContext(ctx, "SELECT $1 = ANY(pg_blocking_pids($2))", blockerPID, writerPID).Scan(&blocked)
					return e == nil && blocked
				}, 3*time.Second, time.Millisecond)
				advanced := int64(5000)
				if expired {
					advanced = 20000
				}
				_, err = s.DB().ExecContext(ctx, "SELECT setval('s15b_clock',$1)", advanced)
				require.NoError(t, err)
				require.NoError(t, s.CommitTransaction(blocking))
				err = <-done
				if expired {
					require.ErrorIs(t, err, ErrFenced)
					require.NoError(t, s.RollbackTransaction(writing))
					return
				}
				require.NoError(t, err)
				require.NoError(t, s.CommitTransaction(writing))
				rows, err := s.ListActivityTasks(ctx, "tasks")
				require.NoError(t, err)
				switch operation {
				case "renew":
					require.Greater(t, *rows[0].ClaimExpiresMS, advanced+10000)
				case "retry":
					require.Greater(t, rows[0].NotBeforeMS, advanced+1000)
					require.Equal(t, rows[0].NotBeforeMS+100, *rows[0].ScheduleDeadlineMS)
				case "release":
					require.Greater(t, rows[0].NotBeforeMS, advanced)
					require.Equal(t, rows[0].NotBeforeMS+100, *rows[0].ScheduleDeadlineMS)
				}
			})
		}
	}
}

func TestPostgresScheduleExpiryDuringClaimLock(t *testing.T) {
	s, f, task := taskFixture(t, postgresHistoryFactory)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := s.DB().Exec("CREATE SEQUENCE s15b_clock START 1000")
	require.NoError(t, err)
	pg := s.(*PostgresStorage)
	pg.driver = s15bSequenceClock{pg.driver}
	task.ScheduleToStartMS = 1000
	_, err = s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	blocking, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(blocking) }()
	claiming, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	defer func() { _ = s.RollbackTransaction(claiming) }()
	var blockerPID, claimPID int
	require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT pg_backend_pid()").Scan(&blockerPID))
	require.NoError(t, s.Conn(claiming).QueryRowContext(claiming, "SELECT pg_backend_pid()").Scan(&claimPID))
	var id int64
	require.NoError(t, s.Conn(blocking).QueryRowContext(blocking, "SELECT id FROM workflow_activity_tasks WHERE instance_id='tasks' FOR UPDATE").Scan(&id))
	type result struct {
		task *ActivityTask
		err  error
	}
	done := make(chan result, 1)
	go func() {
		v, e := s.ClaimActivityTask(claiming, "tasks", "#1", "first", time.Second, nil)
		done <- result{v, e}
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		e := s.DB().QueryRowContext(ctx, "SELECT $1 = ANY(pg_blocking_pids($2))", blockerPID, claimPID).Scan(&blocked)
		return e == nil && blocked
	}, 3*time.Second, time.Millisecond)
	_, err = s.DB().ExecContext(ctx, "SELECT setval('s15b_clock',5000)")
	require.NoError(t, err)
	require.NoError(t, s.CommitTransaction(blocking))
	got := <-done
	require.NoError(t, got.err)
	require.Nil(t, got.task, "schedule timeout must be rechecked after the lock wait")
	require.NoError(t, s.CommitTransaction(claiming))
	rows, err := s.ListActivityTasks(ctx, "tasks")
	require.NoError(t, err)
	require.Zero(t, rows[0].Attempts)
}
