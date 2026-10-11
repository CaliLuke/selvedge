//go:build integration

package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReviewRegressionsPostgres(t *testing.T) {
	for name, test := range map[string]func(*testing.T, replayAppFactory){
		"schema idempotence": testDispatchIndexCreationIsIdempotent,
		"terminal timer":     testTerminalTimerIsRemoved,
		"timer hook":         testTimerHookAfterCommit,
		"context fences":     testContextWritesRequireMatchingFence,
		"final check":        testFinalCheckStopsBeforeCommitAttempt,
		"commit check":       testCommitCheckStopsBeforeSQL,
		"suspend parks":      testSuspendWithoutInputParks,
	} {
		t.Run(name, func(t *testing.T) { test(t, createTestAppWithPostgres) })
	}
}

func TestRunnableNotificationsAfterCommitPostgres(t *testing.T) {
	h, s := newLeaseStore(t, createTestAppWithPostgres)
	pg := h.app.storage.(*storage.PostgresStorage)
	pg.SetNotifyEnabled(true)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	writes := map[string]func(context.Context) error{
		"mark": func(ctx context.Context) error { return pg.MarkRunnable(ctx, "lease") },
		"timer": func(ctx context.Context) error {
			return pg.FireTimer(ctx, &storage.TimerSubscription{InstanceID: "lease", TimerID: "pause", ExpiresAt: time.Now()})
		},
		"signal": func(ctx context.Context) error {
			_, err := pg.AppendSignal(ctx, "lease", "messages", []byte(`1`))
			return err
		},
		"result": func(ctx context.Context) error {
			_, err := pg.SetHistoryValueIfNull(ctx, nil, "lease", "activity", "activity:A", storage.HistoryActivityCompleted, []byte(`1`))
			return err
		},
		"create": func(ctx context.Context) error {
			return pg.CreateInstance(ctx, &storage.WorkflowInstance{InstanceID: "new", WorkflowName: "lease", Status: storage.StatusRunning})
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			listener, err := pgx.Connect(h.app.ctx, h.app.config.databaseURL)
			require.NoError(t, err)
			defer func() { require.NoError(t, listener.Close(context.Background())) }()
			_, err = listener.Exec(h.app.ctx, "LISTEN workflow_resumable")
			require.NoError(t, err)
			for _, commit := range []bool{false, true} {
				tx, err := pg.BeginTransaction(h.app.ctx)
				require.NoError(t, err)
				require.NoError(t, write(tx))
				assertNoRunnableNotification(t, listener)
				if !commit {
					require.NoError(t, pg.RollbackTransaction(tx))
					assertNoRunnableNotification(t, listener)
					continue
				}
				require.NoError(t, pg.CommitTransaction(tx))
				wait, cancel := context.WithTimeout(h.app.ctx, time.Second)
				defer cancel()
				n, err := listener.WaitForNotification(wait)
				require.NoError(t, err, "committed wake must reach other workers")
				require.Equal(t, "workflow_resumable", n.Channel)
			}
		})
	}
	t.Run("release pending input", func(t *testing.T) {
		listener, err := pgx.Connect(h.app.ctx, h.app.config.databaseURL)
		require.NoError(t, err)
		defer func() { require.NoError(t, listener.Close(context.Background())) }()
		_, err = listener.Exec(h.app.ctx, "LISTEN workflow_resumable")
		require.NoError(t, err)
		require.NoError(t, pg.ReleaseLease(h.app.ctx, f, false))
		wait, cancel := context.WithTimeout(h.app.ctx, time.Second)
		defer cancel()
		_, err = listener.WaitForNotification(wait)
		require.NoError(t, err)
	})
}
func assertNoRunnableNotification(t *testing.T, listener *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := listener.WaitForNotification(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "wake must be invisible before commit and after rollback")
}

func TestAbandoningWorkflowUsesPollingPostgres(t *testing.T) {
	testAbandoningWorkflowUsesPolling(t, createTestAppWithPostgres)
}

func TestAbandonReleaseDoesNotNotifyPostgres(t *testing.T) {
	h, s := newLeaseStore(t, createTestAppWithPostgres)
	pg := h.app.storage.(*storage.PostgresStorage)
	pg.SetNotifyEnabled(true)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	listener, err := pgx.Connect(h.app.ctx, h.app.config.databaseURL)
	require.NoError(t, err)
	defer func() { require.NoError(t, listener.Close(context.Background())) }()
	_, err = listener.Exec(h.app.ctx, "LISTEN workflow_resumable")
	require.NoError(t, err)
	require.NoError(t, pg.ReleaseLease(h.app.ctx, f, true))
	wait, cancel := context.WithTimeout(h.app.ctx, 100*time.Millisecond)
	defer cancel()
	_, err = listener.WaitForNotification(wait)
	require.ErrorIs(t, err, context.DeadlineExceeded, "abandon must not emit a runnable notification")
	ids, err := pg.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"lease"}, ids, "abandon must remain discoverable by polling")
}
