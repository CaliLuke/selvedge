//go:build integration

package selvedge

import (
	"context"
	"database/sql"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestWaitWorkflowWakeBetweenReadAndSelect_Postgres(t *testing.T) {
	s13WaitWakeBetweenReadAndSelect(t, createTestAppWithPostgres)
}

func TestClientAfterShutdown_Postgres(t *testing.T) {
	s13ClientAfterShutdown(t, createTestAppWithPostgres)
}

func TestClientStartRejectionSingleLine_Postgres(t *testing.T) {
	s13StartRejectionSingleLine(t, createTestAppWithPostgres)
}

func TestWaitWorkflowClosedNotifyHandler_Postgres(t *testing.T) {
	worker, cleanup := createTestAppWithPostgres(t, WithWorkflowResumptionInterval(time.Hour))
	t.Cleanup(cleanup)
	require.NoError(t, worker.Start(context.Background()))
	dsn, err := url.Parse(worker.config.databaseURL)
	require.NoError(t, err)
	query := dsn.Query()
	query.Set("application_name", "s13_notify_client")
	// One round trip per SELECT: an idle backend then means the read finished,
	// rather than a prepare round trip finishing before execution starts.
	query.Set("default_query_exec_mode", "exec")
	dsn.RawQuery = query.Encode()
	// The same database has different literal DSNs, so the local bus cannot
	// deliver the worker's close to the client. Only PostgreSQL NOTIFY can.
	require.NotEqual(t, worker.config.databaseURL, dsn.String())
	client := NewApp(WithDatabase(dsn.String()), WithAutoMigrate(false), WithWaitPollInterval(time.Hour))
	require.NoError(t, client.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, client.Shutdown(context.Background())) })
	require.Eventually(t, client.notifyListener.IsActive, 5*time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := StartWorkflowByName(ctx, worker, "unregistered", nil)
	require.NoError(t, err)
	grant, acquired, err := worker.storage.AcquireLease(ctx, id, "test-owner", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	// Hold the table until Wait's SELECT is visible as blocked, then wait for
	// that query to finish. This proves it read running before we close, even
	// if the goroutine has not yet reached select. No timing sleep is needed.
	tx, err := worker.storage.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, "LOCK TABLE workflow_instances IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)
	type outcome struct {
		result *WorkflowResult[string]
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := WaitWorkflow[string](ctx, client, id)
		done <- outcome{result, err}
	}()
	pid := s13BlockedWaitPID(t, ctx, worker.storage.DB())
	require.NoError(t, tx.Commit())
	require.Eventually(t, func() bool {
		var finished bool
		err := worker.storage.DB().QueryRowContext(ctx, `SELECT state = 'idle' OR query NOT LIKE '%FROM workflow_instances WHERE instance_id = $1%'
			FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&finished)
		return err == nil && finished
	}, 3*time.Second, time.Millisecond, "Wait's first SELECT must finish before the close")

	require.NoError(t, worker.storage.CloseInstance(ctx, storage.Fence{InstanceID: id, Owner: "test-owner", Epoch: grant.Epoch}, storage.StatusCompleted, []byte(`"done"`), nil))
	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, "completed", got.result.Status)
	require.Equal(t, "done", got.result.Output)
}

func s13BlockedWaitPID(t *testing.T, ctx context.Context, db *sql.DB) int {
	t.Helper()
	var pid int
	require.Eventually(t, func() bool {
		err := db.QueryRowContext(ctx, `SELECT pid FROM pg_stat_activity
			WHERE application_name = 's13_notify_client' AND wait_event_type = 'Lock'
			AND query LIKE '%FROM workflow_instances WHERE instance_id = $1%'`).Scan(&pid)
		return err == nil
	}, 3*time.Second, time.Millisecond, "Wait must reach its first status SELECT")
	return pid
}
