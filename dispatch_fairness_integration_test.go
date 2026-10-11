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

func TestDispatcherMixedFleetFirstPollPostgres(t *testing.T) {
	testDispatcherMixedFleetFirstPoll(t, createTestAppWithPostgres)
}

func TestDispatcherSelectionUsesOneQueryPostgres(t *testing.T) {
	testDispatcherSelectionUsesOneQuery(t, createTestAppWithPostgres)
}

func TestShutdownNotifiesAbandonedLeasesPostgres(t *testing.T) {
	h := newTypedReplayHarness(t, createTestAppWithPostgres)
	entered := make(chan struct{}, 2)
	w := DefineWorkflow("shutdown_notify", func(ctx *WorkflowContext, _ string) (string, error) {
		entered <- struct{}{}
		<-ctx.Context().Done()
		return "", ctx.Context().Err()
	})
	RegisterWorkflow(h.app, w)
	for _, id := range []string{"abandon-one", "abandon-two"} {
		require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{
			InstanceID: id, WorkflowName: w.Name(), Status: storage.StatusRunning,
		}))
	}
	pg := h.app.storage.(*storage.PostgresStorage)
	pg.SetNotifyEnabled(true)
	listener, err := pgx.Connect(context.Background(), h.app.config.databaseURL)
	require.NoError(t, err)
	defer func() { require.NoError(t, listener.Close(context.Background())) }()
	_, err = listener.Exec(context.Background(), "LISTEN workflow_resumable")
	require.NoError(t, err)
	count, err := h.app.resumeResumableWorkflows()
	require.NoError(t, err)
	require.Equal(t, 2, count)
	defer func() { h.app.cancel(); h.app.wg.Wait() }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("workflow did not acquire its lease")
		}
	}
	assertNoRunnableNotification(t, listener)
	h.app.running = true // Drive Shutdown without background polling loops.
	require.NoError(t, h.app.Shutdown(context.Background()))
	wait, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	n, err := listener.WaitForNotification(wait)
	require.NoError(t, err, "shutdown must wake idle workers after abandoning leases")
	require.Equal(t, "workflow_resumable", n.Channel)
	var runnable int
	err = listener.QueryRow(context.Background(), `SELECT count(*) FROM workflow_instances
		WHERE instance_id IN ('abandon-one', 'abandon-two') AND status='running'
		AND runnable AND lease_owner IS NULL`).Scan(&runnable)
	require.NoError(t, err)
	require.Equal(t, 2, runnable, "both abandoned leases must be committed before the wake")
	assertNoRunnableNotification(t, listener)
}
