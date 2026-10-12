package selvedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestCancelWhileAwaitingActivityRecordsCanceled(t *testing.T) {
	testCancelWhileAwaitingActivityRecordsCanceled(t, createTestApp)
}
func testCancelWhileAwaitingActivityRecordsCanceled(t *testing.T, factory replayAppFactory) {
	for _, race := range []bool{false, true} {
		name := "cancel"
		if race {
			name = "completion_wins"
		}
		t.Run(name, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			entered := make(chan struct{})
			cancelled := make(chan error, 1)
			release := make(chan struct{})
			defer close(release)
			a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
				close(entered)
				<-ctx.Done()
				cancelled <- context.Cause(ctx)
				<-release
				return "late", nil
			})
			var observed error
			w := DefineWorkflow("cancel", func(ctx *WorkflowContext, in string) (string, error) {
				f, err := a.ExecuteAsync(ctx, in)
				if err != nil {
					return "", err
				}
				v, err := f.Get(ctx)
				observed = err
				if errors.Is(err, context.Canceled) {
					if sleepErr := Sleep(ctx, time.Hour, WithSleepID("after-cancel")); sleepErr != nil {
						return "", sleepErr
					}
				}
				return v, err
			})
			id := seedErrorWorkflow(t, h, w)
			seedTask(t, h, id)
			claim, err := h.ClaimTask(id, "#1")
			require.NoError(t, err)
			require.NotNil(t, claim)
			require.NoError(t, h.RunOnce(id))
			var done chan error
			if !race {
				done = make(chan error, 1)
				go func() { done <- claim.Run() }()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("attempt did not start")
				}
				defer func() { h.app.cancel() }()
			}
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			if race {
				fs.Before = func(_ context.Context, method string) error {
					if method == "CancelActivityAtYield" {
						return claim.Complete(`"winner"`)
					}
					return nil
				}
			}
			if !race {
				// S11 scopes do not exist yet. Clear only the global request when the
				// follow-up Sleep reserves, so the workflow stays open for F5 assertions.
				fs.Before = func(_ context.Context, method string) error {
					if method == "ReserveTimer" {
						_, err := fs.DB().Exec("UPDATE workflow_instances SET cancel_requested_at=NULL WHERE instance_id=$1", id)
						return err
					}
					return nil
				}
			}
			err = h.RunOnce(id)
			row, readErr := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, readErr)
			require.NotNil(t, row)
			if race {
				require.NoError(t, err)
				require.Equal(t, storage.HistoryActivityCompleted, row.EventType)
				require.JSONEq(t, `"winner"`, string(row.EventData))
				require.Contains(t, fs.Calls(), "CancelActivityAtYield")
				assertCompletedLeaseRun(t, h, id)
				return
			}
			require.NoError(t, err)
			var typed *WorkflowCancelledError
			require.ErrorAs(t, observed, &typed)
			require.Equal(t, "stop", typed.Reason)
			require.Equal(t, storage.HistoryActivityCancelled, row.EventType)
			inst, err := fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, storage.StatusRunning, inst.Status)
			require.NotContains(t, fs.Calls(), "CloseInstance")
			var status string
			require.NoError(t, fs.DB().QueryRow("SELECT status FROM workflow_activity_tasks WHERE instance_id=$1", id).Scan(&status))
			require.Equal(t, "cancelled", status)
			select {
			case cause := <-cancelled:
				require.ErrorIs(t, cause, errClaimLost)
			case <-time.After(3 * h.app.config.lease.RenewInterval):
				t.Fatal("renewal did not cancel attempt")
			}
			// Let the attempt return after cancellation; it must not overwrite the row.
			release <- struct{}{}
			require.Error(t, <-done)
			again, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			require.Equal(t, row.EventData, again.EventData)
			// Replay the recorded cancellation while the workflow is still open,
			// then finish the reserved sleep before propagating cancellation to close.
			require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "after-cancel"}))
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))

			require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			require.ErrorAs(t, observed, &typed)
			require.Equal(t, "stop", typed.Reason)
			inst, err = fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, storage.StatusCancelled, inst.Status)
		})
	}
}
