package selvedge

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReplayDeterministicAfterCrashChildScopes(t *testing.T) {
	testReplayDeterministicAfterCrashChildScopes(t, createTestApp)
}
func testReplayDeterministicAfterCrashChildScopes(t *testing.T, factory replayAppFactory) {
	for _, method := range []string{"StartChild", "CommitObservation", "RecordHistory"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after_%t", method, after), func(t *testing.T) {
				h, _, fs := leaseHarness(t, factory)
				w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
					scoped, cancel := WithCancel(ctx)
					c, err := StartChild[string](scoped, "child", "")
					if err != nil {
						return "", err
					}
					cancel()
					if err = ctx.execCtx.Check(); err != nil {
						return "", err
					}
					if !c.IsReady() {
						return "", errors.New("scope child was not cancelled")
					}
					if _, err = c.Get(ctx); !errors.Is(err, context.Canceled) {
						return "", fmt.Errorf("scope cancel lost: %v", err)
					}
					later, err := StartChild[string](scoped, "never", "")
					if err != nil {
						return "", err
					}
					if !later.IsReady() {
						return "", errors.New("cancelled start was not ready")
					}
					if _, err = later.Get(ctx); !errors.Is(err, context.Canceled) {
						return "", fmt.Errorf("cancelled start lost: %v", err)
					}
					_, err = Receive[int](ctx, "finish")
					return "done", err
				})
				id := seedErrorWorkflow(t, h, w)
				injected := false
				inject := func(_ context.Context, m string) error {
					if m == method && !injected {
						injected = true
						return storage.ErrFenced
					}
					return nil
				}
				if after {
					fs.After = inject
				} else {
					fs.Before = inject
				}
				require.ErrorIs(t, h.RunOnce(id), storage.ErrFenced)
				require.True(t, injected)
				fs.Before, fs.After = nil, nil
				row, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
				require.NoError(t, err)
				var requested bool
				err = fs.DB().QueryRow(`SELECT EXISTS(SELECT 1 FROM workflow_instances WHERE instance_id=$1 AND cancel_requested_at IS NOT NULL)`, id+"/#1").Scan(&requested)
				require.NoError(t, err)
				require.Equal(t, row != nil, requested, "cancel record and child request commit together")
				setStoreClock(t, h, 4000)
				require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
				require.NoError(t, h.RunOnce(id))
				require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
				require.NoError(t, h.RunOnce(id))
				assertCompletedLeaseRun(t, h, id)
				row, err = fs.GetHistoryEvent(h.app.ctx, id, "#1")
				require.NoError(t, err)
				require.EqualValues(t, 2, row.DeliveredSeq)
				row, err = fs.GetHistoryEvent(h.app.ctx, id, "#3")
				require.NoError(t, err)
				require.EqualValues(t, 3, row.DeliveredSeq)
				var count int
				require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_instances WHERE parent_instance_id=$1`, id).Scan(&count))
				require.Equal(t, 1, count)
			})
		}
	}
}
