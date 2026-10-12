package selvedge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReplayCommandMismatchFailsFastChild(t *testing.T) {
	testReplayCommandMismatchFailsFastChild(t, createTestApp)
}
func testReplayCommandMismatchFailsFastChild(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"activity", "workflow", "id"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			changed := false
			a := DefineActivity("A", func(context.Context, string) (string, error) { return "", nil })
			w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
				if kind == "activity" && !changed {
					_, e := a.ExecuteAsync(ctx, "")
					if e != nil {
						return "", e
					}
					return "", Sleep(ctx, time.Hour)
				}
				name, id := "child", "child-id"
				if changed && kind == "workflow" {
					name = "other"
				}
				if changed && kind == "id" {
					id = "other-id"
				}
				_, e := StartChild[string](ctx, name, "", WithChildID(id))
				if e != nil {
					return "", e
				}
				return "", Sleep(ctx, time.Hour)
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			changed = true
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			var target *NondeterminismError
			require.ErrorAs(t, h.RunOnce(id), &target)
			require.EqualValues(t, 1, target.Seq)
			switch kind {
			case "activity":
				require.Equal(t, "activity:A", target.Recorded)
				require.Equal(t, "child:child", target.Issued)
			case "workflow":
				require.Equal(t, "child:child", target.Recorded)
				require.Equal(t, "child:other", target.Issued)
			case "id":
				require.Equal(t, "child ID changed", target.Reason)
				require.Equal(t, "child-id", target.Recorded)
				require.Equal(t, "other-id", target.Issued)
			}
			rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
			require.NoError(t, err)
			require.Len(t, rows, 3, "the mismatch may add only the workflow failure row")
			require.Equal(t, "$workflow_failed", rows[2].ActivityID)
			var n int
			require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_instances WHERE instance_id='other-id'`).Scan(&n))
			require.Zero(t, n)
		})
	}
}
func TestStalledExecutionCommitRejectedChild(t *testing.T) {
	testStalledExecutionCommitRejectedChild(t, createTestApp)
}
func testStalledExecutionCommitRejectedChild(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		_, err := StartChild[string](ctx, "child", "")
		return "", err
	})
	id := seedErrorWorkflow(t, h, w)
	tookOver := false
	fs.Before = func(ctx context.Context, m string) error {
		if m == "StartChild" {
			setStoreClock(t, h, 4000)
			_, ok, err := fs.Storage.AcquireLease(ctx, id, "new-owner", time.Second)
			require.NoError(t, err)
			require.True(t, ok)
			tookOver = true
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), storage.ErrFenced)
	require.True(t, tookOver)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Nil(t, row)
	child, err := fs.GetInstance(h.app.ctx, id+"/#1")
	require.NoError(t, err)
	require.Nil(t, child)
}
func TestChildRunsWithoutParentRegistration(t *testing.T) {
	testChildRunsWithoutParentRegistration(t, createTestApp)
}
func testChildRunsWithoutParentRegistration(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	other := NewApp()
	other.storage = fs
	other.ctx = h.app.ctx
	other.engine = replay.NewEngine(fs, other.hooks, "child-worker", errorCodec{})
	RegisterWorkflow(other, DefineWorkflow("child", func(_ *WorkflowContext, in string) (string, error) { return in + " child", nil }))
	parent := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		c, e := StartChild[string](ctx, "child", "remote")
		if e != nil {
			return "", e
		}
		return c.Get(ctx)
	})
	id := seedErrorWorkflow(t, h, parent)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, other.resumeWorkflow(other.ctx, id+"/#1"))
	require.NoError(t, h.RunOnce(id))
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
	require.JSONEq(t, `"remote child"`, string(inst.OutputData))
}
func TestChildGuards(t *testing.T) { testChildGuards(t, createTestApp) }
func testChildGuards(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"encode", "predicate-start", "predicate-get", "predicate-cancel", "foreign-get", "closing"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var closing []error
			w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
				if kind == "encode" {
					_, _ = StartChild[string](ctx, "child", make(chan int))
					return "ignored", nil
				}
				c, e := StartChild[string](ctx, "child", "")
				if e != nil {
					return "", e
				}
				switch kind {
				case "predicate-start", "predicate-get", "predicate-cancel":
					_ = Await(ctx, func() bool {
						switch kind {
						case "predicate-start":
							_, _ = StartChild[string](ctx, "child", "")
						case "predicate-get":
							_, _ = c.Get(ctx)
						case "predicate-cancel":
							_ = c.Cancel(ctx)
						}
						return true
					})
					return "ignored", nil
				case "foreign-get":
					done := make(chan error, 1)
					go func() { _, e := c.Get(ctx); done <- e }()
					return "", <-done
				case "closing":
					defer func() {
						_, e := StartChild[string](ctx, "child", "")
						closing = append(closing, e)
						_, e = c.Get(ctx)
						closing = append(closing, e)
						closing = append(closing, c.Cancel(ctx))
					}()
					return "", Sleep(ctx, time.Hour)
				}
				return "", nil
			})
			id := seedErrorWorkflow(t, h, w)
			err := h.RunOnce(id)
			if kind == "closing" {
				require.NoError(t, err)
				require.Len(t, closing, 3)
				for _, e := range closing {
					require.Error(t, e)
				}
				require.NotContains(t, fs.Calls(), "CancelChild")
			} else {
				require.Error(t, err)
				if kind != "encode" {
					var target *NondeterminismError
					require.ErrorAs(t, err, &target)
				}
			}
		})
	}
}
func TestReplayDeterministicAfterCrashChild(t *testing.T) {
	testReplayDeterministicAfterCrashChild(t, createTestApp)
}
func testReplayDeterministicAfterCrashChild(t *testing.T, factory replayAppFactory) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%t", cancel), func(t *testing.T) {
			baseline, writes := runChildCrash(t, factory, cancel, 0, false)
			for cut := 1; cut <= writes; cut++ {
				for _, after := range []bool{false, true} {
					t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) {
						got, _ := runChildCrash(t, factory, cancel, cut, after)
						require.Equal(t, baseline, got)
					})
				}
			}
		})
	}
}
func runChildCrash(t *testing.T, factory replayAppFactory, cancel bool, cut int, after bool) ([]historyValue, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	RegisterWorkflow(h.app, DefineWorkflow("child", func(*WorkflowContext, string) (string, error) { return "done", nil }))
	w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		c, e := StartChild[string](ctx, "child", "")
		if e != nil {
			return "", e
		}
		timer, e := NewTimer(ctx, time.Hour)
		if e != nil {
			return "", e
		}
		if cancel {
			if e = c.Cancel(ctx); e != nil {
				return "", e
			}
		} else {
			if e = Await(ctx, func() bool { return c.IsReady() || timer.IsReady() }); e != nil {
				return "", e
			}
		}
		value, e := c.Get(ctx)
		if cancel && errors.Is(e, context.Canceled) {
			return "cancelled child", nil
		}
		return value, e
	})
	id := seedErrorWorkflow(t, h, w)
	writes := 0
	crashed := false
	inject := func(_ context.Context, m string) error {
		switch m {
		case "AcquireLease", "StartChild", "ReserveTimer", "CommitObservation", "CancelChild", "CloseInstance", "ReleaseLease":
			writes++
		default:
			return nil
		}
		if cut > 0 && !crashed && writes == cut {
			crashed = true
			return storage.ErrFenced
		}
		return nil
	}
	if after {
		fs.After = inject
	} else {
		fs.Before = inject
	}
	for attempt := range 15 {
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "parent: %v", err)
		inst, e := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, e)
		if inst.Status == storage.StatusCompleted {
			break
		}
		setStoreClock(t, h, int64(4000+attempt*4000))
		child, e := fs.GetInstance(h.app.ctx, id+"/#1")
		require.NoError(t, e)
		if child != nil && !cancel {
			err = h.RunOnce(child.InstanceID)
			require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "child: %v", err)
		}
		require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
	}
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
	if cancel {
		require.JSONEq(t, `"cancelled child"`, string(inst.OutputData))
	} else {
		require.JSONEq(t, `"done"`, string(inst.OutputData))
	}
	if cut > 0 {
		require.True(t, crashed)
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	result := make([]historyValue, 0, len(rows))
	for _, r := range rows {
		result = append(result, historyValue{r.Seq, r.DeliveredSeq, r.ActivityID, r.Command, r.EventType, string(r.EventData)})
	}
	return result, writes
}
