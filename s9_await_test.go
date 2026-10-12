package selvedge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestAwaitImmediatelyTrueWritesNothing(t *testing.T) {
	testAwaitImmediatelyTrueWritesNothing(t, createTestApp)
}
func testAwaitImmediatelyTrueWritesNothing(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var positions []int64
	w := DefineWorkflow("true", func(ctx *WorkflowContext, _ string) (string, error) {
		if err := Await(ctx, func() bool { return true }); err != nil {
			return "", err
		}
		positions = append(positions, ctx.execCtx.Position())
		if err := Await(ctx, func() bool { return Pending(ctx, "c") > 0 }); err != nil {
			return "", err
		}
		positions = append(positions, ctx.execCtx.Position())
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 1))
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []int64{0, 1, 0, 1}, positions)
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "await", rows[0].Command)
	require.Equal(t, 1, countMethod(fs.Calls(), "CommitObservation"))
}
func TestAwaitDecisionRecorded(t *testing.T) { testAwaitDecisionRecorded(t, createTestApp) }
func testAwaitDecisionRecorded(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	var pre, overlay int
	w := DefineWorkflow("decision", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		if err := Await(ctx, func() bool {
			if ctx.execCtx.Position() == 1 {
				pre++
			} else {
				overlay++
			}
			return f.IsReady()
		}); err != nil {
			return "", err
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NoError(t, claim.Complete(`"A"`))
	injected := false
	fs.After = func(_ context.Context, method string) error {
		if method == "CommitObservation" && !injected {
			injected = true
			_, err := fs.Storage.AppendSignal(h.app.ctx, id, "late", []byte(`1`))
			return err
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 2, pre)
	require.Equal(t, 1, overlay)
	require.Equal(t, 1, countMethod(fs.Calls(), "ArrivedUndelivered"))
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
	require.NoError(t, err)
	require.JSONEq(t, `{"history":1,"signals":0}`, string(row.EventData))
	batch, err := fs.Storage.ArrivedUndelivered(h.app.ctx, id)
	require.NoError(t, err)
	require.Empty(t, batch.HistoryKeys)
	require.Len(t, batch.Signals, 1)
}
func TestAwaitPredicateCommandFailsRun(t *testing.T) {
	testAwaitPredicateCommandFailsRun(t, createTestApp)
}
func testAwaitPredicateCommandFailsRun(t *testing.T, factory replayAppFactory) {
	commands := []string{"Execute", "ExecuteAsync", "Get", "Receive", "ReceiveAsync", "Sleep", "Now", "ReceiveInvalid", "ReceiveAsyncInvalid", "SleepInvalid", "ExecuteInvalid", "AsyncInvalid", "AsyncMarshal"}
	for _, overlay := range []bool{false, true} {
		for _, name := range commands {
			t.Run(fmt.Sprintf("%s/overlay_%t", name, overlay), func(t *testing.T) {
				h, _, fs := leaseHarness(t, factory)
				a := DefineActivity("A", func(context.Context, string) (string, error) { t.Error("predicate ran activity"); return "", nil })
				w := DefineWorkflow("guard", func(ctx *WorkflowContext, in string) (string, error) {
					var f *Future[string]
					if name == "Get" {
						var err error
						f, err = a.ExecuteAsync(ctx, in)
						if err != nil {
							return "", err
						}
					}
					initial := ctx.execCtx.Position()
					_ = Await(ctx, func() bool {
						if overlay && ctx.execCtx.Position() == initial {
							return false
						}
						switch name {
						case "Execute":
							_, _ = a.Execute(ctx, in)
						case "ExecuteAsync":
							_, _ = a.ExecuteAsync(ctx, in)
						case "Get":
							_, _ = f.Get(ctx)
						case "Receive":
							_, _ = Receive[int](ctx, "c")
						case "ReceiveAsync":
							_, _, _ = ReceiveAsync[int](ctx, "c")
						case "Sleep":
							_ = Sleep(ctx, 0)
						case "Now":
							_, _ = Now(ctx)
						case "ReceiveInvalid":
							_, _ = Receive[int](ctx, "")
						case "ReceiveAsyncInvalid":
							_, _, _ = ReceiveAsync[int](ctx, "")
						case "SleepInvalid":
							_ = Sleep(ctx, 0, WithSleepID("#reserved"))
						case "ExecuteInvalid":
							_, _ = a.Execute(ctx, in, WithActivityID("#reserved"))
						case "AsyncInvalid":
							_, _ = a.ExecuteAsync(ctx, in, WithActivityID("#reserved"))
						case "AsyncMarshal":
							invalid := DefineActivity("invalid", func(context.Context, chan int) (string, error) { return "", nil })
							_, _ = invalid.ExecuteAsync(ctx, make(chan int))
						}
						return true
					})
					return "ignored", nil
				})
				id := seedErrorWorkflow(t, h, w)
				var nondet *NondeterminismError
				require.ErrorAs(t, h.RunOnce(id), &nondet)
				require.Equal(t, "command issued off the workflow coroutine", nondet.Reason)
				require.NotContains(t, fs.Calls(), "CommitObservation")
				require.NotContains(t, fs.Calls(), "RecordHistory")
			})
		}
	}
	for _, kind := range []string{"nil", "panic", "invalid-channel"} {
		t.Run(kind, func(t *testing.T) {
			h, _, _ := leaseHarness(t, factory)
			w := DefineWorkflow("invalid", func(ctx *WorkflowContext, _ string) (string, error) {
				switch kind {
				case "nil":
					_ = Await(ctx, nil)
				case "panic":
					_ = Await(ctx, func() bool { panic("predicate") })
				case "invalid-channel":
					_ = Pending(ctx, "")
				}
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			err := h.RunOnce(id)
			require.Error(t, err)
			if kind == "panic" {
				var panicErr *WorkflowPanicError
				require.ErrorAs(t, err, &panicErr)
			}
			if kind == "invalid-channel" {
				require.ErrorIs(t, err, ErrInvalidSignalChannel)
			}
		})
	}
}
func TestAwaitParallelFutures(t *testing.T) { testAwaitParallelFutures(t, createTestApp) }
func testAwaitParallelFutures(t *testing.T, factory replayAppFactory) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			h, _, _ := leaseHarness(t, factory)
			var observed []string
			a := DefineActivity("A", func(_ context.Context, in string) (string, error) {
				if failed && in == "B" {
					return "", &ApplicationError{Type: "test", Message: "failure", NonRetryable: true}
				}
				return in, nil
			})
			w := DefineWorkflow("parallel", func(ctx *WorkflowContext, _ string) (string, error) {
				pending := make([]*Future[string], 0, 2)
				for _, name := range []string{"A", "B"} {
					f, err := a.ExecuteAsync(ctx, name)
					if err != nil {
						return "", err
					}
					pending = append(pending, f)
				}
				for len(pending) > 0 {
					if err := Await(ctx, func() bool {
						for _, f := range pending {
							if f.IsReady() {
								return true
							}
						}
						return false
					}); err != nil {
						return "", err
					}
					rest := pending[:0]
					for _, f := range pending {
						if !f.IsReady() {
							rest = append(rest, f)
							continue
						}
						v, e := f.Get(ctx)
						v2, e2 := f.Get(ctx)
						if v != v2 || e != e2 {
							return "", errors.New("Get changed")
						}
						if e != nil {
							var app *ApplicationError
							if !errors.As(e, &app) {
								return "", e
							}
							observed = append(observed, app.Type)
						} else {
							observed = append(observed, v)
						}
					}
					pending = rest
				}
				return "done", nil
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, h.RunTasks(id))
			require.NoError(t, h.RunOnce(id))
			assertCompletedLeaseRun(t, h, id)
			expected := []string{"A", "B"}
			if failed {
				expected[1] = "test"
			}
			require.Equal(t, expected, observed)
		})
	}
}
func TestCancelEndsBlockedWaitsAwait(t *testing.T) { testCancelEndsBlockedWaitsAwait(t, createTestApp) }
func testCancelEndsBlockedWaitsAwait(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var seen []error
	w := DefineWorkflow("cancel", func(ctx *WorkflowContext, _ string) (string, error) {
		err := Await(ctx, func() bool { return false })
		seen = append(seen, err)
		return "", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryAwaitCancelled, row.EventType)
	require.NoError(t, h.RunOnce(id))
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCancelled, inst.Status)
	// Reopen solely to force decoding the recorded cancellation in the harness.
	_, err = fs.DB().Exec(`UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1`, id)
	require.NoError(t, err)
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	require.Len(t, seen, 2)
	var cancelled *WorkflowCancelledError
	require.ErrorAs(t, seen[1], &cancelled)
	require.Equal(t, "stop", cancelled.Reason)
}

func TestAwaitPredicateBeforeCancel(t *testing.T) {
	testAwaitPredicateBeforeCancel(t, createTestApp)
}
func testAwaitPredicateBeforeCancel(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	w := DefineWorkflow("predicate-before-cancel", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		return "", Await(ctx, f.IsReady)
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NoError(t, claim.Complete(`"A"`))
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	require.NoError(t, h.RunOnce(id))
	observation, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryAwaitCompleted, observation.EventType)
	require.EqualValues(t, 2, observation.DeliveredSeq)
	activity, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, observation.Seq, activity.DeliveredSeq)
}
