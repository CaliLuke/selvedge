package selvedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestCancelScopeTimer(t *testing.T) { testCancelScopeTimer(t, createTestApp) }
func testCancelScopeTimer(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var observed []error
	w := DefineWorkflow("scope-timer", func(ctx *WorkflowContext, _ string) (string, error) {
		scoped, cancel := WithCancel(ctx)
		timer, err := NewTimer(scoped, time.Hour)
		if err != nil {
			return "", err
		}
		cancel()
		if !timer.IsReady() {
			return "", errors.New("cancelled timer is not delivered")
		}
		_, err = timer.Get(ctx)
		observed = append(observed, err)
		if !errors.Is(err, context.Canceled) {
			return "", errors.New("timer did not cancel")
		}
		_, err = Receive[int](ctx, "finish")
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Len(t, observed, 1)
	var n int
	require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1`, id).Scan(&n))
	require.Zero(t, n)
	require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1"}))
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.False(t, inst.Runnable)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
	require.NoError(t, h.RunOnce(id))
	require.Len(t, observed, 2)
	assertCompletedLeaseRun(t, h, id)
}

func TestCommandsInCancelledScopeRecordAtIssue(t *testing.T) {
	testCommandsInCancelledScopeRecordAtIssue(t, createTestApp)
}
func testCommandsInCancelledScopeRecordAtIssue(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	calls := 0
	a := DefineActivity("never", func(context.Context, string) (string, error) { calls++; return "bad", nil })
	var positions []int64
	w := DefineWorkflow("cancelled-issue", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		cancel()
		cancel()
		check := func(err error) error {
			if !errors.Is(err, context.Canceled) {
				return errors.New("missing scope cancellation")
			}
			var durable *WorkflowCancelledError
			if errors.As(err, &durable) {
				return errors.New("local cancellation is durable")
			}
			return nil
		}
		f, err := a.ExecuteAsync(c, "")
		if err != nil {
			return "", err
		}
		if !f.IsReady() {
			return "", errors.New("activity not ready")
		}
		_, err = f.Get(ctx)
		if e := check(err); e != nil {
			return "", e
		}
		timer, err := NewTimer(c, 0)
		if err != nil {
			return "", err
		}
		if !timer.IsReady() {
			return "", errors.New("timer not ready")
		}
		_, err = timer.Get(ctx)
		if e := check(err); e != nil {
			return "", e
		}
		if e := check(Sleep(c, 0)); e != nil {
			return "", e
		}
		_, err = Receive[int](c, "c")
		if e := check(err); e != nil {
			return "", e
		}
		_, err = ReceiveWithTimeout[int](c, "c", time.Hour)
		if e := check(err); e != nil {
			return "", e
		}
		if e := check(Await(c, func() bool { return false })); e != nil {
			return "", e
		}
		if err := Await(c, func() bool { _ = Detached(c); _, _ = WithCancel(c); return true }); err != nil {
			return "", err
		}
		_, err = ReceiveWithTimeout[int](c, "c", 0)
		if !errors.Is(err, context.DeadlineExceeded) {
			return "", errors.New("nonpositive timeout lost")
		}
		if _, err = Now(c); err != nil {
			return "", err
		}
		if Pending(c, "c") != 1 {
			return "", errors.New("pending signal was consumed")
		}
		_, ok, err := ReceiveAsync[int](c, "c")
		if err != nil || !ok {
			return "", errors.New("async must ignore cancellation")
		}
		positions = append(positions, ctx.execCtx.Position())
		_, err = Receive[int](ctx, "finish")
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 7))
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []int64{11, 11}, positions)
	require.Zero(t, calls)
	require.NotContains(t, fs.Calls(), "ReserveActivity")
	require.NotContains(t, fs.Calls(), "ReserveTimer")
	for _, table := range []string{"workflow_activity_tasks", "workflow_timer_subscriptions"} {
		var n int
		require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE instance_id=$1`, id).Scan(&n))
		require.Zero(t, n)
	}
}

func TestDetachedCleanupRunsAfterCancel(t *testing.T) {
	testDetachedCleanupRunsAfterCancel(t, createTestApp)
}
func testDetachedCleanupRunsAfterCancel(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"activity", "sleep", "await", "root"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			calls := 0
			a := DefineActivity("cleanup", func(context.Context, string) (string, error) { calls++; return "clean", nil })
			announce := DefineActivity("announce", func(context.Context, string) (string, error) { return "announced", nil })
			w := DefineWorkflow("cleanup", func(ctx *WorkflowContext, _ string) (string, error) {
				if _, err := announce.Execute(ctx, ""); err != nil {
					return "", err
				}
				_, cancelErr := Receive[int](ctx, "cancel")
				if cancelErr == nil {
					return "", errors.New("expected cancellation")
				}
				cleanup := Detached(ctx)
				if kind == "root" {
					cleanup = ctx
				}
				switch kind {
				case "sleep":
					if err := Sleep(cleanup, time.Hour); err != nil {
						return "", err
					}
				case "await":
					if err := Await(cleanup, func() bool { return Pending(cleanup, "finish") > 0 }); err != nil {
						return "", err
					}
				default:
					_, err := a.Execute(cleanup, "")
					if kind != "root" && err != nil {
						return "", err
					}
				}
				return "", cancelErr
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			err := h.RunOnce(id)
			if kind == "sleep" || kind == "await" {
				require.NoError(t, err)
				inst, e := fs.GetInstance(h.app.ctx, id)
				require.NoError(t, e)
				require.Equal(t, storage.StatusRunning, inst.Status)
				if kind == "sleep" {
					require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#3"}))
				} else {
					require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
				}
				err = h.RunOnce(id)
			}
			require.ErrorIs(t, err, context.Canceled)
			if kind == "activity" {
				require.Equal(t, 1, calls)
				row, e := fs.GetHistoryEvent(h.app.ctx, id, "#3")
				require.NoError(t, e)
				require.Equal(t, storage.HistoryActivityCompleted, row.EventType)
			}
			if kind == "root" {
				require.Zero(t, calls)
				row, e := fs.GetHistoryEvent(h.app.ctx, id, "#3")
				require.NoError(t, e)
				require.Equal(t, storage.HistoryActivityCancelled, row.EventType)
			}
			inst, e := fs.GetInstance(h.app.ctx, id)
			require.NoError(t, e)
			require.Equal(t, storage.StatusCancelled, inst.Status)
			_, e = fs.DB().Exec(`UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1`, id)
			require.NoError(t, e)
			require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			if kind == "activity" {
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestNestedScopesAndDetached(t *testing.T) { testNestedScopesAndDetached(t, createTestApp) }
func testNestedScopesAndDetached(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("nested", func(ctx *WorkflowContext, _ string) (string, error) {
		parent, cancel := WithCancel(ctx)
		child, cancelChild := WithCancel(parent)
		detached, ownCancel := WithCancel(Detached(parent))
		a, err := NewTimer(child, time.Hour)
		if err != nil {
			return "", err
		}
		b, err := NewTimer(detached, time.Hour)
		if err != nil {
			return "", err
		}
		cancel()
		cancelChild()
		if !a.IsReady() || b.IsReady() {
			return "", errors.New("scope ancestry wrong")
		}
		if child.Err() != nil {
			return "", errors.New("scope changed Go context")
		}
		select {
		case <-child.Done():
			return "", errors.New("scope closed Go context")
		default:
		}
		ownCancel()
		if !b.IsReady() {
			return "", errors.New("detached child not cancelled")
		}
		_, err = b.Get(ctx)
		if !errors.Is(err, context.Canceled) {
			return "", errors.New("detached own cancellation lost")
		}
		copyCtx := child.WithContext(child.Context())
		if err = Sleep(copyCtx, 0); !errors.Is(err, context.Canceled) {
			return "", errors.New("WithContext lost scope")
		}
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	require.Len(t, rows, 5)
}

func TestScopeCancelErrorTyped(t *testing.T) { testScopeCancelErrorTyped(t, createTestApp) }
func testScopeCancelErrorTyped(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("scope-error", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		cancel()
		return "", Sleep(c, 0)
	})
	id := seedErrorWorkflow(t, h, w)
	for i := 0; i < 2; i++ {
		err := h.RunOnce(id)
		require.ErrorIs(t, err, context.Canceled)
		var durable *WorkflowCancelledError
		require.False(t, errors.As(err, &durable))
		inst, e := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, e)
		require.Equal(t, storage.StatusFailed, inst.Status)
		if i == 0 {
			_, e = fs.DB().Exec(`UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1`, id)
			require.NoError(t, e)
		}
	}
}

func TestCancelFuncGuards(t *testing.T) { testCancelFuncGuards(t, createTestApp) }
func testCancelFuncGuards(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"empty", "predicate", "foreign", "foreign-create", "defer"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			w := DefineWorkflow("scope-guard", func(ctx *WorkflowContext, _ string) (string, error) {
				_, cancel := WithCancel(ctx)
				switch kind {
				case "empty":
					cancel()
					cancel()
				case "predicate":
					_ = Await(ctx, func() bool { cancel(); return true })
				case "foreign", "foreign-create":
					done := make(chan struct{})
					go func() {
						defer close(done)
						if kind == "foreign-create" {
							_, foreignCancel := WithCancel(Detached(ctx))
							foreignCancel()
						} else {
							cancel()
						}
					}()
					<-done
				case "defer":
					defer cancel()
					return "", Sleep(ctx, time.Hour)
				}
				return "done", nil
			})
			id := seedErrorWorkflow(t, h, w)
			err := h.RunOnce(id)
			if kind == "predicate" || kind == "foreign" || kind == "foreign-create" {
				var nondet *NondeterminismError
				require.ErrorAs(t, err, &nondet)
			} else {
				require.NoError(t, err)
			}
			var n int
			require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_history WHERE instance_id=$1 AND command='cancel_scope'`, id).Scan(&n))
			if kind == "empty" {
				require.Equal(t, 1, n)
			} else {
				require.Zero(t, n)
			}
		})
	}
}

func TestFutureCancellationFollowsIssuingScope(t *testing.T) {
	testFutureCancellationFollowsIssuingScope(t, createTestApp)
}
func testFutureCancellationFollowsIssuingScope(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"local", "root", "detached"} {
		for _, timerKind := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "-activity", true: "-timer"}[timerKind], func(t *testing.T) {
				h, _, fs := leaseHarness(t, factory)
				a := DefineActivity("A", func(context.Context, string) (string, error) { return "value", nil })
				w := DefineWorkflow("ownership", func(ctx *WorkflowContext, _ string) (string, error) {
					c, cancel := WithCancel(ctx)
					getCtx := ctx
					switch kind {
					case "root":
						c = ctx
						getCtx = Detached(ctx)
					case "detached":
						c = Detached(ctx)
					}
					var err error
					if timerKind {
						timer, e := NewTimer(c, time.Hour)
						if e != nil {
							return "", e
						}
						if kind == "local" {
							cancel()
						}
						_, err = timer.Get(getCtx)
					} else {
						f, e := a.ExecuteAsync(c, "")
						if e != nil {
							return "", e
						}
						if kind == "local" {
							cancel()
						}
						_, err = f.Get(getCtx)
					}
					if kind == "detached" {
						return "done", err
					}
					if !errors.Is(err, context.Canceled) {
						return "", errors.New("issuing cancellation lost")
					}
					return "done", nil
				})
				id := seedErrorWorkflow(t, h, w)
				if kind != "local" {
					require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
				}
				require.NoError(t, h.RunOnce(id))
				if timerKind && kind == "detached" {
					inst, e := fs.GetInstance(h.app.ctx, id)
					require.NoError(t, e)
					require.Equal(t, storage.StatusRunning, inst.Status)
					require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1"}))
					require.NoError(t, h.RunOnce(id))
				}
				assertCompletedLeaseRun(t, h, id)
			})
		}
	}
}

func TestCancelEndsBlockedWaitsScopes(t *testing.T) {
	testCancelEndsBlockedWaitsScopes(t, createTestApp)
}
func testCancelEndsBlockedWaitsScopes(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"sleep", "receive", "await", "timer", "future", "timeout"} {
		for _, detached := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "-attached", true: "-detached"}[detached], func(t *testing.T) {
				h, _, fs := leaseHarness(t, factory)
				a := DefineActivity("A", func(context.Context, string) (string, error) { return "value", nil })
				w := DefineWorkflow("blocked", func(ctx *WorkflowContext, _ string) (string, error) {
					c, _ := WithCancel(ctx)
					if detached {
						c = Detached(c)
					}
					var err error
					switch kind {
					case "sleep":
						err = Sleep(c, time.Hour)
					case "receive":
						_, err = Receive[int](c, "c")
					case "await":
						err = Await(c, func() bool { return Pending(c, "c") > 0 })
					case "timer":
						var tm *Timer
						tm, err = NewTimer(c, time.Hour)
						if err == nil {
							_, err = tm.Get(ctx)
						}
					case "future":
						var f *Future[string]
						f, err = a.ExecuteAsync(c, "")
						if err == nil {
							_, err = f.Get(ctx)
						}
					case "timeout":
						_, err = ReceiveWithTimeout[int](c, "c", time.Hour)
					}
					return "done", err
				})
				id := seedErrorWorkflow(t, h, w)
				if kind == "future" {
					seedTask(t, h, id)
					claim, e := h.ClaimTask(id, "#1")
					require.NoError(t, e)
					require.NotNil(t, claim)
				}
				require.NoError(t, h.RunOnce(id))
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
				err := h.RunOnce(id)
				inst, e := fs.GetInstance(h.app.ctx, id)
				require.NoError(t, e)
				if detached {
					require.NoError(t, err)
					require.Equal(t, storage.StatusRunning, inst.Status)
				} else {
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, storage.StatusCancelled, inst.Status)
				}
			})
		}
	}
}

func TestReplayCommandMismatchFailsFastScope(t *testing.T) {
	testReplayCommandMismatchFailsFastScope(t, createTestApp)
}
func testReplayCommandMismatchFailsFastScope(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"removed", "inserted", "sleep"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			changed := false
			a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
			w := DefineWorkflow("mismatch-scope", func(ctx *WorkflowContext, _ string) (string, error) {
				_, cancel := WithCancel(ctx)
				if kind == "removed" {
					if !changed {
						cancel()
					}
					return "done", nil
				}
				if changed {
					cancel()
				}
				if kind == "sleep" {
					return "", Sleep(ctx, 0)
				}
				return a.Execute(ctx, "")
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			changed = true
			_, err := fs.DB().Exec(`UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1`, id)
			require.NoError(t, err)
			var nondet *NondeterminismError
			require.ErrorAs(t, h.RunOnce(id), &nondet)
			require.EqualValues(t, 1, nondet.Seq)
		})
	}
}

func TestScopeCancelPreservesTimerStartOnReplay(t *testing.T) {
	testScopeCancelPreservesTimerStartOnReplay(t, createTestApp)
}
func testScopeCancelPreservesTimerStartOnReplay(t *testing.T, factory replayAppFactory) {
	observed := &historyMismatchHooks{}
	wrapped := func(t *testing.T, opts ...Option) (*App, func()) {
		return factory(t, append(opts, WithHooks(observed))...)
	}
	h, _, _ := leaseHarness(t, wrapped)
	w := DefineWorkflow("timer-hooks", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		if _, err := NewTimer(c, time.Hour); err != nil {
			return "", err
		}
		cancel()
		_, err := Receive[int](ctx, "finish")
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 1, observed.starts)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 2, observed.starts)
}

func TestScopeCancelTracksSleep(t *testing.T) { testScopeCancelTracksSleep(t, createTestApp) }
func testScopeCancelTracksSleep(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("sleep-target", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		if err := Sleep(c, 0); err != nil {
			return "", err
		}
		cancel()
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.JSONEq(t, `{"targets":1}`, string(row.EventData))
	row, err = fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryTimerExpired, row.EventType)
	require.EqualValues(t, 1, row.DeliveredSeq)
}

func TestScopeTargetsOnlyCancellableScopes(t *testing.T) {
	testScopeTargetsOnlyCancellableScopes(t, createTestApp)
}
func testScopeTargetsOnlyCancellableScopes(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	var root, detached, parent, child, detachedChild *WorkflowContext
	w := DefineWorkflow("scope-target-retention", func(ctx *WorkflowContext, _ string) (string, error) {
		root = ctx
		parent, _ = WithCancel(root)
		child, _ = WithCancel(parent)
		detached = Detached(parent)
		detachedChild, _ = WithCancel(detached)
		for _, scoped := range []*WorkflowContext{root, parent, child, detached, detachedChild} {
			if _, err := NewTimer(scoped, 0); err != nil {
				return "", err
			}
		}
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Empty(t, root.scope.targets)
	require.Empty(t, detached.scope.targets)
	require.Equal(t, []storage.ScopeTarget{{Key: "#2", Command: "sleep", Kind: "timer"}, {Key: "#3", Command: "sleep", Kind: "timer"}}, parent.scope.targets)
	require.Equal(t, []storage.ScopeTarget{{Key: "#3", Command: "sleep", Kind: "timer"}}, child.scope.targets)
	require.Equal(t, []storage.ScopeTarget{{Key: "#5", Command: "sleep", Kind: "timer"}}, detachedChild.scope.targets)
}
