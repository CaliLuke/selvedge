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

func TestChildResultDelivery(t *testing.T) { testChildResultDelivery(t, createTestApp) }
func testChildResultDelivery(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"success", "failure", "panic", "nondeterminism", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			child := DefineWorkflow("child", func(ctx *WorkflowContext, in string) (string, error) {
				switch kind {
				case "failure":
					return "", &ApplicationError{Type: "child_failure", Message: "child failed"}
				case "panic":
					panic("child panic")
				case "nondeterminism":
					return "", &NondeterminismError{Reason: "child mismatch"}
				case "cancelled":
					return "", &WorkflowCancelledError{InstanceID: ctx.InstanceID(), Reason: "child cancel"}
				}
				return in + " result", nil
			})
			RegisterWorkflow(h.app, child)
			var outcomes []error
			var values []string
			var ready []bool
			parent := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
				c, err := StartChild[string](ctx, "child", "input")
				if err != nil {
					return "", err
				}
				if c.ID() != ctx.InstanceID()+"/#1" || c.Key() != "#1" {
					return "", errors.New("wrong child identity")
				}
				ready = append(ready, c.IsReady())
				if err := Await(ctx, c.IsReady); err != nil {
					return "", err
				}
				ready = append(ready, c.IsReady())
				v, e := c.Get(ctx)
				v2, e2 := c.Get(ctx)
				if v != v2 || e != e2 {
					return "", errors.New("Get did not cache outcome")
				}
				values = append(values, v)
				outcomes = append(outcomes, e)
				return "", Sleep(ctx, time.Hour)
			})
			id := seedErrorWorkflow(t, h, parent)
			require.NoError(t, h.RunOnce(id))
			if kind == "cancelled" {
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id+"/#1", "child cancel"))
			}
			childErr := h.RunOnce(id + "/#1")
			if kind == "success" {
				require.NoError(t, childErr)
			} else {
				require.Error(t, childErr)
			}
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			require.NotNil(t, row.EventData)
			require.Zero(t, row.DeliveredSeq)
			if kind == "cancelled" {
				childInstance, err := fs.GetInstance(h.app.ctx, id+"/#1")
				require.NoError(t, err)
				require.Equal(t, storage.StatusCancelled, childInstance.Status)
				require.Equal(t, storage.HistoryChildCancelled, row.EventType)
			}
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, []bool{false, false, true, false, true}, ready)
			require.Len(t, outcomes, 2)
			switch kind {
			case "success":
				require.Equal(t, []string{"input result", "input result"}, values)
				require.NoError(t, outcomes[0])
			case "failure":
				var target *ApplicationError
				require.ErrorAs(t, outcomes[0], &target)
				require.Equal(t, "child failed", target.Message)
			case "panic":
				var target *WorkflowPanicError
				require.ErrorAs(t, outcomes[0], &target)
			case "nondeterminism":
				var target *NondeterminismError
				require.ErrorAs(t, outcomes[0], &target)
			case "cancelled":
				require.ErrorIs(t, outcomes[0], context.Canceled)
			}
			row, err = fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			require.EqualValues(t, 2, row.DeliveredSeq)
		})
	}
}
func TestChildIsReadyReadsDeliveredView(t *testing.T) {
	testChildIsReadyReadsDeliveredView(t, createTestApp)
}
func testChildIsReadyReadsDeliveredView(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var ready []bool
	RegisterWorkflow(h.app, DefineWorkflow("child", func(*WorkflowContext, string) (string, error) { return "done", nil }))
	w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		c, err := StartChild[string](ctx, "child", "")
		if err != nil {
			return "", err
		}
		ready = append(ready, c.IsReady())
		v, err := c.Get(ctx)
		if err != nil {
			return "", err
		}
		ready = append(ready, c.IsReady())
		return v, Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.RunOnce(id+"/#1"))
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []bool{false, false, true, false, true}, ready)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Zero(t, row.DeliveredSeq)
	require.NotContains(t, fs.Calls(), "CommitObservation")
}
func TestChildCancelCommand(t *testing.T) { testChildCancelCommand(t, createTestApp) }
func testChildCancelCommand(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		c, err := StartChild[string](ctx, "child", "")
		if err != nil {
			return "", err
		}
		if err = c.Cancel(ctx); err != nil {
			return "", err
		}
		p := ctx.execCtx.Position()
		if err = c.Cancel(ctx); err != nil {
			return "", err
		}
		if ctx.execCtx.Position() != p || !c.IsReady() {
			return "", errors.New("cancel not immediate or repeated command")
		}
		_, err = c.Get(ctx)
		if !errors.Is(err, context.Canceled) {
			return "", fmt.Errorf("expected child cancellation: %v", err)
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryChildCancelled, row.EventType)
	require.EqualValues(t, 2, row.DeliveredSeq)
	row, err = fs.GetHistoryEvent(h.app.ctx, id, "#2")
	require.NoError(t, err)
	require.Equal(t, "child_cancel", row.Command)
	require.EqualValues(t, 2, row.DeliveredSeq)
	requested, err := fs.IsCancelRequested(h.app.ctx, id+"/#1")
	require.NoError(t, err)
	require.True(t, requested)
}
func TestCancelEndsBlockedWaitsChild(t *testing.T) { testCancelEndsBlockedWaitsChild(t, createTestApp) }
func testCancelEndsBlockedWaitsChild(t *testing.T, factory replayAppFactory) {
	for _, resultFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("result_first_%t", resultFirst), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var waits []replay.Wait
			h.app.ctx = replay.WithTestYieldObserver(h.app.ctx, func(w replay.Wait) { waits = append(waits, w) })
			RegisterWorkflow(h.app, DefineWorkflow("child", func(*WorkflowContext, string) (string, error) { return "late", nil }))
			w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
				c, e := StartChild[string](ctx, "child", "")
				if e != nil {
					return "", e
				}
				if _, e = NewTimer(ctx, time.Hour); e != nil {
					return "", e
				}
				return c.Get(ctx)
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, []replay.Wait{{Seq: 2, Kind: replay.WaitChild, Key: "#1"}}, waits)
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			if resultFirst {
				fs.Before = func(_ context.Context, method string) error {
					if method == "CancelChild" {
						return h.RunOnce(id + "/#1")
					}
					return nil
				}
				require.NoError(t, h.RunOnce(id))
			} else {
				require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			}
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			expectedEvent, expectedStatus := storage.HistoryChildCancelled, storage.StatusCancelled
			if resultFirst {
				expectedEvent, expectedStatus = storage.HistoryChildCompleted, storage.StatusCompleted
			}
			require.Equal(t, expectedEvent, row.EventType)
			require.Zero(t, row.DeliveredSeq)
			fs.Before = nil
			require.NoError(t, h.RunOnce(id+"/#1"))
			late, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			require.Equal(t, row.EventData, late.EventData)
			inst, err := fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, expectedStatus, inst.Status)
		})
	}
}

func TestChildStartAtomic(t *testing.T) { testChildStartAtomic(t, createTestApp) }
func testChildStartAtomic(t *testing.T, factory replayAppFactory) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_%t", after), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
				c, e := StartChild[string](ctx, "child", "")
				if e != nil {
					return "", e
				}
				return c.Get(ctx)
			})
			id := seedErrorWorkflow(t, h, w)
			injected := false
			inject := func(_ context.Context, m string) error {
				if m == "StartChild" && !injected {
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
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			child, err := fs.GetInstance(h.app.ctx, id+"/#1")
			require.NoError(t, err)
			require.Equal(t, after, row != nil)
			require.Equal(t, after, child != nil)
			fs.After = nil
			fs.Before = nil
			setStoreClock(t, h, 4000)
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			require.NoError(t, h.RunOnce(id))
			var n int
			require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_instances WHERE parent_instance_id=$1`, id).Scan(&n))
			require.Equal(t, 1, n)
		})
	}
	t.Run("duplicate_replay", func(t *testing.T) {
		h, _, fs := leaseHarness(t, factory)
		require.NoError(t, fs.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: "foreign", WorkflowName: "foreign", Status: storage.StatusRunning}))
		var outcomes []error
		w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
			_, err := StartChild[string](ctx, "child", "", WithChildID("foreign"))
			outcomes = append(outcomes, err)
			return "", Sleep(ctx, time.Hour)
		})
		id := seedErrorWorkflow(t, h, w)
		require.NoError(t, h.RunOnce(id))
		require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
		require.NoError(t, h.RunOnce(id))
		require.Len(t, outcomes, 2)
		for _, e := range outcomes {
			require.ErrorIs(t, e, ErrWorkflowAlreadyStarted)
			require.Equal(t, ErrWorkflowAlreadyStarted, e)
		}
	})
}
func TestParentCloseCancelsRunningChild(t *testing.T) {
	testParentCloseCancelsRunningChild(t, createTestApp)
}
func testParentCloseCancelsRunningChild(t *testing.T, factory replayAppFactory) {
	for _, status := range []string{"completed", "failed", "cancelled"} {
		for _, cut := range []string{"none", "before", "after"} {
			t.Run(status+"/"+cut, func(t *testing.T) {
				h, _, fs := leaseHarness(t, factory)
				RegisterWorkflow(h.app, DefineWorkflow("child", func(ctx *WorkflowContext, _ string) (string, error) { return Receive[string](ctx, "wait") }))
				w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
					_, e := StartChild[string](ctx, "child", "")
					if e != nil {
						return "", e
					}
					_, e = Receive[string](ctx, "close")
					if e != nil {
						return "", e
					}
					switch status {
					case "failed":
						return "", errors.New("parent failure")
					case "cancelled":
						return "", &WorkflowCancelledError{InstanceID: ctx.InstanceID()}
					}
					return "done", nil
				})
				id := seedErrorWorkflow(t, h, w)
				require.NoError(t, h.RunOnce(id))
				require.NoError(t, h.RunOnce(id+"/#1"))
				require.NoError(t, h.app.Signal(h.app.ctx, id, "close", "go"))
				if status == "cancelled" {
					require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
				}
				injected := false
				inject := func(_ context.Context, m string) error {
					if m == "CloseInstance" && !injected {
						injected = true
						return storage.ErrFenced
					}
					return nil
				}
				if cut == "before" {
					fs.Before = inject
				}
				if cut == "after" {
					fs.After = inject
				}
				_ = h.RunOnce(id)
				parent, err := fs.GetInstance(h.app.ctx, id)
				require.NoError(t, err)
				requested, err := fs.IsCancelRequested(h.app.ctx, id+"/#1")
				require.NoError(t, err)
				require.Equal(t, parent.Status != storage.StatusRunning, requested)
				if cut != "none" {
					require.True(t, injected)
				}
				fs.Before = nil
				fs.After = nil
				setStoreClock(t, h, 4000)
				require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
				_ = h.RunOnce(id)
				parent, err = fs.GetInstance(h.app.ctx, id)
				require.NoError(t, err)
				require.Equal(t, storage.WorkflowStatus(status), parent.Status)
				if status == "cancelled" {
					closed, err := fs.GetHistoryEvent(h.app.ctx, id, "$workflow_failed")
					require.NoError(t, err)
					require.Equal(t, storage.HistoryWorkflowCancelled, closed.EventType)
				}
				child, err := fs.GetInstance(h.app.ctx, id+"/#1")
				require.NoError(t, err)
				require.True(t, child.Runnable)
				require.Equal(t, "parent closed", child.CancelReason)
				require.ErrorIs(t, h.RunOnce(id+"/#1"), context.Canceled)
				child, err = fs.GetInstance(h.app.ctx, id+"/#1")
				require.NoError(t, err)
				require.Equal(t, storage.StatusCancelled, child.Status)
			})
		}
	}
}

func TestChildGetRequestsCancelWhileParentRunning(t *testing.T) {
	testChildGetRequestsCancelWhileParentRunning(t, createTestApp)
}

func testChildGetRequestsCancelWhileParentRunning(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	caught := make(chan error, 1)
	resume := make(chan struct{})
	RegisterWorkflow(h.app, DefineWorkflow("child", func(ctx *WorkflowContext, _ string) (string, error) {
		return Receive[string](ctx, "wait")
	}))
	w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		child, err := StartChild[string](ctx, "child", "")
		if err != nil {
			return "", err
		}
		_, err = child.Get(ctx)
		caught <- err
		<-resume
		return "", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.RunOnce(id+"/#1"))
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	defer func() {
		close(resume)
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Error("parent did not finish after release")
		}
	}()
	select {
	case err := <-caught:
		var cancelled *WorkflowCancelledError
		require.ErrorAs(t, err, &cancelled)
		require.Equal(t, "stop", cancelled.Reason)
		require.Equal(t, id, cancelled.InstanceID)
	case <-time.After(5 * time.Second):
		t.Fatal("Get did not observe durable cancellation")
	}
	parent, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, parent.Status)
	requested, err := fs.IsCancelRequested(h.app.ctx, id+"/#1")
	require.NoError(t, err)
	require.True(t, requested, "Get must request child cancellation before the parent closes")
	child, err := fs.GetInstance(h.app.ctx, id+"/#1")
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, child.Status)
	require.True(t, child.Runnable)
	require.Equal(t, "parent cancelled child", child.CancelReason)
	require.NotEqual(t, "parent closed", child.CancelReason)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryChildCancelled, row.EventType)
	require.Zero(t, row.DeliveredSeq)
}
