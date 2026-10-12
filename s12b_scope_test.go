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

func TestCancelScopeCancelsChildren(t *testing.T) { testCancelScopeCancelsChildren(t, createTestApp) }
func testCancelScopeCancelsChildren(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	cleanup := DefineActivity("cleanup", func(context.Context, string) (string, error) { return "clean", nil })
	RegisterWorkflow(h.app, DefineWorkflow("child", func(ctx *WorkflowContext, _ string) (string, error) {
		_, err := Receive[string](ctx, "work")
		if errors.Is(err, context.Canceled) {
			if _, e := cleanup.Execute(Detached(ctx), ""); e != nil {
				return "", e
			}
		}
		return "", err
	}))
	var ready [][3]bool
	w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		scoped, cancel := WithCancel(ctx)
		nested, nestedCancel := WithCancel(scoped)
		a, err := StartChild[string](scoped.WithContext(ctx.Context()), "child", "")
		if err != nil {
			return "", err
		}
		b, err := StartChild[string](nested, "child", "")
		if err != nil {
			return "", err
		}
		d, err := StartChild[string](Detached(scoped), "child", "")
		if err != nil {
			return "", err
		}
		if _, err = Receive[int](ctx, "cancel"); err != nil {
			return "", err
		}
		ready = append(ready, [3]bool{a.IsReady(), b.IsReady(), d.IsReady()})
		cancel()
		nestedCancel()
		cancel()
		if ctx.execCtx.Position() != 5 || !a.IsReady() || !b.IsReady() || d.IsReady() {
			return "", errors.New("scope cancel did not deliver only attached children at position 5")
		}
		for _, c := range []*Child[string]{a, b} {
			_, e := c.Get(Detached(ctx))
			var durable *WorkflowCancelledError
			if !errors.Is(e, context.Canceled) || errors.As(e, &durable) {
				return "", fmt.Errorf("expected local child cancellation: %v", e)
			}
		}
		_, err = Receive[int](ctx, "finish")
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	for _, key := range []string{"#1", "#2", "#3"} {
		require.NoError(t, h.RunOnce(id+"/"+key))
	}
	require.NoError(t, h.app.Signal(h.app.ctx, id, "cancel", 1))
	require.NoError(t, h.RunOnce(id))
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#5")
	require.NoError(t, err)
	require.JSONEq(t, `{"targets":2}`, string(row.EventData))
	for _, key := range []string{"#1", "#2"} {
		row, err := fs.GetHistoryEvent(h.app.ctx, id, key)
		require.NoError(t, err)
		require.Equal(t, storage.HistoryChildCancelled, row.EventType)
		require.EqualValues(t, 5, row.DeliveredSeq)
		child, err := fs.GetInstance(h.app.ctx, id+"/"+key)
		require.NoError(t, err)
		requested, err := fs.IsCancelRequested(h.app.ctx, child.InstanceID)
		require.NoError(t, err)
		require.True(t, requested)
		require.True(t, child.Runnable)
		require.ErrorIs(t, h.RunOnce(child.InstanceID), context.Canceled)
		clean, err := fs.GetHistoryEvent(h.app.ctx, child.InstanceID, "#2")
		require.NoError(t, err)
		require.Equal(t, storage.HistoryActivityCompleted, clean.EventType)
		late, err := fs.GetHistoryEvent(h.app.ctx, id, key)
		require.NoError(t, err)
		require.Equal(t, row, late)
	}
	d, err := fs.GetInstance(h.app.ctx, id+"/#3")
	require.NoError(t, err)
	requested, err := fs.IsCancelRequested(h.app.ctx, d.InstanceID)
	require.NoError(t, err)
	require.False(t, requested)
	require.False(t, d.Runnable)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, [][3]bool{{false, false, false}, {false, false, false}}, ready)
	assertCompletedLeaseRun(t, h, id)
	d, err = fs.GetInstance(h.app.ctx, id+"/#3")
	require.NoError(t, err)
	requested, err = fs.IsCancelRequested(h.app.ctx, d.InstanceID)
	require.NoError(t, err)
	require.True(t, requested, "parent close still cancels detached children")
}

func TestStartChildInCancelledScope(t *testing.T) { testStartChildInCancelledScope(t, createTestApp) }
func testStartChildInCancelledScope(t *testing.T, factory replayAppFactory) {
	for _, ownDetached := range []bool{false, true} {
		t.Run(fmt.Sprintf("detached_%t", ownDetached), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var positions []int64
			w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
				base := ctx
				if ownDetached {
					base = Detached(ctx)
				}
				scoped, cancel := WithCancel(base)
				nested, _ := WithCancel(scoped)
				cancel()
				c, err := StartChild[string](nested, "never", "", WithChildID("cancelled-child"))
				if err != nil {
					return "", err
				}
				if !c.IsReady() {
					return "", errors.New("cancelled-at-issue child is not ready")
				}
				_, err = c.Get(ctx)
				var durable *WorkflowCancelledError
				if !errors.Is(err, context.Canceled) || errors.As(err, &durable) {
					return "", fmt.Errorf("expected local cancellation: %v", err)
				}
				if err = c.Cancel(ctx); err != nil {
					return "", err
				}
				positions = append(positions, ctx.execCtx.Position())
				_, err = Receive[int](ctx, "finish")
				return "done", err
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#2")
			require.NoError(t, err)
			require.Equal(t, storage.HistoryChildCancelled, row.EventType)
			require.EqualValues(t, 2, row.DeliveredSeq)
			require.JSONEq(t, `{"child_id":"cancelled-child","workflow":"never"}`, string(row.ReservationData))
			child, err := fs.GetInstance(h.app.ctx, "cancelled-child")
			require.NoError(t, err)
			require.Nil(t, child)
			require.NotContains(t, fs.Calls(), "StartChild")
			require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, []int64{3, 3}, positions)
			assertCompletedLeaseRun(t, h, id)
		})
	}
}

func TestDetachedChildGetIgnoresParentCancel(t *testing.T) {
	testDetachedChildGetIgnoresParentCancel(t, createTestApp)
}
func testDetachedChildGetIgnoresParentCancel(t *testing.T, factory replayAppFactory) {
	for _, detached := range []bool{true, false} {
		t.Run(fmt.Sprintf("issued_detached_%t", detached), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			RegisterWorkflow(h.app, DefineWorkflow("child", func(*WorkflowContext, string) (string, error) { return "result", nil }))
			w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
				issuing, get := ctx, Detached(ctx)
				if detached {
					issuing, get = Detached(ctx), ctx
				}
				c, err := StartChild[string](issuing, "child", "")
				if err != nil {
					return "", err
				}
				v, err := c.Get(get)
				if detached {
					return v, err
				}
				if !errors.Is(err, context.Canceled) {
					return "", fmt.Errorf("attached child escaped cancellation: %v", err)
				}
				_, err = Receive[int](Detached(ctx), "finish")
				return "done", err
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			require.NoError(t, h.RunOnce(id))
			parent, err := fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, storage.StatusRunning, parent.Status)
			child, err := fs.GetInstance(h.app.ctx, id+"/#1")
			require.NoError(t, err)
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			if detached {
				requested, err := fs.IsCancelRequested(h.app.ctx, child.InstanceID)
				require.NoError(t, err)
				require.False(t, requested)
				require.Nil(t, row.EventData)
				require.NotContains(t, fs.Calls(), "CancelChild")
				require.NoError(t, h.RunOnce(child.InstanceID))
			} else {
				requested, err := fs.IsCancelRequested(h.app.ctx, child.InstanceID)
				require.NoError(t, err)
				require.True(t, requested)
				require.Equal(t, storage.HistoryChildCancelled, row.EventType)
				require.Zero(t, row.DeliveredSeq)
				var durable *WorkflowCancelledError
				require.ErrorAs(t, decodeRecordedError(row), &durable)
				require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
			}
			require.NoError(t, h.RunOnce(id))
			parent, err = fs.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, storage.StatusCompleted, parent.Status)
		})
	}
}

func TestCancelScopeChildrenStoredResultWins(t *testing.T) {
	testCancelScopeChildrenStoredResultWins(t, createTestApp)
}
func testCancelScopeChildrenStoredResultWins(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	RegisterWorkflow(h.app, DefineWorkflow("child", func(*WorkflowContext, string) (string, error) { return "winner", nil }))
	var ready [][2]bool
	w := DefineWorkflow("parent", func(ctx *WorkflowContext, _ string) (string, error) {
		scoped, cancel := WithCancel(ctx)
		winner, err := StartChild[string](scoped, "child", "")
		if err != nil {
			return "", err
		}
		pending, err := StartChild[string](scoped, "child", "")
		if err != nil {
			return "", err
		}
		cancel()
		ready = append(ready, [2]bool{winner.IsReady(), pending.IsReady()})
		if winner.IsReady() || !pending.IsReady() {
			return "", errors.New("scope cancel changed stored-result delivery")
		}
		v, err := winner.Get(Detached(ctx))
		if err != nil {
			return "", err
		}
		if v != "winner" {
			return "", errors.New("stored result lost")
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	injected := false
	fs.Before = func(_ context.Context, method string) error {
		if method == "CommitObservation" && !injected {
			injected = true
			return h.RunOnce(id + "/#1")
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	fs.Before = nil
	require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, [][2]bool{{false, true}, {false, true}}, ready)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryChildCompleted, row.EventType)
	require.Zero(t, row.DeliveredSeq)
}
