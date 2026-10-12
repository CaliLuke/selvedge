package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestQueryReadsDeliveredView(t *testing.T) { s14TestQueryReadsDeliveredView(t, createTestApp) }
func s14TestQueryReadsDeliveredView(t *testing.T, factory replayAppFactory) {
	t.Run("signal", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			own := Pending(ctx, "ch")
			_ = SetQueryHandler(ctx, "pending", func() ([]int, error) { return []int{own, Pending(ctx, "ch")}, nil })
			if err := Await(ctx, func() bool { return Pending(ctx, "ch") > 0 }); err != nil {
				return "", err
			}
			own = Pending(ctx, "ch")
			return Receive[string](ctx, "stop")
		})
		require.NoError(t, h.RunOnce(id))
		require.NoError(t, h.app.Signal(context.Background(), id, "ch", 7))
		require.Equal(t, []int{0, 0}, s14Query[[]int](t, h, id, "pending"))
		require.NoError(t, h.RunOnce(id))
		require.Equal(t, []int{1, 1}, s14Query[[]int](t, h, id, "pending"))
	})

	for _, waitKind := range []string{"sleep", "timer_get", "future_get"} {
		t.Run("ready_after_wake/"+waitKind, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
			b := DefineActivity("B", func(context.Context, string) (string, error) { return "B", nil })
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				f, err := a.ExecuteAsync(ctx, "")
				if err != nil {
					return "", err
				}
				switch waitKind {
				case "sleep":
					err = Sleep(ctx, time.Hour)
				case "timer_get":
					tm, e := NewTimer(ctx, time.Hour)
					if e != nil {
						return "", e
					}
					_, err = tm.Get(ctx)
				case "future_get":
					fb, e := b.ExecuteAsync(ctx, "")
					if e != nil {
						return "", e
					}
					_, err = fb.Get(ctx)
				}
				if err != nil {
					return "", err
				}
				_ = SetQueryHandler(ctx, "ready", func() (bool, error) { return f.IsReady(), nil })
				return Receive[string](ctx, "stop")
			})
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, h.RunTasks(id, "A"))
			if waitKind != "future_get" {
				require.NoError(t, h.app.storage.FireTimer(context.Background(), &storage.TimerSubscription{InstanceID: id, TimerID: "#2"}))
			}
			require.False(t, s14Query[bool](t, h, id, "ready"))
		})
	}
	t.Run("child", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		RegisterWorkflow(h.app, DefineWorkflow("child", func(*WorkflowContext, string) (string, error) { return "child", nil }))
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			child, err := StartChild[string](ctx, "child", "")
			if err != nil {
				return "", err
			}
			_ = SetQueryHandler(ctx, "ready", func() (bool, error) { return child.IsReady(), nil })
			if err = Await(ctx, child.IsReady); err != nil {
				return "", err
			}
			return Receive[string](ctx, "stop")
		})
		require.NoError(t, h.RunOnce(id))
		require.NoError(t, h.RunOnce(id+"/#1"))
		require.False(t, s14Query[bool](t, h, id, "ready"))
		require.NoError(t, h.RunOnce(id))
		require.True(t, s14Query[bool](t, h, id, "ready"))
	})
}

func TestQueryEnginetestShapes(t *testing.T) { s14TestQueryEnginetestShapes(t, createTestApp) }
func s14TestQueryEnginetestShapes(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	a := DefineActivity("announce", func(context.Context, string) (string, error) { return "root-123", nil })
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		root, err := a.Execute(ctx, "")
		if err != nil {
			return "", err
		}
		_ = SetQueryHandler(ctx, "root_run_id", func() (string, error) { return root, nil })
		_ = SetQueryHandler(ctx, "tool_awaits", func() (int, error) { return Pending(ctx, "results"), nil })
		if err = Await(ctx, func() bool { return Pending(ctx, "results") > 0 }); err != nil {
			return "", err
		}
		_, _, err = ReceiveAsync[string](ctx, "results")
		if err != nil {
			return "", err
		}
		return Receive[string](ctx, "stop")
	})
	_, err := Query[string](context.Background(), h.app, id, "root_run_id")
	var nf *QueryNotFoundError
	require.ErrorAs(t, err, &nf)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, "root-123", s14Query[string](t, h, id, "root_run_id"))
	require.Zero(t, s14Query[int](t, h, id, "tool_awaits"))
	require.NoError(t, h.app.Signal(context.Background(), id, "results", "result"))
	require.Zero(t, s14Query[int](t, h, id, "tool_awaits"))
	require.NoError(t, h.RunOnce(id))
	require.Zero(t, s14Query[int](t, h, id, "tool_awaits"))
}

func TestQuerySnapshotConsistent(t *testing.T) { s14TestQuerySnapshotConsistent(t, createTestApp) }
func s14TestQuerySnapshotConsistent(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	for i := 0; i < 50; i++ {
		w := DefineWorkflow("consistent", func(ctx *WorkflowContext, _ int) (int, error) {
			n := 0
			_ = SetQueryHandler(ctx, "view", func() ([]int, error) { return []int{n, Pending(ctx, "ch")}, nil })
			if _, err := Receive[int](ctx, "ch"); err != nil {
				return 0, err
			}
			n = 1
			return 1, nil
		})
		RegisterWorkflow(h.app, w)
		id, err := StartWorkflow(context.Background(), h.app, w, i)
		require.NoError(t, err)
		require.NoError(t, h.app.Signal(context.Background(), id, "ch", 1))
		done := make(chan error, 1)
		go func() { done <- h.RunOnce(id) }()
		v, err := Query[[]int](context.Background(), h.app, id, "view")
		require.NoError(t, err)
		require.Contains(t, [][]int{{0, 0}, {1, 0}}, v)
		require.NoError(t, <-done)
	}
}
