package selvedge

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func s14Harness(t *testing.T, factory replayAppFactory) (*typedReplayHarness, *faultstore.Store) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	fs := faultstore.New(h.app.storage)
	h.app.storage = fs
	h.app.engine = replay.NewEngine(fs, h.app.hooks, "s14", errorCodec{})
	return h, fs
}
func s14Seed(t *testing.T, h *typedReplayHarness, fn func(*WorkflowContext, string) (string, error)) string {
	t.Helper()
	return seedErrorWorkflow(t, h, DefineWorkflow("s14", fn))
}
func s14Query[T any](t *testing.T, h *typedReplayHarness, id, name string) T {
	t.Helper()
	v, err := Query[T](context.Background(), h.app, id, name)
	require.NoError(t, err)
	return v
}
func s14RegisterPosition(ctx *WorkflowContext) error {
	return SetQueryHandler(ctx, "q", func() (int64, error) { return ctx.execCtx.Position(), nil })
}
func TestQueryHandlerLifecycle(t *testing.T) { s14TestQueryHandlerLifecycle(t, createTestApp) }
func s14TestQueryHandlerLifecycle(t *testing.T, factory replayAppFactory) {
	for _, mode := range []string{"replace", "remove", "after_stop", "before_registration", "normal_return", "defer"} {
		t.Run(mode, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				if mode == "before_registration" {
					_ = Sleep(ctx, time.Hour)
				}
				if err := SetQueryHandler(ctx, "value", func() (string, error) { return "first", nil }); err != nil {
					return "", err
				}
				if mode == "replace" {
					_ = SetQueryHandler(ctx, "value", func() (string, error) { return "second", nil })
				}
				if mode == "remove" {
					_ = SetQueryHandler[string](ctx, "value", nil)
				}
				if mode == "normal_return" {
					return "done", nil
				}
				defer func() { _ = SetQueryHandler(ctx, "value", func() (string, error) { return "defer", nil }) }()
				_ = Sleep(ctx, time.Hour)
				_ = SetQueryHandler(ctx, "value", func() (string, error) { return "after", nil })
				return "done", nil
			})
			got, err := Query[string](context.Background(), h.app, id, "value")
			if mode == "remove" || mode == "before_registration" {
				var nf *QueryNotFoundError
				require.ErrorAs(t, err, &nf)
				return
			}
			require.NoError(t, err)
			want := "first"
			if mode == "replace" {
				want = "second"
			}
			require.Equal(t, want, got)
		})
	}
	for _, name := range []string{"", strings.Repeat("x", 256)} {
		t.Run("invalid_name", func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = SetQueryHandler(ctx, name, func() (bool, error) { return true, nil })
				return "", nil
			})
			require.Error(t, h.RunOnce(id))
		})
	}
	t.Run("predicate", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			return "", Await(ctx, func() bool { _ = SetQueryHandler(ctx, "bad", func() (bool, error) { return true, nil }); return true })
		})
		var nd *NondeterminismError
		require.ErrorAs(t, h.RunOnce(id), &nd)
	})
	t.Run("foreign_coroutine", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			done := make(chan error, 1)
			go func() { done <- SetQueryHandler(ctx, "bad", func() (bool, error) { return true, nil }) }()
			return "", <-done
		})
		var nd *NondeterminismError
		require.ErrorAs(t, h.RunOnce(id), &nd)
	})
}

func TestQueryStopsAtFirstLiveAction(t *testing.T) {
	s14TestQueryStopsAtFirstLiveAction(t, createTestApp)
}
func s14TestQueryStopsAtFirstLiveAction(t *testing.T, factory replayAppFactory) {
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "bad", nil })
	actions := map[string]func(*WorkflowContext){
		"execute":       func(c *WorkflowContext) { _, _ = a.Execute(c, "") },
		"sleep":         func(c *WorkflowContext) { _ = Sleep(c, time.Hour) },
		"await":         func(c *WorkflowContext) { _ = Await(c, func() bool { return false }) },
		"async_receive": func(c *WorkflowContext) { _, _, _ = ReceiveAsync[int](c, "ch") },
		"receive":       func(c *WorkflowContext) { _, _ = Receive[int](c, "ch") },
		"now":           func(c *WorkflowContext) { _, _ = Now(c) },
		"child":         func(c *WorkflowContext) { _, _ = StartChild[string](c, "child", "") },
		"cancel_scope":  func(c *WorkflowContext) { _, cancel := WithCancel(c); cancel() },
	}
	for name, action := range actions {
		t.Run(name, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = s14RegisterPosition(ctx)
				action(ctx)
				_ = SetQueryHandler(ctx, "q", func() (int64, error) { return 99, nil })
				return "", nil
			})
			require.Zero(t, s14Query[int64](t, h, id, "q"))
		})
	}
	t.Run("reserved_get", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			_ = s14RegisterPosition(ctx)
			f, err := a.ExecuteAsync(ctx, "")
			if err != nil {
				return "", err
			}
			return f.Get(ctx)
		})
		f, ok, err := h.app.storage.AcquireLease(context.Background(), id, "fixture", time.Minute)
		require.NoError(t, err)
		require.True(t, ok)
		_, err = h.app.storage.ReserveActivity(context.Background(), &f, 1, "#1", "activity:A", &storage.ActivityTask{InstanceID: id, ActivityName: "A", InputData: []byte(`""`)})
		require.NoError(t, err)
		require.EqualValues(t, 1, s14Query[int64](t, h, id, "q"))
	})
}

func TestQueryResultJSONRoundTrip(t *testing.T) { s14TestQueryResultJSONRoundTrip(t, createTestApp) }
func s14TestQueryResultJSONRoundTrip(t *testing.T, factory replayAppFactory) {
	marker := errors.New("handler error")
	for _, mode := range []string{"value", "error", "panic", "panic_error", "encode", "decode"} {
		t.Run(mode, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = SetQueryHandler(ctx, "value", func() (any, error) {
					switch mode {
					case "error":
						return nil, marker
					case "panic":
						panic("boom")
					case "panic_error":
						panic(marker)
					case "encode":
						return make(chan int), nil
					}
					return map[string]int{"n": 7}, nil
				})
				return "", nil
			})
			if mode == "decode" {
				_, err := Query[int](context.Background(), h.app, id, "value")
				var failed *QueryFailedError
				require.ErrorAs(t, err, &failed)
				return
			}
			v, err := Query[any](context.Background(), h.app, id, "value")
			if mode == "value" {
				require.NoError(t, err)
				require.Equal(t, map[string]any{"n": float64(7)}, v)
				return
			}
			var failed *QueryFailedError
			require.ErrorAs(t, err, &failed)
			require.Equal(t, "value", failed.Name)
			if mode == "error" || mode == "panic_error" {
				require.ErrorIs(t, err, marker)
			}
		})
	}
}

func TestQueryFromSecondApp(t *testing.T) { s14TestQueryFromSecondApp(t, createTestApp) }
func s14TestQueryFromSecondApp(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	w := DefineWorkflow("second", func(ctx *WorkflowContext, _ string) (string, error) {
		_ = SetQueryHandler(ctx, "value", func() (string, error) { return "answer", nil })
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	other := NewApp()
	other.storage = h.app.storage
	other.engine = replay.NewEngine(other.storage, other.hooks, "second", errorCodec{})
	_, err := Query[string](context.Background(), other, id, "value")
	var unregistered *WorkflowNotRegisteredError
	require.ErrorAs(t, err, &unregistered)
	RegisterWorkflow(other, w)
	got, err := Query[string](context.Background(), other, id, "value")
	require.NoError(t, err)
	require.Equal(t, "answer", got)
	_, err = Query[string](context.Background(), other, "missing", "value")
	var missing *WorkflowNotFoundError
	require.ErrorAs(t, err, &missing)
}
