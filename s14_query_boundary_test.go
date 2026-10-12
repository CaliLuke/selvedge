package selvedge

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestQueryExecutionBoundaries(t *testing.T) { s14TestQueryExecutionBoundaries(t, createTestApp) }
func s14TestQueryExecutionBoundaries(t *testing.T, factory replayAppFactory) {
	t.Run("no_fence", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			_ = SetQueryHandler(ctx, "fence", func() (bool, error) { return ctx.execCtx.Fence() == nil, nil })
			return "", nil
		})
		require.True(t, s14Query[bool](t, h, id, "fence"))
	})
	t.Run("cancel_at_command", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		callCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			_ = s14RegisterPosition(ctx)
			cancel()
			_, err := Now(ctx)
			return "", err
		})
		_, err := Query[int64](callCtx, h.app, id, "q")
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("leftover", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) { _ = s14RegisterPosition(ctx); return "", nil })
		f, ok, err := h.app.storage.AcquireLease(context.Background(), id, "fixture", time.Minute)
		require.NoError(t, err)
		require.True(t, ok)
		_, err = h.app.storage.ReserveHistory(context.Background(), &f, id, "#1", "activity:A", 1)
		require.NoError(t, err)
		_, err = Query[int64](context.Background(), h.app, id, "q")
		var nd *NondeterminismError
		require.ErrorAs(t, err, &nd)
	})
	t.Run("handler_before_defers", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			value := "before"
			defer func() { value = "after" }()
			_ = SetQueryHandler(ctx, "value", func() (string, error) { return value, nil })
			return "", Sleep(ctx, time.Hour)
		})
		require.Equal(t, "before", s14Query[string](t, h, id, "value"))
	})
	t.Run("history_conflict", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
			_ = s14RegisterPosition(ctx)
			_, err := ctx.execCtx.Storage().ReserveHistory(ctx.Context(), nil, ctx.InstanceID(), "#1", "wrong", 1)
			return "", err
		})
		f, ok, err := h.app.storage.AcquireLease(context.Background(), id, "fixture", time.Minute)
		require.NoError(t, err)
		require.True(t, ok)
		_, err = h.app.storage.ReserveHistory(context.Background(), &f, id, "#1", "activity:A", 1)
		require.NoError(t, err)
		_, err = Query[int64](context.Background(), h.app, id, "q")
		require.ErrorIs(t, err, storage.ErrHistoryKeyConflict)
	})
	// Every unlisted method must hand off without returning into workflow code.
	served := map[string]bool{"GetInstance": true, "IsCancelRequested": true, "GetHistoryEvent": true, "GetHistoryPaginated": true, "DeliveredSignals": true, "ReserveTimer": true, "ReserveHistory": true, "ReserveActivity": true}
	iface := reflect.TypeFor[storage.Storage]()
	for i := 0; i < iface.NumMethod(); i++ {
		method := iface.Method(i)
		if served[method.Name] {
			continue
		}
		t.Run(method.Name, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = s14RegisterPosition(ctx)
				target := reflect.ValueOf(ctx.execCtx.Storage()).MethodByName(method.Name)
				args := make([]reflect.Value, target.Type().NumIn())
				for i := range args {
					args[i] = reflect.Zero(target.Type().In(i))
				}
				if target.Type().IsVariadic() {
					target.CallSlice(args)
				} else {
					target.Call(args)
				}
				ctx.execCtx.Fail(errors.New("denied storage method returned"))
				return "", nil
			})
			require.Zero(t, s14Query[int64](t, h, id, "q"))
		})
	}
}

func TestQueryPreservesReplayFailure(t *testing.T) {
	s14TestQueryPreservesReplayFailure(t, createTestApp)
}
func s14TestQueryPreservesReplayFailure(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		_ = s14RegisterPosition(ctx)
		_, _ = Now(ctx)
		panic("workflow ignored replay failure")
	})
	f, ok, err := h.app.storage.AcquireLease(context.Background(), id, "fixture", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = h.app.storage.ReserveHistory(context.Background(), &f, id, "#1", "sleep", 1)
	require.NoError(t, err)
	before := s14Dump(t, h.app.storage)
	_, err = Query[int64](context.Background(), h.app, id, "q")
	var nd *NondeterminismError
	require.ErrorAs(t, err, &nd)
	require.Equal(t, before, s14Dump(t, h.app.storage))
}

func TestQuerySnapshotReservations(t *testing.T) { s14TestQuerySnapshotReservations(t, createTestApp) }
func s14TestQuerySnapshotReservations(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"history", "timer", "activity"} {
		t.Run(kind, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			command := "activity:A"
			if kind == "timer" {
				command = "sleep"
			}
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				cmd, err := ctx.execCtx.Issue(command, "")
				if err != nil {
					return "", err
				}
				cmd.End()
				var row *storage.HistoryEvent
				switch kind {
				case "history":
					row, err = ctx.execCtx.Storage().ReserveHistory(ctx.Context(), nil, ctx.InstanceID(), "#1", command, 1)
				case "timer":
					row, err = ctx.execCtx.Storage().ReserveTimer(ctx.Context(), nil, 1, &storage.TimerSubscription{InstanceID: ctx.InstanceID(), TimerID: "#1", ActivityID: "subscription-metadata"})
				case "activity":
					row, err = ctx.execCtx.Storage().ReserveActivity(ctx.Context(), nil, 1, "#1", command, &storage.ActivityTask{InstanceID: ctx.InstanceID()})
				}
				if err != nil {
					return "", err
				}
				row.EventData = []byte(`"mutated"`)
				again, err := ctx.Storage().GetHistoryEvent(ctx.Context(), ctx.InstanceID(), "#1")
				if err != nil {
					return "", err
				}
				_ = SetQueryHandler(ctx, "unchanged", func() (bool, error) { return again.EventData == nil, nil })
				return "", nil
			})
			f, ok, err := h.app.storage.AcquireLease(context.Background(), id, "fixture", time.Minute)
			require.NoError(t, err)
			require.True(t, ok)
			_, err = h.app.storage.ReserveHistory(context.Background(), &f, id, "#1", command, 1)
			require.NoError(t, err)
			before := s14Dump(t, h.app.storage)
			require.True(t, s14Query[bool](t, h, id, "unchanged"))
			require.Equal(t, before, s14Dump(t, h.app.storage))
		})
	}
}

func TestQueryClosingStorageStop(t *testing.T) { s14TestQueryClosingStorageStop(t, createTestApp) }
func s14TestQueryClosingStorageStop(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		defer func() { _, _ = ctx.Storage().GetHistoryEvent(ctx.Context(), "outside-snapshot", "#1") }()
		_ = s14RegisterPosition(ctx)
		return "", Sleep(ctx, time.Hour)
	})
	done := make(chan error, 1)
	go func() { _, err := Query[int64](context.Background(), h.app, id, "q"); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("query closing attempted another storage handoff")
	}
}
