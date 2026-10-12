package selvedge

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryForeignGoroutineStops(t *testing.T) {
	s14TestQueryForeignGoroutineStops(t, createTestApp)
}

func s14TestQueryForeignGoroutineStops(t *testing.T, factory replayAppFactory) {
	actions := map[string]func(*WorkflowContext) error{
		"other_instance": func(ctx *WorkflowContext) error {
			_, err := ctx.Storage().GetHistoryEvent(context.Background(), "other-instance", "k")
			return err
		},
		"unrecorded_command": func(ctx *WorkflowContext) error {
			_, err := ctx.execCtx.Issue("now", "foreign")
			return err
		},
		"missing_reservation": func(ctx *WorkflowContext) error {
			_, err := ctx.execCtx.Storage().ReserveHistory(context.Background(), nil, ctx.InstanceID(), "missing", "sleep", 1)
			return err
		},
		"denied_write": func(ctx *WorkflowContext) error {
			return ctx.execCtx.Storage().MarkRunnable(context.Background(), ctx.InstanceID())
		},
		"no_error_result": func(ctx *WorkflowContext) error {
			ctx.execCtx.Storage().NotifyRunnable(context.Background())
			return nil
		},
	}
	for name, action := range actions {
		t.Run(name, func(t *testing.T) {
			h, fs := s14Harness(t, factory)
			var foreignErr error
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = SetQueryHandler(ctx, "value", func() (int, error) { return 1, nil })
				done := make(chan error, 1)
				// No recovery here: a panic must fail the test process.
				go func() { done <- action(ctx) }()
				foreignErr = <-done
				return "", nil // A swallowed error must still fail the query.
			})
			before := s14Dump(t, fs)
			_, err := Query[int](context.Background(), h.app, id, "value")
			var nd *NondeterminismError
			require.ErrorAs(t, err, &nd)
			if name != "no_error_result" {
				require.Error(t, foreignErr, "the foreign call must return an error")
			}
			require.Equal(t, before, s14Dump(t, fs))
		})
	}
}

func TestQueryPanicFailedInstance(t *testing.T) {
	s14TestQueryPanicFailedInstance(t, createTestApp)
}

func s14TestQueryPanicFailedInstance(t *testing.T, factory replayAppFactory) {
	h, fs := s14Harness(t, factory)
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		_ = SetQueryHandler(ctx, "value", func() (int, error) { return 42, nil })
		_ = SetQueryHandler(ctx, "command", func() (int, error) { _, err := Now(ctx); return 0, err })
		panic("workflow failed after registering queries")
	})
	require.Error(t, h.RunOnce(id))
	description, err := Describe(context.Background(), h.app, id)
	require.NoError(t, err)
	require.Equal(t, StatusFailed, description.Status)
	before := s14Dump(t, fs)
	require.Equal(t, 42, s14Query[int](t, h, id, "value"))
	_, err = Query[int](context.Background(), h.app, id, "missing")
	var missing *QueryNotFoundError
	require.ErrorAs(t, err, &missing)
	_, err = Query[int](context.Background(), h.app, id, "command")
	require.ErrorIs(t, err, ErrQueryCommand)
	require.Equal(t, before, s14Dump(t, fs))
}

func TestQueryLifecycleRowsExcludedFromReplay(t *testing.T) {
	s14TestQueryLifecycleRowsExcludedFromReplay(t, createTestApp)
}

func s14TestQueryLifecycleRowsExcludedFromReplay(t *testing.T, factory replayAppFactory) {
	h, fs := s14Harness(t, factory)
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		_ = SetQueryHandler(ctx, "lifecycle_absent", func() (bool, error) {
			row, err := ctx.Storage().GetHistoryEvent(context.Background(), ctx.InstanceID(), "$workflow_failed")
			return row == nil, err
		})
		return "", errors.New("fixture failure")
	})
	require.Error(t, h.RunOnce(id))
	row, err := fs.GetHistoryEvent(context.Background(), id, "$workflow_failed")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Zero(t, row.Seq)
	require.True(t, s14Query[bool](t, h, id, "lifecycle_absent"))
}
