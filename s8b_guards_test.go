package selvedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestAsyncValidationIsSticky(t *testing.T) { testAsyncValidationIsSticky(t, createTestApp) }
func testAsyncValidationIsSticky(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"input", "option"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			a := DefineActivity("A", func(context.Context, any) (string, error) { t.Error("invalid input ran"); return "", nil })
			w := DefineWorkflow("invalid", func(ctx *WorkflowContext, _ string) (string, error) {
				if kind == "input" {
					_, _ = a.ExecuteAsync(ctx, make(chan int))
				} else {
					_, _ = a.ExecuteAsync(ctx, "", WithActivityID("#reserved"))
				}
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			err := h.RunOnce(id)
			require.Error(t, err)
			if kind == "option" {
				require.ErrorIs(t, err, ErrHistoryKeyConflict)
			}
			require.Zero(t, taskCount(t, h, id))
			require.NotContains(t, fs.Calls(), "ReserveActivity")
		})
	}
}

func TestFutureDecodeFailureIsSticky(t *testing.T) { testFutureDecodeFailureIsSticky(t, createTestApp) }
func testFutureDecodeFailureIsSticky(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"value", "envelope", "panic"} {
		t.Run(kind, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			a := DefineActivity("A", func(context.Context, string) (string, error) { return "", nil })
			w := DefineWorkflow("decode", func(ctx *WorkflowContext, in string) (string, error) {
				f, err := a.ExecuteAsync(ctx, in)
				if err != nil {
					return "", err
				}
				_, _ = f.Get(ctx)
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			seedTask(t, h, id)
			claim, err := h.ClaimTask(id, "#1")
			require.NoError(t, err)
			typ := storage.HistoryActivityCompleted
			data := []byte(`123`)
			if kind == "envelope" {
				typ = storage.HistoryActivityFailed
				data = []byte(`{"kind":`)
			}
			if kind == "panic" {
				typ = storage.HistoryActivityFailed
				data = encodeError(&WorkflowPanicError{Value: "boom", Stack: "test"})
			}
			require.NoError(t, h.app.storage.CompleteActivityTask(h.app.ctx, claim.task.Claim(), typ, data))
			err = h.RunOnce(id)
			if kind == "panic" {
				var target *WorkflowPanicError
				require.ErrorAs(t, err, &target)
			} else {
				var target *ReplayDecodeError
				require.ErrorAs(t, err, &target)
			}
		})
	}
}

func TestFutureClosingRejectsCommands(t *testing.T) {
	testFutureClosingRejectsCommands(t, createTestApp)
}
func testFutureClosingRejectsCommands(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { t.Error("deferred activity ran"); return "", nil })
	var getErr, issueErr error
	w := DefineWorkflow("closing", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		defer func() { _, getErr = f.Get(ctx); _, issueErr = a.ExecuteAsync(ctx, in) }()
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Error(t, getErr)
	require.Error(t, issueErr)
	require.Equal(t, 1, taskCount(t, h, id))
	require.NotContains(t, fs.Calls(), "ClaimActivityTask")
}

func TestSyncAsyncShareCommand(t *testing.T) { testSyncAsyncShareCommand(t, createTestApp) }
func testSyncAsyncShareCommand(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	calls := 0
	async := false
	a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "value", nil })
	w := DefineWorkflow("switch", func(ctx *WorkflowContext, in string) (string, error) {
		var err error
		if async {
			f, e := a.ExecuteAsync(ctx, in)
			if e != nil {
				return "", e
			}
			_, err = f.Get(ctx)
		} else {
			_, err = a.Execute(ctx, in)
		}
		if err != nil {
			return "", err
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	async = true
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 1, calls)
}

func TestFutureMissingTaskDoesNotRun(t *testing.T) { testFutureMissingTaskDoesNotRun(t, createTestApp) }
func testFutureMissingTaskDoesNotRun(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "", errors.New("must not run") })
	w := DefineWorkflow("missing", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		return f.Get(ctx)
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	_, err := fs.DB().Exec("DELETE FROM workflow_activity_tasks WHERE instance_id=$1", id)
	require.NoError(t, err)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.RunTasks(id))
	require.Zero(t, taskCount(t, h, id))
	require.NotContains(t, fs.Calls(), "ReserveActivity")
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, inst.Status)
}
