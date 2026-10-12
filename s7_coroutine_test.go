package selvedge

import (
	"context"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func TestSwallowedYieldDoesNotRunLaterCommands(t *testing.T) {
	testSwallowedYieldDoesNotRunLaterCommands(t, createTestApp)
}
func testSwallowedYieldDoesNotRunLaterCommands(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"sleep", "receive"} {
		t.Run(kind, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			calls := 0
			a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "done", nil })
			w := DefineWorkflow("swallow", func(ctx *WorkflowContext, input string) (string, error) {
				if kind == "sleep" {
					_ = Sleep(ctx, time.Hour, WithSleepID("pause"))
				} else {
					_, _ = Receive[string](ctx, "messages")
				}
				return a.Execute(ctx, input)
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.Zero(t, calls)
			inst, err := h.app.storage.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, storage.StatusRunning, inst.Status)
			require.Empty(t, inst.LeaseOwner)
			require.False(t, inst.Runnable)
			if kind == "sleep" {
				h.forceFire(t, id)
			} else {
				require.NoError(t, h.app.Signal(h.app.ctx, id, "messages", "ready"))
			}
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, 1, calls)
			assertCompletedLeaseRun(t, h, id)
		})
	}
}

func TestDeferredCommandAfterYieldIsRejected(t *testing.T) {
	testDeferredCommandAfterYieldIsRejected(t, createTestApp)
}
func testDeferredCommandAfterYieldIsRejected(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	calls := 0
	a := DefineActivity("D", func(context.Context, string) (string, error) { calls++; return "", nil })
	var deferredErr error
	var recovered any
	w := DefineWorkflow("deferred", func(ctx *WorkflowContext, input string) (string, error) {
		defer func() { recovered = recover(); _, deferredErr = a.Execute(ctx, input) }()
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Zero(t, calls)
	require.Error(t, deferredErr)
	require.Nil(t, recovered, "Goexit cannot be caught by recover")
	rows, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "sleep", rows[0].Command)
	inst, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, inst.Status)
}

func TestCommandOffCoroutineFailsRun(t *testing.T) { testCommandOffCoroutineFailsRun(t, createTestApp) }
func testCommandOffCoroutineFailsRun(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"overlapping_goroutines"} {
		t.Run(kind, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			entered, release := make(chan struct{}), make(chan struct{})
			a := DefineActivity("A", func(ctx context.Context, input string) (string, error) {
				if input == "first" {
					close(entered)
					<-release
				}
				return "", nil
			})
			w := DefineWorkflow("off_coroutine", func(ctx *WorkflowContext, _ string) (string, error) {
				first, second := make(chan struct{}), make(chan struct{})
				go func() { defer close(first); _, _ = a.Execute(ctx, "first", WithActivityID("first")) }()
				<-entered
				go func() { defer close(second); _, _ = a.Execute(ctx, "second", WithActivityID("second")) }()
				<-second
				close(release)
				<-first
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			err := h.RunOnce(id)
			var detail *NondeterminismError
			require.ErrorAs(t, err, &detail)
			require.Equal(t, "command issued off the workflow coroutine", detail.Reason)
			result, getErr := GetWorkflowResult[string](h.app.ctx, h.app, id)
			require.NoError(t, getErr)
			require.Equal(t, "failed", result.Status)
			require.ErrorAs(t, result.Error, &detail)
			rows, _, getErr := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
			require.NoError(t, getErr)
			require.Len(t, rows, 2, "only the first activity and workflow failure may be written")
		})
	}
}

func TestWorkflowPanicFailsRun(t *testing.T) { testWorkflowPanicFailsRun(t, createTestApp) }
func testWorkflowPanicFailsRun(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	w := DefineWorkflow("panic", func(*WorkflowContext, string) (string, error) { panic("broken workflow") })
	id := seedErrorWorkflow(t, h, w)
	var runErr error
	require.NotPanics(t, func() { runErr = h.RunOnce(id) })
	require.Error(t, runErr)
	var panicErr *WorkflowPanicError
	require.ErrorAs(t, runErr, &panicErr)
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	var persisted *WorkflowPanicError
	require.ErrorAs(t, result.Error, &persisted)
	require.Equal(t, panicErr, persisted)
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "$workflow_failed")
	require.NoError(t, err)
	var envelope errorEnvelope
	require.NoError(t, json.Unmarshal(row.EventData, &envelope))
	require.Equal(t, "panic", envelope.Kind)
	var detail struct{ Value, Stack string }
	require.NoError(t, json.Unmarshal(envelope.Details, &detail))
	require.Equal(t, "broken workflow", detail.Value)
	require.Contains(t, detail.Stack, "testWorkflowPanicFailsRun")
	good := DefineWorkflow("after_panic", func(*WorkflowContext, string) (string, error) { return "alive", nil })
	RegisterWorkflow(h.app, good)
	next := "after-panic-instance"
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: next, WorkflowName: good.Name(), Status: storage.StatusRunning, InputData: []byte(`"input"`)}))
	require.NoError(t, h.RunOnce(next))
	assertCompletedLeaseRun(t, h, next)
}

func TestCoroutineExitsBeforeLeaseRelease(t *testing.T) {
	testCoroutineExitsBeforeLeaseRelease(t, createTestApp)
}
func testCoroutineExitsBeforeLeaseRelease(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	fs := faultstore.New(h.app.storage)
	h.app.engine = replay.NewEngine(fs, h.app.hooks, "coroutine", errorCodec{})
	var exited atomic.Bool
	releases := 0
	fs.Before = func(_ context.Context, method string) error {
		if method == "ReleaseLease" {
			require.True(t, exited.Load(), "defer must finish before lease release")
			releases++
		}
		return nil
	}
	w := DefineWorkflow("exit", func(ctx *WorkflowContext, _ string) (string, error) {
		defer exited.Store(true)
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	// Eventually evaluates its condition on one additional probe goroutine.
	baseline := runtime.NumGoroutine() + 1
	for range 100 {
		require.NoError(t, h.app.Signal(h.app.ctx, id, "wake", true))
		exited.Store(false)
		require.NoError(t, h.RunOnce(id))
	}
	require.Equal(t, 100, releases)
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline }, 5*time.Second, 10*time.Millisecond)
}

func TestCoroutineAbnormalExitFailsRun(t *testing.T) {
	testCoroutineAbnormalExitFailsRun(t, createTestApp)
}
func testCoroutineAbnormalExitFailsRun(t *testing.T, factory replayAppFactory) {
	for _, tc := range []struct {
		name, value string
		run         func(*WorkflowContext)
	}{
		{"goexit", "runtime.Goexit", func(*WorkflowContext) { runtime.Goexit() }},
		{"panic_in_close_defer", "defer panic", func(ctx *WorkflowContext) { defer func() { panic("defer panic") }(); _ = Sleep(ctx, time.Hour) }},
		{"nil_panic", (&runtime.PanicNilError{}).Error(), func(*WorkflowContext) { panic(nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			w := DefineWorkflow("abnormal", func(ctx *WorkflowContext, _ string) (string, error) { tc.run(ctx); return "unreachable", nil })
			id := seedErrorWorkflow(t, h, w)
			var detail *WorkflowPanicError
			require.ErrorAs(t, h.RunOnce(id), &detail)
			require.Equal(t, tc.value, detail.Value)
			require.NotEmpty(t, detail.Stack)
			result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
			require.NoError(t, err)
			require.Equal(t, "failed", result.Status)
			var persisted *WorkflowPanicError
			require.ErrorAs(t, result.Error, &persisted)
			require.Equal(t, detail, persisted)
		})
	}
}
