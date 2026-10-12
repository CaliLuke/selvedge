package selvedge

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

func TestCoroutineForeignYieldFailsRun(t *testing.T) {
	testCoroutineForeignYieldFailsRun(t, createTestApp)
}

func testCoroutineForeignYieldFailsRun(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"sleep", "receive"} {
		t.Run(kind, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			done := make(chan error, 1)
			foreignExited := make(chan struct{})
			workflowExited := make(chan struct{})
			w := DefineWorkflow("foreign_yield", func(ctx *WorkflowContext, _ string) (string, error) {
				defer close(workflowExited)
				go func() {
					defer close(foreignExited)
					if kind == "sleep" {
						done <- Sleep(ctx, time.Hour)
					} else {
						_, err := Receive[string](ctx, "messages")
						done <- err
					}
				}()
				return "", <-done
			})
			id := seedErrorWorkflow(t, h, w)
			// Eventually runs its check on one additional probe goroutine.
			baseline := runtime.NumGoroutine() + 1
			runExited := make(chan struct{})
			var runErr error
			go func() { defer close(runExited); runErr = h.RunOnce(id) }()
			t.Cleanup(func() {
				// On the old implementation, Goexit kills the sender before it sends.
				// Unblock the actual workflow so the failing regression cleans up too.
				select {
				case done <- context.Canceled:
				default:
				}
				select {
				case <-runExited:
				case <-time.After(5 * time.Second):
					t.Error("dispatcher did not exit during test cleanup")
				}
			})
			select {
			case <-runExited:
			case <-time.After(5 * time.Second):
				inst, err := h.app.storage.GetInstance(h.app.ctx, id)
				require.NoError(t, err)
				t.Fatalf("RunOnce hung after foreign %s yield; status=%s lease_owner=%q", kind, inst.Status, inst.LeaseOwner)
			}
			var detail *NondeterminismError
			require.ErrorAs(t, runErr, &detail)
			require.Equal(t, "command issued off the workflow coroutine", detail.Reason)
			require.EqualValues(t, 1, detail.Seq)
			require.Equal(t, "#1", detail.Key)
			result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
			require.NoError(t, err)
			require.Equal(t, "failed", result.Status)
			var persisted *NondeterminismError
			require.ErrorAs(t, result.Error, &persisted)
			require.Equal(t, detail, persisted)
			inst, err := h.app.storage.GetInstance(h.app.ctx, id)
			require.NoError(t, err)
			require.Equal(t, storage.StatusFailed, inst.Status)
			require.Empty(t, inst.LeaseOwner)
			require.Nil(t, inst.LeaseExpiresMS)
			select {
			case <-foreignExited:
			case <-time.After(5 * time.Second):
				t.Fatal("foreign goroutine leaked")
			}
			select {
			case <-workflowExited:
			default:
				t.Fatal("workflow coroutine leaked")
			}
			require.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline }, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestCoroutineActivityPanicFailsWithoutRetry(t *testing.T) {
	testCoroutineActivityPanicFailsWithoutRetry(t, createTestApp)
}

func testCoroutineActivityPanicFailsWithoutRetry(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	calls := 0
	a := DefineActivity("panic", func(context.Context, string) (string, error) {
		calls++
		panic("activity panic")
	}, WithRetryPolicy[string, string](retry.Fixed(3, time.Millisecond)))
	w := DefineWorkflow("activity_panic", func(ctx *WorkflowContext, input string) (string, error) {
		return a.Execute(ctx, input)
	})
	id := seedErrorWorkflow(t, h, w)
	var detail *WorkflowPanicError
	require.ErrorAs(t, h.RunOnce(id), &detail)
	require.Equal(t, "activity panic", detail.Value)
	require.Contains(t, detail.Stack, "testCoroutineActivityPanicFailsWithoutRetry")
	require.Equal(t, 1, calls, "activity panics bypass retries")
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	var persisted *WorkflowPanicError
	require.ErrorAs(t, result.Error, &persisted)
	require.Equal(t, detail, persisted)
}
