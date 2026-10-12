package selvedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

func TestRetryIsDurable(t *testing.T) { testRetryIsDurable(t, createTestApp) }
func testRetryIsDurable(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var attempts []int
	a := DefineActivity("retry", func(ctx context.Context, in string) (string, error) {
		attempts = append(attempts, GetActivityInfo(ctx).Attempt)
		if len(attempts) == 1 {
			return "", errors.New("try again")
		}
		return "done", nil
	}, WithRetryPolicy[string, string](retry.Fixed(2, time.Second)))
	w := DefineWorkflow("durable_retry", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []int{1}, attempts, "retry must yield instead of looping inside the claim")
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Nil(t, row.EventData)
	setStoreClock(t, h, 1999)
	require.NoError(t, h.RunTasks(id))
	require.Equal(t, []int{1}, attempts)
	setStoreClock(t, h, 2000)
	require.NoError(t, h.RunTasks(id))
	require.Equal(t, []int{1, 2}, attempts)
	require.NoError(t, h.RunOnce(id))
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
}

func TestRecordedOptionsDriveRetries(t *testing.T) { testRecordedOptionsDriveRetries(t, createTestApp) }
func testRecordedOptionsDriveRetries(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var attempts []int
	fn := func(ctx context.Context, _ string) (string, error) {
		attempts = append(attempts, GetActivityInfo(ctx).Attempt)
		return "", errors.New("persistent")
	}
	a := DefineActivity("recorded", fn, WithActivityDefaults[string, string](ActivityOptions{Queue: "a", RetryPolicy: retry.Fixed(2, time.Second)}))
	w := DefineWorkflow("recorded", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []int{1}, attempts)
	// Another App uses a changed definition but the same durable task store.
	second := NewApp()
	second.storage = h.app.storage
	second.ctx = h.app.ctx
	v2 := DefineActivity("recorded", fn, WithActivityDefaults[string, string](ActivityOptions{Queue: "b", RetryPolicy: retry.Fixed(5, time.Second)}))
	require.NoError(t, RegisterActivity(second, v2))
	setStoreClock(t, h, 2000)
	task, err := second.storage.ClaimActivityTask(second.ctx, id, "#1", "second", time.Second, []string{"a"})
	require.NoError(t, err)
	require.NotNil(t, task)
	require.NoError(t, second.runActivityTask(second.ctx, second.storage, task, nil))
	require.Equal(t, []int{1, 2}, attempts)
	err = h.RunOnce(id)
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, 2, exhausted.Attempts)
	rows, err := ListActivityTasks(h.app.ctx, second, id)
	require.NoError(t, err)
	require.Equal(t, "a", rows[0].Queue)
	require.Equal(t, 2, rows[0].Options.RetryPolicy.MaxAttempts)
	before := len(fs.Calls())
	_, err = fs.DB().Exec("UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1", id)
	require.NoError(t, err)
	require.Error(t, h.RunOnce(id))
	require.NotContains(t, fs.Calls()[before:], "ReserveActivity")
	require.Equal(t, []int{1, 2}, attempts)
}
func TestRetryExhaustedRecorded(t *testing.T) { testRetryExhaustedRecorded(t, createTestApp) }
func testRetryExhaustedRecorded(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	var attempts []int
	a := DefineActivity("exhaust", func(ctx context.Context, _ string) (string, error) {
		attempts = append(attempts, GetActivityInfo(ctx).Attempt)
		return "", errors.New("application message")
	}, WithRetryPolicy[string, string](retry.Fixed(3, time.Second)))
	w := DefineWorkflow("exhaust", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	setStoreClock(t, h, 2000)
	require.NoError(t, h.RunTasks(id))
	setStoreClock(t, h, 3000)
	require.NoError(t, h.RunTasks(id))
	err := h.RunOnce(id)
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, 3, exhausted.Attempts)
	require.Contains(t, exhausted.LastErr.Error(), "application message")
	require.Equal(t, []int{1, 2, 3}, attempts)
}
func TestNonRetryableStopsRetries(t *testing.T) { testNonRetryableStopsRetries(t, createTestApp) }
func testNonRetryableStopsRetries(t *testing.T, factory replayAppFactory) {
	sentinel := errors.New("no retry")
	for _, kind := range []string{"terminal", "application", "definition", "panic", "encode", "decode"} {
		t.Run(kind, func(t *testing.T) {
			h, _, _ := leaseHarness(t, factory)
			calls := 0
			a := DefineActivity("nonretry", func(context.Context, string) (any, error) {
				calls++
				switch kind {
				case "terminal":
					return nil, NewTerminalError(sentinel)
				case "application":
					return nil, NewNonRetryableApplicationError("stop", "kind", nil, nil)
				case "definition":
					return nil, sentinel
				case "panic":
					panic("stop")
				case "encode":
					return make(chan int), nil
				}
				return nil, nil
			}, WithRetryPolicy[string, any](&retry.Policy{MaxAttempts: 5, NonRetryableErrors: []error{sentinel}}))
			w := DefineWorkflow("nonretry", func(ctx *WorkflowContext, in string) (any, error) { return a.Execute(ctx, in) })
			RegisterWorkflow(h.app, w)
			id := "nonretry"
			require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: id, WorkflowName: w.Name(), Status: storage.StatusRunning, InputData: []byte(`"input"`)}))
			if kind == "decode" {
				f, ok, err := h.app.storage.AcquireLease(h.app.ctx, id, "seed", time.Second)
				require.NoError(t, err)
				require.True(t, ok)
				_, err = h.app.storage.ReserveActivity(h.app.ctx, &f, 1, "#1", "activity:nonretry", &storage.ActivityTask{InstanceID: id, ActivityName: a.Name(), InputData: []byte(`123`)})
				require.NoError(t, err)
				require.NoError(t, h.app.storage.ReleaseLease(h.app.ctx, f, true))
			}
			require.Error(t, h.RunOnce(id))
			if kind == "decode" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}
func TestRetryStopsOnCancel(t *testing.T) { testRetryStopsOnCancel(t, createTestApp) }
func testRetryStopsOnCancel(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"get", "scope", "close", "deleted"} {
		t.Run(kind, func(t *testing.T) {
			h, _, _ := leaseHarness(t, factory)
			calls := 0
			a := DefineActivity("cancel_retry", func(context.Context, string) (string, error) { calls++; return "", errors.New("retry") }, WithRetryPolicy[string, string](retry.Fixed(3, time.Second)))

			w := DefineWorkflow("cancel_retry", func(ctx *WorkflowContext, in string) (string, error) {
				if kind == "scope" || kind == "close" {
					scope, cancel := WithCancel(ctx)
					if _, err := a.ExecuteAsync(scope, in); err != nil {
						return "", err
					}
					if _, err := Receive[string](ctx, "finish"); err != nil {
						return "", err
					}
					if kind == "scope" {
						cancel()
					}
					return "done", nil
				}
				return a.Execute(ctx, in)
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			if kind == "scope" || kind == "close" {
				require.NoError(t, h.RunTasks(id))
			}
			require.Equal(t, 1, calls)
			switch kind {
			case "get":
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
				require.Error(t, h.RunOnce(id))
			case "deleted":
				_, err := h.app.storage.DB().Exec("DELETE FROM workflow_activity_tasks WHERE instance_id=$1", id)
				require.NoError(t, err)
			default:
				require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", "now"))
				require.NoError(t, h.RunOnce(id))
			}
			if kind != "deleted" {
				rows, err := ListActivityTasks(h.app.ctx, h.app, id)
				require.NoError(t, err)
				require.Len(t, rows, 1)
				require.Equal(t, "cancelled", rows[0].Status)
				instance, err := h.app.storage.GetInstance(h.app.ctx, id)
				require.NoError(t, err)
				require.NotEqual(t, storage.StatusRunning, instance.Status)
			}

			setStoreClock(t, h, 4000)
			require.NoError(t, h.RunTasks(id))
			require.Equal(t, 1, calls)
		})
	}
}
