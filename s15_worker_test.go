package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

type s15Hooks struct {
	hooks.NoOpHooks
	retry    func(context.Context, hooks.ActivityRetryInfo)
	failed   func(context.Context, hooks.ActivityFailedInfo)
	complete func(context.Context, hooks.ActivityCompleteInfo)
}

func (h *s15Hooks) OnActivityRetry(ctx context.Context, i hooks.ActivityRetryInfo) {
	if h.retry != nil {
		h.retry(ctx, i)
	}
}
func (h *s15Hooks) OnActivityFailed(ctx context.Context, i hooks.ActivityFailedInfo) {
	if h.failed != nil {
		h.failed(ctx, i)
	}
}
func (h *s15Hooks) OnActivityComplete(ctx context.Context, i hooks.ActivityCompleteInfo) {
	if h.complete != nil {
		h.complete(ctx, i)
	}
}

func TestActivityAttemptNumbers(t *testing.T) { testActivityAttemptNumbers(t, createTestApp) }
func testActivityAttemptNumbers(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	var attempts, retries, failed []int
	observe := &s15Hooks{}
	observe.retry = func(ctx context.Context, i hooks.ActivityRetryInfo) {
		rows, err := ListActivityTasks(ctx, h.app, i.InstanceID)
		require.NoError(t, err)
		require.Equal(t, "queued", rows[0].Status)
		require.Equal(t, 1, rows[0].Failures)
		require.Equal(t, 2, i.MaxAttempts)
		retries = append(retries, i.Attempt)
	}
	observe.failed = func(ctx context.Context, i hooks.ActivityFailedInfo) {
		rows, err := ListActivityTasks(ctx, h.app, i.InstanceID)
		require.NoError(t, err)
		require.Equal(t, "completed", rows[0].Status)
		failed = append(failed, i.Attempt)
	}
	h.app.hooks = observe
	a := DefineActivity("numbers", func(ctx context.Context, _ string) (string, error) {
		attempts = append(attempts, GetActivityInfo(ctx).Attempt)
		return "", errors.New("again")
	}, WithRetryPolicy[string, string](retry.Fixed(2, time.Second)))
	w := DefineWorkflow("numbers", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	// Commit a claim, then release it without running user code.
	f, ok, err := fs.AcquireLease(h.app.ctx, id, "seed", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	options, err := resolveActivityOptions(a.options, ActivityOptions{})
	require.NoError(t, err)
	data, err := json.Marshal(options)
	require.NoError(t, err)
	_, err = fs.ReserveActivity(h.app.ctx, &f, 1, "#1", "activity:numbers", &storage.ActivityTask{InstanceID: id, ActivityName: "numbers", InputData: []byte(`"input"`), Options: data, MaxAttempts: 2})
	require.NoError(t, err)
	task, err := fs.ClaimActivityTask(h.app.ctx, id, "#1", "released", time.Second, nil)
	require.NoError(t, err)
	require.NoError(t, fs.ReleaseActivityTask(h.app.ctx, task.Claim()))
	require.NoError(t, fs.ReleaseLease(h.app.ctx, f, true))
	require.NoError(t, h.RunOnce(id))
	setStoreClock(t, h, 2000)
	require.NoError(t, h.RunTasks(id))
	require.Error(t, h.RunOnce(id))
	require.Equal(t, []int{2, 3}, attempts)
	require.Equal(t, []int{2}, retries)
	require.Equal(t, []int{3}, failed)
}
func TestActivityQueues(t *testing.T) { testActivityQueues(t, createTestApp) }
func testActivityQueues(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	h.app.config.activityQueues = []string{"a"}
	calls := make(chan string, 2)
	a := DefineActivity("queues", func(ctx context.Context, _ string) (string, error) {
		q := GetActivityInfo(ctx).Options.Queue
		calls <- q
		return q, nil
	}, WithActivityDefaults[string, string](ActivityOptions{Queue: "a"}))
	w := DefineWorkflow("queues", func(ctx *WorkflowContext, in string) (string, error) {
		return a.Execute(ctx, in, WithActivityOptions(ActivityOptions{Queue: "b"}))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Empty(t, calls, "inline Get must yield on unserved queue")
	count, err := h.app.dispatchActivityTasks()
	require.NoError(t, err)
	require.Zero(t, count)
	second := NewApp(WithActivityQueues("b"))
	second.ctx = h.app.ctx
	second.storage = h.app.storage
	require.NoError(t, RegisterActivity(second, a))
	count, err = second.dispatchActivityTasks()
	require.NoError(t, err)
	require.Equal(t, 1, count)
	second.wg.Wait()
	require.Equal(t, "b", <-calls)
	require.NoError(t, h.RunOnce(id))
}
func TestRetryBackoffHonoured(t *testing.T) { testRetryBackoffHonoured(t, createTestApp) }
func testRetryBackoffHonoured(t *testing.T, factory replayAppFactory) {
	for _, dueNow := range []bool{false, true} {
		t.Run(fmt.Sprint(dueNow), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			h.app.storage = fs
			h.app.config.activityPollInterval = time.Hour
			h.app.config.maxPollInterval = time.Hour
			calls := make(chan int, 2)
			a := DefineActivity("due", func(ctx context.Context, _ string) (string, error) {
				attempt := GetActivityInfo(ctx).Attempt
				calls <- attempt
				if attempt == 1 {
					return "", errors.New("again")
				}
				return "done", nil
			}, WithRetryPolicy[string, string](retry.Fixed(2, 50*time.Millisecond)))
			w := DefineWorkflow("due", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, 1, <-calls)
			// Drain the local requeue wake: the poll loop must discover the due time itself.
			select {
			case <-h.app.activityWakeEvent:
			default:
			}
			queried := make(chan struct{}, 1)
			fs.Before = func(_ context.Context, method string) error {
				if dueNow && method == "NextActivityDue" {
					setStoreClock(t, h, 1050)
				}
				return nil
			}
			fs.After = func(_ context.Context, method string) error {
				if method == "NextActivityDue" {
					select {
					case queried <- struct{}{}:
					default:
					}
				}
				return nil
			}
			h.app.wg.Add(1)
			go h.app.runActivityWorkers()
			select {
			case <-queried:
			case <-time.After(2 * time.Second):
				t.Fatal("pool did not query due time")
			}
			if !dueNow {
				setStoreClock(t, h, 1050)
			}
			select {
			case attempt := <-calls:
				require.Equal(t, 2, attempt)
			case <-time.After(time.Second):
				t.Error("due retry waited for idle poll")
			}
			h.app.cancel()
			h.app.wg.Wait()
		})
	}
}

func TestRetrySurvivesAppRestart(t *testing.T) { testRetrySurvivesAppRestart(t, createTestApp) }
func testRetrySurvivesAppRestart(t *testing.T, factory replayAppFactory) {
	retryCommitted := make(chan struct{}, 1)
	observer := &s15Hooks{retry: func(context.Context, hooks.ActivityRetryInfo) { retryCommitted <- struct{}{} }}
	first, cleanup := factory(t, WithHooks(observer))
	t.Cleanup(cleanup)
	attempts := make(chan int, 3)
	a := DefineActivity("restart", func(ctx context.Context, _ string) (string, error) {
		attempts <- GetActivityInfo(ctx).Attempt
		return "", errors.New("first failure")
	}, WithActivityDefaults[string, string](ActivityOptions{Queue: "a", RetryPolicy: retry.Fixed(2, time.Second)}))
	w := DefineWorkflow("restart", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	require.NoError(t, RegisterActivity(first, a))
	RegisterWorkflow(first, w)
	require.NoError(t, first.Start(context.Background()))
	id, err := StartWorkflow(context.Background(), first, w, "")
	require.NoError(t, err)
	select {
	case <-retryCommitted:
	case <-time.After(5 * time.Second):
		t.Fatal("retry not committed")
	}
	require.NoError(t, first.Shutdown(context.Background()))
	require.Equal(t, 1, <-attempts)
	second := NewApp(WithDatabase(first.config.databaseURL), WithAutoMigrate(false), WithActivityQueues("a"), WithActivityPollInterval(time.Hour), WithMaxPollInterval(time.Hour))
	b := DefineActivity("restart", func(ctx context.Context, _ string) (string, error) {
		attempts <- GetActivityInfo(ctx).Attempt
		return "recovered", nil
	}, WithActivityDefaults[string, string](ActivityOptions{Queue: "b", RetryPolicy: retry.Fixed(5, time.Second)}))
	w2 := DefineWorkflow("restart", func(ctx *WorkflowContext, in string) (string, error) { return b.Execute(ctx, in) })
	require.NoError(t, RegisterActivity(second, b))
	RegisterWorkflow(second, w2)
	require.NoError(t, second.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, second.Shutdown(context.Background())) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := WaitWorkflow[string](ctx, second, id)
	require.NoError(t, err)
	require.Equal(t, "completed", result.Status)
	require.Equal(t, "recovered", result.Output)
	require.Equal(t, 2, <-attempts)
	require.Empty(t, attempts)
	rows, err := ListActivityTasks(ctx, second, id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "a", rows[0].Queue)
	require.Equal(t, 2, rows[0].Options.RetryPolicy.MaxAttempts)
	require.Equal(t, 1, rows[0].Failures)
	require.Contains(t, rows[0].LastError.Error(), "first failure")
}
