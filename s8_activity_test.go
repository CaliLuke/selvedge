package selvedge

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func (h *typedReplayHarness) RunTasks(id string, names ...string) error {
	if len(names) == 0 {
		names = h.app.registeredActivityNames()
	}
	tasks, err := h.app.storage.ClaimActivityTasks(h.app.ctx, "harness-pool", h.app.config.lease.TTL, 100, names...)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if task.InstanceID != id {
			if err := h.app.storage.ReleaseActivityTask(h.app.ctx, task.Claim()); err != nil {
				return err
			}
			continue
		}
		if err := h.app.runActivityTask(h.app.ctx, h.app.storage, task, nil); err != nil {
			return err
		}
	}
	return nil
}

type harnessTaskClaim struct {
	h    *typedReplayHarness
	task *storage.ActivityTask
}

func (h *typedReplayHarness) ClaimTask(id, key string) (*harnessTaskClaim, error) {
	task, err := h.app.storage.ClaimActivityTask(h.app.ctx, id, key, "harness", time.Second)
	if err != nil || task == nil {
		return nil, err
	}
	return &harnessTaskClaim{h, task}, nil
}
func (c *harnessTaskClaim) Run() error {
	return c.h.app.runActivityTask(c.h.app.ctx, c.h.app.storage, c.task, nil)
}
func (c *harnessTaskClaim) Complete(value string) error {
	return c.h.app.storage.CompleteActivityTask(c.h.app.ctx, c.task.Claim(), storage.HistoryActivityCompleted, []byte(value))
}

func seedTask(t *testing.T, h *typedReplayHarness, id string) {
	t.Helper()
	f, ok, err := h.app.storage.AcquireLease(h.app.ctx, id, "seed", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = h.app.storage.ReserveActivity(h.app.ctx, &f, 1, "#1", "activity:A", &storage.ActivityTask{InstanceID: id, ActivityName: "A", InputData: []byte(`"input"`)})
	require.NoError(t, err)
	require.NoError(t, h.app.storage.ReleaseLease(h.app.ctx, f, true))
}
func taskCount(t *testing.T, h *typedReplayHarness, id string) int {
	t.Helper()
	var n int
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT COUNT(*) FROM workflow_activity_tasks WHERE instance_id=$1", id).Scan(&n))
	return n
}
func TestActivityStartsOnlyFromTaskRow(t *testing.T) {
	testActivityStartsOnlyFromTaskRow(t, createTestApp)
}
func testActivityStartsOnlyFromTaskRow(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"before", "after", "deleted", "cancelled", "attempt"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			calls := 0
			a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
				calls++
				info := GetActivityInfo(ctx)
				require.NotNil(t, info)
				require.Equal(t, 1, info.Attempt)
				var attempts int
				require.NoError(t, h.app.storage.DB().QueryRow("SELECT attempts FROM workflow_activity_tasks WHERE instance_id=$1 AND activity_id=$2", info.InstanceID, info.ActivityID).Scan(&attempts))
				require.Equal(t, attempts, info.Attempt)
				require.Equal(t, "A", info.ActivityName)
				return "stored", nil
			})
			w := DefineWorkflow("task_only", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
			id := seedErrorWorkflow(t, h, w)
			if kind == "deleted" || kind == "cancelled" {
				seedTask(t, h, id)
				query := "DELETE FROM workflow_activity_tasks"
				if kind == "cancelled" {
					query = "UPDATE workflow_activity_tasks SET status='cancelled'"
				}
				_, err := h.app.storage.DB().Exec(query)
				require.NoError(t, err)
			}
			injected := false
			inject := func(_ context.Context, method string) error {
				if method == "ReserveActivity" && !injected {
					injected = true
					return storage.ErrFenced
				}
				return nil
			}
			if kind == "before" {
				fs.Before = inject
			}
			if kind == "after" {
				fs.After = inject
			}
			err := h.RunOnce(id)
			switch kind {
			case "before", "after":
				require.ErrorIs(t, err, storage.ErrFenced)
				require.Zero(t, calls)
				if kind == "before" {
					require.Zero(t, taskCount(t, h, id))
					row, e := fs.GetHistoryEvent(h.app.ctx, id, "#1")
					require.NoError(t, e)
					require.Nil(t, row)
					return
				}
				fs.After = nil
				setStoreClock(t, h, 4000)
				require.NoError(t, h.RunOnce(id))
				require.Equal(t, 1, calls)
				require.Equal(t, 1, countMethod(fs.Calls(), "ReserveActivity"))
				require.Contains(t, fs.Calls(), "ClaimActivityTask")
				require.Less(t, slices.Index(fs.Calls(), "ReserveActivity"), slices.Index(fs.Calls(), "ClaimActivityTask"))
			case "deleted", "cancelled":
				require.NoError(t, err)
				require.NoError(t, h.RunTasks(id))
				require.Zero(t, calls)
				inst, e := fs.GetInstance(h.app.ctx, id)
				require.NoError(t, e)
				require.Equal(t, storage.StatusRunning, inst.Status)
				require.Empty(t, inst.LeaseOwner)
				require.False(t, inst.Runnable)
			default:
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				require.Equal(t, 1, taskCount(t, h, id))
				before := len(fs.Calls())
				_, err = h.app.storage.DB().Exec("UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1", id)
				require.NoError(t, err)
				require.NoError(t, h.RunOnce(id))
				require.NotContains(t, fs.Calls()[before:], "ReserveActivity")
				require.NotContains(t, fs.Calls()[before:], "ClaimActivityTask")
				require.Equal(t, 1, calls)
			}
		})
	}
}
func countMethod(calls []string, name string) int {
	n := 0
	for _, call := range calls {
		if call == name {
			n++
		}
	}
	return n
}

func TestFutureGetReadsHistory(t *testing.T) { testFutureGetReadsHistory(t, createTestApp) }
func testFutureGetReadsHistory(t *testing.T, factory replayAppFactory) {
	h, clock, _ := leaseHarness(t, factory)
	// Only the activity claim lapses. Keep the independent workflow lease valid across the store-clock jump.
	require.NoError(t, h.app.engine.ConfigureLease(replay.LeaseConfig{TTL: time.Hour, RenewInterval: time.Minute, Margin: time.Second, MaxSkew: time.Millisecond}, func() replay.Clock { return clock }))
	calls := 0
	a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
		calls++
		id := GetActivityInfo(ctx).InstanceID
		setStoreClock(t, h, 100000)
		claim, err := h.ClaimTask(id, "#1")
		require.NoError(t, err)
		require.NotNil(t, claim)
		require.NoError(t, claim.Complete(`"recorded"`))
		return "live", nil
	})
	var values []string
	w := DefineWorkflow("read_history", func(ctx *WorkflowContext, in string) (string, error) {
		v, err := a.Execute(ctx, in)
		values = append(values, v)
		if err != nil {
			return "", err
		}
		return v, Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []string{"recorded"}, values)
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []string{"recorded", "recorded"}, values)
	require.Equal(t, 1, calls)
	require.NoError(t, h.app.storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#2"}))
	require.NoError(t, h.RunOnce(id))
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "recorded", result.Output)
}
func TestActivityResultDuringSuspendIsNotLost(t *testing.T) {
	testActivityResultDuringSuspendIsNotLost(t, createTestApp)
}
func testActivityResultDuringSuspendIsNotLost(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	calls := 0
	a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "wrong", nil })
	w := DefineWorkflow("wake_activity", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NotNil(t, claim)
	completed := false
	fs.Before = func(_ context.Context, m string) error {
		if m == "ReleaseLease" && !completed {
			completed = true
			return claim.Complete(`"winner"`)
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	require.True(t, completed)
	require.Zero(t, calls)
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.True(t, inst.Runnable)
	fs.Before = nil
	require.NoError(t, h.RunOnce(id))
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "winner", result.Output)
}
func TestActivityPanicOnWorkerFailsRun(t *testing.T) {
	testActivityPanicOnWorkerFailsRun(t, createTestApp)
}
func testActivityPanicOnWorkerFailsRun(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	calls := 0
	a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; panic("pool panic") })
	require.NoError(t, RegisterActivity(h.app, a))
	w := DefineWorkflow("pool_panic", func(ctx *WorkflowContext, in string) (string, error) { _, _ = a.Execute(ctx, in); return "caught", nil })
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	require.NoError(t, h.RunTasks(id))
	require.Equal(t, 1, calls)
	var panicErr *WorkflowPanicError
	require.ErrorAs(t, h.RunOnce(id), &panicErr)
	require.Equal(t, "pool panic", panicErr.Value)
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.ErrorAs(t, result.Error, &panicErr)
}
func TestActivityRegistryConflict(t *testing.T) { testActivityRegistryConflict(t, createTestApp) }
func testActivityRegistryConflict(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "a", nil })
	b := DefineActivity("A", func(context.Context, string) (string, error) { return "b", nil })
	require.NoError(t, RegisterActivity(h.app, a))
	require.NoError(t, RegisterActivity(h.app, a))
	require.ErrorIs(t, RegisterActivity(h.app, b), ErrActivityNameConflict)
	w := DefineWorkflow("conflict", func(ctx *WorkflowContext, in string) (string, error) { _, _ = b.Execute(ctx, in); return "caught", nil })
	id := seedErrorWorkflow(t, h, w)
	require.ErrorIs(t, h.RunOnce(id), ErrActivityNameConflict)
	require.Zero(t, taskCount(t, h, id))
}
func TestActivityShutdownReleasesClaim(t *testing.T) {
	testActivityShutdownReleasesClaim(t, createTestApp)
}
func testActivityShutdownReleasesClaim(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	entered := make(chan struct{})
	a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
		close(entered)
		<-ctx.Done()
		return "discard", nil
	})
	require.NoError(t, RegisterActivity(h.app, a))
	w := DefineWorkflow("shutdown_task", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)

	// Use a separate connection so the durable claim can be inspected after Shutdown closes the first App's pool.
	disabled := false
	second := NewApp(WithDatabase(h.app.config.databaseURL), WithListenNotify(&disabled))
	second.ctx, second.cancel = context.WithCancel(context.Background())
	require.NoError(t, second.initStorage())
	t.Cleanup(func() { second.cancel(); require.NoError(t, second.storage.Close()) })
	// The synchronous harness tracks one attempt in the same wait group used by the production pool.
	h.app.running = true
	h.app.wg.Add(1)
	done := make(chan error, 1)
	go func() {
		defer h.app.wg.Done()
		done <- h.app.runActivityTask(h.app.ctx, h.app.storage, claim.task, nil)
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("attempt did not enter: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("attempt did not enter")
	}
	require.NoError(t, h.app.Shutdown(context.Background()))
	<-done
	row, err := second.storage.GetHistoryEvent(second.ctx, id, "#1")
	require.NoError(t, err)
	require.Nil(t, row.EventData)
	var status string
	require.NoError(t, second.storage.DB().QueryRow("SELECT status FROM workflow_activity_tasks WHERE instance_id=$1", id).Scan(&status))
	require.Equal(t, "queued", status)
	calls := 0
	b := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
		calls++
		require.Equal(t, 2, GetActivityInfo(ctx).Attempt)
		return "recovered", nil
	})
	require.NoError(t, RegisterActivity(second, b))
	reclaimed, err := second.storage.ClaimActivityTask(second.ctx, id, "#1", "second", time.Second)
	require.NoError(t, err)
	require.NotNil(t, reclaimed)
	require.NoError(t, second.runActivityTask(second.ctx, second.storage, reclaimed, nil))
	require.Equal(t, 1, calls)
	row, err = second.storage.GetHistoryEvent(second.ctx, id, "#1")
	require.NoError(t, err)
	require.JSONEq(t, `"recovered"`, string(row.EventData))
}

func TestActivityPoolHonorsLimitAndNames(t *testing.T) {
	testActivityPoolHonorsLimitAndNames(t, createTestApp)
}
func testActivityPoolHonorsLimitAndNames(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory, WithMaxConcurrentActivities(1), WithActivityPollInterval(time.Millisecond))
	entered := make(chan string, 2)
	release := make(chan struct{})
	defer close(release)
	a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
		entered <- GetActivityInfo(ctx).InstanceID
		select {
		case <-release:
		case <-ctx.Done():
		}
		return "done", nil
	})
	require.NoError(t, RegisterActivity(h.app, a))
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: "unknown", WorkflowName: "pool", Status: storage.StatusRunning}))
	f, ok, err := h.app.storage.AcquireLease(h.app.ctx, "unknown", "seed", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = h.app.storage.ReserveActivity(h.app.ctx, &f, 1, "#1", "activity:B", &storage.ActivityTask{InstanceID: "unknown", ActivityName: "B", InputData: []byte(`null`)})
	require.NoError(t, err)
	require.NoError(t, h.app.storage.ReleaseLease(h.app.ctx, f, false))
	for _, id := range []string{"pool1", "pool2"} {
		require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: id, WorkflowName: "pool", Status: storage.StatusRunning}))
		seedTask(t, h, id)
	}
	h.app.wg.Add(1)
	go h.app.runActivityWorkers()
	defer func() { h.app.cancel(); h.app.wg.Wait() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("pool did not execute queued task")
	}
	select {
	case <-entered:
		t.Fatal("pool exceeded concurrency limit")
	case <-time.After(30 * time.Millisecond):
	}
	var attempts int
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT attempts FROM workflow_activity_tasks WHERE instance_id='unknown'").Scan(&attempts))
	require.Zero(t, attempts)
}

func TestActivityShutdownBeforeStartReleasesClaim(t *testing.T) {
	testActivityShutdownBeforeStartReleasesClaim(t, createTestApp)
}
func testActivityShutdownBeforeStartReleasesClaim(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { t.Error("cancelled attempt ran"); return "", nil })
	require.NoError(t, RegisterActivity(h.app, a))
	w := DefineWorkflow("cancel_before_task", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NotNil(t, claim)
	ctx, cancel := context.WithCancel(h.app.ctx)
	cancel()
	require.Error(t, h.app.runActivityTask(ctx, h.app.storage, claim.task, nil))
	var status string
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT status FROM workflow_activity_tasks WHERE instance_id=$1", id).Scan(&status))
	require.Equal(t, "queued", status)
}

// N7 exercises the engine close path after a reservation survives a claim error.
func TestCloseCancelsInFlightTasks(t *testing.T) {
	testCloseCancelsInFlightTasks(t, createTestApp)
}
func testCloseCancelsInFlightTasks(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	claimFailure := errors.New("injected inline claim failure")
	calls := 0
	a := DefineActivity("A", func(context.Context, string) (string, error) {
		calls++
		return "unexpected", nil
	})
	w := DefineWorkflow("claim_failure", func(ctx *WorkflowContext, input string) (string, error) {
		return a.Execute(ctx, input)
	})
	id := seedErrorWorkflow(t, h, w)
	fs.Before = func(_ context.Context, method string) error {
		if method == "ClaimActivityTask" {
			return claimFailure
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), claimFailure)
	require.Zero(t, calls)
	require.Equal(t, 1, countMethod(fs.Calls(), "ReserveActivity"))
	require.Equal(t, 1, countMethod(fs.Calls(), "ClaimActivityTask"))
	require.Equal(t, 1, countMethod(fs.Calls(), "CloseInstance"))
	require.NotContains(t, fs.Calls(), "CompleteActivityTask")
	instance, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusFailed, instance.Status)
	var status string
	var attempts int
	require.NoError(t, fs.DB().QueryRow("SELECT status,attempts FROM workflow_activity_tasks WHERE instance_id=$1", id).Scan(&status, &attempts))
	require.Equal(t, "cancelled", status)
	require.Zero(t, attempts)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Nil(t, row.EventData, "close must leave the activity outcome unfilled")
}
