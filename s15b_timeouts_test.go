package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

func TestScheduleToStartTimeout(t *testing.T) { testScheduleToStartTimeout(t, createTestApp) }
func testScheduleToStartTimeout(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	h.app.config.activityQueues = []string{"other"}
	a := DefineActivity("schedule", func(context.Context, string) (string, error) { t.Error("expired task ran"); return "", nil }, WithActivityDefaults[string, string](ActivityOptions{ScheduleToStartTimeout: 10 * time.Millisecond}))
	w := DefineWorkflow("schedule", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	setStoreClock(t, h, 1010)
	require.NoError(t, h.app.checkActivityTimeouts(h.app.ctx))
	rows, err := ListActivityTasks(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "queued", rows[0].Status)
	setStoreClock(t, h, 1011)
	require.NoError(t, h.app.checkActivityTimeouts(h.app.ctx))
	err = h.RunOnce(id)
	var timeout *ActivityTimeoutError
	require.ErrorAs(t, err, &timeout)
	require.Equal(t, TimeoutScheduleToStart, timeout.Kind)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	replayed := decodeError(encodeError(timeout))
	require.ErrorAs(t, replayed, &timeout)
	require.Equal(t, "schedule", timeout.Activity)
}
func TestActivityTimeoutEnvelope(t *testing.T) {
	for _, kind := range []TimeoutKind{TimeoutScheduleToStart, TimeoutStartToClose, TimeoutHeartbeat} {
		original := &RetryExhaustedError{ActivityName: "A", Attempts: 3, LastErr: &ActivityTimeoutError{Activity: "A", Kind: kind, Attempt: 5}}
		actual := decodeError(encodeError(original))
		var timeout *ActivityTimeoutError
		require.ErrorAs(t, actual, &timeout)
		require.Equal(t, kind, timeout.Kind)
		require.Equal(t, 5, timeout.Attempt)
		require.ErrorIs(t, actual, context.DeadlineExceeded)
	}
}
func TestRecordHeartbeatAPI(t *testing.T) { testRecordHeartbeatAPI(t, createTestApp) }
func testRecordHeartbeatAPI(t *testing.T, factory replayAppFactory) {
	require.ErrorIs(t, RecordHeartbeat(context.Background()), ErrNotActivityContext)
	h, _, fs := leaseHarness(t, factory)
	a := DefineActivity("heartbeat_api", func(ctx context.Context, _ string) (string, error) {
		before := len(fs.Calls())
		require.NoError(t, RecordHeartbeat(ctx))
		require.Len(t, fs.Calls(), before)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, RecordHeartbeat(cancelled), context.Canceled)
		return "ok", nil
	})
	w := DefineWorkflow("heartbeat_api", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
}
func TestHeartbeatTimeoutStartsNewAttempt(t *testing.T) {
	testHeartbeatTimeoutStartsNewAttempt(t, createTestApp)
}
func testHeartbeatTimeoutStartsNewAttempt(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	returned := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	a := DefineActivity("heartbeat_lapse", func(ctx context.Context, _ string) (string, error) {
		if GetActivityInfo(ctx).Attempt == 1 {
			started <- ctx
			<-release
			close(returned)
			return "v1", nil
		}
		return "v2", nil
	}, WithActivityDefaults[string, string](ActivityOptions{HeartbeatTimeout: 30 * time.Millisecond, RetryPolicy: retry.Fixed(2, time.Millisecond)}))
	w := DefineWorkflow("heartbeat_lapse", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	ran := make(chan error, 1)
	go func() { ran <- h.RunOnce(id) }()
	var activityCtx context.Context
	select {
	case activityCtx = <-started:
	case <-time.After(time.Second):
		t.Fatal("attempt not started")
	}
	tasks, err := fs.ListActivityTasks(h.app.ctx, id)
	require.NoError(t, err)
	firstClaim := tasks[0].Claim()
	setStoreClock(t, h, 1031)
	select {
	case err := <-ran:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("inline waiter stayed blocked on an expired uncooperative attempt")
	}
	require.ErrorIs(t, context.Cause(activityCtx), errClaimLost)
	require.ErrorIs(t, RecordHeartbeat(activityCtx), errClaimLost)
	require.NoError(t, h.RunTasks(id))
	require.NoError(t, h.RunOnce(id))
	close(release)
	<-returned
	require.ErrorIs(t, fs.CompleteActivityTask(h.app.ctx, firstClaim, storage.HistoryActivityCompleted, []byte(`"v1"`)), storage.ErrFenced)
	_, err = fs.DB().Exec("UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1", id)
	require.NoError(t, err)
	require.NoError(t, h.RunOnce(id))
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.JSONEq(t, `"v2"`, string(row.EventData))
}
func TestStartToCloseTimeout(t *testing.T) { testStartToCloseTimeout(t, createTestApp) }
func testStartToCloseTimeout(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	started := make(chan context.Context, 2)
	release := make(chan struct{})
	defer close(release)
	a := DefineActivity("deadline", func(ctx context.Context, _ string) (string, error) { started <- ctx; <-release; return "late", nil }, WithActivityDefaults[string, string](ActivityOptions{StartToCloseTimeout: 30 * time.Millisecond, RetryPolicy: retry.Fixed(2, time.Millisecond)}))
	w := DefineWorkflow("deadline", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	for attempt := 1; attempt <= 2; attempt++ {
		ran := make(chan error, 1)
		go func() { ran <- h.RunOnce(id) }()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("attempt not started")
		}
		setStoreClock(t, h, int64(1000+attempt*31))
		select {
		case err := <-ran:
			// The local deadline may finish the attempt before the store-clock
			// update; either path must produce the same persisted timeout.
			require.True(t, err == nil || errors.Is(err, context.DeadlineExceeded), "unexpected execution error: %v", err)
		case <-time.After(time.Second):
			t.Fatal("deadline did not unblock inline waiter")
		}
		if attempt == 1 {
			require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
		}
	}
	require.NoError(t, h.app.checkActivityTimeouts(h.app.ctx))
	runErr := h.RunOnce(id)
	require.True(t, runErr == nil || errors.Is(runErr, context.DeadlineExceeded), "unexpected execution error: %v", runErr)
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	err = result.Error
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.Equal(t, 2, exhausted.Attempts)
	var timeout *ActivityTimeoutError
	require.ErrorAs(t, err, &timeout)
	require.Equal(t, TimeoutStartToClose, timeout.Kind)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
func TestActivityTimeoutRecordedOnce(t *testing.T) { testActivityTimeoutRecordedOnce(t, createTestApp) }
func testActivityTimeoutRecordedOnce(t *testing.T, factory replayAppFactory) {
	for _, race := range []string{"claim_wins", "timeout_wins"} {
		t.Run(race, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			h.app.storage = fs
			h.app.config.activityQueues = []string{"other"}
			a := DefineActivity("timeout_race", func(context.Context, string) (string, error) { return "winner", nil }, WithActivityDefaults[string, string](ActivityOptions{ScheduleToStartTimeout: time.Millisecond}))
			w := DefineWorkflow("timeout_race", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			setStoreClock(t, h, 1002)
			injected := false
			fs.Before = func(ctx context.Context, method string) error {
				if method == "TimeoutActivityTask" && !injected {
					injected = true
					if race == "claim_wins" {
						setStoreClock(t, h, 1001)
						claimed, err := fs.Storage.ClaimActivityTask(ctx, id, "#1", "race", time.Second, nil)
						require.NoError(t, err)
						require.NotNil(t, claimed)
						require.NoError(t, fs.Storage.CompleteActivityTask(ctx, claimed.Claim(), storage.HistoryActivityCompleted, []byte(`"winner"`)))
						setStoreClock(t, h, 1002)
					}
				}
				return nil
			}
			require.NoError(t, h.app.checkActivityTimeouts(h.app.ctx))
			require.True(t, injected)
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			if race == "claim_wins" {
				require.JSONEq(t, `"winner"`, string(row.EventData))
			} else {
				require.Equal(t, storage.HistoryActivityFailed, row.EventType)
				require.True(t, errors.Is(decodeError(row.EventData), context.DeadlineExceeded))
			}
		})
	}
}

func TestActivityHeartbeatKeepsClaim(t *testing.T) { testActivityHeartbeatKeepsClaim(t, createTestApp) }
func testActivityHeartbeatKeepsClaim(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	defer close(release)
	a := DefineActivity("keeps", func(ctx context.Context, _ string) (string, error) { started <- ctx; <-release; return "done", nil }, WithActivityDefaults[string, string](ActivityOptions{HeartbeatTimeout: 30 * time.Millisecond}))
	w := DefineWorkflow("keeps", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	ctx := <-started
	for now := int64(1010); now <= 1300; now += 10 {
		setStoreClock(t, h, now)
		require.NoError(t, RecordHeartbeat(ctx))
		require.Eventually(t, func() bool {
			rows, err := h.app.storage.ListActivityTasks(h.app.ctx, id)
			return err == nil && len(rows) == 1 && rows[0].ClaimExpiresMS != nil && *rows[0].ClaimExpiresMS == now+30
		}, time.Second, time.Millisecond)
		require.NoError(t, ctx.Err())
	}
	// Cancelling the scope's task must also stop a heartbeating invocation.
	_, err := h.app.storage.DB().Exec("UPDATE workflow_activity_tasks SET status='cancelled' WHERE instance_id=$1", id)
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("cancelled task kept its claim")
	}
	require.ErrorIs(t, RecordHeartbeat(ctx), errClaimLost)
}
func TestActivityGoexitDoesNotHangClaim(t *testing.T) {
	testActivityGoexitDoesNotHangClaim(t, createTestApp)
}
func testActivityGoexitDoesNotHangClaim(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	a := DefineActivity("goexit", func(context.Context, string) (string, error) { runtime.Goexit(); return "", nil })
	w := DefineWorkflow("goexit", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	select {
	case err := <-done:
		var panicErr *WorkflowPanicError
		require.ErrorAs(t, err, &panicErr)
	case <-time.After(time.Second):
		t.Fatal("Goexit hung the claim")
	}
}

type s15bPanicInput string

func (*s15bPanicInput) UnmarshalJSON([]byte) error { panic("decoder panic") }
func TestActivityDecoderPanicIsRecorded(t *testing.T) {
	testActivityDecoderPanicIsRecorded(t, createTestApp)
}
func testActivityDecoderPanicIsRecorded(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	a := DefineActivity("decoder_panic", func(context.Context, s15bPanicInput) (string, error) {
		t.Error("invoked after decoder panic")
		return "", nil
	})
	w := DefineWorkflow("decoder_panic", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, s15bPanicInput(in)) })
	id := seedErrorWorkflow(t, h, w)
	err := h.RunOnce(id)
	var panicErr *WorkflowPanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "decoder panic", panicErr.Value)
	rows, err := ListActivityTasks(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, 1, rows[0].Attempts)
	require.Equal(t, "completed", rows[0].Status)
}

func TestCooperativeStartToCloseTimeout(t *testing.T) {
	testCooperativeStartToCloseTimeout(t, createTestApp)
}
func testCooperativeStartToCloseTimeout(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	a := DefineActivity("cooperative", func(ctx context.Context, _ string) (string, error) { <-ctx.Done(); return "", ctx.Err() }, WithActivityDefaults[string, string](ActivityOptions{StartToCloseTimeout: 10 * time.Millisecond, RetryPolicy: retry.Fixed(2, time.Millisecond)}))
	w := DefineWorkflow("cooperative", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	rows, err := ListActivityTasks(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "queued", rows[0].Status)
	var timeout *ActivityTimeoutError
	require.ErrorAs(t, rows[0].LastError, &timeout)
	require.Equal(t, TimeoutStartToClose, timeout.Kind)
	setStoreClock(t, h, 1001)
	require.NoError(t, h.RunTasks(id))
	err = h.RunOnce(id)
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestExhaustedTimeoutFencesLateCompletion(t *testing.T) {
	testExhaustedTimeoutFencesLateCompletion(t, createTestApp)
}
func testExhaustedTimeoutFencesLateCompletion(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	h.app.config.activityQueues = []string{"other"}
	a := DefineActivity("exhausted", func(context.Context, string) (string, error) { t.Error("unexpected invocation"); return "", nil }, WithActivityDefaults[string, string](ActivityOptions{HeartbeatTimeout: time.Millisecond, RetryPolicy: retry.Fixed(1, time.Millisecond)}))
	w := DefineWorkflow("exhausted", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	claim, err := fs.ClaimActivityTask(h.app.ctx, id, "#1", "late", time.Second, nil)
	require.NoError(t, err)
	require.NotNil(t, claim)
	setStoreClock(t, h, 1002)
	injected := false
	fs.Before = func(ctx context.Context, method string) error {
		if method == "TimeoutActivityTask" {
			injected = true
			require.ErrorIs(t, fs.Storage.CompleteActivityTask(ctx, claim.Claim(), storage.HistoryActivityCompleted, []byte(`"late"`)), storage.ErrFenced)
		}
		return nil
	}
	require.NoError(t, h.app.checkActivityTimeouts(h.app.ctx))
	require.True(t, injected)
	require.ErrorIs(t, fs.CompleteActivityTask(h.app.ctx, claim.Claim(), storage.HistoryActivityCompleted, []byte(`"later"`)), storage.ErrFenced)
	err = h.RunOnce(id)
	var exhausted *RetryExhaustedError
	require.ErrorAs(t, err, &exhausted)
	var timeout *ActivityTimeoutError
	require.ErrorAs(t, err, &timeout)
	require.Equal(t, TimeoutHeartbeat, timeout.Kind)
}

func TestPoolWaitDoesNotConsumeExecutionTimeout(t *testing.T) {
	testPoolWaitDoesNotConsumeExecutionTimeout(t, createTestApp)
}
func testPoolWaitDoesNotConsumeExecutionTimeout(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	observed := make(chan error, 1)
	options := ActivityOptions{StartToCloseTimeout: 20 * time.Millisecond, RetryPolicy: retry.NoRetry()}
	a := DefineActivity("delay", func(ctx context.Context, _ string) (string, error) { observed <- ctx.Err(); return "ok", ctx.Err() }, WithActivityDefaults[string, string](options))
	w := DefineWorkflow("delay", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, RegisterActivity(h.app, a))
	f, ok, err := h.app.storage.AcquireLease(h.app.ctx, id, "seed", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	recorded, err := json.Marshal(options)
	require.NoError(t, err)
	_, err = h.app.storage.ReserveActivity(h.app.ctx, &f, 1, "#1", "activity:delay", &storage.ActivityTask{InstanceID: id, ActivityName: "delay", InputData: []byte(`"input"`), Options: recorded, MaxAttempts: 1, StartToCloseMS: 20})
	require.NoError(t, err)
	db := h.app.storage.DB()
	db.SetMaxOpenConns(1)
	held, err := db.Conn(h.app.ctx)
	require.NoError(t, err)
	defer func() { _ = held.Close() }()
	initialWaits := db.Stats().WaitCount
	claimed := make(chan *storage.ActivityTask, 1)
	errs := make(chan error, 1)
	go func() {
		task, e := h.app.storage.ClaimActivityTask(h.app.ctx, id, "#1", "worker", time.Second, nil)
		claimed <- task
		errs <- e
	}()
	require.Eventually(t, func() bool { return db.Stats().WaitCount > initialWaits }, time.Second, time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, held.Close())
	task := <-claimed
	require.NoError(t, <-errs)
	require.NotNil(t, task)
	require.EqualValues(t, 1020, *task.AttemptDeadlineMS)
	require.NoError(t, h.app.runActivityTask(h.app.ctx, h.app.storage, task, nil))
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	t.Logf("store time=1000, durable deadline=%d; persisted outcome=%s", *task.AttemptDeadlineMS, row.EventData)
	require.NoError(t, <-observed, "attempt began with an expired local deadline after waiting for a database connection")
}
