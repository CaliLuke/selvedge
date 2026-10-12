package selvedge

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
	"github.com/CaliLuke/selvedge/internal/testkit"
)

func leaseHarness(t *testing.T, factory replayAppFactory) (*typedReplayHarness, *testkit.Clock, *faultstore.Store) {
	t.Helper()
	h := newTypedReplayHarness(t, factory, WithLease(time.Second, 200*time.Millisecond, 200*time.Millisecond, 100*time.Millisecond))
	setStoreClock(t, h, 1000)
	clk := &testkit.Clock{}
	fs := faultstore.New(h.app.storage)
	h.app.engine = replay.NewEngine(fs, h.app.hooks, "worker", errorCodec{})
	require.NoError(t, h.app.engine.ConfigureLease(replay.LeaseConfig{TTL: time.Second, RenewInterval: 200 * time.Millisecond, Margin: 200 * time.Millisecond, MaxSkew: 100 * time.Millisecond}, func() replay.Clock { return clk }))
	return h, clk, fs
}
func TestLeaseCheckStopsBeforeTakeover(t *testing.T) {
	testLeaseCheckStopsBeforeTakeover(t, createTestApp)
}
func testLeaseCheckStopsBeforeTakeover(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	calls := 0
	a := DefineActivity("never", func(context.Context, string) (string, error) { calls++; return "", nil })
	w := DefineWorkflow("lease_check", func(ctx *WorkflowContext, input string) (string, error) {
		clk.Advance(801 * time.Millisecond)
		_, err := a.Execute(ctx, input)
		require.ErrorIs(t, err, replay.ErrLeaseLost)
		return "swallowed", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	require.Zero(t, calls)
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{id}, ids)
	require.Equal(t, []string{"AcquireLease", "ReleaseLease"}, executionWrites(fs))
	releases := 0
	for _, stmt := range fs.Statements() {
		if strings.Contains(stmt.SQL, "lease_owner=NULL") {
			releases++
			require.Contains(t, stmt.SQL, "runnable=CASE")
			require.Equal(t, true, stmt.Args[0])
		}
	}
	require.Equal(t, 1, releases, "abandon clears ownership and sets runnable in one statement")
}
func TestTwoHandlersOneProcessDoNotShareLease(t *testing.T) {
	testTwoHandlersOneProcessDoNotShareLease(t, createTestApp)
}
func testTwoHandlersOneProcessDoNotShareLease(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	entered := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var handlers sync.WaitGroup
	defer func() {
		releaseOnce.Do(func() { close(release) })
		stopped := make(chan struct{})
		go func() { handlers.Wait(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("handlers did not stop")
		}
	}()
	w := DefineWorkflow("owners", func(ctx *WorkflowContext, _ string) (string, error) {
		entered <- ctx.execCtx.Fence().Owner
		<-release
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	done := make(chan error, 1)
	handlers.Add(1)
	go func() { defer handlers.Done(); done <- h.RunOnce(id) }()
	var owner string
	select {
	case owner = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not enter")
	}
	second := make(chan error, 1)
	handlers.Add(1)
	go func() { defer handlers.Done(); second <- h.RunOnce(id) }()
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-entered:
		t.Fatal("second handler entered an already leased workflow")
	case <-time.After(5 * time.Second):
		t.Fatal("second handler did not return")
	}
	require.Empty(t, entered)
	require.Contains(t, owner, "worker/")
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not finish")
	}
	var owners []string
	for _, stmt := range fs.Statements() {
		if strings.Contains(stmt.SQL, "SET lease_owner=$1") {
			owners = append(owners, stmt.Args[0].(string))
		}
	}
	require.Len(t, owners, 2)
	require.NotEqual(t, owners[0], owners[1])
}
func TestInputDuringSuspendIsNotLost(t *testing.T) { testInputDuringSuspendIsNotLost(t, createTestApp) }
func testInputDuringSuspendIsNotLost(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("suspend_input", func(ctx *WorkflowContext, _ string) (string, error) {
		return "done", Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	id := seedErrorWorkflow(t, h, w)
	var once sync.Once
	fs.Before = func(ctx context.Context, method string) error {
		if method == "ReleaseLease" {
			once.Do(func() {
				require.NoError(t, fs.Storage.FireTimer(ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "pause"}))
			})
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
}
func TestAbandonedRunIsRedispatched(t *testing.T) { testAbandonedRunIsRedispatched(t, createTestApp) }
func testAbandonedRunIsRedispatched(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	first := true
	a := DefineActivity("after_timer", func(context.Context, string) (string, error) { return "done", nil })
	w := DefineWorkflow("abandon", func(ctx *WorkflowContext, input string) (string, error) {
		if err := Sleep(ctx, time.Hour, WithSleepID("pause")); err != nil {
			return "", err
		}
		if first {
			first = false
			clk.Advance(time.Second)
		}
		return a.Execute(ctx, input)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "pause"}))
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
}

func TestTakeoverAfterCrashCompletes(t *testing.T) { testTakeoverAfterCrashCompletes(t, createTestApp) }
func testTakeoverAfterCrashCompletes(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	crash := errors.New("crash after grant")
	w := DefineWorkflow("takeover", func(*WorkflowContext, string) (string, error) { return "done", nil })
	id := seedErrorWorkflow(t, h, w)
	fs.After = func(_ context.Context, method string) error {
		if method == "AcquireLease" {
			return crash
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), crash)
	fs.After = nil
	setStoreClock(t, h, 2001)
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
}
func TestRunsTerminateWithoutFaults(t *testing.T) { testRunsTerminateWithoutFaults(t, createTestApp) }
func testRunsTerminateWithoutFaults(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "value", nil })
	w := DefineWorkflow("terminate", func(ctx *WorkflowContext, input string) (string, error) {
		v, err := a.Execute(ctx, input)
		if err != nil {
			return "", err
		}
		return v, Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	// Drain the activity result's wake so only the timer can make this runnable.
	require.NoError(t, h.RunOnce(id))
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.NoError(t, fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "pause"}))
	ids, err = fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{id}, ids, "timer must wake the parked workflow")
	require.NoError(t, h.RunOnce(ids[0]))
	assertCompletedLeaseRun(t, h, id)
}
func assertCompletedLeaseRun(t *testing.T, h *typedReplayHarness, id string) {
	t.Helper()
	inst, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
}

func TestLeaseRenewedDuringLongActivity(t *testing.T) {
	testLeaseRenewedDuringLongActivity(t, createTestApp)
}
func testLeaseRenewedDuringLongActivity(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	require.NoError(t, h.app.engine.ConfigureLease(replay.LeaseConfig{TTL: time.Second, RenewInterval: 10 * time.Millisecond, Margin: 200 * time.Millisecond, MaxSkew: 100 * time.Millisecond}, func() replay.Clock { return clk }))
	started := make(chan struct{})
	release := make(chan struct{})
	renewed := make(chan struct{}, 8)
	var elapsed int64
	fs.Before = func(_ context.Context, method string) error {
		if method == "RenewLease" {
			elapsed += 200
			clk.Advance(200 * time.Millisecond)
			setStoreClock(t, h, 1000+elapsed)
		}
		return nil
	}
	fs.After = func(_ context.Context, method string) error {
		if method == "RenewLease" {
			select {
			case renewed <- struct{}{}:
			default:
			}
		}
		return nil
	}
	a := DefineActivity("long", func(context.Context, string) (string, error) { close(started); <-release; return "done", nil })
	w := DefineWorkflow("long_activity", func(ctx *WorkflowContext, input string) (string, error) { return a.Execute(ctx, input) })
	id := seedErrorWorkflow(t, h, w)
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	<-started
	for range 6 {
		select {
		case <-renewed:
		case <-time.After(5 * time.Second):
			t.Fatal("renewal did not progress")
		}
	}
	_, ok, err := fs.Storage.AcquireLease(h.app.ctx, id, "competitor", time.Second)
	require.NoError(t, err)
	require.False(t, ok)
	close(release)
	require.NoError(t, <-done)
	assertCompletedLeaseRun(t, h, id)
}

func TestLeaseRequestLatencyCountsAgainstBudget(t *testing.T) {
	testLeaseRequestLatencyCountsAgainstBudget(t, createTestApp)
}
func testLeaseRequestLatencyCountsAgainstBudget(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	calls := 0
	w := DefineWorkflow("slow_grant", func(*WorkflowContext, string) (string, error) { calls++; return "unexpected", nil })
	id := seedErrorWorkflow(t, h, w)
	fs.After = func(_ context.Context, method string) error {
		if method == "AcquireLease" {
			clk.Advance(801 * time.Millisecond)
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	require.Zero(t, calls)
}

func TestLeaseConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		ttl, renew, margin, skew time.Duration
		valid                    bool
	}{
		{"equal", time.Second, 100 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond, true},
		{"below_skew", time.Second, 100 * time.Millisecond, 199 * time.Millisecond, 200 * time.Millisecond, false},
		{"no_renewal_budget", time.Second, 800 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond, false},
		{"zero_renewal", time.Second, 0, 200 * time.Millisecond, 100 * time.Millisecond, false},
		{"negative_skew", time.Second, 100 * time.Millisecond, 200 * time.Millisecond, -time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithLease(tc.ttl, tc.renew, tc.margin, tc.skew)(cfg)
			err := cfg.lease.Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	cfg := defaultConfig()
	require.Greater(t, cfg.lease.Margin, cfg.lease.MaxSkew)
}

func TestNewWorkflowWaitsForDispatcher(t *testing.T) {
	testNewWorkflowWaitsForDispatcher(t, createTestApp)
}
func testNewWorkflowWaitsForDispatcher(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	calls := 0
	w := DefineWorkflow("dispatch", func(*WorkflowContext, string) (string, error) { calls++; return "done", nil })
	RegisterWorkflow(h.app, w)
	id, err := h.app.startWorkflow(h.app.ctx, w.Name(), "input", &startOptions{})
	require.NoError(t, err)
	require.Zero(t, calls)
	require.Len(t, h.app.resumeWakeEvent, 1)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 1, calls)
	assertCompletedLeaseRun(t, h, id)
}
func TestLeaderLeasePreservesMilliseconds(t *testing.T) {
	testLeaderLeasePreservesMilliseconds(t, createTestApp)
}
func testLeaderLeasePreservesMilliseconds(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	ok, err := h.app.storage.TryAcquireSystemLock(h.app.ctx, "leader", "one", 750*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = h.app.storage.TryAcquireSystemLock(h.app.ctx, "leader", "two", time.Second)
	require.NoError(t, err)
	require.False(t, ok)
	var start, end any
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT locked_at,lock_expires_at FROM system_locks WHERE lock_name='leader'").Scan(&start, &end))
	asTime := func(v any) time.Time {
		switch v := v.(type) {
		case time.Time:
			return v
		case string:
			tt, err := time.Parse("2006-01-02 15:04:05.000", v)
			require.NoError(t, err)
			return tt
		default:
			t.Fatalf("unexpected timestamp %T", v)
			return time.Time{}
		}
	}
	require.Equal(t, 750*time.Millisecond, asTime(end).Sub(asTime(start)))
}

func TestSwallowedFenceRejectionStopsExecution(t *testing.T) {
	testSwallowedFenceRejectionStopsExecution(t, createTestApp)
}
func testSwallowedFenceRejectionStopsExecution(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	fenced := make(chan struct{})
	renewalProcessed := make(chan struct{})
	renewals := 0 // Only accessed by the keepalive goroutine.
	calls := 0
	a := DefineActivity("forbidden", func(context.Context, string) (string, error) { calls++; return "", nil })
	w := DefineWorkflow("rejected_signal", func(ctx *WorkflowContext, input string) (string, error) {
		_, err := Receive[string](ctx, "messages")
		require.ErrorIs(t, err, storage.ErrFenced)
		close(fenced)
		select {
		case <-renewalProcessed:
		case <-time.After(5 * time.Second):
			return "", errors.New("renewal did not progress after fence rejection")
		}
		_, err = a.Execute(ctx, input)
		require.ErrorIs(t, err, storage.ErrFenced)
		return "caught", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "messages", "payload"))
	fs.Before = func(ctx context.Context, method string) error {
		if method == "RenewLease" {
			// Delay renewal until Receive has observed the real store rejection.
			select {
			case <-fenced:
			case <-ctx.Done():
				return ctx.Err()
			}
			renewals++
			if renewals == 2 {
				// The next renewal proves the previous result was processed by
				// Execution.renew, beyond the faultstore's After hook.
				close(renewalProcessed)
			}
		}
		if method == "CommitObservation" {
			setStoreClock(t, h, 2001)
			_, ok, err := fs.Storage.AcquireLease(ctx, id, "other/owner", time.Second)
			require.NoError(t, err)
			require.True(t, ok)
		}
		return nil
	}
	err := h.RunOnce(id)
	assert.ErrorIs(t, err, storage.ErrFenced)
	assert.NotErrorIs(t, err, replay.ErrLeaseLost)
	assert.GreaterOrEqual(t, renewals, 2, "renewal must be processed before the workflow returns")
	require.Zero(t, calls)
	require.NotContains(t, fs.Calls(), "CloseInstance")
	assert.NotContains(t, fs.Calls(), "ReleaseLease")
}

func TestReplayDeterministicAfterCrash(t *testing.T) {
	testReplayDeterministicAfterCrash(t, createTestApp)
}
func testReplayDeterministicAfterCrash(t *testing.T, factory replayAppFactory) {
	t.Run("two_attempts_of_A_return_different_values", func(t *testing.T) { testFutureGetReadsHistory(t, factory) })
	baseline, writes := runCrashScenario(t, factory, 0, false)
	for i := 1; i <= writes; i++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("write_%d/after_%t", i, after), func(t *testing.T) { got, _ := runCrashScenario(t, factory, i, after); require.Equal(t, baseline, got) })
		}
	}
}

type historyValue struct {
	Seq          int64
	DeliveredSeq int64
	Key, Command string
	Type         storage.HistoryEventType
	Data         string
}

func runCrashScenario(t *testing.T, factory replayAppFactory, crashAt int, after bool) ([]historyValue, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	calls := 0
	writeIndex := 0
	var injectionMu sync.Mutex
	crashed := false
	a := DefineActivity("A", func(context.Context, string) (string, error) {
		calls++

		return "durable", nil
	})
	w := DefineWorkflow("crash_replay", func(ctx *WorkflowContext, input string) (string, error) {
		v, err := a.Execute(ctx, input)
		if err != nil {
			return "", err
		}
		if err := Sleep(ctx, time.Hour); err != nil {
			return "", err
		}
		if _, err := Receive[string](ctx, "messages"); err != nil {
			return "", err
		}
		return a.Execute(ctx, v)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "messages", "signal"))
	inject := func(_ context.Context, method string) error {
		injectionMu.Lock()
		defer injectionMu.Unlock()
		switch method {
		case "AcquireLease", "ReserveHistory", "ReserveActivity", "ClaimActivityTask", "ClaimActivityTasks", "RenewActivityTask", "CompleteActivityTask", "ReleaseActivityTask", "CancelActivityAtYield", "CommitObservation", "SetHistoryValueIfNull", "ReserveTimer", "ReleaseLease", "FireTimer", "CloseInstance":
			writeIndex++
		}
		if !crashed && crashAt > 0 && writeIndex == crashAt {
			crashed = true
			return storage.ErrFenced
		}
		return nil
	}
	if after {
		fs.After = inject
	} else {
		fs.Before = inject
	}
	for attempt := 0; attempt < 10; attempt++ {
		err := h.RunOnce(id)
		if err != nil {
			require.ErrorIs(t, err, storage.ErrFenced)
		}
		inst, err := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		if inst.Status == storage.StatusCompleted {
			break
		}
		var armed int
		require.NoError(t, fs.Storage.DB().QueryRow("SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1", id).Scan(&armed))
		if armed > 0 {
			err = fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#2"})
			if err != nil {
				require.ErrorIs(t, err, storage.ErrFenced)
			}
		}
		setStoreClock(t, h, int64(4000+attempt*3000))
		tasks, claimErr := fs.ClaimActivityTasks(h.app.ctx, "crash-pool", h.app.config.lease.TTL, 10, h.app.registeredActivityNames()...)
		if claimErr != nil {
			require.ErrorIs(t, claimErr, storage.ErrFenced)
		} else {
			for _, task := range tasks {
				if taskErr := h.app.runActivityTask(h.app.ctx, fs, task, nil); taskErr != nil {
					require.ErrorIs(t, taskErr, storage.ErrFenced)
				}
			}
		}
	}
	assertCompletedLeaseRun(t, h, id)
	if crashAt > 0 {
		require.True(t, crashed, "write index must be reached")
	}

	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	values := make([]historyValue, 0, len(rows))
	for _, row := range rows {
		var seq int64
		require.NoError(t, fs.Storage.DB().QueryRow("SELECT seq FROM workflow_history WHERE id=$1", row.ID).Scan(&seq))
		require.Equal(t, fmt.Sprintf("#%d", seq), row.ActivityID)
		values = append(values, historyValue{row.Seq, row.DeliveredSeq, row.ActivityID, row.Command, row.EventType, string(row.EventData)})
	}
	return values, writeIndex
}

func TestMessageDuringSuspendIsNotLost(t *testing.T) {
	testMessageDuringSuspendIsNotLost(t, createTestApp)
}
func testMessageDuringSuspendIsNotLost(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var once sync.Once
	w := DefineWorkflow("message_suspend", func(ctx *WorkflowContext, _ string) (string, error) {
		ev, err := Receive[string](ctx, "messages")
		if err != nil {
			return "", err
		}
		return ev, nil
	})
	id := seedErrorWorkflow(t, h, w)
	fs.Before = func(ctx context.Context, method string) error {
		if method == "ReleaseLease" {
			once.Do(func() { require.NoError(t, h.app.Signal(ctx, id, "messages", "payload")) })
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
}

func TestShutdownAbandonsLease(t *testing.T) { testShutdownAbandonsLease(t, createTestApp) }
func testShutdownAbandonsLease(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	entered := make(chan struct{})
	first := true
	w := DefineWorkflow("shutdown", func(ctx *WorkflowContext, _ string) (string, error) {
		if first {
			first = false
			close(entered)
			<-ctx.Context().Done()
		}
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	runCtx, cancel := context.WithCancel(h.app.ctx)
	done := make(chan error, 1)
	go func() { done <- h.app.engine.Run(runCtx, id, h.app.workflowRunners[w.Name()]) }()
	<-entered
	cancel()
	require.ErrorIs(t, <-done, replay.ErrLeaseLost)
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	require.NotContains(t, fs.Calls(), "CloseInstance")
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
}

func testStalledEngineCommit(t *testing.T, factory replayAppFactory, owner string) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	checked := make(chan struct{})
	resume := make(chan struct{})
	w := DefineWorkflow("stalled_commit", func(ctx *WorkflowContext, _ string) (string, error) {
		_ = Sleep(ctx, time.Hour, WithSleepID("pause"))
		return "caught", nil
	})
	id := seedErrorWorkflow(t, h, w)
	fs.Before = func(_ context.Context, method string) error {
		if method == "ReserveTimer" {
			close(checked)
			<-resume
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	<-checked
	setStoreClock(t, h, 2001)
	_, ok, err := fs.Storage.AcquireLease(h.app.ctx, id, owner, time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	close(resume)
	require.ErrorIs(t, <-done, storage.ErrFenced)
	require.Equal(t, []string{"AcquireLease", "ReserveTimer"}, executionWrites(fs))
	rows, err := fs.GetHistoryCount(h.app.ctx, id)
	require.NoError(t, err)
	require.Zero(t, rows)
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, owner, inst.LeaseOwner)
	require.Equal(t, storage.StatusRunning, inst.Status)
}

// executionWrites excludes the independent workflow lease heartbeat.
func executionWrites(s *faultstore.Store) []string {
	var writes []string
	for _, method := range s.Calls() {
		switch method {
		case "AcquireLease", "ReleaseLease", "ReserveHistory", "ReserveActivity", "ClaimActivityTask", "ClaimActivityTasks", "RenewActivityTask", "CompleteActivityTask", "ReleaseActivityTask", "ReserveTimer", "RecordHistory", "SetHistoryValueIfNull", "CloseInstance", "UpdateInstanceActivity", "CommitObservation":
			writes = append(writes, method)
		}
	}
	return writes
}

func TestDispatcherAdmitsWorkDuringLongExecution(t *testing.T) {
	testDispatcherAdmitsWorkDuringLongExecution(t, createTestApp)
}
func testDispatcherAdmitsWorkDuringLongExecution(t *testing.T, factory replayAppFactory) {
	app, cleanup := factory(t, WithMaxConcurrentResumptions(2), WithWorkflowResumptionInterval(10*time.Millisecond))
	t.Cleanup(cleanup)
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	t.Cleanup(func() { close(release) })
	slow := DefineWorkflow("slow_dispatch", func(*WorkflowContext, string) (string, error) { close(entered); <-release; return "slow", nil })
	fast := DefineWorkflow("fast_dispatch", func(*WorkflowContext, string) (string, error) { close(finished); return "fast", nil })
	RegisterWorkflow(app, slow)
	RegisterWorkflow(app, fast)
	require.NoError(t, app.Start(context.Background()))
	_, err := StartWorkflow(context.Background(), app, slow, "")
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first dispatch did not start")
	}
	_, err = StartWorkflow(context.Background(), app, fast, "")
	require.NoError(t, err)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("free dispatcher slot was blocked by an active execution")
	}
}
