package selvedge

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/notify"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func TestDispatcherBacksOffUnregisteredWorkflow(t *testing.T) {
	h, _ := newLeaseStore(t, createTestApp)
	h.app.config.workflowResumptionInterval = 20 * time.Millisecond
	fs := faultstore.New(h.app.storage)
	h.app.storage = fs
	var queries atomic.Int64
	fs.Before = func(_ context.Context, method string) error {
		if method == "FindDispatchable" {
			queries.Add(1)
		}
		return nil
	}
	h.app.wg.Add(1)
	go h.app.runWorkflowResumption()
	time.Sleep(350 * time.Millisecond)
	h.app.cancel()
	h.app.wg.Wait()
	require.Positive(t, queries.Load())
	require.LessOrEqual(t, queries.Load(), int64(8), "unregistered instances must leave the dispatcher in backoff")
}

func TestTerminalTimerIsRemoved(t *testing.T) { testTerminalTimerIsRemoved(t, createTestApp) }
func testTerminalTimerIsRemoved(t *testing.T, factory replayAppFactory) {
	for _, status := range []storage.WorkflowStatus{storage.StatusCompleted, storage.StatusFailed, storage.StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			h, s := newLeaseStore(t, factory)
			f, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker", time.Minute)
			require.NoError(t, err)
			require.True(t, ok)
			ctx := storage.ContextWithFence(h.app.ctx, f)
			timer := &storage.TimerSubscription{InstanceID: "lease", TimerID: "pause", ExpiresAt: time.Now().Add(-time.Hour)}
			require.NoError(t, fixtureTimer(ctx, h.app.storage, timer))
			require.NoError(t, h.app.storage.CloseInstance(ctx, f, status, nil, nil))
			// Simulate a stale subscription left by a faulty writer after close.
			_, err = h.app.storage.DB().Exec("INSERT INTO workflow_timer_subscriptions(instance_id,timer_id,expires_at,activity_id) VALUES($1,$2,$3,$4)", "lease", "pause", time.Now().Add(-time.Hour).UTC().Format("2006-01-02 15:04:05"), "pause")
			require.NoError(t, err)
			tx, err := h.app.storage.BeginTransaction(h.app.ctx)
			require.NoError(t, err)
			require.NoError(t, h.app.storage.FireTimer(tx, timer))
			require.NoError(t, h.app.storage.RollbackTransaction(tx))
			timers, err := h.app.storage.FindExpiredTimers(h.app.ctx, 1)
			require.NoError(t, err)
			require.Len(t, timers, 1)
			fs := faultstore.New(h.app.storage)
			require.NoError(t, fs.FireTimer(h.app.ctx, timer))
			timers, err = fs.FindExpiredTimers(h.app.ctx, 1)
			require.NoError(t, err)
			require.Empty(t, timers)
			inst, err := fs.GetInstance(h.app.ctx, "lease")
			require.NoError(t, err)
			require.Equal(t, status, inst.Status)
			require.False(t, inst.Runnable)
			n, err := fs.GetHistoryCount(h.app.ctx, "lease")
			require.NoError(t, err)
			require.Zero(t, n)
			for _, stmt := range fs.Statements() {
				require.NotContains(t, stmt.SQL, "SET status")
			}
		})
	}
}

type timerCommitHooks struct {
	hooks.NoOpHooks
	fired func(hooks.TimerFiredInfo)
}

func (h *timerCommitHooks) OnTimerFired(_ context.Context, info hooks.TimerFiredInfo) { h.fired(info) }
func TestTimerHookAfterCommit(t *testing.T)                                           { testTimerHookAfterCommit(t, createTestApp) }
func testTimerHookAfterCommit(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	timer := &storage.TimerSubscription{InstanceID: "lease", TimerID: "pause", ExpiresAt: time.Now().Add(-time.Hour)}
	require.NoError(t, fixtureTimer(h.app.ctx, h.app.storage, timer))
	require.NoError(t, s.ReleaseLease(h.app.ctx, f, false))
	calls := 0
	h.app.config.hooks = &timerCommitHooks{fired: func(info hooks.TimerFiredInfo) {
		calls++
		require.Equal(t, "lease", info.InstanceID)
		require.Equal(t, "lease", info.WorkflowName)
		require.Equal(t, "pause", info.TimerID)
		require.False(t, info.FiredAt.IsZero())
		ev, err := h.app.storage.GetHistoryEvent(context.Background(), "lease", "pause")
		require.NoError(t, err)
		require.NotNil(t, ev)
		timers, err := h.app.storage.FindExpiredTimers(context.Background(), 1)
		require.NoError(t, err)
		require.Empty(t, timers)
	}}
	fs := faultstore.New(h.app.storage)
	h.app.storage = fs
	failure := errors.New("fire failed")
	fs.Before = func(_ context.Context, method string) error {
		if method == "FireTimer" {
			return failure
		}
		return nil
	}
	require.ErrorIs(t, h.app.handleExpiredTimer(timer), failure)
	require.Zero(t, calls)
	fs.Before = nil
	require.NoError(t, h.app.handleExpiredTimer(timer))
	require.Equal(t, 1, calls)
}

func TestContextWritesRequireMatchingFence(t *testing.T) {
	testContextWritesRequireMatchingFence(t, createTestApp)
}
func testContextWritesRequireMatchingFence(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: "other", WorkflowName: "lease", Status: storage.StatusRunning}))
	other, ok, err := s.AcquireLease(h.app.ctx, "other", "worker", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEqual(t, f.InstanceID, other.InstanceID)
	writes := map[string]func(context.Context) error{
		"activity":     func(c context.Context) error { return h.app.storage.UpdateInstanceActivity(c, "lease", "next") },
		"timer remove": func(c context.Context) error { return h.app.storage.RemoveTimerSubscription(c, "lease", "pause") },
	}
	for name, ctx := range map[string]context.Context{"missing": h.app.ctx, "other instance": storage.ContextWithFence(h.app.ctx, other)} {
		for method, write := range writes {
			t.Run(name+"/"+method, func(t *testing.T) { require.ErrorIs(t, write(ctx), storage.ErrFenced) })
		}
	}
}

func TestCommitCheckStopsBeforeSQL(t *testing.T) { testCommitCheckStopsBeforeSQL(t, createTestApp) }
func testCommitCheckStopsBeforeSQL(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	a := DefineActivity("never", func(context.Context, string) (string, error) {
		t.Error("activity ran after lost lease")
		return "", nil
	})
	w := DefineWorkflow("commit_check", func(ctx *WorkflowContext, input string) (string, error) {
		if err := Sleep(ctx, time.Hour, WithSleepID("pause")); err != nil {
			return "", err
		}
		return a.Execute(ctx, input)
	})
	id := seedErrorWorkflow(t, h, w)
	fs.Before = func(_ context.Context, method string) error {
		if method == "ReserveTimer" {
			clk.Advance(801 * time.Millisecond)
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	for _, stmt := range fs.Statements() {
		require.False(t, strings.Contains(stmt.SQL, "SET lease_epoch=lease_epoch"), "commit guard attempted after local Check failed")
		require.NotContains(t, stmt.SQL, "INSERT INTO workflow_history")
	}
}

func TestSuspendWithoutInputParks(t *testing.T) { testSuspendWithoutInputParks(t, createTestApp) }
func testSuspendWithoutInputParks(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("park", func(ctx *WorkflowContext, _ string) (string, error) {
		return "", Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.False(t, inst.Runnable)
	require.Empty(t, inst.LeaseOwner)
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestResumableNotificationOnlyWakesDispatcher(t *testing.T) {
	h, _, fs := leaseHarness(t, createTestApp)
	h.app.storage = fs
	// A notification must return even when every execution slot is occupied.
	capacity := int64(h.app.config.maxConcurrentResumptions)
	require.NoError(t, h.app.resumptionSem.Acquire(h.app.ctx, capacity))
	defer h.app.resumptionSem.Release(capacity)
	ctx, cancel := context.WithCancel(h.app.ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		h.app.handleWorkflowResumableNotify(ctx, notify.ChannelWorkflowResumable, `{"instance_id":"lease"}`)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("notification tried to acquire an execution slot outside the dispatcher")
	}
	select {
	case <-h.app.resumeWakeEvent:
	default:
		t.Fatal("dispatcher not woken")
	}
	require.Empty(t, fs.Calls())
}

func TestFinalCheckStopsBeforeCommitAttempt(t *testing.T) {
	testFinalCheckStopsBeforeCommitAttempt(t, createTestApp)
}
func testFinalCheckStopsBeforeCommitAttempt(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	w := DefineWorkflow("final_check", func(*WorkflowContext, string) (string, error) {
		clk.Advance(801 * time.Millisecond)
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	require.Equal(t, []string{"AcquireLease", "ReleaseLease"}, executionWrites(fs), "final Check must stop execution before any commit method is called")
}

func TestDispatchIndexCreationIsIdempotent(t *testing.T) {
	testDispatchIndexCreationIsIdempotent(t, createTestApp)
}
func testDispatchIndexCreationIsIdempotent(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	dialect := "sqlite"
	if _, ok := h.app.storage.(*storage.PostgresStorage); ok {
		dialect = "postgresql"
	}
	schema, err := fs.ReadFile(EmbeddedMigrationsFS(), dialect+"/20251217000000_initial_schema.sql")
	require.NoError(t, err)
	up, _, ok := strings.Cut(string(schema), "-- migrate:down")
	require.True(t, ok)
	_, err = h.app.storage.DB().ExecContext(h.app.ctx, up)
	require.NoError(t, err, "initial schema including dispatch index must be idempotent")
}

func TestAbandoningWorkflowUsesPolling(t *testing.T) {
	testAbandoningWorkflowUsesPolling(t, createTestApp)
}

func testAbandoningWorkflowUsesPolling(t *testing.T, factory replayAppFactory) {
	app, cleanup := factory(t, WithWorkflowResumptionInterval(time.Second))
	t.Cleanup(cleanup)
	var attempts atomic.Int64
	entered := make(chan struct{}, 1)
	workflow := DefineWorkflow("always_abandon", func(*WorkflowContext, string) (string, error) {
		attempts.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		return "", ErrLeaseLost
	})
	RegisterWorkflow(app, workflow)
	require.NoError(t, app.Start(context.Background()))
	if _, ok := app.storage.(*storage.PostgresStorage); ok {
		require.NotNil(t, app.notifyListener, "exercise the worker's real LISTEN/NOTIFY path")
		require.Eventually(t, app.notifyListener.IsActive, 5*time.Second, 10*time.Millisecond)
	}
	_, err := StartWorkflow(context.Background(), app, workflow, "")
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow was not dispatched")
	}
	time.Sleep(2 * time.Second)
	// Stop all dispatchers before reading the final count.
	require.NoError(t, app.Shutdown(context.Background()))
	count := attempts.Load()
	t.Logf("dispatches in two seconds: %d", count)
	require.Positive(t, count, "workflow must have been dispatched")
	// Polls have 30% jitter; allow startup wakes without allowing a self-wake loop.
	require.LessOrEqual(t, count, int64(6), "failed attempts must not wake their own dispatcher")
}
