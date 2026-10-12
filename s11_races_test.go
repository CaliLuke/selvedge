package selvedge

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestCancelScopeCancelsInFlightActivities(t *testing.T) {
	testCancelScopeCancelsInFlightActivities(t, createTestApp)
}
func testCancelScopeCancelsInFlightActivities(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	entered := make(chan struct{})
	cancelled := make(chan error, 1)
	release := make(chan struct{})
	defer close(release)
	a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
		close(entered)
		<-ctx.Done()
		cancelled <- context.Cause(ctx)
		<-release
		return "late", nil
	})
	b := DefineActivity("B", func(context.Context, string) (string, error) { return "unexpected", nil })
	w := DefineWorkflow("inflight", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		fa, err := a.ExecuteAsync(c, "")
		if err != nil {
			return "", err
		}
		fb, err := b.ExecuteAsync(c, "")
		if err != nil {
			return "", err
		}
		if _, err = Receive[int](ctx, "cancel"); err != nil {
			return "", err
		}
		cancel()
		if !fa.IsReady() || !fb.IsReady() {
			return "", errors.New("cancel did not deliver activities")
		}
		if _, err = fa.Get(ctx); !errors.Is(err, context.Canceled) {
			return "", errors.New("A not cancelled")
		}
		if _, err = fb.Get(ctx); !errors.Is(err, context.Canceled) {
			return "", errors.New("B not cancelled")
		}
		_, err = Receive[int](ctx, "finish")
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- claim.Run() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("attempt not started")
	}
	require.NoError(t, h.app.Signal(h.app.ctx, id, "cancel", 1))
	require.NoError(t, h.RunOnce(id))
	for _, key := range []string{"#1", "#2"} {
		row, e := fs.GetHistoryEvent(h.app.ctx, id, key)
		require.NoError(t, e)
		require.Equal(t, storage.HistoryActivityCancelled, row.EventType)
		require.EqualValues(t, 4, row.DeliveredSeq)
		var status string
		require.NoError(t, fs.DB().QueryRow(`SELECT status FROM workflow_activity_tasks WHERE instance_id=$1 AND activity_id=$2`, id, key).Scan(&status))
		require.Equal(t, "cancelled", status)
	}
	select {
	case cause := <-cancelled:
		require.ErrorIs(t, cause, errClaimLost)
	case <-time.After(3 * h.app.config.lease.RenewInterval):
		t.Fatal("renewal did not cancel attempt")
	}
	release <- struct{}{}
	require.Error(t, <-done)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
}

func TestCancelScopeStoredValueWins(t *testing.T) { testCancelScopeStoredValueWins(t, createTestApp) }
func testCancelScopeStoredValueWins(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	var ready []bool
	w := DefineWorkflow("stored-wins", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		f, err := a.ExecuteAsync(c, "")
		if err != nil {
			return "", err
		}
		tm, err := NewTimer(c, time.Hour)
		if err != nil {
			return "", err
		}
		cancel()
		ready = append(ready, f.IsReady(), tm.IsReady())
		v, err := f.Get(ctx)
		if err != nil {
			return "", err
		}
		if v != "winner" {
			return "", errors.New("stored activity lost")
		}
		if _, err = tm.Get(ctx); err != nil {
			return "", err
		}
		_, err = Receive[int](ctx, "finish")
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	injected := false
	fs.Before = func(_ context.Context, method string) error {
		if method != "CommitObservation" || injected {
			return nil
		}
		injected = true
		claim, err := h.ClaimTask(id, "#1")
		if err != nil {
			return err
		}
		if err = claim.Complete(`"winner"`); err != nil {
			return err
		}
		return fs.Storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#2"})
	}
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "finish", 1))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []bool{false, false, false, false}, ready)
}

func TestStalledExecutionCommitRejectedScope(t *testing.T) {
	testStalledExecutionCommitRejectedScope(t, createTestApp)
}
func testStalledExecutionCommitRejectedScope(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	dbFence := &databaseFenceOnlyStore{Storage: fs.Storage}
	fs.Storage = dbFence
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	var runs atomic.Int32
	w := DefineWorkflow("stalled-scope", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		if _, err := a.ExecuteAsync(c, ""); err != nil {
			return "", err
		}
		if _, err := NewTimer(c, time.Hour); err != nil {
			return "", err
		}
		if runs.Add(1) == 1 {
			cancel()
		}
		_, err := Receive[int](ctx, "finish")
		return "", err
	})
	id := seedErrorWorkflow(t, h, w)
	stalled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fs.Before = func(_ context.Context, method string) error {
		if method == "CommitObservation" {
			close(stalled)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	select {
	case <-stalled:
	case err := <-done:
		t.Fatalf("no scope commit: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("no scope commit")
	}
	setStoreClock(t, h, 3000)
	second := replay.NewEngine(fs.Storage, h.app.hooks, "takeover", errorCodec{})
	old := h.app.engine
	h.app.engine = second
	require.NoError(t, h.RunOnce(id))
	h.app.engine = old
	before, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	release <- struct{}{}
	require.ErrorIs(t, <-done, storage.ErrFenced)
	after, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, before, after)
	var status string
	require.NoError(t, fs.DB().QueryRow(`SELECT status FROM workflow_activity_tasks WHERE instance_id=$1`, id).Scan(&status))
	require.Equal(t, "queued", status)
	var n int
	require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1`, id).Scan(&n))
	require.Equal(t, 1, n)
}

func TestReplayDeterministicAfterCrashScope(t *testing.T) {
	testReplayDeterministicAfterCrashScope(t, createTestApp)
}
func testReplayDeterministicAfterCrashScope(t *testing.T, factory replayAppFactory) {
	for _, race := range []bool{false, true} {
		t.Run(fmt.Sprint(race), func(t *testing.T) {
			baseline, writes := runScopeCrash(t, factory, 0, false, race)
			for cut := 1; cut <= writes; cut++ {
				for _, after := range []bool{false, true} {
					t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) {
						got, _ := runScopeCrash(t, factory, cut, after, race)
						require.Equal(t, baseline, got)
					})
				}
			}
		})
	}
}

type scopeSnapshot struct {
	History       []historyValue
	Tasks         []string
	Subscriptions int
	Output        string
}

func runScopeCrash(t *testing.T, factory replayAppFactory, cut int, after, race bool) (scopeSnapshot, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	WithClock(func() time.Time { return time.Unix(123, 0).UTC() })(h.app.config)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	cleanup := DefineActivity("cleanup", func(context.Context, string) (string, error) { return "clean", nil })
	w := DefineWorkflow("scope-crash", func(ctx *WorkflowContext, _ string) (string, error) {
		c, cancel := WithCancel(ctx)
		f, err := a.ExecuteAsync(c, "")
		if err != nil {
			return "", err
		}
		tm, err := NewTimer(c, time.Hour)
		if err != nil {
			return "", err
		}
		cancel()
		if e := ctx.execCtx.Check(); e != nil {
			return "", e
		}
		value, err := f.Get(ctx)
		if race {
			if err != nil {
				return "", err
			}
			if value != "winner" {
				return "", errors.New("race value lost")
			}
		} else if !errors.Is(err, context.Canceled) {
			return "", errors.New("activity not cancelled")
		}
		if _, err = tm.Get(ctx); !errors.Is(err, context.Canceled) {
			return "", errors.New("timer not cancelled")
		}
		return cleanup.Execute(Detached(ctx), "")
	})
	id := seedErrorWorkflow(t, h, w)
	writes := 0
	crashed := false
	injected := false
	inject := func(_ context.Context, method string) error {
		switch method {
		case "AcquireLease", "ReserveActivity", "ReserveTimer", "CommitObservation", "ClaimActivityTask", "CompleteActivityTask", "RetryActivityTask", "TimeoutActivityTask", "ReleaseActivityTask", "ReleaseLease", "CloseInstance":
			writes++
		default:
			return nil
		}
		if cut > 0 && !crashed && writes == cut {
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
	oldBefore := fs.Before
	fs.Before = func(ctx context.Context, method string) error {
		if race && !injected && method == "CommitObservation" {
			row, err := fs.GetHistoryEvent(ctx, id, "#1")
			if err != nil {
				return err
			}
			if row.EventData == nil {
				claim, err := fs.ClaimActivityTask(ctx, id, "#1", "race", time.Second, nil)
				if err != nil {
					return err
				}
				if claim == nil {
					return errors.New("race claim did not recover")
				}
				if err = fs.CompleteActivityTask(ctx, claim.Claim(), storage.HistoryActivityCompleted, []byte(`"winner"`)); err != nil {
					return err
				}
			}
			injected = true
		}
		if oldBefore != nil {
			return oldBefore(ctx, method)
		}
		return nil
	}
	for attempt := 0; attempt < 20; attempt++ {
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "dispatch: %v", err)
		inst, e := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, e)
		if inst.Status == storage.StatusCompleted {
			break
		}
		setStoreClock(t, h, int64(4000+attempt*4000000))
		err = h.RunTasks(id, "cleanup")
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "cleanup recovery: %v", err)
	}
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
	if cut > 0 {
		require.True(t, crashed)
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	result := scopeSnapshot{Output: string(inst.OutputData)}
	for _, row := range rows {
		result.History = append(result.History, historyValue{row.Seq, row.DeliveredSeq, row.ActivityID, row.Command, row.EventType, string(row.EventData)})
	}
	taskRows, err := fs.DB().Query(`SELECT status FROM workflow_activity_tasks WHERE instance_id=$1 ORDER BY seq`, id)
	require.NoError(t, err)
	for taskRows.Next() {
		var status string
		require.NoError(t, taskRows.Scan(&status))
		result.Tasks = append(result.Tasks, status)
	}
	require.NoError(t, taskRows.Err())
	require.NoError(t, taskRows.Close())
	require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1`, id).Scan(&result.Subscriptions))
	return result, writes
}
