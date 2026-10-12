package selvedge

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestFutureKeysFollowIssueOrder(t *testing.T) { testFutureKeysFollowIssueOrder(t, createTestApp) }
func testFutureKeysFollowIssueOrder(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var keys, completed []string
	a := DefineActivity("A", func(context.Context, string) (string, error) { completed = append(completed, "A"); return "A", nil })
	b := DefineActivity("B", func(context.Context, string) (string, error) { completed = append(completed, "B"); return "B", nil })
	w := DefineWorkflow("order", func(ctx *WorkflowContext, in string) (string, error) {
		fa, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		fb, err := b.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		keys = []string{fa.Key(), fb.Key()}
		vb, err := fb.Get(ctx)
		if err != nil {
			return "", err
		}
		va, err := fa.Get(ctx)
		return va + vb, err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []string{"#1", "#2"}, keys)
	require.Equal(t, []string{"B", "A"}, completed)
	require.Equal(t, 2, taskCount(t, h, id))
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "activity:A", rows[0].Command)
	require.EqualValues(t, 1, rows[0].Seq)
	require.Equal(t, "activity:B", rows[1].Command)
	require.EqualValues(t, 2, rows[1].Seq)
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.JSONEq(t, `"AB"`, string(inst.OutputData))
	calls := fs.Calls()
	require.Equal(t, 2, countMethod(calls, "CompleteActivityTask"))
}

func TestAsyncActivitiesRunConcurrently(t *testing.T) {
	testAsyncActivitiesRunConcurrently(t, createTestApp)
}
func testAsyncActivitiesRunConcurrently(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory, WithMaxConcurrentActivities(3), WithActivityPollInterval(time.Millisecond))
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var calls atomic.Int32
	a := DefineActivity("parallel", func(ctx context.Context, in string) (string, error) {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	w := DefineWorkflow("parallel", func(ctx *WorkflowContext, in string) (string, error) {
		fs := make([]*Future[string], 3)
		for i := range fs {
			f, err := a.ExecuteAsync(ctx, in)
			if err != nil {
				return "", err
			}
			fs[i] = f
		}
		var out string
		for _, f := range fs {
			v, err := f.Get(ctx)
			if err != nil {
				return "", err
			}
			out += v
		}
		return out, nil
	})
	id := seedErrorWorkflow(t, h, w)
	h.app.wg.Add(1)
	go h.app.runActivityWorkers()
	defer func() { h.app.cancel(); h.app.wg.Wait() }()
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	for range 3 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("activities did not run concurrently")
		}
	}
	close(release)
	require.NoError(t, <-done)
	require.Eventually(t, func() bool {
		var n int
		err := h.app.storage.DB().QueryRow("SELECT COUNT(*) FROM workflow_activity_tasks WHERE instance_id=$1 AND status='completed'", id).Scan(&n)
		return err == nil && n == 3
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
	require.EqualValues(t, 3, calls.Load())
}

func TestGetYieldsUntilResult(t *testing.T) { testGetYieldsUntilResult(t, createTestApp) }
func testGetYieldsUntilResult(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	calls := 0
	a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "result", nil })
	w := DefineWorkflow("yield", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		return f.Get(ctx)
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NotNil(t, claim)
	require.NoError(t, h.RunOnce(id))
	require.Zero(t, calls)
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, inst.Status)
	require.False(t, inst.Runnable)
	require.Empty(t, inst.LeaseOwner)
	require.NotContains(t, fs.Calls(), "ReserveActivity")
	require.NotContains(t, fs.Calls(), "CompleteActivityTask")
	require.NotContains(t, fs.Calls(), "CancelActivityAtYield")
	require.NoError(t, claim.Run())
	inst, err = fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.True(t, inst.Runnable)
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
	require.Equal(t, 1, calls)
}

func TestFutureGetIsRepeatable(t *testing.T) { testFutureGetIsRepeatable(t, createTestApp) }
func testFutureGetIsRepeatable(t *testing.T, factory replayAppFactory) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			calls := 0
			var values []string
			var outcomes []error
			var positions []int64
			a := DefineActivity("A", func(context.Context, string) (string, error) {
				calls++
				if failed {
					return "", &ApplicationError{Type: "test", Message: "failure", NonRetryable: true}
				}
				return "value", nil
			})
			w := DefineWorkflow("repeat", func(ctx *WorkflowContext, in string) (string, error) {
				f, err := a.ExecuteAsync(ctx, in)
				if err != nil {
					return "", err
				}
				positions = append(positions, ctx.execCtx.Position())
				for range 2 {
					v, e := f.Get(ctx)
					values = append(values, v)
					outcomes = append(outcomes, e)
					positions = append(positions, ctx.execCtx.Position())
				}
				return "ok", Sleep(ctx, time.Hour, WithSleepID("pause"))
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, 1, calls)
			require.Equal(t, []int64{1, 1, 1, 1, 1, 1}, positions)
			require.Len(t, values, 4)
			require.Equal(t, values[0], values[3])
			if failed {
				for _, err := range outcomes {
					var appErr *ApplicationError
					require.ErrorAs(t, err, &appErr)
					require.Equal(t, "test", appErr.Type)
				}
				require.Same(t, outcomes[0], outcomes[1])
				require.Same(t, outcomes[2], outcomes[3])
			} else {
				require.Equal(t, []string{"value", "value", "value", "value"}, values)
				for _, err := range outcomes {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestGetOffCoroutineFailsRun(t *testing.T) { testGetOffCoroutineFailsRun(t, createTestApp) }
func testGetOffCoroutineFailsRun(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { t.Error("claimed task ran"); return "", nil })
	w := DefineWorkflow("foreign", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		done := make(chan error, 1)
		go func() { _, e := f.Get(ctx); done <- e }()
		select {
		case e := <-done:
			return "", e
		case <-time.After(3 * time.Second):
			return "", errors.New("Get deadlocked")
		}
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NotNil(t, claim)
	var nondet *NondeterminismError
	require.ErrorAs(t, h.RunOnce(id), &nondet)
	require.Equal(t, "command issued off the workflow coroutine", nondet.Reason)
}

func TestRecordedActivityNotRescheduledOnReplayAsync(t *testing.T) {
	testRecordedActivityNotRescheduledOnReplayAsync(t, createTestApp)
}
func testRecordedActivityNotRescheduledOnReplayAsync(t *testing.T, factory replayAppFactory) {
	for _, recorded := range []bool{false, true} {
		t.Run(fmt.Sprint(recorded), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			a := DefineActivity("A", func(context.Context, string) (string, error) { return "done", nil })
			w := DefineWorkflow("reserved", func(ctx *WorkflowContext, in string) (string, error) {
				f, err := a.ExecuteAsync(ctx, in)
				if err != nil {
					return "", err
				}
				return f.Get(ctx)
			})
			id := seedErrorWorkflow(t, h, w)
			seedTask(t, h, id)
			claim, err := h.ClaimTask(id, "#1")
			require.NoError(t, err)
			if recorded {
				require.NoError(t, claim.Complete(`"stored"`))
			}
			require.NoError(t, h.RunOnce(id))
			require.NotContains(t, fs.Calls(), "ReserveActivity")
			require.Equal(t, 1, taskCount(t, h, id))
			if recorded {
				require.NotContains(t, fs.Calls(), "ClaimActivityTask")
			}
		})
	}
}

func TestHistoryKeyWrittenOnceAsync(t *testing.T) { testHistoryKeyWrittenOnceAsync(t, createTestApp) }
func testHistoryKeyWrittenOnceAsync(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "", nil })
	w := DefineWorkflow("duplicate", func(ctx *WorkflowContext, in string) (string, error) {
		if err := Sleep(ctx, 0, WithSleepID("same")); err != nil {
			return "", err
		}
		_, _ = a.ExecuteAsync(ctx, in, WithActivityID("same"))
		return "ignored", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.ErrorIs(t, h.RunOnce(id), ErrHistoryKeyConflict)
	require.Zero(t, taskCount(t, h, id))
}

func TestReplayCommandMismatchFailsFastAsync(t *testing.T) {
	testReplayCommandMismatchFailsFastAsync(t, createTestApp)
}
func testReplayCommandMismatchFailsFastAsync(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	a := DefineActivity("B", func(context.Context, string) (string, error) { t.Error("mismatched activity ran"); return "", nil })
	w := DefineWorkflow("mismatch", func(ctx *WorkflowContext, in string) (string, error) {
		_, _ = a.ExecuteAsync(ctx, in)
		return "ignored", nil
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	var nondet *NondeterminismError
	require.ErrorAs(t, h.RunOnce(id), &nondet)
	require.EqualValues(t, 1, nondet.Seq)
	require.Equal(t, "activity:A", nondet.Recorded)
	require.Equal(t, "activity:B", nondet.Issued)
	require.NotContains(t, fs.Calls(), "ReserveActivity")
	require.NotContains(t, fs.Calls(), "ClaimActivityTask")
	require.Equal(t, 1, taskCount(t, h, id))
}

func TestUnawaitedFutureClosesNormally(t *testing.T) {
	testUnawaitedFutureClosesNormally(t, createTestApp)
}
func testUnawaitedFutureClosesNormally(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) {
		t.Error("unawaited activity ran inline")
		return "", nil
	})
	w := DefineWorkflow("unawaited", func(ctx *WorkflowContext, in string) (string, error) {
		_, err := a.ExecuteAsync(ctx, in)
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
	var status string
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT status FROM workflow_activity_tasks WHERE instance_id=$1", id).Scan(&status))
	require.Equal(t, "cancelled", status)
}
