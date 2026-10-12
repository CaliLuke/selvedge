package selvedge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestPendingAndReceiveAsyncAgree_Query(t *testing.T) {
	s14TestPendingAndReceiveAsyncAgreeQuery(t, createTestApp)
}
func s14TestPendingAndReceiveAsyncAgreeQuery(t *testing.T, factory replayAppFactory) {
	for _, race := range []string{"after_snapshot", "during_commit", "forced_replay"} {
		t.Run(race, func(t *testing.T) {
			h, fs := s14Harness(t, factory)
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = SetQueryHandler(ctx, "pending", func() (int, error) { return Pending(ctx, "ch"), nil })
				if err := Await(ctx, func() bool { return Pending(ctx, "ch") > 0 }); err != nil {
					return "", err
				}
				_, _, err := ReceiveAsync[int](ctx, "ch")
				if err != nil {
					return "", err
				}
				return "", Await(ctx, func() bool { return false })
			})
			require.NoError(t, h.RunOnce(id))
			if race == "after_snapshot" {
				fired := false
				fs.After = func(ctx context.Context, method string) error {
					if method == "ReadSnapshot" && !fired {
						fired = true
						_, err := fs.Storage.AppendSignal(ctx, id, "ch", []byte(`1`))
						return err
					}
					return nil
				}
				require.Zero(t, s14Query[int](t, h, id, "pending"))
				fs.After = nil
			} else {
				require.NoError(t, h.app.Signal(context.Background(), id, "ch", 1))
			}
			if race == "during_commit" {
				reached, release := make(chan struct{}), make(chan struct{})
				first := true
				fs.Before = func(_ context.Context, method string) error {
					if method == "CommitObservation" && first {
						first = false
						close(reached)
						<-release
					}
					return nil
				}
				done := make(chan error, 1)
				go func() { done <- h.RunOnce(id) }()
				<-reached
				require.Zero(t, s14Query[int](t, h, id, "pending"))
				close(release)
				require.NoError(t, <-done)
				fs.Before = nil
			} else {
				require.NoError(t, h.RunOnce(id))
			}
			require.Zero(t, s14Query[int](t, h, id, "pending"))
			require.NoError(t, fs.MarkRunnable(context.Background(), id))
			require.NoError(t, h.RunOnce(id))
			require.Zero(t, s14Query[int](t, h, id, "pending"))
		})
	}
}

func TestReplayDeterministicAfterCrash_Queries(t *testing.T) {
	s14TestReplayDeterministicAfterCrashQueries(t, createTestApp)
}
func s14TestReplayDeterministicAfterCrashQueries(t *testing.T, factory replayAppFactory) {
	for _, scenario := range []string{"late_B", "cancel_scope", "cancel_scope_stored"} {
		t.Run(scenario, func(t *testing.T) {
			baseline, writes := s14RunQueryCrash(t, factory, scenario, 0, false, false)
			for cut := 0; cut <= writes; cut++ {
				for _, after := range []bool{false, true} {
					t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) {
						got, _ := s14RunQueryCrash(t, factory, scenario, cut, after, true)
						require.Equal(t, baseline, got)
					})
				}
			}
		})
	}
}
func s14RunQueryCrash(t *testing.T, factory replayAppFactory, scenario string, cut int, after, queries bool) (scopeSnapshot, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	WithClock(func() time.Time { return time.Unix(123, 0).UTC() })(h.app.config)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	b := DefineActivity("B", func(context.Context, string) (string, error) { return "B", nil })
	cleanup := DefineActivity("cleanup", func(context.Context, string) (string, error) { return "clean", nil })
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		_ = s14RegisterPosition(ctx)
		if scenario == "cancel_scope" || scenario == "cancel_scope_stored" {
			scoped, cancel := WithCancel(ctx)
			fa, err := a.ExecuteAsync(scoped, "")
			if err != nil {
				return "", err
			}
			tm, err := NewTimer(scoped, time.Hour)
			if err != nil {
				return "", err
			}
			cancel()
			if err = ctx.execCtx.Check(); err != nil {
				return "", err
			}
			value, getErr := fa.Get(ctx)
			if scenario == "cancel_scope_stored" {
				if getErr != nil {
					return "", getErr
				}
				if value != "winner" {
					return "", errors.New("stored activity result lost")
				}
			} else if !errors.Is(getErr, context.Canceled) {
				return "", fmt.Errorf("activity cancel: %w", getErr)
			}
			if _, err = tm.Get(ctx); !errors.Is(err, context.Canceled) {
				return "", fmt.Errorf("timer cancel: %w", err)
			}
			return cleanup.Execute(Detached(ctx), "")
		}
		fa, err := a.ExecuteAsync(ctx, "")
		if err != nil {
			return "", err
		}
		fb, err := b.ExecuteAsync(ctx, "")
		if err != nil {
			return "", err
		}
		if err = Await(ctx, func() bool { return fa.IsReady() || fb.IsReady() }); err != nil {
			return "", err
		}
		if fb.IsReady() {
			return "", errors.New("late B delivered early")
		}
		va, err := fa.Get(ctx)
		if err != nil {
			return "", err
		}
		vb, err := fb.Get(ctx)
		return va + vb, err
	})
	writes := 0
	crashed := false
	queryCount := 0
	isWrite := func(method string) bool {
		switch method {
		case "AcquireLease", "ReserveActivity", "ReserveTimer", "CommitObservation", "ClaimActivityTask", "ClaimActivityTasks", "CompleteActivityTask", "RetryActivityTask", "ReleaseActivityTask", "ReleaseLease", "CloseInstance":
			return true
		}
		return false
	}
	inject := func(atAfter bool) func(context.Context, string) error {
		return func(_ context.Context, method string) error {
			if !isWrite(method) {
				return nil
			}
			if queries {
				_, err := Query[int64](context.Background(), h.app, id, "q")
				if err != nil {
					return fmt.Errorf("query at %s after=%t: %w", method, atAfter, err)
				}
				queryCount++
			}
			if atAfter == after {
				writes++
				if cut > 0 && !crashed && writes == cut {
					crashed = true
					return storage.ErrFenced
				}
			}
			return nil
		}
	}
	before := inject(false)
	fs.Before = func(ctx context.Context, method string) error {
		if err := before(ctx, method); err != nil {
			return err
		}
		if scenario != "cancel_scope_stored" || method != "CommitObservation" {
			return nil
		}
		row, err := fs.GetHistoryEvent(ctx, id, "#1")
		if err != nil {
			return err
		}
		if row.EventData != nil {
			return nil
		}
		task, err := fs.ClaimActivityTask(ctx, id, "#1", "race", time.Second, nil)
		if err != nil {
			return err
		}
		if task == nil {
			return errors.New("race task unavailable")
		}
		return fs.CompleteActivityTask(ctx, task.Claim(), storage.HistoryActivityCompleted, []byte(`"winner"`))
	}
	fs.After = inject(true)
	for attempt := 0; attempt < 20; attempt++ {
		if scenario == "late_B" {
			// Hold B until the Await has delivered A; Get must first block on B.
			_, err := fs.Storage.ClaimActivityTasks(context.Background(), "hold-B", 24*time.Hour, 1, storage.TaskFilter{Names: []string{"B"}})
			require.NoError(t, err)
		}
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "dispatch: %v", err)
		inst, err := fs.GetInstance(context.Background(), id)
		require.NoError(t, err)
		if inst.Status == StatusCompleted {
			break
		}
		setStoreClock(t, h, int64(4000+attempt*100000000))
		name := "cleanup"
		if scenario == "late_B" {
			row, err := fs.GetHistoryEvent(context.Background(), id, "#3")
			require.NoError(t, err)
			name = "A"
			if row != nil {
				name = "B"
			}
		}
		err = h.RunTasks(id, name)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "tasks: %v", err)
	}
	inst, err := fs.GetInstance(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, StatusCompleted, inst.Status)
	if queries {
		require.Positive(t, queryCount)
	}
	if cut > 0 {
		require.True(t, crashed)
	}
	rows, _, err := fs.GetHistoryPaginated(context.Background(), id, 0, 100)
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
