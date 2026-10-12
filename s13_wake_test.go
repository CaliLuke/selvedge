package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func s13ClientFor(t *testing.T, worker *App) *App {
	t.Helper()
	client := NewApp(WithDatabase(worker.config.databaseURL), WithAutoMigrate(false))
	client.ctx, client.cancel = context.WithCancel(context.Background())
	require.NoError(t, client.initStorage())
	t.Cleanup(func() { client.cancel(); require.NoError(t, client.storage.Close()) })
	return client
}

func TestSignalBeforeFirstReceiveIsDelivered_FromClientApp(t *testing.T) {
	s13SignalBeforeReceive(t, createTestApp)
}
func s13SignalBeforeReceive(t *testing.T, factory replayAppFactory) {
	for _, when := range []string{"before_execution", "during_activity"} {
		t.Run(when, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			client := s13ClientFor(t, h.app)
			activity := DefineActivity("s13_send", func(context.Context, string) (string, error) {
				return "", client.Signal(context.Background(), "early", "go", "value")
			})
			RegisterWorkflow(h.app, DefineWorkflow("s13_early", func(ctx *WorkflowContext, _ string) (string, error) {
				if when == "during_activity" {
					if _, err := activity.Execute(ctx, ""); err != nil {
						return "", err
					}
				}
				return Receive[string](ctx, "go")
			}))
			id, err := StartWorkflowByName(h.app.ctx, client, "s13_early", "", WithInstanceID("early"))
			require.NoError(t, err)
			if when == "before_execution" {
				require.NoError(t, client.Signal(h.app.ctx, id, "go", "value"))
			}
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, "value", s13Wait[string](t, client, id).Output)
		})
	}
}

func TestInputDuringSuspendIsNotLost_FromClientApp(t *testing.T) { s13DuringSuspend(t, createTestApp) }
func s13DuringSuspend(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	client := s13ClientFor(t, h.app)
	RegisterWorkflow(h.app, DefineWorkflow("s13_suspend", func(ctx *WorkflowContext, _ string) (string, error) { return Receive[string](ctx, "go") }))
	id, err := StartWorkflowByName(h.app.ctx, client, "s13_suspend", "")
	require.NoError(t, err)
	var once sync.Once
	fs.Before = func(ctx context.Context, method string) error {
		if method == "ReleaseLease" {
			once.Do(func() { require.NoError(t, client.Signal(ctx, id, "go", "arrived")) })
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, "arrived", s13Wait[string](t, client, id).Output)
}

func TestPendingAndReceiveAsyncAgree_FromClientApp(t *testing.T) { s13PendingAsync(t, createTestApp) }
func s13PendingAsync(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	client := s13ClientFor(t, h.app)
	RegisterWorkflow(h.app, DefineWorkflow("s13_await", func(ctx *WorkflowContext, _ string) ([]string, error) {
		if Pending(ctx, "go") != 0 {
			return nil, errors.New("arrival delivered before await")
		}
		if err := Await(ctx, func() bool { return Pending(ctx, "go") > 0 }); err != nil {
			return nil, err
		}
		if Pending(ctx, "go") != 1 {
			return nil, errors.New("late arrival changed batch")
		}
		value, ok, err := ReceiveAsync[string](ctx, "go")
		if err != nil {
			return nil, err
		}
		if !ok || Pending(ctx, "go") != 0 {
			return nil, errors.New("pending did not match receive")
		}
		_, ok, err = ReceiveAsync[string](ctx, "go")
		if err != nil {
			return nil, err
		}
		if ok {
			return nil, errors.New("late arrival visible")
		}
		next, err := Receive[string](ctx, "go")
		if err != nil {
			return nil, err
		}
		return []string{value, next}, Sleep(ctx, time.Hour, WithSleepID("pause"))
	}))
	id, err := StartWorkflowByName(h.app.ctx, client, "s13_await", "")
	require.NoError(t, err)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, client.Signal(h.app.ctx, id, "go", "first"))
	var once sync.Once
	fs.After = func(ctx context.Context, method string) error {
		if method == "CommitObservation" {
			once.Do(func() { require.NoError(t, client.Signal(ctx, id, "go", "second")) })
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	h.forceFire(t, id)
	require.NoError(t, h.RunOnce(id))
	result := s13Wait[[]string](t, client, id)
	require.NoError(t, result.Error)
	require.Equal(t, []string{"first", "second"}, result.Output)
}

func TestCancelWhileSleepingStaysCancelled_FromClientApp(t *testing.T) {
	s13CancelBlocked(t, createTestApp, []string{"sleep", "receive", "child"})
}
func TestCancelEndsBlockedWaits_FromClientApp(t *testing.T) {
	s13CancelBlocked(t, createTestApp, []string{"await", "timer", "timeout"})
}
func s13CancelBlocked(t *testing.T, factory replayAppFactory, kinds []string) {
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			client := s13ClientFor(t, h.app)
			RegisterWorkflow(h.app, DefineWorkflow("s13_block", func(ctx *WorkflowContext, _ string) (string, error) {
				switch kind {
				case "sleep":
					return "", Sleep(ctx, time.Hour)
				case "receive":
					return Receive[string](ctx, "go")
				case "child":
					child, err := StartChild[string](ctx, "unregistered", "")
					if err != nil {
						return "", err
					}
					return child.Get(ctx)
				case "await":
					return "", Await(ctx, func() bool { return false })
				case "timer":
					timer, err := NewTimer(ctx, time.Hour)
					if err != nil {
						return "", err
					}
					_, err = timer.Get(ctx)
					return "", err
				default:
					return ReceiveWithTimeout[string](ctx, "go", time.Hour)
				}
			}))
			id, err := StartWorkflowByName(h.app.ctx, client, "s13_block", "")
			require.NoError(t, err)
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, CancelWorkflow(h.app.ctx, client, id, "from client"))
			err = h.RunOnce(id)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, "cancelled", s13Wait[string](t, client, id).Status)
			if kind == "sleep" || kind == "timer" || kind == "timeout" {
				require.NoError(t, h.app.storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1"}))
			}
			require.NoError(t, h.RunOnce(id))
			require.Equal(t, "cancelled", s13Wait[string](t, client, id).Status)
		})
	}
}

func TestCrossAppWakeSameProcess(t *testing.T) { s13CrossAppWake(t, createTestApp) }
func s13CrossAppWake(t *testing.T, factory replayAppFactory) {
	worker, client := s13AppPair(t, factory)
	client.config.waitPollInterval = time.Hour
	reached := make(chan struct{}, 2)
	RegisterWorkflow(worker, DefineWorkflow("s13_wake", func(ctx *WorkflowContext, _ string) (string, error) {
		select {
		case reached <- struct{}{}:
		default:
		}
		return Receive[string](ctx, "go")
	}))
	for _, cancelRun := range []bool{false, true} {
		id, err := StartWorkflowByName(context.Background(), client, "s13_wake", "")
		require.NoError(t, err)
		select {
		case <-reached:
		case <-time.After(2 * time.Second):
			t.Fatal("start did not wake worker")
		}
		require.Eventually(t, func() bool {
			instance, err := client.GetInstance(context.Background(), id)
			return err == nil && instance.LeaseOwner == "" && !instance.Runnable
		}, 2*time.Second, time.Millisecond)
		// Start waiting before closure. With polling set to an hour, only the close wake can finish it.
		done := make(chan *WorkflowResult[string], 1)
		waitErr := make(chan error, 1)
		ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		go func() {
			result, err := WaitWorkflow[string](ctx, client, id)
			if err != nil {
				waitErr <- err
				return
			}
			done <- result
		}()
		require.Eventually(t, func() bool {
			client.waitMu.Lock()
			defer client.waitMu.Unlock()
			return len(client.waiters[id]) > 0
		}, time.Second, time.Millisecond)
		if cancelRun {
			require.NoError(t, CancelWorkflow(context.Background(), client, id, "wake"))
		} else {
			require.NoError(t, client.Signal(context.Background(), id, "go", "awake"))
		}
		select {
		case result := <-done:
			if cancelRun {
				require.ErrorIs(t, result.Error, context.Canceled)
			} else {
				require.Equal(t, "awake", result.Output)
			}
		case err := <-waitErr:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal("close did not wake waiter")
		}
		stop()
	}
}

func TestDurableRestartHandleStaysUsable(t *testing.T) { s13Restart(t, createTestApp) }
func s13Restart(t *testing.T, factory replayAppFactory) {
	worker, client := s13AppPair(t, factory)
	var calls atomic.Int32
	var checkpoint atomic.Bool
	once := DefineActivity("s13_recorded_once", func(context.Context, string) (string, error) { calls.Add(1); return "recorded", nil })
	workflow := DefineWorkflow("s13_restart", func(ctx *WorkflowContext, _ string) (string, error) {
		value, err := once.Execute(ctx, "")
		if err != nil {
			return "", err
		}
		if value != "recorded" {
			return "", errors.New("recorded activity changed")
		}
		checkpoint.Store(true)
		return Receive[string](ctx, "resume")
	})
	RegisterWorkflow(worker, workflow)
	id, err := StartWorkflowByName(context.Background(), client, workflow.Name(), "")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		instance, err := client.GetInstance(context.Background(), id)
		return err == nil && checkpoint.Load() && instance.LeaseOwner == "" && !instance.Runnable
	}, 2*time.Second, time.Millisecond)
	require.EqualValues(t, 1, calls.Load())
	require.NoError(t, worker.Shutdown(context.Background()))
	require.NoError(t, client.Signal(context.Background(), id, "resume", "after restart"))
	restarted := NewApp(WithDatabase(worker.config.databaseURL), WithAutoMigrate(false))
	RegisterWorkflow(restarted, workflow)
	require.NoError(t, restarted.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, restarted.Shutdown(context.Background())) })
	require.Equal(t, "after restart", s13Wait[string](t, client, id).Output)
	require.EqualValues(t, 1, calls.Load())
}

func TestConcurrentSendersKeepChannelOrder(t *testing.T) { s13ConcurrentSenders(t, createTestApp) }

type s13OrderedSignal struct{ Sender, Sequence int }

func s13ConcurrentSenders(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	client := s13ClientFor(t, h.app)
	RegisterWorkflow(h.app, DefineWorkflow("s13_order", func(ctx *WorkflowContext, _ string) ([]s13OrderedSignal, error) {
		result := make([]s13OrderedSignal, 0, 400)
		for range 400 {
			value, err := Receive[s13OrderedSignal](ctx, "go")
			if err != nil {
				return nil, err
			}
			result = append(result, value)
		}
		return result, nil
	}))
	id, err := StartWorkflowByName(h.app.ctx, client, "s13_order", "", WithInstanceID("order"))
	require.NoError(t, err)
	for round := range 50 {
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for sender := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- client.Signal(h.app.ctx, id, "go", s13OrderedSignal{Sender: sender, Sequence: round})
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	}
	rows, err := client.storage.DB().Query("SELECT payload FROM workflow_signals WHERE instance_id=$1 AND channel='go' ORDER BY id", id)
	require.NoError(t, err)
	expected := make([]s13OrderedSignal, 0, 400)
	next := make([]int, 8)
	for rows.Next() {
		var data []byte
		require.NoError(t, rows.Scan(&data))
		var value s13OrderedSignal
		require.NoError(t, json.Unmarshal(data, &value))
		require.Equal(t, next[value.Sender], value.Sequence)
		next[value.Sender]++
		expected = append(expected, value)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Len(t, expected, 400)
	for _, count := range next {
		require.Equal(t, 50, count)
	}
	require.NoError(t, h.RunOnce(id))
	result := s13Wait[[]s13OrderedSignal](t, client, id)
	require.NoError(t, result.Error)
	require.Equal(t, expected, result.Output)
}
