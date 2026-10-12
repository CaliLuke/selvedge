package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestSignalBeforeFirstReceiveIsDelivered(t *testing.T) {
	testSignalBeforeFirstReceiveIsDelivered(t, createTestApp)
}
func testSignalBeforeFirstReceiveIsDelivered(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	activity := DefineActivity("block", func(context.Context, string) (string, error) { close(entered); <-release; return "", nil })
	var values []string
	w := DefineWorkflow("early_signal", func(ctx *WorkflowContext, in string) (string, error) {
		if _, err := activity.Execute(ctx, in); err != nil {
			return "", err
		}
		for range 2 {
			v, err := Receive[string](ctx, "inbox")
			if err != nil {
				return "", err
			}
			values = append(values, v)
		}
		return "done", nil
	})
	RegisterWorkflow(h.app, w)
	id, err := StartWorkflow(h.app.ctx, h.app, w, "input")
	require.NoError(t, err)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "inbox", "before"))
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("activity did not start")
	}
	require.NoError(t, h.app.Signal(h.app.ctx, id, "inbox", "during"))
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("workflow did not finish")
	}
	require.Equal(t, []string{"before", "during"}, values)
	assertCompletedLeaseRun(t, h, id)
}

func TestChannelsHaveSeparateCursors(t *testing.T) { testChannelsHaveSeparateCursors(t, createTestApp) }
func testChannelsHaveSeparateCursors(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	var values []string
	w := DefineWorkflow("channels", func(ctx *WorkflowContext, _ string) (string, error) {
		for _, ch := range []string{"c2", "c1", "c1"} {
			v, err := Receive[string](ctx, ch)
			if err != nil {
				return "", err
			}
			values = append(values, v)
		}
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c1", "a"))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c2", "b"))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "c1", "c"))
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []string{"b", "a", "c"}, values)
}

func TestSignalReplayReadsHistory(t *testing.T) { testSignalReplayReadsHistory(t, createTestApp) }
func testSignalReplayReadsHistory(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	var values []string
	w := DefineWorkflow("signal_replay", func(ctx *WorkflowContext, _ string) (string, error) {
		v, err := Receive[string](ctx, "inbox")
		if err != nil {
			return "", err
		}
		values = append(values, v)
		return v, nil
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "inbox", "first"))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "inbox", "second"))
	crash := fmt.Errorf("lost consume acknowledgement: %w", replay.ErrLeaseLost)
	fs.After = func(_ context.Context, method string) error {
		if method == "CommitObservation" {
			return crash
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), crash)
	fs.After = nil
	fs.Before = func(_ context.Context, method string) error {
		if method == "NextSignal" || method == "CommitObservation" {
			return errors.New("replay touched inbox")
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []string{"first"}, values)
	next, err := h.app.storage.NextSignal(h.app.ctx, id, "inbox")
	require.NoError(t, err)
	require.Equal(t, `"second"`, string(next.Payload))
	var consumed int
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT COUNT(*) FROM workflow_signals WHERE instance_id=$1 AND consumed_key IS NOT NULL", id).Scan(&consumed))
	require.Equal(t, 1, consumed)
}

func TestConsumeAtomicWithRecord(t *testing.T) { testConsumeAtomicWithRecord(t, createTestApp) }
func testConsumeAtomicWithRecord(t *testing.T, factory replayAppFactory) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			w := DefineWorkflow("atomic_signal", func(ctx *WorkflowContext, _ string) (string, error) { return Receive[string](ctx, "inbox") })
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.app.Signal(h.app.ctx, id, "inbox", "value"))
			crash := fmt.Errorf("crash at consume boundary: %w", replay.ErrLeaseLost)
			hook := func(_ context.Context, method string) error {
				if method == "CommitObservation" {
					return crash
				}
				return nil
			}
			if after {
				fs.After = hook
			} else {
				fs.Before = hook
			}
			require.ErrorIs(t, h.RunOnce(id), crash)
			row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "#1")
			require.NoError(t, err)
			next, err := h.app.storage.NextSignal(h.app.ctx, id, "inbox")
			require.NoError(t, err)
			if after {
				require.NotNil(t, row)
				require.Nil(t, next)
			} else {
				require.Nil(t, row)
				require.NotNil(t, next)
			}
			fs.Before = nil
			fs.After = nil
			require.NoError(t, h.RunOnce(id))
			assertCompletedLeaseRun(t, h, id)
		})
	}
	t.Run("FencedOut", func(t *testing.T) {
		h, s := newLeaseStore(t, factory)
		setStoreClock(t, h, 1000)
		f, ok, err := s.AcquireLease(h.app.ctx, "lease", "old", time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		id, err := h.app.storage.AppendSignal(h.app.ctx, "lease", "inbox", []byte(`1`))
		require.NoError(t, err)
		setStoreClock(t, h, 2001)
		_, ok, err = s.AcquireLease(h.app.ctx, "lease", "new", time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		_, err = fixtureConsume(h.app.storage, h.app.ctx, &f, id, &storage.HistoryEvent{Seq: 1, InstanceID: "lease", ActivityID: "#1", Command: "receive:inbox", EventType: storage.HistorySignalReceived, EventData: []byte(`1`), DataType: "json"})
		require.ErrorIs(t, err, ErrFenced)
		row, err := h.app.storage.GetHistoryEvent(h.app.ctx, "lease", "#1")
		require.NoError(t, err)
		require.Nil(t, row)
		next, err := h.app.storage.NextSignal(h.app.ctx, "lease", "inbox")
		require.NoError(t, err)
		require.NotNil(t, next)
	})
}

func TestSignalToClosedOrUnknownInstanceTyped(t *testing.T) {
	testSignalToClosedOrUnknownInstanceTyped(t, createTestApp)
}
func testSignalToClosedOrUnknownInstanceTyped(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	require.ErrorIs(t, h.app.Signal(h.app.ctx, "unknown", "c", nil), ErrInstanceNotFound)
	for _, status := range []storage.WorkflowStatus{storage.StatusCompleted, storage.StatusFailed, storage.StatusCancelled} {
		id := string(status)
		require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: id, WorkflowName: "closed", Status: status, InputData: []byte(`null`)}))
		var closed *InstanceClosedError
		err := h.app.Signal(h.app.ctx, id, "c", nil)
		require.ErrorAs(t, err, &closed)
		require.ErrorIs(t, err, ErrInstanceClosed)
		require.Equal(t, status, closed.Status)
	}
}

func TestCancelWhileReceivingStaysCancelled(t *testing.T) {
	testCancelWhileReceivingStaysCancelled(t, createTestApp)
}
func testCancelWhileReceivingStaysCancelled(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	w := DefineWorkflow("cancel_receive", func(ctx *WorkflowContext, _ string) (string, error) { return Receive[string](ctx, "inbox") })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "inbox", "must remain pending"))
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	assertCancelledRun(t, h, id)
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistorySignalCancelled, row.EventType)
	next, err := h.app.storage.NextSignal(h.app.ctx, id, "inbox")
	require.NoError(t, err)
	require.NotNil(t, next)
	require.NoError(t, h.RunOnce(id))
	assertCancelledRun(t, h, id)
}

func TestSignalInputDuringSuspendIsNotLost(t *testing.T) {
	testSignalInputDuringSuspendIsNotLost(t, createTestApp)
}
func testSignalInputDuringSuspendIsNotLost(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	w := DefineWorkflow("signal_suspend", func(ctx *WorkflowContext, _ string) (string, error) { return Receive[string](ctx, "inbox") })
	id := seedErrorWorkflow(t, h, w)
	var once sync.Once
	fs.Before = func(ctx context.Context, method string) error {
		if method == "ReleaseLease" {
			once.Do(func() { require.NoError(t, h.app.Signal(ctx, id, "inbox", "wake")) })
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

func TestAbandonedRunWithSignalAndCancelIsRedispatched(t *testing.T) {
	testAbandonedRunWithSignalAndCancelIsRedispatched(t, createTestApp)
}
func testAbandonedRunWithSignalAndCancelIsRedispatched(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	first := true
	w := DefineWorkflow("abandoned_signal", func(ctx *WorkflowContext, _ string) (string, error) {
		if first {
			first = false
			require.NoError(t, h.app.Signal(ctx.Context(), ctx.InstanceID(), "inbox", "pending"))
			require.NoError(t, CancelWorkflow(ctx.Context(), h.app, ctx.InstanceID(), "stop"))
			clk.Advance(time.Second)
		}
		return Receive[string](ctx, "inbox")
	})
	id := seedErrorWorkflow(t, h, w)
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	ids, err := fs.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Contains(t, ids, id)
	require.ErrorIs(t, h.RunOnce(id), context.Canceled)
	assertCancelledRun(t, h, id)
	next, err := h.app.storage.NextSignal(h.app.ctx, id, "inbox")
	require.NoError(t, err)
	require.NotNil(t, next)
}

func TestSignalsNotLostRandomized(t *testing.T) { testSignalsNotLostRandomized(t, createTestApp) }
func testSignalsNotLostRandomized(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	rng := rand.New(rand.NewSource(6011))
	var received [6][]int
	w := DefineWorkflow("random_signals", func(ctx *WorkflowContext, _ string) (string, error) {
		for ch := range 6 {
			for range 5 {
				v, err := Receive[int](ctx, fmt.Sprint(ch))
				if err != nil {
					return "", err
				}
				received[ch] = append(received[ch], v)
			}
		}
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	sent := [6]int{}
	crash := fmt.Errorf("random crash: %w", replay.ErrLeaseLost)
	for _, n := range rng.Perm(30) {
		ch := n % 6
		sent[ch]++
		require.NoError(t, h.app.Signal(h.app.ctx, id, fmt.Sprint(ch), sent[ch]))
		point := rng.Intn(3)
		hook := func(_ context.Context, method string) error {
			if method == "CommitObservation" {
				return crash
			}
			return nil
		}
		fs.Before = nil
		fs.After = nil
		if point == 0 {
			fs.Before = hook
		}
		if point == 1 {
			fs.After = hook
		}
		err := h.RunOnce(id)
		if err != nil {
			require.ErrorIs(t, err, crash)
		}
		for c := range 6 {
			var pending, consumed int
			require.NoError(t, h.app.storage.DB().QueryRow(`SELECT COUNT(*) FILTER (WHERE consumed_key IS NULL),COUNT(*) FILTER (WHERE consumed_key IS NOT NULL) FROM workflow_signals WHERE instance_id=$1 AND channel=$2`, id, fmt.Sprint(c)).Scan(&pending, &consumed))
			require.Equal(t, sent[c], pending+consumed)
			var observations int
			require.NoError(t, h.app.storage.DB().QueryRow(`SELECT COUNT(*) FROM workflow_history WHERE instance_id=$1 AND command=$2`, id, "receive:"+fmt.Sprint(c)).Scan(&observations))
			require.Equal(t, consumed, observations)
		}
	}
	fs.Before = nil
	fs.After = nil
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.NoError(t, h.RunOnce(id))
	assertCompletedLeaseRun(t, h, id)
	for c := range 6 {
		for i := 1; i <= 5; i++ {
			row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, fmt.Sprintf("#%d", c*5+i))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprint(i), string(row.EventData))
		}
	}
}

func TestTypedSignalPayload(t *testing.T) { testTypedSignalPayload(t, createTestApp) }
func testTypedSignalPayload(t *testing.T, factory replayAppFactory) {
	type answer struct {
		ID    int64
		Data  json.RawMessage
		Items []string
		Meta  map[string]any
	}
	want := answer{9007199254740993, json.RawMessage(`{"answer":42}`), []string{"yes"}, map[string]any{"ok": true}}
	h, _, _ := leaseHarness(t, factory)
	var values []answer
	w := DefineWorkflow("typed_signal", func(ctx *WorkflowContext, _ string) (string, error) {
		v, err := Receive[answer](ctx, "typed")
		if err != nil {
			return "", err
		}
		values = append(values, v)
		return "done", Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "typed", want))
	require.NoError(t, h.RunOnce(id))
	h.forceFire(t, id)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []answer{want, want}, values)
}

func TestEmptySignalInboxParks(t *testing.T) { testEmptySignalInboxParks(t, createTestApp) }
func testEmptySignalInboxParks(t *testing.T, factory replayAppFactory) {
	h, _, _ := leaseHarness(t, factory)
	w := DefineWorkflow("empty_signal", func(ctx *WorkflowContext, _ string) (string, error) { return Receive[string](ctx, "wanted") })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	inst, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.False(t, inst.Runnable)
	require.Empty(t, inst.LeaseOwner)
	count, err := h.app.storage.GetHistoryCount(h.app.ctx, id)
	require.NoError(t, err)
	require.Zero(t, count)
	// Even an unrelated channel wakes the instance; the signal stays buffered.
	require.NoError(t, h.app.Signal(h.app.ctx, id, "other", "buffered"))
	inst, err = h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.True(t, inst.Runnable)
	require.NoError(t, h.RunOnce(id))
	inst, err = h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.False(t, inst.Runnable)
	next, err := h.app.storage.NextSignal(h.app.ctx, id, "other")
	require.NoError(t, err)
	require.NotNil(t, next)
	count, err = h.app.storage.GetHistoryCount(h.app.ctx, id)
	require.NoError(t, err)
	require.Zero(t, count)
}
