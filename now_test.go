package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestNowRecordedOnce(t *testing.T) { testNowRecordedOnce(t, createTestApp) }
func testNowRecordedOnce(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	first := time.Date(2026, 10, 11, 12, 0, 0, 123456789, time.UTC)
	clock := first
	WithClock(func() time.Time { return clock })(h.app.config)
	var values []time.Time
	w := DefineWorkflow("now_lost_ack", func(ctx *WorkflowContext, _ string) (string, error) {
		v, err := Now(ctx)
		values = append(values, v)
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	fs.After = func(_ context.Context, method string) error {
		if method == "RecordHistory" {
			return storage.ErrFenced
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), storage.ErrFenced)
	require.Equal(t, []time.Time{{}}, values, "an unacknowledged value must not advance the floor")
	fs.After = nil
	clock = first.Add(time.Hour)
	setStoreClock(t, h, 2001)
	require.NoError(t, h.RunOnce(id))
	require.True(t, values[1] == first) //nolint:staticcheck // The contract requires structural time.Time equality.
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, storage.HistoryNowRecorded, rows[0].EventType)
	require.EqualValues(t, 1, rows[0].Seq)
	require.Equal(t, "#1", rows[0].ActivityID)
	require.Equal(t, "now", rows[0].Command)
	require.JSONEq(t, fmt.Sprintf("%q", first.Format(time.RFC3339Nano)), string(rows[0].EventData))
	assertCompletedLeaseRun(t, h, id)
}

func TestNowMonotonic(t *testing.T) { testNowMonotonic(t, createTestApp) }
func testNowMonotonic(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	first := time.Date(2026, 10, 11, 12, 0, 0, 123, time.UTC)
	clock := first
	clockCalls := 0
	WithClock(func() time.Time { clockCalls++; return clock })(h.app.config)
	var values []time.Time
	w := DefineWorkflow("now_floor", func(ctx *WorkflowContext, _ string) (string, error) {
		type copyContextKey struct{}
		copyCtx := ctx.WithContext(context.WithValue(ctx.Context(), copyContextKey{}, "copy"))
		v, err := Now(copyCtx)
		if err != nil {
			return "", err
		}
		values = append(values, v)
		clock = clock.Add(-time.Hour)
		v, err = Now(ctx)
		if err != nil {
			return "", err
		}
		values = append(values, v)
		if _, err = Receive[string](copyCtx, "go"); err != nil {
			return "", err
		}
		v, err = Now(copyCtx)
		values = append(values, v)
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, h.app.Signal(h.app.ctx, id, "go", "go"))
	clock = first.Add(-2 * time.Hour)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []time.Time{first, first, first, first, first}, values)
	require.Equal(t, 3, clockCalls, "replay must not read the wall clock")
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 4, "equal Now values still occupy separate command positions")
	records := 0
	for _, method := range fs.Calls() {
		if method == "RecordHistory" {
			records++
		}
	}
	require.Equal(t, 3, records, "one fenced observation per live Now")
	require.NotContains(t, fs.Calls(), "ReserveHistory")
	require.NotContains(t, fs.Calls(), "SetHistoryValueIfNull")
}

func TestNowReplayValueIdentical(t *testing.T) { testNowReplayValueIdentical(t, createTestApp) }
func testNowReplayValueIdentical(t *testing.T, factory replayAppFactory) {
	for _, mode := range []string{"monotonic", "offset", "default", "nil_option"} {
		t.Run(mode, func(t *testing.T) {
			supplied := time.Now()
			opts := make([]Option, 0, 2)
			switch mode {
			case "monotonic":
				opts = []Option{WithClock(func() time.Time { return supplied })}
			case "offset":
				supplied = time.Date(2026, 10, 11, 12, 0, 0, 123456789, time.FixedZone("worker", 3*60*60))
				opts = []Option{WithClock(func() time.Time { return supplied })}
			case "nil_option":
				opts = []Option{WithClock(nil)}
			}
			observed := &historyMismatchHooks{}
			opts = append(opts, WithHooks(observed))
			h := newTypedReplayHarness(t, factory, opts...)
			var values []time.Time
			var before, after time.Time
			w := DefineWorkflow("now_identity", func(ctx *WorkflowContext, _ string) (string, error) {
				v, err := Now(ctx)
				if err != nil {
					return "", err
				}
				values = append(values, v)
				_, err = Receive[string](ctx, "go")
				return "done", err
			})
			id := seedErrorWorkflow(t, h, w)
			before = time.Now().UTC()
			require.NoError(t, h.RunOnce(id))
			after = time.Now().UTC()
			require.NoError(t, h.app.Signal(h.app.ctx, id, "go", "go"))
			require.NoError(t, h.RunOnce(id))
			require.Len(t, values, 2)
			require.True(t, values[0] == values[1], "live and replay must be structurally identical") //nolint:staticcheck // The contract requires structural time.Time equality.
			require.True(t, values[0].Equal(values[1]))
			require.Same(t, time.UTC, values[0].Location())
			require.True(t, values[0] == values[0].Round(0), "strip monotonic reading") //nolint:staticcheck // The contract requires structural time.Time equality.
			if mode == "default" || mode == "nil_option" {
				require.False(t, values[0].Before(before))
				require.False(t, values[0].After(after))
			} else {
				require.True(t, values[0] == supplied.UTC()) //nolint:staticcheck // The contract requires structural time.Time equality.
			}
			require.Zero(t, observed.starts)
		})
	}
}

func TestNowOffCoroutineAndClosing(t *testing.T) { testNowOffCoroutineAndClosing(t, createTestApp) }
func testNowOffCoroutineAndClosing(t *testing.T, factory replayAppFactory) {
	t.Run("foreign_during_activity", func(t *testing.T) {
		h := newTypedReplayHarness(t, factory)
		var foreignErr error
		var value time.Time
		var workflowCtx *WorkflowContext
		a := DefineActivity("foreign_now", func(ctx context.Context, _ string) (string, error) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				value, foreignErr = Now(workflowCtx)
			}()
			<-done
			return "ignored", nil
		})
		w := DefineWorkflow("now_foreign", func(ctx *WorkflowContext, input string) (string, error) {
			workflowCtx = ctx
			_, _ = a.Execute(ctx, input)
			return "ignored", nil
		})
		id := seedErrorWorkflow(t, h, w)
		var detail *NondeterminismError
		require.ErrorAs(t, h.RunOnce(id), &detail)
		require.ErrorAs(t, foreignErr, &detail)
		require.Equal(t, "command issued off the workflow coroutine", detail.Reason)
		require.True(t, value.IsZero())
		rows, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
		require.NoError(t, err)
		for _, row := range rows {
			require.NotEqual(t, "now", row.Command)
		}
	})
	t.Run("closing", func(t *testing.T) {
		first := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
		h := newTypedReplayHarness(t, factory, WithClock(func() time.Time { return first }))
		var deferredValue time.Time
		var deferredErr error
		w := DefineWorkflow("now_closing", func(ctx *WorkflowContext, _ string) (string, error) {
			if _, err := Now(ctx); err != nil {
				return "", err
			}
			defer func() { deferredValue, deferredErr = Now(ctx.WithContext(ctx.Context())) }()
			_, err := Receive[string](ctx, "go")
			return "", err
		})
		id := seedErrorWorkflow(t, h, w)
		require.NoError(t, h.RunOnce(id))
		require.EqualError(t, deferredErr, "workflow execution closed")
		require.True(t, deferredValue == first) //nolint:staticcheck // The contract requires structural time.Time equality.
		rows, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		inst, err := h.app.storage.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		require.Equal(t, storage.StatusRunning, inst.Status)
		require.Empty(t, inst.LeaseOwner)
	})
	t.Run("standalone", func(t *testing.T) {
		ctx := NewWorkflowContext(context.Background(), "standalone", "", "")
		v, err := Now(ctx.WithContext(ctx.Context()))
		var detail *NondeterminismError
		require.ErrorAs(t, err, &detail)
		require.True(t, v.IsZero())
		require.ErrorIs(t, ctx.execCtx.Fatal(), err)
	})
}

func TestNowErrorsReturnFloorAndStaySticky(t *testing.T) {
	testNowErrorsReturnFloorAndStaySticky(t, createTestApp)
}
func testNowErrorsReturnFloorAndStaySticky(t *testing.T, factory replayAppFactory) {
	for _, mode := range []string{"decode", "null", "event_type", "reserved", "write", "marshal", "lease"} {
		t.Run(mode, func(t *testing.T) {
			h, clk, fs := leaseHarness(t, factory)
			first := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			clock := first
			WithClock(func() time.Time { return clock })(h.app.config)
			var got, again time.Time
			var failure, repeated error
			w := DefineWorkflow("now_failure", func(ctx *WorkflowContext, _ string) (string, error) {
				if _, err := Now(ctx); err != nil {
					return "", err
				}
				if mode == "lease" {
					clk.Advance(time.Second)
				}
				if mode == "marshal" {
					clock = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
				}
				got, failure = Now(ctx)
				again, repeated = Now(ctx)
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			data, err := json.Marshal(first)
			require.NoError(t, err)
			_, err = fixtureRecord(h.app.ctx, fs.Storage, &storage.HistoryEvent{InstanceID: id, Seq: 1, ActivityID: "#1", Command: "now", EventType: storage.HistoryNowRecorded, EventData: data, DataType: "json"})
			require.NoError(t, err)
			if mode == "decode" || mode == "null" || mode == "event_type" || mode == "reserved" {
				kind := storage.HistoryNowRecorded
				payload := []byte(`"not-a-time"`)
				switch mode {
				case "null":
					payload = []byte("null")
				case "event_type":
					kind = storage.HistorySignalReceived
					payload = data
				case "reserved":
					kind = storage.HistoryReserved
					payload = nil
				}
				_, err = fixtureRecord(h.app.ctx, fs.Storage, &storage.HistoryEvent{InstanceID: id, Seq: 2, ActivityID: "#2", Command: "now", EventType: kind, EventData: payload, DataType: "json"})
				require.NoError(t, err)
			}
			writeErr := errors.New("now write unavailable")
			fs.Before = func(_ context.Context, method string) error {
				if mode == "write" && method == "RecordHistory" {
					return writeErr
				}
				return nil
			}
			runErr := h.RunOnce(id)
			require.Error(t, failure)
			require.ErrorIs(t, runErr, failure)
			require.ErrorIs(t, repeated, failure)
			require.True(t, got == first)   //nolint:staticcheck // The contract requires structural time.Time equality.
			require.True(t, again == first) //nolint:staticcheck // The contract requires structural time.Time equality.
			if mode == "write" {
				require.ErrorIs(t, failure, writeErr)
			}
			if mode == "lease" {
				require.ErrorIs(t, failure, replay.ErrLeaseLost)
			}
		})
	}
}

func TestReplayCommandMismatchFailsFast_Now(t *testing.T) {
	testReplayCommandMismatchFailsFastNow(t, createTestApp)
}
func testReplayCommandMismatchFailsFastNow(t *testing.T, factory replayAppFactory) {
	for _, mode := range []string{"at_activity", "removed", "inserted"} {
		t.Run(mode, func(t *testing.T) {
			observed := &historyMismatchHooks{}
			h := newTypedReplayHarness(t, factory, WithHooks(observed))
			calls := 0
			a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "live", nil })
			w := DefineWorkflow("now_mismatch", func(ctx *WorkflowContext, input string) (string, error) {
				if mode == "removed" {
					return "done", nil
				}
				if _, err := Now(ctx); err != nil {
					return "", err
				}
				if mode == "inserted" {
					return a.Execute(ctx, input)
				}
				return "done", nil
			})
			id := seedErrorWorkflow(t, h, w)
			command, issued := "activity:A", "now"
			kind, data := storage.HistoryActivityCompleted, []byte(`"recorded"`)
			if mode == "removed" {
				command, issued = "now", ""
				kind, data = storage.HistoryNowRecorded, []byte(`"2026-10-11T12:00:00Z"`)
			}
			_, err := fixtureRecord(h.app.ctx, h.app.storage, &storage.HistoryEvent{InstanceID: id, Seq: 1, ActivityID: "#1", Command: command, EventType: kind, EventData: data, DataType: "json"})
			require.NoError(t, err)
			before, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
			require.NoError(t, err)
			assertNondeterminism(t, h, id, h.RunOnce(id), 1, command, issued)
			require.Zero(t, calls)
			require.Zero(t, observed.starts)
			after, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
			require.NoError(t, err)
			require.Len(t, after, 2)
			require.Equal(t, before[0], after[0])
			require.Equal(t, "$workflow_failed", after[1].ActivityID)
		})
	}
}

func TestStalledExecutionCommitRejected_Now(t *testing.T) {
	testStalledExecutionCommitRejectedNow(t, createTestApp)
}
func testStalledExecutionCommitRejectedNow(t *testing.T, factory replayAppFactory) {
	h, clk, fs := leaseHarness(t, factory)
	require.NoError(t, h.app.engine.ConfigureLease(replay.LeaseConfig{
		TTL: time.Hour, RenewInterval: time.Minute, Margin: 200 * time.Millisecond, MaxSkew: 100 * time.Millisecond,
	}, func() replay.Clock { return clk }))
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(resume) }) })
	var returned time.Time
	w := DefineWorkflow("now_stalled", func(ctx *WorkflowContext, _ string) (string, error) {
		var err error
		returned, err = Now(ctx)
		return "ignored", err
	})
	id := seedErrorWorkflow(t, h, w)
	fs.Before = func(_ context.Context, method string) error {
		if method == "RecordHistory" {
			close(entered)
			<-resume
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("Now did not attempt its fenced write: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Now did not reach RecordHistory")
	}
	setStoreClock(t, h, 1001+time.Hour.Milliseconds())
	_, ok, err := fs.Storage.AcquireLease(h.app.ctx, id, "worker/new-execution", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	once.Do(func() { close(resume) })
	select {
	case err = <-done:
		require.ErrorIs(t, err, storage.ErrFenced)
	case <-time.After(5 * time.Second):
		t.Fatal("stalled Now did not exit")
	}
	require.True(t, returned.IsZero())
	rows, err := fs.GetHistoryCount(h.app.ctx, id)
	require.NoError(t, err)
	require.Zero(t, rows)
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, "worker/new-execution", inst.LeaseOwner)
	require.Equal(t, storage.StatusRunning, inst.Status)
	require.Equal(t, []string{"AcquireLease", "RecordHistory"}, executionWrites(fs))
}

func TestReplayDeterministicAfterCrash_DeadlinePassesBetweenRuns(t *testing.T) {
	testReplayDeterministicAfterCrashDeadlinePassesBetweenRuns(t, createTestApp)
}
func testReplayDeterministicAfterCrashDeadlinePassesBetweenRuns(t *testing.T, factory replayAppFactory) {
	baseline, writes := runNowCrashScenario(t, factory, 0, false)
	require.Contains(t, writes, "RecordHistory")
	require.Contains(t, writes, "AppendSignal")
	for i, method := range writes {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%02d_%s/after_%t", i+1, method, after), func(t *testing.T) {
				got, _ := runNowCrashScenario(t, factory, i+1, after)
				require.Equal(t, baseline, got)
			})
		}
	}
}

func runNowCrashScenario(t *testing.T, factory replayAppFactory, crashAt int, after bool) ([]historyValue, []string) {
	t.Helper()
	h, clk, fs := leaseHarness(t, factory)
	// Renewal must not introduce timing-dependent writes into the crash matrix.
	require.NoError(t, h.app.engine.ConfigureLease(replay.LeaseConfig{
		TTL: time.Hour, RenewInterval: time.Minute, Margin: 200 * time.Millisecond, MaxSkew: 100 * time.Millisecond,
	}, func() replay.Clock { return clk }))
	first := time.Date(2026, 10, 11, 12, 0, 0, 123, time.UTC)
	clock := first
	WithClock(func() time.Time { return clock })(h.app.config)
	calls, observed := 0, false
	var writes []string
	crashed := false
	a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "durable", nil })
	w := DefineWorkflow("now_deadline", func(ctx *WorkflowContext, input string) (string, error) {
		deadline, err := time.Parse(time.RFC3339Nano, input)
		if err != nil {
			return "", err
		}
		v, err := Now(ctx)
		if err != nil {
			return "", err
		}
		if !v.Before(deadline) {
			return "late", nil
		}
		if _, err = a.Execute(ctx, input); err != nil {
			return "", err
		}
		if _, err = Receive[string](ctx, "go"); err != nil {
			return "", err
		}
		return "early", nil
	})
	id := seedErrorWorkflow(t, h, w)
	input, err := json.Marshal(first.Add(time.Minute).Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = fs.DB().Exec("UPDATE workflow_instances SET input_data=$1 WHERE instance_id=$2", string(input), id)
	require.NoError(t, err)
	inject := func(_ context.Context, method string) error {
		// S10a owns this list; S8 can extend its separate activity crash scenario.
		switch method {
		case "AcquireLease", "RenewLease", "RecordHistory", "ReserveHistory", "SetHistoryValueIfNull", "ConsumeSignal", "AppendSignal", "ReleaseLease", "CloseInstance":
			writes = append(writes, method)
		default:
			return nil
		}
		if !crashed && crashAt > 0 && len(writes) == crashAt {
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
	signaled := false
	for attempt := 0; attempt < 12; attempt++ {
		runErr := h.RunOnce(id)
		if runErr != nil {
			require.ErrorIs(t, runErr, storage.ErrFenced)
		}
		inst, err := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		if inst.Status == storage.StatusCompleted {
			break
		}
		// Cross the deadline only after the original early path reaches its
		// blocked receive. A crash before Now commits has no time to replay yet.
		if runErr == nil {
			observed = true
		}
		if observed {
			clock = first.Add(time.Hour)
			if !signaled {
				_, err = fs.AppendSignal(h.app.ctx, id, "go", []byte(`"go"`))
				if err != nil {
					require.ErrorIs(t, err, storage.ErrFenced)
				}
				// A lost acknowledgement must not append a second input.
				var count int
				require.NoError(t, fs.DB().QueryRow("SELECT COUNT(*) FROM workflow_signals WHERE instance_id=$1", id).Scan(&count))
				signaled = count > 0
			}
		}
		setStoreClock(t, h, int64(1000+(attempt+1)*int(time.Hour.Milliseconds()+1)))
	}
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "completed", result.Status)
	require.Equal(t, "early", result.Output)
	if crashAt == 0 {
		require.Equal(t, 1, calls)
	} else {
		require.True(t, crashed, "injected write must be reached")
		require.GreaterOrEqual(t, calls, 1)
		require.LessOrEqual(t, calls, 2, "only an unrecorded activity outcome can repeat")
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	values := make([]historyValue, 0, len(rows))
	for _, row := range rows {
		values = append(values, historyValue{row.Seq, row.ActivityID, row.Command, row.EventType, string(row.EventData)})
	}
	return values, writes
}

func TestNowUsesStoredValueOnConflict(t *testing.T) {
	testNowUsesStoredValueOnConflict(t, createTestApp)
}
func testNowUsesStoredValueOnConflict(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	first := time.Date(2026, 10, 11, 12, 0, 0, 123, time.UTC)
	WithClock(func() time.Time { return first.Add(time.Hour) })(h.app.config)
	var fence *storage.Fence
	var got, deferred time.Time
	w := DefineWorkflow("now_conflict", func(ctx *WorkflowContext, _ string) (string, error) {
		fence = ctx.execCtx.Fence()
		var err error
		got, err = Now(ctx)
		if err != nil {
			return "", err
		}
		defer func() { deferred, _ = Now(ctx) }()
		_, err = Receive[string](ctx, "go")
		return "", err
	})
	id := seedErrorWorkflow(t, h, w)
	data, err := json.Marshal(first)
	require.NoError(t, err)
	fs.Before = func(ctx context.Context, method string) error {
		if method == "RecordHistory" {
			_, err := fs.Storage.RecordHistory(ctx, fence, &storage.HistoryEvent{
				InstanceID: id, Seq: 1, ActivityID: "#1", Command: "now",
				EventType: storage.HistoryNowRecorded, EventData: data, DataType: "json",
			})
			return err
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	require.True(t, got == first, "return the stored winner, not the candidate")     //nolint:staticcheck // The contract requires structural time.Time equality.
	require.True(t, deferred == first, "only the returned value advances the floor") //nolint:staticcheck // The contract requires structural time.Time equality.
	rows, err := fs.GetHistoryCount(h.app.ctx, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
}
