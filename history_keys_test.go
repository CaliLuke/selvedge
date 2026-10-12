package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestHistoryKeyWrittenOnce(t *testing.T) { testHistoryKeyWrittenOnce(t, createTestApp) }
func testHistoryKeyWrittenOnce(t *testing.T, factory replayAppFactory) {
	t.Helper()
	for _, tc := range []struct{ name, firstID, secondID, secondName string }{
		{"different_activities", "k", "k", "B"},
		{"automatic_then_explicit", "", "#1", "B"},

		{"same_activity", "k", "k", "A"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			calls := 0
			fn := func(context.Context, string) (string, error) { calls++; return "first", nil }
			a, b := DefineActivity("A", fn), DefineActivity(tc.secondName, fn)
			w := DefineWorkflow("duplicate", func(ctx *WorkflowContext, input string) (string, error) {
				_, err := a.Execute(ctx, input, WithActivityID(tc.firstID))
				require.NoError(t, err)
				_, err = b.Execute(ctx.WithContext(ctx.Context()), input, WithActivityID(tc.secondID))
				require.ErrorIs(t, err, ErrHistoryKeyConflict)
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			require.ErrorIs(t, h.RunOnce(id), ErrHistoryKeyConflict)
			require.Equal(t, 1, calls)
			kind := "history_key_conflict"
			if tc.name == "automatic_then_explicit" {
				kind = "generic"
			}
			assertHistoryFailure(t, h, id, ErrHistoryKeyConflict, kind)
		})
	}
	t.Run("stored_first_value", func(t *testing.T) {
		h := newTypedReplayHarness(t, factory)
		s := h.app.storage
		ctx := h.app.ctx
		require.NoError(t, s.CreateInstance(ctx, &storage.WorkflowInstance{InstanceID: "writes", WorkflowName: "writes", Status: storage.StatusRunning}))
		first, err := fixtureRecord(ctx, s, &storage.HistoryEvent{InstanceID: "writes", ActivityID: "k", Command: "activity:A", EventType: storage.HistoryActivityCompleted, EventData: []byte(`"first"`), DataType: "json"})
		require.NoError(t, err)
		second, err := fixtureRecord(ctx, s, &storage.HistoryEvent{InstanceID: "writes", ActivityID: "k", Command: "activity:A", EventType: storage.HistoryActivityFailed, EventData: []byte(`"second"`), DataType: "json"})
		require.NoError(t, err)
		require.Equal(t, first, second)
		_, err = s.ReserveHistory(ctx, fixtureFence(t, s, "writes"), "writes", "timer", "sleep", 2)
		require.NoError(t, err)
		first, err = s.SetHistoryValueIfNull(ctx, nil, "writes", "timer", "sleep", storage.HistoryTimerExpired, []byte(`true`))
		require.NoError(t, err)
		second, err = s.SetHistoryValueIfNull(ctx, nil, "writes", "timer", "sleep", storage.HistoryActivityFailed, []byte(`false`))
		require.NoError(t, err)
		require.Equal(t, first, second)
	})
	for _, tc := range []struct {
		name, command string
		want          error
	}{
		{"another_writer_same_command", "activity:A", nil},
		{"another_writer_different_command", "activity:B", ErrHistoryKeyConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
				_, err := fixtureRecord(ctx, h.app.storage, &storage.HistoryEvent{InstanceID: GetWorkflowContext(ctx).InstanceID(), ActivityID: "k", Command: tc.command, EventType: storage.HistoryActivityCompleted, EventData: []byte(`"other writer"`), DataType: "json"})
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
					return "", NewTerminalError(err)
				}
				require.NoError(t, err)
				return "live", nil
			})
			w := DefineWorkflow("writer", func(ctx *WorkflowContext, input string) (string, error) {
				value, err := a.Execute(ctx, input, WithActivityID("k"))
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
				} else {
					require.NoError(t, err)
					require.Equal(t, "other writer", value)
				}
				return value, err
			})
			id := seedErrorWorkflow(t, h, w)
			err := h.RunOnce(id)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				assertHistoryFailure(t, h, id, tc.want, "terminal")
			} else {
				require.NoError(t, err)
			}
			row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "k")
			require.NoError(t, err)
			if tc.want == nil {
				require.JSONEq(t, `"other writer"`, string(row.EventData))
			} else {
				require.Equal(t, "activity:A", row.Command)
			}
		})
	}
}

func assertHistoryFailure(t *testing.T, h *typedReplayHarness, id string, want error, kind string) {
	t.Helper()
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.ErrorIs(t, result.Error, want)
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "$workflow_failed")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, "close", row.Command)
	var ev errorEnvelope
	require.NoError(t, json.Unmarshal(row.EventData, &ev))
	require.Equal(t, kind, ev.Kind)
}

func TestReplayCommandMismatchFailsFast(t *testing.T) {
	testReplayCommandMismatchFailsFast(t, createTestApp)
}
func testReplayCommandMismatchFailsFast(t *testing.T, factory replayAppFactory) {
	t.Run("S7", func(t *testing.T) { testS7ReplayMismatches(t, factory) })
	t.Helper()
	for _, tc := range []struct{ name, key, command string }{
		{"activity", "#1", "activity:A"}, {"empty_command", "#1", ""}, {"sleep", "#1", "activity:A"}, {"receive", "#1", "activity:A"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := &historyMismatchHooks{}
			h := newTypedReplayHarness(t, factory, WithHooks(observed))
			calls := 0
			b := DefineActivity("B", func(context.Context, string) (string, error) { calls++; return "side effect", nil })
			w := DefineWorkflow("mismatch", func(ctx *WorkflowContext, input string) (string, error) {
				switch tc.name {
				case "sleep":
					require.ErrorIs(t, Sleep(ctx, time.Hour), ErrDeterminismViolation)
				case "receive":
					_, err := Receive[string](ctx, "messages")
					require.ErrorIs(t, err, ErrDeterminismViolation)
				default:
					_, err := b.Execute(ctx, input)
					require.ErrorIs(t, err, ErrDeterminismViolation)
				}
				_, err := b.Execute(ctx, input, WithActivityID("after-fatal"))
				require.ErrorIs(t, err, ErrDeterminismViolation)
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			original, err := fixtureRecord(h.app.ctx, h.app.storage, &storage.HistoryEvent{InstanceID: id, ActivityID: tc.key, Command: tc.command, EventType: storage.HistoryActivityCompleted, EventData: []byte(`"recorded"`), DataType: "json"})
			require.NoError(t, err)
			runErr := h.RunOnce(id)
			issued := "activity:B"
			if tc.name == "sleep" {
				issued = "sleep"
			}
			if tc.name == "receive" {
				issued = "receive:messages"
			}
			assertNondeterminism(t, h, id, runErr, 1, tc.command, issued)
			require.Zero(t, calls)
			require.Zero(t, observed.starts, "command validation must precede user hooks")
			assertHistoryFailure(t, h, id, ErrDeterminismViolation, "nondeterminism")
			rows, more, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
			require.NoError(t, err)
			require.False(t, more)
			require.Len(t, rows, 2)
			require.Equal(t, original, rows[0])
		})
	}
}

func TestHistoryWriteClassesStorageConformance(t *testing.T) {
	testHistoryWriteClassesStorageConformance(t, createTestApp)
}
func testHistoryWriteClassesStorageConformance(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	s := h.app.storage
	ctx := h.app.ctx
	require.NoError(t, s.CreateInstance(ctx, &storage.WorkflowInstance{InstanceID: "classes", WorkflowName: "classes", Status: storage.StatusRunning}))
	missing, err := s.GetHistoryEvent(ctx, "classes", "missing")
	require.NoError(t, err)
	require.Nil(t, missing)
	f := fixtureFence(t, s, "classes")
	first, err := s.ReserveHistory(ctx, f, "classes", "k", "sleep", 1)
	require.NoError(t, err)
	require.Equal(t, storage.HistoryReserved, first.EventType)
	require.Nil(t, first.EventData)
	require.Positive(t, first.ID)
	require.False(t, first.CreatedAt.IsZero())
	again, err := s.ReserveHistory(ctx, f, "classes", "k", "sleep", 1)
	require.NoError(t, err)
	require.Equal(t, first, again)
	_, err = s.ReserveHistory(ctx, f, "classes", "k", "receive:x", 1)
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	_, err = fixtureRecord(ctx, s, &storage.HistoryEvent{InstanceID: "classes", ActivityID: "k", Command: "activity:A", EventType: storage.HistoryActivityCompleted, EventData: []byte(`1`), DataType: "json"})
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	_, err = s.SetHistoryValueIfNull(ctx, f, "classes", "k", "receive:x", storage.HistorySignalReceived, []byte(`2`))
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	unchanged, err := s.GetHistoryEvent(ctx, "classes", "k")
	require.NoError(t, err)
	require.Equal(t, first, unchanged)
	filled, err := s.SetHistoryValueIfNull(ctx, nil, "classes", "k", "sleep", storage.HistoryTimerExpired, []byte(`null`))
	require.NoError(t, err)
	require.Equal(t, first.ID, filled.ID)
	require.Equal(t, storage.HistoryTimerExpired, filled.EventType)
	require.Equal(t, []byte(`null`), filled.EventData)
	again, err = s.SetHistoryValueIfNull(ctx, nil, "classes", "k", "sleep", storage.HistoryTimerExpired, []byte(`true`))
	require.NoError(t, err)
	require.Equal(t, filled, again, "JSON null is a recorded value, not SQL NULL")
	tx, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = fixtureRecord(tx, s, &storage.HistoryEvent{InstanceID: "classes", ActivityID: "rollback", Command: "activity:A", EventType: storage.HistoryActivityCompleted, EventData: []byte(`1`), DataType: "json"})
	require.NoError(t, err)
	require.NoError(t, s.RollbackTransaction(tx))
	missing, err = s.GetHistoryEvent(ctx, "classes", "rollback")
	require.NoError(t, err)
	require.Nil(t, missing)
}

func TestStartWorkflowDuplicateIDRejected(t *testing.T) {
	testStartWorkflowDuplicateIDRejected(t, createTestApp)
}
func testStartWorkflowDuplicateIDRejected(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	w := DefineWorkflow("duplicate_start", func(*WorkflowContext, string) (string, error) {
		t.Error("duplicate start must not dispatch")
		return "", nil
	})
	id := seedErrorWorkflow(t, h, w)
	before, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	_, err = StartWorkflow(h.app.ctx, h.app, w, "replacement", WithInstanceID(id))
	require.ErrorIs(t, err, ErrWorkflowAlreadyStarted)
	after, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestReservedActivityIDsRejected(t *testing.T) { testReservedActivityIDsRejected(t, createTestApp) }
func testReservedActivityIDsRejected(t *testing.T, factory replayAppFactory) {
	t.Helper()
	for _, command := range []string{"activity", "sleep"} {
		for _, tc := range []struct{ key, prefix string }{
			{"$workflow_failed", "$"}, {"$future_runtime", "$"}, {"#1", "#"},
			{"#future", "#"},
		} {
			t.Run(command+"/"+tc.key, func(t *testing.T) {
				h := newTypedReplayHarness(t, factory)
				calls := 0
				a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "", nil })
				var rejected error
				w := DefineWorkflow("reserved", func(ctx *WorkflowContext, input string) (string, error) {
					if command == "sleep" {
						rejected = Sleep(ctx, time.Hour, WithSleepID(tc.key))
						return "", errors.New("workflow error after sleep")
					}
					_, rejected = a.Execute(ctx, input, WithActivityID(tc.key))
					return "ignored", nil
				})
				id := seedErrorWorkflow(t, h, w)
				runErr := h.RunOnce(id)
				require.ErrorIs(t, rejected, ErrHistoryKeyConflict)
				require.Contains(t, rejected.Error(), fmt.Sprintf("%s ID %q", command, tc.key))
				require.Contains(t, rejected.Error(), fmt.Sprintf("reserved prefix %q", tc.prefix))
				require.Same(t, rejected, runErr, "the sticky rejection must override the workflow return")
				require.Zero(t, calls)
				assertHistoryFailure(t, h, id, ErrHistoryKeyConflict, "generic")
				rows, more, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
				require.NoError(t, err)
				require.False(t, more)
				require.Len(t, rows, 1, "only the close row may be written")
				require.Equal(t, "$workflow_failed", rows[0].ActivityID)
				var envelope errorEnvelope
				require.NoError(t, json.Unmarshal(rows[0].EventData, &envelope))
				require.NotNil(t, envelope.Cause)
				require.Equal(t, "history_key_conflict", envelope.Cause.Kind)
				result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
				require.NoError(t, err)
				require.EqualError(t, result.Error, rejected.Error(), "the diagnostic must survive persistence")
			})
		}
	}
	t.Run("subscribe_prefix_allowed", func(t *testing.T) {
		h := newTypedReplayHarness(t, factory)
		calls := 0
		a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "done", nil })
		w := DefineWorkflow("subscribe_prefix", func(ctx *WorkflowContext, input string) (string, error) {
			value, err := a.Execute(ctx, input, WithActivityID("subscribe_activity:1"))
			if err != nil {
				return "", err
			}
			return value, Sleep(ctx, time.Hour, WithSleepID("subscribe_timer:1"))
		})
		id := seedErrorWorkflow(t, h, w)
		require.NoError(t, h.RunOnce(id))
		require.Equal(t, 1, calls)
		instance, err := h.app.storage.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		require.Equal(t, storage.StatusRunning, instance.Status)
		row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "subscribe_timer:1")
		require.NoError(t, err)
		require.NotNil(t, row)
		require.Equal(t, "sleep", row.Command)
	})
}

type failureWriteStore struct {
	storage.Storage
	err error
}

func (s failureWriteStore) CloseInstance(ctx context.Context, f storage.Fence, status storage.WorkflowStatus, output []byte, ev *storage.HistoryEvent) error {
	if ev != nil && ev.ActivityID == "$workflow_failed" {
		return s.err
	}
	return s.Storage.CloseInstance(ctx, f, status, output, ev)
}
func TestWorkflowFailureWriteErrorReported(t *testing.T) {
	testWorkflowFailureWriteErrorReported(t, createTestApp)
}
func testWorkflowFailureWriteErrorReported(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	writeErr := errors.New("failure write unavailable")
	runErr := errors.New("workflow failed")
	h.app.engine = replay.NewEngine(failureWriteStore{h.app.storage, writeErr}, h.app.hooks, "failure-writer", errorCodec{})
	w := DefineWorkflow("failure_write", func(*WorkflowContext, string) (string, error) { return "", runErr })
	id := seedErrorWorkflow(t, h, w)
	err := h.RunOnce(id)
	require.ErrorIs(t, err, writeErr)
	require.ErrorIs(t, err, runErr)
	row, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, row.Status, "a failed failure write must leave the run recoverable")
}

func TestHistoryConcurrentFirstWriterWins(t *testing.T) {
	testHistoryConcurrentFirstWriterWins(t, createTestApp)
}
func testHistoryConcurrentFirstWriterWins(t *testing.T, factory replayAppFactory) {
	t.Helper()
	for _, fill := range []bool{false, true} {
		t.Run(fmt.Sprintf("set_if_null_%t", fill), func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			s := h.app.storage
			ctx := h.app.ctx
			require.NoError(t, s.CreateInstance(ctx, &storage.WorkflowInstance{InstanceID: "race", WorkflowName: "race", Status: storage.StatusRunning}))
			f := fixtureFence(t, s, "race")
			if fill {
				_, err := s.ReserveHistory(ctx, f, "race", "k", "sleep", 1)
				require.NoError(t, err)
			}
			type outcome struct {
				row *storage.HistoryEvent
				err error
			}
			results := make(chan outcome, 4)
			start := make(chan struct{})
			for i := range 4 {
				go func() {
					<-start
					data := []byte(fmt.Sprintf("%d", i))
					var row *storage.HistoryEvent
					var err error
					if fill {
						row, err = s.SetHistoryValueIfNull(ctx, nil, "race", "k", "sleep", storage.HistoryTimerExpired, data)
					} else {
						row, err = fixtureRecord(ctx, s, &storage.HistoryEvent{InstanceID: "race", ActivityID: "k", Command: "activity:A", EventType: storage.HistoryActivityCompleted, EventData: data, DataType: "json"})
					}
					results <- outcome{row, err}
				}()
			}
			close(start)
			first := <-results
			require.NoError(t, first.err)
			for range 3 {
				next := <-results
				require.NoError(t, next.err)
				require.Equal(t, first.row, next.row)
			}
			stored, err := s.GetHistoryEvent(ctx, "race", "k")
			require.NoError(t, err)
			require.Equal(t, first.row, stored)
		})
	}
}

func TestDuplicateClaimPreservesOriginalCommand(t *testing.T) {
	testDuplicateClaimPreservesOriginalCommand(t, createTestApp)
}
func testDuplicateClaimPreservesOriginalCommand(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	a := DefineActivity("A", func(ctx context.Context, _ string) (string, error) {
		wc := GetWorkflowContext(ctx)
		_, err := wc.execCtx.Issue("activity:B", "k")
		require.ErrorIs(t, err, ErrHistoryKeyConflict)
		return "first", nil
	})
	w := DefineWorkflow("original_command", func(ctx *WorkflowContext, input string) (string, error) {
		return a.Execute(ctx, input, WithActivityID("k"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.ErrorIs(t, h.RunOnce(id), ErrHistoryKeyConflict)
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "k")
	require.NoError(t, err)
	require.Equal(t, "activity:A", row.Command)
}

func TestSleepReservationReplaysUntilFilled(t *testing.T) {
	testSleepReservationReplaysUntilFilled(t, createTestApp)
}
func testSleepReservationReplaysUntilFilled(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	w := DefineWorkflow("reservation", func(ctx *WorkflowContext, _ string) (string, error) {
		return "done", Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	first, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "pause")
	require.NoError(t, err)
	require.Equal(t, "sleep", first.Command)
	require.Equal(t, storage.HistoryReserved, first.EventType)
	require.Nil(t, first.EventData)
	require.NoError(t, h.RunOnce(id))
	inst, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, inst.Status)
	h.forceFire(t, id)
	require.NoError(t, h.RunOnce(id))
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "completed", result.Status)
	require.Equal(t, "done", result.Output)
	filled, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "pause")
	require.NoError(t, err)
	require.Equal(t, first.ID, filled.ID)
}

type historyMismatchHooks struct {
	hooks.NoOpHooks
	starts int
}

func (h *historyMismatchHooks) OnActivityStart(context.Context, hooks.ActivityStartInfo) { h.starts++ }
func (h *historyMismatchHooks) OnEventWaitStart(context.Context, hooks.EventWaitStartInfo) {
	h.starts++
}
func (h *historyMismatchHooks) OnTimerStart(context.Context, hooks.TimerStartInfo) { h.starts++ }
