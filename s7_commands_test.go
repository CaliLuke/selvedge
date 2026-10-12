package selvedge

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestKeysFollowIssueOrderAcrossCommands(t *testing.T) {
	testKeysFollowIssueOrderAcrossCommands(t, createTestApp)
}
func testKeysFollowIssueOrderAcrossCommands(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "ok", nil })
	w := DefineWorkflow("order", func(ctx *WorkflowContext, in string) (string, error) {
		if _, err := a.Execute(ctx, in); err != nil {
			return "", err
		}
		if err := Sleep(ctx, time.Hour); err != nil {
			return "", err
		}
		if _, err := Receive[string](ctx, "messages"); err != nil {
			return "", err
		}
		return a.Execute(ctx, in)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.app.Signal(h.app.ctx, id, "messages", "value"))
	require.NoError(t, h.RunOnce(id))
	rows, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Equal(t, "#1", rows[0].ActivityID)
	require.NoError(t, h.app.storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#2"}))
	require.NoError(t, h.RunOnce(id))
	rows, _, err = h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 4)
	for i, row := range rows {
		require.Equal(t, fmt.Sprintf("#%d", i+1), row.ActivityID)
		var seq int64
		require.NoError(t, h.app.storage.DB().QueryRow("SELECT seq FROM workflow_history WHERE id=$1", row.ID).Scan(&seq))
		require.Equal(t, int64(i+1), seq)
	}
}

func TestSleepUntilPastDeadlineKeepsCommandOrder(t *testing.T) {
	testSleepUntilPastDeadlineKeepsCommandOrder(t, createTestApp)
}
func testSleepUntilPastDeadlineKeepsCommandOrder(t *testing.T, factory replayAppFactory) {
	t.Run("immediate_fires", func(t *testing.T) {
		h, _, fs := leaseHarness(t, factory)
		w := DefineWorkflow("immediate", func(ctx *WorkflowContext, _ string) (string, error) {
			if err := Sleep(ctx, 0); err != nil {
				return "", err
			}
			return "done", SleepUntil(ctx, time.Now().Add(-time.Hour))
		})
		id := seedErrorWorkflow(t, h, w)
		require.NoError(t, h.RunOnce(id))
		assertCompletedLeaseRun(t, h, id)
		rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 10)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		require.Equal(t, storage.HistoryTimerExpired, rows[0].EventType)
		require.Equal(t, storage.HistoryTimerExpired, rows[1].EventType)
		var subscriptions int
		require.NoError(t, fs.DB().QueryRow("SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1", id).Scan(&subscriptions))
		require.Zero(t, subscriptions)
		require.NotContains(t, fs.Calls(), "ReserveTimer")
		records := 0
		for _, method := range fs.Calls() {
			if method == "RecordHistory" {
				records++
			}
		}
		require.Equal(t, 2, records)
	})

	h := newTypedReplayHarness(t, factory)
	deadline := time.Now().Add(200 * time.Millisecond)
	w := DefineWorkflow("deadline", func(ctx *WorkflowContext, input string) (string, error) {
		d, err := time.Parse(time.RFC3339Nano, input)
		if err != nil {
			return "", err
		}
		if err = SleepUntil(ctx, d); err != nil {
			return "", err
		}
		return "done", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	_, err := h.app.storage.DB().Exec("UPDATE workflow_instances SET input_data=$1 WHERE instance_id=$2", fmt.Sprintf("%q", deadline.Format(time.RFC3339Nano)), id)
	require.NoError(t, err)
	require.NoError(t, h.RunOnce(id))
	rows, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NoError(t, h.app.storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: rows[0].ActivityID}))
	time.Sleep(time.Until(deadline) + 10*time.Millisecond)
	require.NoError(t, h.RunOnce(id))
	inst, err := h.app.storage.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusRunning, inst.Status)
	rows, _, err = h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Nil(t, rows[1].EventData)
}

type positionHooks struct {
	hooks.NoOpHooks
	replay []bool
}

func (h *positionHooks) OnActivityStart(_ context.Context, i hooks.ActivityStartInfo) {
	h.replay = append(h.replay, i.IsReplay)
}
func TestIsReplayIsPerPosition(t *testing.T) { testIsReplayIsPerPosition(t, createTestApp) }
func testIsReplayIsPerPosition(t *testing.T, factory replayAppFactory) {
	observed := &positionHooks{}
	h := newTypedReplayHarness(t, factory, WithHooks(observed))
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "ok", nil })
	w := DefineWorkflow("positions", func(ctx *WorkflowContext, in string) (string, error) {
		if _, err := a.Execute(ctx, in, WithActivityID("first")); err != nil {
			return "", err
		}
		return a.Execute(ctx, in)
	})
	id := seedErrorWorkflow(t, h, w)
	_, err := fixtureRecord(h.app.ctx, h.app.storage, &storage.HistoryEvent{InstanceID: id, ActivityID: "first", Command: "activity:A", EventType: storage.HistoryActivityCompleted, EventData: []byte(`"ok"`), DataType: "json"})
	require.NoError(t, err)
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, []bool{true, false}, observed.replay)
}

func testS7ReplayMismatches(t *testing.T, factory replayAppFactory) {
	for _, tc := range []struct {
		name                     string
		recorded, issued         []string
		keys, issueKeys          []string
		seq                      int64
		wantRecorded, wantIssued string
	}{
		{name: "reorder", recorded: []string{"activity:A", "activity:B"}, issued: []string{"activity:B", "activity:A"}, seq: 1, wantRecorded: "activity:A", wantIssued: "activity:B"},
		{name: "insert", recorded: []string{"activity:A"}, issued: []string{"activity:C", "activity:A"}, seq: 1, wantRecorded: "activity:A", wantIssued: "activity:C"},
		{name: "remove_middle", recorded: []string{"activity:B", "activity:A", "activity:C"}, issued: []string{"activity:B", "activity:C"}, seq: 2, wantRecorded: "activity:A", wantIssued: "activity:C"},
		{name: "remove_final_sleep", recorded: []string{"activity:A", "sleep"}, issued: []string{"activity:A"}, seq: 2, wantRecorded: "sleep"},
		{name: "explicit_change", recorded: []string{"activity:A"}, issued: []string{"activity:A"}, keys: []string{"old"}, issueKeys: []string{"new"}, seq: 1, wantRecorded: "activity:A", wantIssued: "activity:A"},
		{name: "explicit_move", recorded: []string{"activity:A", "activity:A"}, issued: []string{"activity:A"}, keys: []string{"first", "second"}, issueKeys: []string{"second"}, seq: 1, wantRecorded: "activity:A", wantIssued: "activity:A"},
		{name: "sleep_at_activity", recorded: []string{"activity:A"}, issued: []string{"sleep"}, seq: 1, wantRecorded: "activity:A", wantIssued: "sleep"},
		{name: "leftover_on_suspend", recorded: []string{"sleep", "activity:A"}, issued: []string{"sleep"}, seq: 2, wantRecorded: "activity:A"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := &historyMismatchHooks{}
			h := newTypedReplayHarness(t, factory, WithHooks(observed))
			calls := 0
			w := DefineWorkflow("s7_mismatch", func(ctx *WorkflowContext, in string) (string, error) {
				for i, command := range tc.issued {
					key := ""
					if len(tc.issueKeys) > i {
						key = tc.issueKeys[i]
					}
					if command == "sleep" {
						if err := Sleep(ctx, time.Hour, WithSleepID(key)); err != nil {
							return "", err
						}
					} else {
						a := DefineActivity(command[len("activity:"):], func(context.Context, string) (string, error) { calls++; return "live", nil })
						if _, err := a.Execute(ctx, in, WithActivityID(key)); err != nil {
							return "", err
						}
					}
				}
				return "done", nil
			})
			id := seedErrorWorkflow(t, h, w)
			for i, command := range tc.recorded {
				key := fmt.Sprintf("#%d", i+1)
				if len(tc.keys) > i {
					key = tc.keys[i]
				}
				event := storage.HistoryActivityCompleted
				data := []byte(`"recorded"`)
				if command == "sleep" {
					event = storage.HistoryTimerExpired
					data = []byte(`true`)
				}
				if tc.name == "leftover_on_suspend" && i == 0 {
					event = storage.HistoryReserved
					data = nil
				}
				_, err := fixtureRecord(h.app.ctx, h.app.storage, &storage.HistoryEvent{InstanceID: id, ActivityID: key, Command: command, EventType: event, EventData: data, ReservationData: []byte(`{"expires_at":"2099-01-01T00:00:00Z"}`), DataType: "json"})
				require.NoError(t, err)
			}
			before, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
			require.NoError(t, err)
			err = h.RunOnce(id)
			assertNondeterminism(t, h, id, err, tc.seq, tc.wantRecorded, tc.wantIssued)
			wantStarts := int(tc.seq - 1)
			require.Equal(t, wantStarts, observed.starts, "only the unchanged prefix may fire hooks")
			require.Zero(t, calls)
			assertHistoryFailure(t, h, id, ErrDeterminismViolation, "nondeterminism")
			after, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
			require.NoError(t, err)
			require.Len(t, after, len(before)+1)
			require.Equal(t, before, after[:len(before)])
		})
	}
}

func assertNondeterminism(t *testing.T, h *typedReplayHarness, id string, err error, seq int64, recorded, issued string) {
	t.Helper()
	var detail *NondeterminismError
	require.ErrorAs(t, err, &detail)
	require.ErrorIs(t, err, ErrDeterminismViolation)
	require.Equal(t, seq, detail.Seq)
	require.Equal(t, recorded, detail.Recorded)
	require.Equal(t, issued, detail.Issued)
	require.NotEmpty(t, detail.Key)
	require.NotEmpty(t, detail.Reason)
	result, getErr := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, getErr)
	var persisted *NondeterminismError
	require.ErrorAs(t, result.Error, &persisted)
	require.Equal(t, detail, persisted)
	row, getErr := h.app.storage.GetHistoryEvent(h.app.ctx, id, "$workflow_failed")
	require.NoError(t, getErr)
	var envelope errorEnvelope
	require.NoError(t, json.Unmarshal(row.EventData, &envelope))
	require.NotEmpty(t, envelope.Details)
	require.Zero(t, row.Seq)
}
