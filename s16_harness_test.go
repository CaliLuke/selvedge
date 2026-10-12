package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestHistoryExportRoundTrip(t *testing.T) { s16TestHistoryExportRoundTrip(t, createTestApp) }
func s16TestHistoryExportRoundTrip(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	a := DefineActivity("export", func(context.Context, string) (string, error) { return "value", nil })
	w := DefineWorkflow("export", func(ctx *WorkflowContext, in string) (string, error) { return a.Execute(ctx, in) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	history, err := ExportHistory(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.NotNil(t, history)
	require.Equal(t, "selvedge.history/v1", history.Format)
	require.Equal(t, StatusCompleted, history.Instance.Status)
	require.Equal(t, json.RawMessage(`"value"`), history.Instance.Output)
	require.Len(t, history.Events, 1)
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, json.RawMessage(row.EventData), history.Events[0].Data)
	data, err := json.Marshal(history)
	require.NoError(t, err)
	parsed, err := ParseHistory(data)
	require.NoError(t, err)
	again, err := json.Marshal(parsed)
	require.NoError(t, err)
	require.Equal(t, data, again)
	for _, invalid := range []string{strings.Replace(string(data), "selvedge.history/v1", "other/v1", 1), strings.Replace(string(data), "selvedge.history/v1", "selvedge.history/v2", 1)} {
		_, err = ParseHistory([]byte(invalid))
		require.ErrorIs(t, err, ErrUnsupportedHistoryFormat)
	}
	for _, invalid := range []string{strings.Replace(string(data), `"format":`, `"unknown":1,"format":`, 1), strings.Replace(string(data), `"instance_id":`, `"unknown":1,"instance_id":`, 1), string(data) + ` {}`} {
		_, err = ParseHistory([]byte(invalid))
		require.Error(t, err)
	}
	_, err = ExportHistory(context.Background(), NewApp(), id)
	require.ErrorIs(t, err, ErrAppNotStarted)
	_, err = ExportHistory(h.app.ctx, h.app, "missing")
	var missing *WorkflowNotFoundError
	require.ErrorAs(t, err, &missing)
}
func TestReplayHarnessPreservesWorkflowError(t *testing.T) {
	s16TestReplayHarnessPreservesWorkflowError(t, createTestApp)
}
func s16TestReplayHarnessPreservesWorkflowError(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	original := DefineWorkflow("sentinel", func(ctx *WorkflowContext, _ string) (string, error) { _, err := Now(ctx); return "ok", err })
	id := seedErrorWorkflow(t, h, original)
	require.NoError(t, h.RunOnce(id))
	history, err := ExportHistory(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.NotNil(t, history)
	sentinel := errors.New("workflow protocol rejected")
	app := NewApp()
	RegisterWorkflow(app, DefineWorkflow("sentinel", func(*WorkflowContext, string) (string, error) { return "", fmt.Errorf("entry: %w", sentinel) }))
	err = ReplayHistory(context.Background(), app, history)
	require.ErrorIs(t, err, sentinel)
	var divergence *NondeterminismError
	require.ErrorAs(t, err, &divergence)
	var replayErr *ReplayError
	require.ErrorAs(t, err, &replayErr)
	require.ErrorIs(t, replayErr.WorkflowErr, sentinel)
	require.Contains(t, err.Error(), "workflow returned: entry: workflow protocol rejected")
}
func TestReplayHarnessRunningInstance(t *testing.T) {
	s16TestReplayHarnessRunningInstance(t, createTestApp)
}
func s16TestReplayHarnessRunningInstance(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	w := DefineWorkflow("frontier", func(ctx *WorkflowContext, _ string) (string, error) { return "", Sleep(ctx, time.Hour) })
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	history, err := ExportHistory(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.NotNil(t, history)
	require.NoError(t, ReplayHistory(context.Background(), h.app, history))
	history.Instance.Status = StatusCompleted
	history.Instance.Output = json.RawMessage(`""`)
	err = ReplayHistory(context.Background(), h.app, history)
	var nd *NondeterminismError
	require.ErrorAs(t, err, &nd)
	require.Equal(t, "replay needs input not in the recorded history", nd.Reason)
	missing := NewApp()
	var unregistered *WorkflowNotRegisteredError
	require.ErrorAs(t, ReplayHistory(context.Background(), missing, history), &unregistered)
}
func TestReplayHarnessOutcomeMismatch(t *testing.T) {
	s16TestReplayHarnessOutcomeMismatch(t, createTestApp)
}
func s16TestReplayHarnessOutcomeMismatch(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"output", "failure_message", "completed_to_failed", "failed_to_completed", "failure_kind", "cancelled_to_completed", "cancellation_flag"} {
		t.Run(kind, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			changed := false
			w := DefineWorkflow("outcome", func(*WorkflowContext, string) (string, error) {
				switch kind {
				case "output":
					if changed {
						return "new", nil
					}
					return "old", nil
				case "failure_kind":
					if changed {
						return "", errors.New("same")
					}
					return "", NewApplicationError("same", "Validation", nil, nil)
				case "cancelled_to_completed", "cancellation_flag":
					if changed && kind == "cancelled_to_completed" {
						return "ok", nil
					}
					return "", context.Canceled
				case "failure_message":
					if changed {
						return "", errors.New("new")
					}
					return "", errors.New("old")
				case "completed_to_failed":
					if changed {
						return "", errors.New("new")
					}
					return "ok", nil
				default:
					if changed {
						return "ok", nil
					}
					return "", errors.New("old")
				}
			})
			id := seedErrorWorkflow(t, h, w)
			if kind == "cancelled_to_completed" || kind == "cancellation_flag" {
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			}
			runErr := h.RunOnce(id)
			if kind == "failure_message" || kind == "failed_to_completed" || kind == "failure_kind" || kind == "cancelled_to_completed" || kind == "cancellation_flag" {
				require.Error(t, runErr)
			} else {
				require.NoError(t, runErr)
			}
			history, err := ExportHistory(h.app.ctx, h.app, id)
			require.NoError(t, err)
			require.NotNil(t, history)
			require.NoError(t, ReplayHistory(context.Background(), h.app, history))
			changed = true
			if kind == "cancellation_flag" {
				history.Instance.CancelRequested = false
			}
			var mismatch *ReplayOutcomeError
			require.ErrorAs(t, ReplayHistory(context.Background(), h.app, history), &mismatch)
			require.Equal(t, id, mismatch.InstanceID)
		})
	}
}
func TestReplayHarnessRejectsClosedFrontier(t *testing.T) {
	app := NewApp()
	RegisterWorkflow(app, DefineWorkflow("empty", func(ctx *WorkflowContext, _ string) (string, error) { return "", Sleep(ctx, time.Hour) }))
	history := &RecordedHistory{Format: "selvedge.history/v1", Instance: RecordedInstance{InstanceID: "x", WorkflowName: "empty", Status: StatusCompleted, Input: json.RawMessage(`""`), Output: json.RawMessage(`""`)}}
	var nd *NondeterminismError
	require.ErrorAs(t, ReplayHistory(context.Background(), app, history), &nd)
}

func TestReplayHarnessJSONValues(t *testing.T) {
	for _, tc := range []struct {
		name, recorded, replayed string
		equal                    bool
	}{
		{"large_distinct_integers", `9007199254740992`, `9007199254740993`, false},
		{"equivalent_numbers", `{"n":[1,2.50,1e3]}`, `{"n":[1.0,2.5,1000]}`, true},
		{"object_order", `{"a":true,"b":null}`, `{"b":null,"a":true}`, true},
		{"invalid_suffix", `1 garbage`, `1`, false},
		{"array_order", `[1,2]`, `[2,1]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := NewApp()
			RegisterWorkflow(app, DefineWorkflow("json", func(*WorkflowContext, string) (json.RawMessage, error) { return json.RawMessage(tc.replayed), nil }))
			h := &RecordedHistory{Format: historyFormat, Instance: RecordedInstance{InstanceID: "json", WorkflowName: "json", Status: StatusCompleted, Input: json.RawMessage(`""`), Output: json.RawMessage(tc.recorded)}}
			err := ReplayHistory(context.Background(), app, h)
			if tc.equal {
				require.NoError(t, err)
			} else {
				var outcome *ReplayOutcomeError
				require.ErrorAs(t, err, &outcome)
			}
		})
	}
}
func TestReplayHarnessErrorUnwrap(t *testing.T) {
	a, b := errors.New("divergence"), errors.New("workflow")
	for _, tc := range []struct {
		divergence, workflow error
		count                int
	}{{a, b, 2}, {a, nil, 1}, {nil, b, 1}, {nil, nil, 0}} {
		err := &ReplayError{Divergence: tc.divergence, WorkflowErr: tc.workflow}
		require.Len(t, err.Unwrap(), tc.count)
		for _, cause := range err.Unwrap() {
			require.NotNil(t, cause)
		}
	}
}

func TestHistoryExportPreservesMissingValues(t *testing.T) {
	s16TestHistoryExportPreservesMissingValues(t, createTestApp)
}
func s16TestHistoryExportPreservesMissingValues(t *testing.T, factory replayAppFactory) {
	for _, filled := range []bool{false, true} {
		t.Run(fmt.Sprint(filled), func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			reached := 0
			a := DefineActivity("nullable", func(context.Context, string) (*string, error) { panic("replay activity called") })
			w := DefineWorkflow("nullable", func(ctx *WorkflowContext, _ string) (string, error) {
				_, err := a.Execute(ctx, "")
				reached++
				return "done", err
			})
			id := seedErrorWorkflow(t, h, w)
			row := &storage.HistoryEvent{InstanceID: id, ActivityID: "#1", Command: "activity:nullable", EventType: storage.HistoryReserved, DataType: "json"}
			if filled {
				row.EventType = storage.HistoryActivityCompleted
				row.EventData = []byte(`null`)
			}
			_, err := fixtureRecord(h.app.ctx, h.app.storage, row)
			require.NoError(t, err)
			exported, err := ExportHistory(h.app.ctx, h.app, id)
			require.NoError(t, err)
			encoded, err := json.Marshal(exported)
			require.NoError(t, err)
			parsed, err := ParseHistory(encoded)
			require.NoError(t, err)
			require.Equal(t, exported.Events[0].Data, parsed.Events[0].Data)
			require.Equal(t, exported.Events[0].Reservation, parsed.Events[0].Reservation)
			require.NoError(t, ReplayHistory(context.Background(), h.app, parsed))
			if filled {
				require.Equal(t, 1, reached)
			} else {
				require.Zero(t, reached)
			}
		})
	}
}

func TestHistoryExportRetainsEventFields(t *testing.T) {
	s16TestHistoryExportRetainsEventFields(t, createTestApp)
}
func s16TestHistoryExportRetainsEventFields(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	id := seedErrorWorkflow(t, h, DefineWorkflow("metadata", func(*WorkflowContext, string) (string, error) { return "", nil }))
	for _, row := range []*storage.HistoryEvent{
		{InstanceID: id, Seq: 2, ActivityID: "later", Command: "receive:b", EventType: storage.HistorySignalReceived, EventData: []byte(`{"n":7}`), EventDataBinary: []byte{0, 255}, DataType: "json", ReservationData: []byte(`{"reserved":true}`), DeliveredSeq: 2},
		{InstanceID: id, Seq: 1, ActivityID: "first", Command: "now", EventType: storage.HistoryNowRecorded, EventData: []byte(`"2026-10-12T00:00:00Z"`), DataType: "json", DeliveredSeq: 1},
	} {
		_, err := fixtureRecord(h.app.ctx, h.app.storage, row)
		require.NoError(t, err)
	}
	_, err := h.app.storage.RequestCancel(h.app.ctx, id, "recorded reason")
	require.NoError(t, err)
	exported, err := ExportHistory(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.True(t, exported.Instance.CancelRequested)
	require.Equal(t, "recorded reason", exported.Instance.CancelReason)
	require.Equal(t, []RecordedEvent{
		{Seq: 1, Key: "first", Command: "now", EventType: string(storage.HistoryNowRecorded), Data: json.RawMessage(`"2026-10-12T00:00:00Z"`), DataType: "json", DeliveredSeq: 1},
		{Seq: 2, Key: "later", Command: "receive:b", EventType: string(storage.HistorySignalReceived), Data: json.RawMessage(`{"n":7}`), Binary: []byte{0, 255}, DataType: "json", Reservation: json.RawMessage(`{"reserved":true}`), DeliveredSeq: 2},
	}, exported.Events)
	data, err := json.Marshal(exported)
	require.NoError(t, err)
	parsed, err := ParseHistory(data)
	require.NoError(t, err)
	require.Equal(t, exported, parsed)
	for _, forbidden := range []string{"lease_owner", "lease_epoch", "runnable", "created_at", "started_at", "worker_id"} {
		require.NotContains(t, string(data), forbidden)
	}
	_, err = ParseHistory([]byte(strings.Replace(string(data), `"seq":`, `"unknown":1,"seq":`, 1)))
	require.Error(t, err)
}
