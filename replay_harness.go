package selvedge

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"slices"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// FindInstancesOptions filters and pages instances for App.FindInstancesWithOptions.
type FindInstancesOptions = storage.ListInstancesOptions

// RecordedHistory is a portable snapshot of the inputs needed to replay one instance.
type RecordedHistory struct {
	Format   string           `json:"format"`
	Instance RecordedInstance `json:"instance"`
	Events   []RecordedEvent  `json:"events"`
	Signals  []RecordedSignal `json:"signals"`
}

// RecordedInstance contains durable workflow metadata without execution ownership.
type RecordedInstance struct {
	InstanceID       string          `json:"instance_id"`
	WorkflowName     string          `json:"workflow_name"`
	Status           WorkflowStatus  `json:"status"`
	Input            json.RawMessage `json:"input,omitempty"`
	Output           json.RawMessage `json:"output,omitempty"`
	CancelRequested  bool            `json:"cancel_requested"`
	CancelReason     string          `json:"cancel_reason"`
	ParentInstanceID string          `json:"parent_instance_id"`
	ParentKey        string          `json:"parent_key"`
}

// RecordedEvent contains one command or workflow-close history row.
type RecordedEvent struct {
	Seq          int64           `json:"seq"`
	Key          string          `json:"key"`
	Command      string          `json:"command"`
	EventType    string          `json:"event_type"`
	Data         json.RawMessage `json:"data,omitempty"`
	DataType     string          `json:"data_type"`
	Binary       []byte          `json:"binary"`
	Reservation  json.RawMessage `json:"reservation,omitempty"`
	DeliveredSeq int64           `json:"delivered_seq"`
}

// RecordedSignal preserves delivery and consumption positions without its payload.
type RecordedSignal struct {
	ID           int64  `json:"id"`
	Channel      string `json:"channel"`
	DeliveredSeq int64  `json:"delivered_seq"`
	ConsumedSeq  int64  `json:"consumed_seq"`
}

// ErrUnsupportedHistoryFormat rejects unknown history formats or versions.
var ErrUnsupportedHistoryFormat = errors.New("unsupported history format")

// ReplayOutcomeError reports a workflow close that differs from its recorded outcome.
type ReplayOutcomeError struct {
	InstanceID                     string
	RecordedStatus, ReplayedStatus WorkflowStatus
	Recorded, Replayed             json.RawMessage
}

func (e *ReplayOutcomeError) Error() string {
	return fmt.Sprintf("workflow outcome changed for %s (%s to %s)", e.InstanceID, e.RecordedStatus, e.ReplayedStatus)
}

// ReplayError preserves both the replay divergence and the live workflow error.
type ReplayError struct {
	InstanceID  string
	Divergence  error
	WorkflowErr error
}

func (e *ReplayError) Error() string {
	s := fmt.Sprintf("replay of %s diverged: %v", e.InstanceID, e.Divergence)
	if e.WorkflowErr != nil {
		s += "; workflow returned: " + e.WorkflowErr.Error()
	}
	return s
}

// Unwrap exposes the divergence and original Go error for errors.Is and errors.As.
func (e *ReplayError) Unwrap() []error {
	causes := make([]error, 0, 2)
	if e.Divergence != nil {
		causes = append(causes, e.Divergence)
	}
	if e.WorkflowErr != nil {
		causes = append(causes, e.WorkflowErr)
	}
	return causes
}

// ExportHistory reads one consistent snapshot from an initialized App.
func ExportHistory(ctx context.Context, app *App, id string) (*RecordedHistory, error) {
	store := app.Storage()
	if app.closed.Load() || store == nil {
		return nil, ErrAppNotStarted
	}
	snapshot, err := store.ReadSnapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, &WorkflowNotFoundError{InstanceID: id}
	}
	inst := snapshot.Instance
	history := &RecordedHistory{Format: historyFormat, Instance: RecordedInstance{InstanceID: inst.InstanceID, WorkflowName: inst.WorkflowName, Status: inst.Status, Input: slices.Clone(inst.InputData), Output: slices.Clone(inst.OutputData), CancelRequested: snapshot.CancelRequested, CancelReason: inst.CancelReason, ParentInstanceID: inst.ParentInstanceID, ParentKey: inst.ParentKey}, Events: make([]RecordedEvent, 0, len(snapshot.History)), Signals: make([]RecordedSignal, 0, len(snapshot.Signals))}
	for _, row := range snapshot.History {
		history.Events = append(history.Events, RecordedEvent{Seq: row.Seq, Key: row.ActivityID, Command: row.Command, EventType: string(row.EventType), Data: slices.Clone(row.EventData), DataType: row.DataType, Binary: slices.Clone(row.EventDataBinary), Reservation: slices.Clone(row.ReservationData), DeliveredSeq: row.DeliveredSeq})
	}
	for _, signal := range snapshot.Signals {
		history.Signals = append(history.Signals, RecordedSignal{signal.ID, signal.Channel, signal.DeliveredSeq, signal.ConsumedSeq})
	}
	slices.SortFunc(history.Events, func(a, b RecordedEvent) int {
		if order := cmp.Compare(a.Seq, b.Seq); order != 0 {
			return order
		}
		return cmp.Compare(a.Key, b.Key)
	})
	slices.SortFunc(history.Signals, func(a, b RecordedSignal) int { return cmp.Compare(a.ID, b.ID) })
	return history, nil
}

// ParseHistory strictly decodes the portable Selvedge history format.
func ParseHistory(data []byte) (*RecordedHistory, error) {
	// Read the discriminator before applying the supported version's field schema.
	var header struct {
		Format string `json:"format"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, err
	}
	if header.Format != historyFormat {
		return nil, ErrUnsupportedHistoryFormat
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var history RecordedHistory
	if err := decoder.Decode(&history); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("history contains trailing JSON")
	}
	if err := validateHistory(&history); err != nil {
		return nil, err
	}
	return &history, nil
}

// ReplayHistory checks registered code against a history without live storage or activities.
func ReplayHistory(ctx context.Context, app *App, history *RecordedHistory) error {
	if err := validateHistory(history); err != nil {
		return err
	}
	app.workflowRunnersMu.RLock()
	runner, registered := app.workflowRunners[history.Instance.WorkflowName]
	app.workflowRunnersMu.RUnlock()
	if !registered {
		return &WorkflowNotRegisteredError{WorkflowName: history.Instance.WorkflowName}
	}
	execution, divergence := new(replay.Engine).Replay(ctx, history.Instance.InstanceID, runner, historySnapshot(history), replay.VerifyPurpose)
	var workflowErr error
	if execution != nil {
		defer execution.Close()
		workflowErr = execution.WorkflowErr
		if divergence == nil && history.Instance.Status != StatusRunning {
			if execution.Stopped {
				divergence = &NondeterminismError{Seq: execution.Position, Reason: "replay needs input not in the recorded history"}
			} else {
				divergence = checkReplayOutcome(history, execution.Result, workflowErr)
			}
		}
	}
	if divergence != nil {
		return &ReplayError{InstanceID: history.Instance.InstanceID, Divergence: divergence, WorkflowErr: workflowErr}
	}
	return nil
}

// ReplayInstance exports one instance and checks it against the App's registered workflow.
func ReplayInstance(ctx context.Context, app *App, id string) error {
	history, err := ExportHistory(ctx, app, id)
	if err != nil {
		return err
	}
	return ReplayHistory(ctx, app, history)
}

const historyFormat = "selvedge.history/v1"

func validateHistory(history *RecordedHistory) error {
	if history == nil || history.Format != historyFormat {
		return ErrUnsupportedHistoryFormat
	}
	if history.Instance.InstanceID == "" || history.Instance.WorkflowName == "" {
		return errors.New("history requires instance and workflow names")
	}
	switch history.Instance.Status {
	case StatusRunning, StatusCompleted, StatusFailed, StatusCancelled:
	default:
		return fmt.Errorf("invalid recorded workflow status %q", history.Instance.Status)
	}
	// Replay indexes commands by both position and key. Reject collisions before
	// building those indexes, otherwise one entry silently hides another.
	keys := make(map[string]struct{}, len(history.Events))
	positions := make(map[int64]struct{}, len(history.Events))
	for i := range history.Events {
		event := &history.Events[i]
		if _, duplicate := keys[event.Key]; duplicate {
			return fmt.Errorf("duplicate recorded history key %q", event.Key)
		}
		keys[event.Key] = struct{}{}
		if event.Seq > 0 {
			if _, duplicate := positions[event.Seq]; duplicate {
				return fmt.Errorf("duplicate recorded command position %d", event.Seq)
			}
			positions[event.Seq] = struct{}{}
		}
	}
	signals := make(map[int64]struct{}, len(history.Signals))
	for _, signal := range history.Signals {
		if _, duplicate := signals[signal.ID]; duplicate {
			return fmt.Errorf("duplicate recorded signal ID %d", signal.ID)
		}
		signals[signal.ID] = struct{}{}
	}
	return nil
}
func historySnapshot(h *RecordedHistory) *storage.InstanceSnapshot {
	inst := h.Instance
	snapshot := &storage.InstanceSnapshot{Instance: &storage.WorkflowInstance{InstanceID: inst.InstanceID, WorkflowName: inst.WorkflowName, Status: inst.Status, InputData: slices.Clone(inst.Input), OutputData: slices.Clone(inst.Output), CancelReason: inst.CancelReason, ParentInstanceID: inst.ParentInstanceID, ParentKey: inst.ParentKey}, CancelRequested: inst.CancelRequested}
	for i := range h.Events {
		row := &h.Events[i]
		snapshot.History = append(snapshot.History, &storage.HistoryEvent{ID: int64(i + 1), InstanceID: inst.InstanceID, ActivityID: row.Key, Seq: row.Seq, Command: row.Command, EventType: storage.HistoryEventType(row.EventType), EventData: slices.Clone(row.Data), EventDataBinary: slices.Clone(row.Binary), DataType: row.DataType, ReservationData: slices.Clone(row.Reservation), DeliveredSeq: row.DeliveredSeq})
	}
	for _, signal := range h.Signals {
		snapshot.Signals = append(snapshot.Signals, storage.SignalPosition{ID: signal.ID, Channel: signal.Channel, DeliveredSeq: signal.DeliveredSeq, ConsumedSeq: signal.ConsumedSeq})
	}
	return snapshot
}

func checkReplayOutcome(history *RecordedHistory, result any, workflowErr error) error {
	status := StatusCompleted
	var data json.RawMessage
	if workflowErr == nil {
		encoded, err := json.Marshal(result)
		if err != nil {
			workflowErr = fmt.Errorf("failed to serialize result: %w", err)
		} else {
			data = encoded
		}
	}
	if workflowErr != nil {
		status = StatusFailed
		if errors.Is(workflowErr, context.Canceled) && history.Instance.CancelRequested {
			status = StatusCancelled
		}
		data = encodeError(workflowErr)
	}
	recorded := history.Instance.Output
	matched := false
	switch history.Instance.Status {
	case StatusCompleted:
		matched = status == StatusCompleted && replayJSONEqual(recorded, data)
	case StatusFailed:
		for i := range history.Events {
			event := &history.Events[i]
			if event.Key == "$workflow_failed" {
				recorded = event.Data
				break
			}
		}
		var original, replayed errorEnvelope
		matched = status == StatusFailed && json.Unmarshal(recorded, &original) == nil && json.Unmarshal(data, &replayed) == nil && original.Kind == replayed.Kind && original.Message == replayed.Message
	case StatusCancelled:
		matched = status == StatusCancelled
		for i := range history.Events {
			event := &history.Events[i]
			if event.Key == "$workflow_failed" {
				recorded = event.Data
				break
			}
		}
	}
	if matched {
		return nil
	}
	return &ReplayOutcomeError{InstanceID: history.Instance.InstanceID, RecordedStatus: history.Instance.Status, ReplayedStatus: status, Recorded: slices.Clone(recorded), Replayed: slices.Clone(data)}
}
func replayJSONEqual(a, b []byte) bool {
	if !json.Valid(a) || !json.Valid(b) {
		return false
	}
	var left, right any
	decode := func(data []byte, value *any) error {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		return decoder.Decode(value)
	}
	return decode(a, &left) == nil && decode(b, &right) == nil && replayValuesEqual(left, right)
}

// Compare numbers exactly, while treating equivalent JSON numeric spellings alike.
func replayValuesEqual(left, right any) bool {
	switch a := left.(type) {
	case json.Number:
		b, ok := right.(json.Number)
		if !ok {
			return false
		}
		ar, aok := new(big.Rat).SetString(string(a))
		br, bok := new(big.Rat).SetString(string(b))
		return aok && bok && ar.Cmp(br) == 0
	case []any:
		b, ok := right.([]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for i := range a {
			if !replayValuesEqual(a[i], b[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		b, ok := right.(map[string]any)
		if !ok || len(a) != len(b) {
			return false
		}
		for key, value := range a {
			other, exists := b[key]
			if !exists || !replayValuesEqual(value, other) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}
