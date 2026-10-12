package selvedge

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

type replayAppFactory func(*testing.T, ...Option) (*App, func())

// typedReplayHarness drives dispatch and timer delivery without background loops.
// It uses the existing backend factories without changing the shared schema helpers.
type typedReplayHarness struct {
	app        *App
	storeClock bool
}

func newTypedReplayHarness(t *testing.T, factory replayAppFactory, opts ...Option) *typedReplayHarness {
	t.Helper()
	listenNotify := false
	opts = append(opts, WithListenNotify(&listenNotify))
	app, cleanup := factory(t, opts...)
	t.Cleanup(cleanup)
	app.ctx, app.cancel = context.WithCancel(context.Background())
	require.NoError(t, app.initStorage())
	t.Cleanup(func() {
		app.cancel()
		require.NoError(t, app.storage.Close())
	})
	return &typedReplayHarness{app: app}
}

func (h *typedReplayHarness) RunOnce(instanceID string) error {
	return h.app.resumeWorkflow(h.app.ctx, instanceID)
}

func (h *typedReplayHarness) forceFire(t *testing.T, instanceID string) {
	t.Helper()
	require.NoError(t, h.app.storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: instanceID, TimerID: "pause"}))
}

func addTypedReplayWorkflow[O any](t *testing.T, h *typedReplayHarness, activity *Activity[string, O], values *[]O) string {
	t.Helper()
	workflow := DefineWorkflow("typed_replay", func(ctx *WorkflowContext, input string) (O, error) {
		value, err := activity.Execute(ctx, input, WithActivityID("result"))
		if err != nil {
			var zero O
			return zero, err
		}
		*values = append(*values, value)
		return value, Sleep(ctx, time.Hour, WithSleepID("pause"))
	})
	RegisterWorkflow(h.app, workflow)
	const instanceID = "typed-replay-instance"
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{
		InstanceID: instanceID, WorkflowName: workflow.Name(), Status: storage.StatusRunning, InputData: []byte(`"input"`),
	}))
	return instanceID
}

func assertTypedReplay[O any](t *testing.T, factory replayAppFactory, live, expected O) {
	t.Helper()
	observed := &typedReplayHooks{}
	h := newTypedReplayHarness(t, factory, WithHooks(observed))
	calls := 0
	activity := DefineActivity("typed", func(context.Context, string) (O, error) {
		calls++
		return live, nil
	})
	var values []O
	instanceID := addTypedReplayWorkflow(t, h, activity, &values)
	require.NoError(t, h.RunOnce(instanceID))
	require.Equal(t, []O{expected}, values, "first execution must use recorded JSON")
	require.NoError(t, h.RunOnce(instanceID))
	h.forceFire(t, instanceID)
	require.NoError(t, h.RunOnce(instanceID))
	require.Equal(t, []O{expected, expected, expected}, values)
	require.Equal(t, []any{expected, expected}, observed.cached, "cache-hit hooks must receive the decoded output type")
	require.Equal(t, 1, calls)
	instance, err := h.app.storage.GetInstance(h.app.ctx, instanceID)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, instance.Status)
}

func TestTypedReplayStructResult(t *testing.T) { testTypedReplayStructResult(t, createTestApp) }

func testTypedReplayStructResult(t *testing.T, factory replayAppFactory) {
	t.Helper()
	value := struct {
		A int64
		B []string
		C json.RawMessage
		D map[string]any
	}{A: 42, B: []string{"one", "two"}, C: json.RawMessage(`{"ok":true}`), D: map[string]any{"number": float64(3), "nested": []any{"x", true}}}
	assertTypedReplay(t, factory, value, value)
}

func TestTypedReplayInt64Precision(t *testing.T) { testTypedReplayInt64Precision(t, createTestApp) }

func testTypedReplayInt64Precision(t *testing.T, factory replayAppFactory) {
	t.Helper()
	assertTypedReplay(t, factory, int64(9007199254740993), int64(9007199254740993))
}

type customReplayValue struct {
	Value     string
	Transient string `json:"-"`
}

func (v customReplayValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct{ Value string }{Value: "stored:" + v.Value})
}

func TestLiveValueEqualsRecordedValue(t *testing.T) {
	testLiveValueEqualsRecordedValue(t, createTestApp)
}

func testLiveValueEqualsRecordedValue(t *testing.T, factory replayAppFactory) {
	t.Helper()
	assertTypedReplay(t, factory, customReplayValue{Value: "live", Transient: "discarded"}, customReplayValue{Value: "stored:live"})
}

func TestReplayDecodeErrorFailsRun(t *testing.T) { testReplayDecodeErrorFailsRun(t, createTestApp) }

func testReplayDecodeErrorFailsRun(t *testing.T, factory replayAppFactory) {
	t.Helper()
	for _, tc := range []struct{ name, query string }{
		{name: "incompatible_type", query: `UPDATE workflow_history SET event_data = '"corrupt"' WHERE activity_id = 'result'`},
		{name: "malformed_json", query: `UPDATE workflow_history SET event_data = '{' WHERE activity_id = 'result'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			calls := 0
			activity := DefineActivity("typed", func(context.Context, string) (int64, error) {
				calls++
				return 42, nil
			}, WithRetryPolicy[string, int64](retry.Fixed(3, time.Millisecond)))
			var values []int64
			instanceID := addTypedReplayWorkflow(t, h, activity, &values)
			require.NoError(t, h.RunOnce(instanceID))
			_, err := h.app.storage.DB().ExecContext(h.app.ctx, tc.query)
			require.NoError(t, err)
			h.forceFire(t, instanceID)
			err = h.RunOnce(instanceID)
			var decodeErr *ReplayDecodeError
			require.ErrorAs(t, err, &decodeErr)
			require.Equal(t, instanceID, decodeErr.InstanceID)
			require.Equal(t, "result", decodeErr.Key)
			require.Equal(t, "int64", decodeErr.GoType)
			require.NotNil(t, decodeErr.Unwrap())
			require.Equal(t, 1, calls, "decode failures must not execute or retry the activity")
			instance, getErr := h.app.storage.GetInstance(h.app.ctx, instanceID)
			require.NoError(t, getErr)
			require.Equal(t, storage.StatusFailed, instance.Status)
		})
	}
}

type typedReplayHooks struct {
	hooks.NoOpHooks
	cached []any
}

func (h *typedReplayHooks) OnActivityCacheHit(_ context.Context, info hooks.ActivityCacheHitInfo) {
	h.cached = append(h.cached, info.CachedResult)
}

func TestRecordedActivityNotRescheduledOnReplay(t *testing.T) {
	testRecordedActivityNotRescheduledOnReplay(t, createTestApp)
}

func testRecordedActivityNotRescheduledOnReplay(t *testing.T, factory replayAppFactory) {
	t.Helper()
	observed := &typedReplayHooks{}
	h := newTypedReplayHarness(t, factory, WithHooks(observed))
	calls := 0
	activity := DefineActivity("once", func(ctx context.Context, _ string) (string, error) {
		wc := GetWorkflowContext(ctx)
		row, err := h.app.storage.GetHistoryEvent(ctx, wc.InstanceID(), "result")
		require.NoError(t, err)
		require.NotNil(t, row, "activity must be reserved before its side effect")
		require.Nil(t, row.EventData)
		calls++
		return "recorded", nil
	})
	var values []string
	instanceID := addTypedReplayWorkflow(t, h, activity, &values)
	require.NoError(t, h.RunOnce(instanceID))
	require.NoError(t, h.RunOnce(instanceID))
	h.forceFire(t, instanceID)
	require.NoError(t, h.RunOnce(instanceID))
	require.Equal(t, 1, calls)
	require.Equal(t, []any{"recorded", "recorded"}, observed.cached)
}

// Old cache fixtures now seed real history rows and load them through the engine.
func workflowContextWithRecordedHistory(t *testing.T, history map[string]any, run func(*WorkflowContext)) {
	t.Helper()
	h := newTypedReplayHarness(t, createTestApp)
	const instanceID = "inst-1"
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: instanceID, WorkflowName: "fixture", Status: storage.StatusRunning}))
	keys := make([]string, 0, len(history))
	for key := range history {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		value := history[key]
		eventType := storage.HistoryActivityCompleted
		payload, err := json.Marshal(value)
		require.NoError(t, err)
		if failure, ok := value.(error); ok {
			eventType = storage.HistoryActivityFailed
			payload = encodeError(failure)
		}
		_, err = fixtureRecord(h.app.ctx, h.app.storage, &storage.HistoryEvent{Seq: int64(i + 1), InstanceID: instanceID, ActivityID: fmt.Sprintf("#%d", i+1), Command: fixtureCommand(key), EventType: eventType, EventData: payload, DataType: "json"})
		require.NoError(t, err)
	}
	require.NoError(t, h.app.engine.Run(h.app.ctx, instanceID, func(ec *replay.ExecutionContext) (any, error) {
		run(NewWorkflowContextFromExecution(ec))
		return nil, nil
	}))
}

type undecodableReplayValue struct{ Value string }

func (undecodableReplayValue) MarshalJSON() ([]byte, error) {
	return []byte(`"incompatible"`), nil
}

func TestLiveDecodeErrorFailsRunEvenWhenCaught(t *testing.T) {
	testLiveDecodeErrorFailsRunEvenWhenCaught(t, createTestApp)
}

func testLiveDecodeErrorFailsRunEvenWhenCaught(t *testing.T, factory replayAppFactory) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	calls := 0
	activity := DefineActivity("undecodable", func(context.Context, string) (undecodableReplayValue, error) {
		calls++
		return undecodableReplayValue{Value: "live"}, nil
	},
		WithRetryPolicy[string, undecodableReplayValue](retry.Fixed(3, time.Millisecond)))
	workflow := DefineWorkflow("caught_decode", func(ctx *WorkflowContext, input string) (string, error) {
		_, err := activity.Execute(ctx, input, WithActivityID("result"))
		var decodeErr *ReplayDecodeError
		require.ErrorAs(t, err, &decodeErr)
		return "caught", nil
	})
	RegisterWorkflow(h.app, workflow)
	const instanceID = "caught-decode-instance"
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{
		InstanceID: instanceID, WorkflowName: workflow.Name(), Status: storage.StatusRunning, InputData: []byte(`"input"`),
	}))
	err := h.RunOnce(instanceID)
	var decodeErr *ReplayDecodeError
	require.ErrorAs(t, err, &decodeErr, "catching a decode error must not turn the run into a success")
	require.Equal(t, instanceID, decodeErr.InstanceID)
	require.Equal(t, "result", decodeErr.Key)
	require.Equal(t, "selvedge.undecodableReplayValue", decodeErr.GoType)
	instance, getErr := h.app.storage.GetInstance(h.app.ctx, instanceID)
	require.NoError(t, getErr)
	require.Equal(t, storage.StatusFailed, instance.Status)
	// The success row must remain durable even when decoding on first execution fails.
	err = h.RunOnce(instanceID)
	require.NoError(t, err) // Terminal instances are no longer dispatched.
	require.Equal(t, 1, calls)
}

func requireRecordedHistory(t *testing.T, s storage.Storage, ctx context.Context, event *storage.HistoryEvent) {
	t.Helper()
	_, err := fixtureRecord(ctx, s, event)
	require.NoError(t, err)
}

func fixtureCommand(key string) string {
	if strings.HasPrefix(key, "receive:") {
		return key[:strings.LastIndex(key, ":")]
	}
	return "activity:" + strings.Split(key, ":")[0]
}
