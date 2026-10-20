package selvedge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReadAPIsRejectUnavailableApp(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unstarted", true: "closed"}[closed], func(t *testing.T) {
			app := NewApp()
			if closed {
				var cleanup func()
				app, cleanup = createTestApp(t)
				t.Cleanup(cleanup)
				require.NoError(t, app.Start(context.Background()))
				require.NoError(t, app.Shutdown(context.Background()))
			}
			_, err := Query[int](context.Background(), app, "missing", "value")
			require.ErrorIs(t, err, ErrAppNotStarted)
			_, err = Describe(context.Background(), app, "missing")
			require.ErrorIs(t, err, ErrAppNotStarted)
		})
	}
}

func TestHistoryRejectsDuplicateCommandIdentity(t *testing.T) {
	for _, test := range []struct {
		name  string
		extra RecordedEvent
	}{
		{"sequence", RecordedEvent{Seq: 1, Key: "hidden", Command: "activity:hidden"}},
		{"key", RecordedEvent{Seq: 0, Key: "#1", Command: "close"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := NewApp()
			RegisterWorkflow(app, DefineWorkflow("history", func(ctx *WorkflowContext, _ string) (string, error) {
				_, err := Now(ctx)
				return "done", err
			}))
			history := &RecordedHistory{
				Format:   historyFormat,
				Instance: RecordedInstance{InstanceID: "history", WorkflowName: "history", Status: StatusCompleted, Input: json.RawMessage(`""`), Output: json.RawMessage(`"done"`)},
				Events:   []RecordedEvent{test.extra, {Seq: 1, Key: "#1", Command: "now", EventType: string(storage.HistoryNowRecorded), Data: json.RawMessage(`"2026-10-20T00:00:00Z"`), DataType: "json"}},
			}
			data, err := json.Marshal(history)
			require.NoError(t, err)
			_, err = ParseHistory(data)
			require.ErrorContains(t, err, "duplicate")
			require.ErrorContains(t, ReplayHistory(context.Background(), app, history), "duplicate")
		})
	}
}

func TestHistoryRejectsDuplicateSignalIdentity(t *testing.T) {
	app := NewApp()
	RegisterWorkflow(app, DefineWorkflow("history", func(*WorkflowContext, string) (string, error) { return "done", nil }))
	history := &RecordedHistory{
		Format:   historyFormat,
		Instance: RecordedInstance{InstanceID: "history", WorkflowName: "history", Status: StatusCompleted, Input: json.RawMessage(`""`), Output: json.RawMessage(`"done"`)},
		Signals:  []RecordedSignal{{ID: 1, Channel: "first"}, {ID: 1, Channel: "second"}},
	}
	data, err := json.Marshal(history)
	require.NoError(t, err)
	_, err = ParseHistory(data)
	require.ErrorContains(t, err, "duplicate recorded signal")
	require.ErrorContains(t, ReplayHistory(context.Background(), app, history), "duplicate recorded signal")
}

func TestReplayRejectsChangedChildCancellationTarget(t *testing.T) {
	testReplayRejectsChangedChildCancellationTarget(t, createTestApp)
}

func testReplayRejectsChangedChildCancellationTarget(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	changed := false
	w := DefineWorkflow("cancel-target", func(ctx *WorkflowContext, _ string) (string, error) {
		first, err := StartChild[string](ctx, "child", "")
		if err != nil {
			return "", err
		}
		second, err := StartChild[string](ctx, "child", "")
		if err != nil {
			return "", err
		}
		target := first
		if changed {
			target = second
		}
		if err = target.Cancel(ctx); err != nil {
			return "", err
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	history, err := ExportHistory(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.NoError(t, ReplayHistory(h.app.ctx, h.app, history))
	changed = true
	var divergence *NondeterminismError
	require.ErrorAs(t, ReplayHistory(h.app.ctx, h.app, history), &divergence)
	require.EqualValues(t, 3, divergence.Seq)
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.ErrorAs(t, h.RunOnce(id), &divergence)
}

func TestReplayRejectsChangedScopeCancellationTargets(t *testing.T) {
	testReplayRejectsChangedScopeCancellationTargets(t, createTestApp)
}

func testReplayRejectsChangedScopeCancellationTargets(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	changed := false
	w := DefineWorkflow("scope-target", func(ctx *WorkflowContext, _ string) (string, error) {
		first, cancelFirst := WithCancel(ctx)
		second, cancelSecond := WithCancel(ctx)
		if _, err := NewTimer(first, time.Hour); err != nil {
			return "", err
		}
		if _, err := NewTimer(second, time.Hour); err != nil {
			return "", err
		}
		if changed {
			cancelSecond()
		} else {
			cancelFirst()
		}
		return "", Sleep(ctx, time.Hour)
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	history, err := ExportHistory(h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.NoError(t, ReplayHistory(h.app.ctx, h.app, history))
	changed = true
	var divergence *NondeterminismError
	require.ErrorAs(t, ReplayHistory(h.app.ctx, h.app, history), &divergence)
	require.EqualValues(t, 3, divergence.Seq)
	require.NoError(t, h.app.storage.MarkRunnable(h.app.ctx, id))
	require.ErrorAs(t, h.RunOnce(id), &divergence)
}
