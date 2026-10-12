package selvedge

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func TestLeftoverHistoryGapFailsWithoutPanic(t *testing.T) {
	testLeftoverHistoryGapFailsWithoutPanic(t, createTestApp)
}
func testLeftoverHistoryGapFailsWithoutPanic(t *testing.T, factory replayAppFactory) {
	for _, tc := range []struct {
		name      string
		positions []int64
	}{
		{"only_second_command", []int64{2}},
		{"smallest_of_sparse_commands", []int64{9, 2, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			w := DefineWorkflow("gap", func(*WorkflowContext, string) (string, error) { return "done", nil })
			id := seedErrorWorkflow(t, h, w)
			for _, seq := range tc.positions {
				_, err := fixtureRecord(h.app.ctx, h.app.storage, &storage.HistoryEvent{Seq: seq, InstanceID: id, ActivityID: fmt.Sprintf("#%d", seq), Command: "activity:A", EventType: storage.HistoryActivityCompleted, EventData: []byte(`"recorded"`), DataType: "json"})
				require.NoError(t, err)
			}
			var runErr error
			require.NotPanics(t, func() { runErr = h.RunOnce(id) })
			assertNondeterminism(t, h, id, runErr, 2, "activity:A", "")
			var detail *NondeterminismError
			require.ErrorAs(t, runErr, &detail)
			require.Equal(t, "#2", detail.Key)
			result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
			require.NoError(t, err)
			require.Equal(t, "failed", result.Status)
			rows, _, err := h.app.storage.GetHistoryPaginated(h.app.ctx, id, 0, 10)
			require.NoError(t, err)
			require.Len(t, rows, len(tc.positions)+1)
		})
	}
}

type recoveryReplayHooks struct {
	hooks.NoOpHooks
	starts    []bool
	completed []hooks.ReplayCompleteInfo
}

func (h *recoveryReplayHooks) OnActivityStart(_ context.Context, info hooks.ActivityStartInfo) {
	h.starts = append(h.starts, info.IsReplay)
}
func (h *recoveryReplayHooks) OnReplayComplete(_ context.Context, info hooks.ReplayCompleteInfo) {
	h.completed = append(h.completed, info)
}

func TestUnfilledActivityRecoveryRunsLive(t *testing.T) {
	testUnfilledActivityRecoveryRunsLive(t, createTestApp)
}
func testUnfilledActivityRecoveryRunsLive(t *testing.T, factory replayAppFactory) {
	observed := &recoveryReplayHooks{}
	h := newTypedReplayHarness(t, factory, WithHooks(observed))
	fs := faultstore.New(h.app.storage)
	h.app.engine = replay.NewEngine(fs, h.app.hooks, "recovery", errorCodec{})
	calls := 0
	activity := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "done", nil })
	var workflowReplay []bool
	w := DefineWorkflow("recovery", func(ctx *WorkflowContext, input string) (string, error) {
		value, err := activity.Execute(ctx, input)
		workflowReplay = append(workflowReplay, ctx.IsReplaying())
		return value, err
	})
	id := seedErrorWorkflow(t, h, w)
	fs.Before = func(_ context.Context, method string) error {
		if method == "CompleteActivityTask" {
			return replay.ErrLeaseLost
		}
		return nil
	}
	require.ErrorIs(t, h.RunOnce(id), replay.ErrLeaseLost)
	require.Equal(t, 1, calls)
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Nil(t, row.EventData)
	fs.Before = nil
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, 2, calls, "the unfilled reservation must rerun exactly once")
	assert.Equal(t, []bool{false, false}, observed.starts, "both activity attempts execute live")
	assert.Equal(t, []bool{false, false}, workflowReplay)
	require.Len(t, observed.completed, 1)
	assert.Zero(t, observed.completed[0].CacheHits, "an unfilled reservation is not a cache hit")
	assertCompletedLeaseRun(t, h, id)
}
