package replay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

type lifecycleHooks struct {
	hooks.NoOpHooks
	starts            []hooks.WorkflowStartInfo
	replayStarts      []hooks.ReplayStartInfo
	replayCompletions []hooks.ReplayCompleteInfo
	terminalName      string
	terminalDuration  time.Duration
}

func (h *lifecycleHooks) OnWorkflowStart(_ context.Context, info hooks.WorkflowStartInfo) {
	h.starts = append(h.starts, info)
}
func (h *lifecycleHooks) OnReplayStart(_ context.Context, info hooks.ReplayStartInfo) {
	h.replayStarts = append(h.replayStarts, info)
}
func (h *lifecycleHooks) OnReplayComplete(_ context.Context, info hooks.ReplayCompleteInfo) {
	h.replayCompletions = append(h.replayCompletions, info)
}
func (h *lifecycleHooks) OnWorkflowComplete(_ context.Context, info hooks.WorkflowCompleteInfo) {
	h.terminalName, h.terminalDuration = info.WorkflowName, info.Duration
}
func (h *lifecycleHooks) OnWorkflowFailed(_ context.Context, info hooks.WorkflowFailedInfo) {
	h.terminalName, h.terminalDuration = info.WorkflowName, info.Duration
}
func (h *lifecycleHooks) OnWorkflowCancelled(_ context.Context, info hooks.WorkflowCancelledInfo) {
	h.terminalName, h.terminalDuration = info.WorkflowName, info.Duration
}

func TestLifecycleHooksRetainWorkflowMetadataAcrossReplay(t *testing.T) {
	for _, test := range []struct {
		name        string
		terminalErr error
	}{
		{"completed", nil}, {"failed", errors.New("workflow rejected")}, {"cancelled", context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := setupTestStorage(t)
			ctx := context.Background()
			startedAt := time.Now().Add(-2 * time.Hour)
			require.NoError(t, store.CreateInstance(ctx, &storage.WorkflowInstance{InstanceID: "named", WorkflowName: "order-processing", Status: storage.StatusRunning, StartedAt: startedAt}))
			observer := new(lifecycleHooks)
			engine := NewEngine(store, observer, "worker", testErrorCodec{})
			runner := func(ec *ExecutionContext) (any, error) {
				cmd, err := ec.Issue("activity:step", "step")
				if err != nil {
					return nil, err
				}
				defer cmd.End()
				if cmd.Recorded == nil {
					_, err = store.RecordHistory(ec.Context(), ec.Fence(), &storage.HistoryEvent{InstanceID: ec.InstanceID(), Seq: cmd.Seq, ActivityID: cmd.Key, Command: "activity:step", EventType: storage.HistoryActivityCompleted, EventData: []byte(`"done"`), DataType: "json"})
					if err != nil {
						return nil, err
					}
					return nil, ec.Yield(Wait{Seq: cmd.Seq, Key: cmd.Key, Kind: WaitSignal})
				}
				return "done", test.terminalErr
			}
			require.NoError(t, engine.Run(ctx, "named", runner))
			if errors.Is(test.terminalErr, context.Canceled) {
				_, err := store.RequestCancel(ctx, "named", "requested")
				require.NoError(t, err)
			}
			require.NoError(t, store.MarkRunnable(ctx, "named"))
			require.ErrorIs(t, engine.Run(ctx, "named", runner), test.terminalErr)
			require.Len(t, observer.starts, 2)
			require.Equal(t, "order-processing", observer.starts[0].WorkflowName)
			require.Equal(t, "order-processing", observer.starts[1].WorkflowName)
			require.WithinDuration(t, startedAt, observer.starts[1].StartTime, time.Second)
			require.Len(t, observer.replayStarts, 1)
			require.Equal(t, "order-processing", observer.replayStarts[0].WorkflowName)
			require.Equal(t, "order-processing", observer.terminalName)
			require.GreaterOrEqual(t, observer.terminalDuration, time.Hour)
			if test.terminalErr == nil {
				require.Len(t, observer.replayCompletions, 1)
				require.Equal(t, "order-processing", observer.replayCompletions[0].WorkflowName)
				require.Less(t, observer.replayCompletions[0].Duration, time.Minute)
			}
		})
	}
}
