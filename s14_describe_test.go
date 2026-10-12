package selvedge

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestDescribeStatus(t *testing.T) { s14TestDescribeStatus(t, createTestApp) }
func s14TestDescribeStatus(t *testing.T, factory replayAppFactory) {
	for _, status := range []WorkflowStatus{StatusRunning, StatusCompleted, StatusFailed, StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			ctx := context.Background()
			require.NoError(t, h.app.storage.CreateInstance(ctx, &storage.WorkflowInstance{InstanceID: "parent", WorkflowName: "parent", Status: StatusRunning}))
			f, ok, err := h.app.storage.AcquireLease(ctx, "parent", "s14", h.app.config.lease.TTL)
			require.NoError(t, err)
			require.True(t, ok)
			_, err = h.app.storage.StartChild(ctx, &f, 1, "#1", "child:child", &storage.WorkflowInstance{InstanceID: "child", WorkflowName: "child", InputData: []byte(`null`)})
			require.NoError(t, err)
			_, err = h.app.storage.RequestCancel(ctx, "child", "requested")
			require.NoError(t, err)
			if status != StatusRunning {
				cf, acquired, acquireErr := h.app.storage.AcquireLease(ctx, "child", "s14", h.app.config.lease.TTL)
				require.NoError(t, acquireErr)
				require.True(t, acquired)
				var event *storage.HistoryEvent
				if status != StatusCompleted {
					event = &storage.HistoryEvent{InstanceID: "child", ActivityID: "$workflow_failed", Command: "close", EventType: storage.HistoryWorkflowFailed, EventData: encodeError(context.Canceled), DataType: "json"}
				}
				require.NoError(t, h.app.storage.CloseInstance(ctx, cf, status, []byte(`null`), event))
			}
			d, err := Describe(ctx, h.app, "child")
			require.NoError(t, err)
			require.NotNil(t, d)
			require.Equal(t, "child", d.InstanceID)
			require.Equal(t, "child", d.WorkflowName)
			require.Equal(t, status, d.Status)
			require.True(t, d.CancelRequested)
			require.Equal(t, "requested", d.CancelReason)
			require.Equal(t, "parent", d.ParentInstanceID)
			require.Equal(t, "#1", d.ParentKey)
			require.False(t, d.StartedAt.IsZero())
			require.False(t, d.UpdatedAt.IsZero())
			d, err = Describe(ctx, h.app, "parent")
			require.NoError(t, err)
			require.False(t, d.CancelRequested)
		})
	}
	t.Run("unknown", func(t *testing.T) {
		h := newTypedReplayHarness(t, factory)
		_, err := Describe(context.Background(), h.app, "missing")
		var nf *WorkflowNotFoundError
		require.ErrorAs(t, err, &nf)
	})
}
