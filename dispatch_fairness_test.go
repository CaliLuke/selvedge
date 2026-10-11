package selvedge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func mixedFleetHarness(t *testing.T, factory replayAppFactory) *typedReplayHarness {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	h.app.config.maxWorkflowsPerBatch = 2
	w := DefineWorkflow("registered", func(_ *WorkflowContext, input string) (string, error) {
		return input, nil
	})
	RegisterWorkflow(h.app, w)
	for i := range 3 {
		require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{
			InstanceID: fmt.Sprintf("older-%d", i), WorkflowName: "unregistered",
			Status: storage.StatusRunning, StartedAt: time.Now().Add(-time.Hour),
		}))
	}
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{
		InstanceID: "owned", WorkflowName: w.Name(), Status: storage.StatusRunning,
		StartedAt: time.Now(), InputData: []byte(`"input"`),
	}))
	return h
}

func TestDispatcherMixedFleetFirstPoll(t *testing.T) {
	testDispatcherMixedFleetFirstPoll(t, createTestApp)
}

func testDispatcherMixedFleetFirstPoll(t *testing.T, factory replayAppFactory) {
	h := mixedFleetHarness(t, factory)
	count, err := h.app.resumeResumableWorkflows()
	h.app.wg.Wait()
	require.NoError(t, err)
	require.Equal(t, 1, count, "first poll must dispatch registered work behind the other fleet's backlog")
	instance, err := h.app.storage.GetInstance(h.app.ctx, "owned")
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, instance.Status)
}

func TestDispatcherSelectionUsesOneQuery(t *testing.T) {
	testDispatcherSelectionUsesOneQuery(t, createTestApp)
}

func testDispatcherSelectionUsesOneQuery(t *testing.T, factory replayAppFactory) {
	h := mixedFleetHarness(t, factory)
	fs := faultstore.New(h.app.storage)
	h.app.storage = fs
	// End the poll after selection so execution SQL cannot affect its query budget.
	fs.After = func(_ context.Context, method string) error {
		if method == "FindDispatchable" {
			h.app.cancel()
		}
		return nil
	}
	_, _ = h.app.resumeResumableWorkflows()
	h.app.wg.Wait()
	require.Len(t, fs.Statements(), 1, "selection must use one SQL query with no per-row instance fetches")
	require.Equal(t, []string{"FindDispatchable"}, fs.Calls())
}
