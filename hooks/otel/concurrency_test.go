package otel

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"

	"github.com/CaliLuke/selvedge/hooks"
)

func TestConcurrentWorkflowSpans(t *testing.T) {
	h, recorder := setupTest()
	ctx := context.Background()
	var workers sync.WaitGroup
	for i := range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			id := fmt.Sprintf("workflow-%d", i)
			h.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{InstanceID: id})
			h.OnActivityStart(ctx, hooks.ActivityStartInfo{InstanceID: id, ActivityID: "activity"})
			h.OnActivityComplete(ctx, hooks.ActivityCompleteInfo{InstanceID: id, ActivityID: "activity"})
			h.OnTimerStart(ctx, hooks.TimerStartInfo{InstanceID: id, TimerID: "timer"})
			h.OnTimerFired(ctx, hooks.TimerFiredInfo{InstanceID: id, TimerID: "timer"})
			h.OnReplayStart(ctx, hooks.ReplayStartInfo{InstanceID: id})
			h.OnReplayComplete(ctx, hooks.ReplayCompleteInfo{InstanceID: id})
			h.OnWorkflowComplete(ctx, hooks.WorkflowCompleteInfo{InstanceID: id})
		}()
	}
	workers.Wait()
	require.Len(t, recorder.Ended(), 256)
}

func TestDelimitedIDsDoNotCollide(t *testing.T) {
	h, recorder := setupTest()
	ctx := context.Background()
	h.OnActivityStart(ctx, hooks.ActivityStartInfo{InstanceID: "a:b", ActivityID: "c"})
	h.OnActivityStart(ctx, hooks.ActivityStartInfo{InstanceID: "a", ActivityID: "b:c"})
	h.OnTimerStart(ctx, hooks.TimerStartInfo{InstanceID: "a:b", TimerID: "c"})
	h.OnTimerStart(ctx, hooks.TimerStartInfo{InstanceID: "a", TimerID: "b:c"})

	require.Len(t, recorder.Ended(), 4, "start markers are exported immediately")
	checkAttribute(t, recorder.Ended()[0].Attributes(), "selvedge.instance_id", "a:b")
	checkAttribute(t, recorder.Ended()[0].Attributes(), "selvedge.activity_id", "c")
	checkAttribute(t, recorder.Ended()[1].Attributes(), "selvedge.instance_id", "a")
	checkAttribute(t, recorder.Ended()[1].Attributes(), "selvedge.activity_id", "b:c")
}

func TestWorkflowClosureEndsOutstandingSpans(t *testing.T) {
	h, recorder := setupTest()
	ctx := context.Background()
	h.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{InstanceID: "workflow"})
	h.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{InstanceID: "workflow"})
	h.OnActivityStart(ctx, hooks.ActivityStartInfo{InstanceID: "workflow", ActivityID: "pending"})
	h.OnTimerStart(ctx, hooks.TimerStartInfo{InstanceID: "workflow", TimerID: "cancelled"})
	h.OnReplayStart(ctx, hooks.ReplayStartInfo{InstanceID: "workflow"})

	h.OnWorkflowCancelled(ctx, hooks.WorkflowCancelledInfo{InstanceID: "workflow", Reason: "stop"})
	require.Len(t, recorder.Ended(), len(recorder.Started()))
}

func TestActivityCacheHitEndsStartedSpan(t *testing.T) {
	h, recorder := setupTest()
	ctx := context.Background()
	h.OnActivityStart(ctx, hooks.ActivityStartInfo{InstanceID: "workflow", ActivityID: "cached"})
	h.OnActivityCacheHit(ctx, hooks.ActivityCacheHitInfo{InstanceID: "workflow", ActivityID: "cached"})
	require.Len(t, recorder.Ended(), len(recorder.Started()))
}

func TestWorkflowCompletionOnAnotherWorkerExportsBothCallbacks(t *testing.T) {
	starter, starts := setupTest()
	worker, completions := setupTest()
	ctx := context.Background()

	starter.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{InstanceID: "remote", WorkflowName: "orders"})
	require.Len(t, starts.Ended(), 1, "the starting worker may never see this workflow again")
	worker.OnWorkflowComplete(ctx, hooks.WorkflowCompleteInfo{InstanceID: "remote", WorkflowName: "orders", Duration: 2 * time.Second})
	require.Len(t, completions.Ended(), 1, "completion must not require a local start")
	span := completions.Ended()[0]
	require.Equal(t, "workflow/orders", span.Name())
	require.Equal(t, 2*time.Second, span.EndTime().Sub(span.StartTime()))
	require.Equal(t, codes.Ok, span.Status().Code)
}

func TestLateStartCallbackLeavesNoOpenSpans(t *testing.T) {
	h, recorder := setupTest()
	ctx := context.Background()

	// Committing a new instance wakes dispatch before OnWorkflowStart is called.
	h.OnWorkflowComplete(ctx, hooks.WorkflowCompleteInfo{InstanceID: "fast", WorkflowName: "orders"})
	h.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{InstanceID: "fast", WorkflowName: "orders"})
	h.OnActivityStart(ctx, hooks.ActivityStartInfo{InstanceID: "fast", ActivityID: "abandoned"})
	h.OnTimerStart(ctx, hooks.TimerStartInfo{InstanceID: "fast", TimerID: "cancelled"})
	require.Len(t, recorder.Ended(), 4)
	require.Len(t, recorder.Started(), 4)
}

func TestCallbackPreservesIncomingParent(t *testing.T) {
	h, recorder := setupTest()
	ctx, parent := h.tracer.Start(context.Background(), "request")
	defer parent.End()

	h.OnActivityComplete(ctx, hooks.ActivityCompleteInfo{InstanceID: "remote", ActivityID: "#1", ActivityName: "charge"})
	require.Len(t, recorder.Ended(), 1)
	require.Equal(t, parent.SpanContext().SpanID(), recorder.Ended()[0].Parent().SpanID())
	require.Equal(t, parent.SpanContext().TraceID(), recorder.Ended()[0].SpanContext().TraceID())
}
