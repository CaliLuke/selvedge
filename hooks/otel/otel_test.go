package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/CaliLuke/selvedge/hooks"
)

// setupTest creates a test tracer provider and returns the hooks and span recorder.
func setupTest() (*OTelHooks, *tracetest.SpanRecorder) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	h := NewOTelHooks(tp)
	return h, sr
}

func TestNewOTelHooks(t *testing.T) {
	// Test with nil tracer provider (uses global)
	h := NewOTelHooks(nil)
	if h == nil {
		t.Fatal("expected non-nil hooks")
	}
	if h.tracer == nil {
		t.Fatal("expected non-nil tracer")
	}

	// Test with custom tracer provider
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	h = NewOTelHooks(tp)
	if h == nil {
		t.Fatal("expected non-nil hooks")
	}
}

func TestWorkflowLifecycle(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	// Start workflow
	h.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		InputData:    map[string]string{"order_id": "O-001"},
		StartTime:    time.Now(),
	})

	// Complete workflow
	h.OnWorkflowComplete(ctx, hooks.WorkflowCompleteInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		OutputData:   map[string]string{"status": "completed"},
		Duration:     100 * time.Millisecond,
	})

	// Check spans
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name() != "workflow/order_workflow" {
		t.Errorf("expected span name 'workflow/order_workflow', got %s", span.Name())
	}
	if span.Status().Code != codes.Ok {
		t.Errorf("expected status OK, got %v", span.Status().Code)
	}

	// Check attributes
	attrs := span.Attributes()
	checkAttribute(t, attrs, "selvedge.instance_id", "wf-123")
	checkAttribute(t, attrs, "selvedge.workflow_name", "order_workflow")
}

func TestWorkflowFailed(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	h.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{
		InstanceID:   "wf-456",
		WorkflowName: "payment_workflow",
		StartTime:    time.Now(),
	})

	h.OnWorkflowFailed(ctx, hooks.WorkflowFailedInfo{
		InstanceID:   "wf-456",
		WorkflowName: "payment_workflow",
		Error:        errors.New("payment failed"),
		Duration:     50 * time.Millisecond,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Status().Code != codes.Error {
		t.Errorf("expected status Error, got %v", span.Status().Code)
	}
}

func TestWorkflowCancelled(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	h.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{
		InstanceID:   "wf-789",
		WorkflowName: "booking_workflow",
		StartTime:    time.Now(),
	})

	h.OnWorkflowCancelled(ctx, hooks.WorkflowCancelledInfo{
		InstanceID:   "wf-789",
		WorkflowName: "booking_workflow",
		Reason:       "user request",
		Duration:     30 * time.Millisecond,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Status().Code != codes.Error {
		t.Errorf("expected status Error (canceled), got %v", span.Status().Code)
	}

	attrs := span.Attributes()
	checkAttribute(t, attrs, "selvedge.cancel_reason", "user request")
}

func TestActivityLifecycle(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	// Start activity
	h.OnActivityStart(ctx, hooks.ActivityStartInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "reserve_inventory:1",
		ActivityName: "reserve_inventory",
		IsReplay:     false,
	})

	// Complete activity
	h.OnActivityComplete(ctx, hooks.ActivityCompleteInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "reserve_inventory:1",
		ActivityName: "reserve_inventory",
		OutputData:   map[string]bool{"reserved": true},
		Duration:     20 * time.Millisecond,
		IsReplay:     false,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name() != "activity/reserve_inventory" {
		t.Errorf("expected span name 'activity/reserve_inventory', got %s", span.Name())
	}
	if span.Status().Code != codes.Ok {
		t.Errorf("expected status OK, got %v", span.Status().Code)
	}

	attrs := span.Attributes()
	checkAttribute(t, attrs, "selvedge.activity_id", "reserve_inventory:1")
}

func TestActivityFailed(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	h.OnActivityStart(ctx, hooks.ActivityStartInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "process_payment:1",
		ActivityName: "process_payment",
		IsReplay:     false,
	})

	h.OnActivityFailed(ctx, hooks.ActivityFailedInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "process_payment:1",
		ActivityName: "process_payment",
		Error:        errors.New("insufficient funds"),
		Duration:     10 * time.Millisecond,
		Attempt:      3,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Status().Code != codes.Error {
		t.Errorf("expected status Error, got %v", span.Status().Code)
	}
}

func TestActivityRetry(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	h.OnActivityStart(ctx, hooks.ActivityStartInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "fetch_data:1",
		ActivityName: "fetch_data",
		IsReplay:     false,
	})

	// Record retry
	h.OnActivityRetry(ctx, hooks.ActivityRetryInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "fetch_data:1",
		ActivityName: "fetch_data",
		Attempt:      1,
		MaxAttempts:  3,
		NextDelay:    5 * time.Second,
		Error:        errors.New("timeout"),
	})

	// Complete activity
	h.OnActivityComplete(ctx, hooks.ActivityCompleteInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "fetch_data:1",
		ActivityName: "fetch_data",
		Duration:     100 * time.Millisecond,
		IsReplay:     false,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	// Check that retry event was added
	span := spans[0]
	events := span.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event (retry), got %d", len(events))
	}
	if events[0].Name != "activity_retry" {
		t.Errorf("expected event name 'activity_retry', got %s", events[0].Name)
	}
}

func TestActivityCacheHit(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	h.OnActivityCacheHit(ctx, hooks.ActivityCacheHitInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		ActivityID:   "reserve_inventory:1",
		ActivityName: "reserve_inventory",
		CachedResult: map[string]bool{"reserved": true},
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name() != "activity_cache_hit/reserve_inventory" {
		t.Errorf("expected span name 'activity_cache_hit/reserve_inventory', got %s", span.Name())
	}
}

func TestTimerHandling(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	expiresAt := time.Now().Add(60 * time.Second)

	h.OnTimerStart(ctx, hooks.TimerStartInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		TimerID:      "payment_timeout",
		ExpiresAt:    expiresAt,
	})

	h.OnTimerFired(ctx, hooks.TimerFiredInfo{
		InstanceID:   "wf-123",
		WorkflowName: "order_workflow",
		TimerID:      "payment_timeout",
		FiredAt:      expiresAt,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name() != "timer/payment_timeout" {
		t.Errorf("expected span name 'timer/payment_timeout', got %s", span.Name())
	}
	if span.Status().Code != codes.Ok {
		t.Errorf("expected status OK, got %v", span.Status().Code)
	}
}

func TestReplayLifecycle(t *testing.T) {
	h, sr := setupTest()
	ctx := context.Background()

	h.OnReplayStart(ctx, hooks.ReplayStartInfo{
		InstanceID:     "wf-123",
		WorkflowName:   "order_workflow",
		HistoryEvents:  5,
		ResumeActivity: "process_payment:1",
	})

	h.OnReplayComplete(ctx, hooks.ReplayCompleteInfo{
		InstanceID:    "wf-123",
		WorkflowName:  "order_workflow",
		CacheHits:     3,
		NewActivities: 2,
		Duration:      10 * time.Millisecond,
	})

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name() != "replay/order_workflow" {
		t.Errorf("expected span name 'replay/order_workflow', got %s", span.Name())
	}
	if span.Status().Code != codes.Ok {
		t.Errorf("expected status OK, got %v", span.Status().Code)
	}

	attrs := span.Attributes()
	checkAttributeInt(t, attrs, "selvedge.cache_hits", 3)
	checkAttributeInt(t, attrs, "selvedge.new_activities", 2)
}

func TestImplementsInterface(t *testing.T) {
	var _ hooks.WorkflowHooks = (*OTelHooks)(nil)
}

// Helper functions

func checkAttribute(t *testing.T, attrs []attribute.KeyValue, key, expected string) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			if attr.Value.AsString() != expected {
				t.Errorf("expected attribute %s=%s, got %s", key, expected, attr.Value.AsString())
			}
			return
		}
	}
	t.Errorf("attribute %s not found", key)
}

func checkAttributeInt(t *testing.T, attrs []attribute.KeyValue, key string, expected int) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			if attr.Value.AsInt64() != int64(expected) {
				t.Errorf("expected attribute %s=%d, got %d", key, expected, attr.Value.AsInt64())
			}
			return
		}
	}
	t.Errorf("attribute %s not found", key)
}
