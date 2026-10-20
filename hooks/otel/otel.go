// Package otel exports Selvedge lifecycle callbacks as completed OpenTelemetry
// spans. Every callback owns and ends its span, including start and retry markers.
// Durable operations can resume on another worker or complete before the starting
// worker observes its callback, so hooks retain no spans or contexts between calls.
// Correlate callbacks by instance and command attributes. Parentage comes only
// from the supplied context; trace context is not propagated through storage.
package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/CaliLuke/selvedge/hooks"
)

const tracerName = "selvedge"

// OTelHooks implements WorkflowHooks with stateless, concurrency-safe tracing.
// Terminal spans use the reported duration; start, retry, cache and timer
// callbacks emit point-in-time markers. Durations do not depend on a local start.
type OTelHooks struct {
	hooks.NoOpHooks
	tracer trace.Tracer
}

// NewOTelHooks creates hooks using the provider, or the global provider if nil.
func NewOTelHooks(provider trace.TracerProvider) *OTelHooks {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &OTelHooks{tracer: provider.Tracer(tracerName)}
}

func (h *OTelHooks) record(ctx context.Context, name, instanceID, workflowName string, duration time.Duration, status codes.Code, description string, err error, attrs ...attribute.KeyValue) {
	duration = max(0, duration)
	ended := time.Now()
	attrs = append(attrs,
		attribute.String("selvedge.instance_id", instanceID),
		attribute.String("selvedge.workflow_name", workflowName),
		attribute.Int64("selvedge.duration_ms", duration.Milliseconds()),
	)
	_, span := h.tracer.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(ended.Add(-duration)),
		trace.WithAttributes(attrs...),
	)
	defer span.End(trace.WithTimestamp(ended))
	if err != nil {
		span.RecordError(err, trace.WithTimestamp(ended))
		description = err.Error()
	}
	span.SetStatus(status, description)
}

func (h *OTelHooks) OnWorkflowStart(ctx context.Context, info hooks.WorkflowStartInfo) {
	h.record(ctx, "workflow_start/"+info.WorkflowName, info.InstanceID, info.WorkflowName, 0, codes.Unset, "", nil,
		attribute.String("selvedge.start_time", info.StartTime.String()))
}

func (h *OTelHooks) OnWorkflowComplete(ctx context.Context, info hooks.WorkflowCompleteInfo) {
	h.record(ctx, "workflow/"+info.WorkflowName, info.InstanceID, info.WorkflowName, info.Duration, codes.Ok, "workflow completed", nil)
}

func (h *OTelHooks) OnWorkflowFailed(ctx context.Context, info hooks.WorkflowFailedInfo) {
	h.record(ctx, "workflow/"+info.WorkflowName, info.InstanceID, info.WorkflowName, info.Duration, codes.Error, "workflow failed", info.Error)
}

func (h *OTelHooks) OnWorkflowCancelled(ctx context.Context, info hooks.WorkflowCancelledInfo) {
	h.record(ctx, "workflow/"+info.WorkflowName, info.InstanceID, info.WorkflowName, info.Duration, codes.Error, "workflow canceled: "+info.Reason, nil,
		attribute.String("selvedge.cancel_reason", info.Reason))
}

func activityAttrs(id, name string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("selvedge.activity_id", id),
		attribute.String("selvedge.activity_name", name),
	}
}

func (h *OTelHooks) OnActivityStart(ctx context.Context, info hooks.ActivityStartInfo) {
	attrs := append(activityAttrs(info.ActivityID, info.ActivityName), attribute.Bool("selvedge.is_replay", info.IsReplay))
	h.record(ctx, "activity_start/"+info.ActivityName, info.InstanceID, info.WorkflowName, 0, codes.Unset, "", nil, attrs...)
}

func (h *OTelHooks) OnActivityComplete(ctx context.Context, info hooks.ActivityCompleteInfo) {
	attrs := append(activityAttrs(info.ActivityID, info.ActivityName), attribute.Bool("selvedge.is_replay", info.IsReplay))
	h.record(ctx, "activity/"+info.ActivityName, info.InstanceID, info.WorkflowName, info.Duration, codes.Ok, "activity completed", nil, attrs...)
}

func (h *OTelHooks) OnActivityFailed(ctx context.Context, info hooks.ActivityFailedInfo) {
	attrs := append(activityAttrs(info.ActivityID, info.ActivityName), attribute.Int("selvedge.attempt", info.Attempt))
	h.record(ctx, "activity/"+info.ActivityName, info.InstanceID, info.WorkflowName, info.Duration, codes.Error, "activity failed", info.Error, attrs...)
}

func (h *OTelHooks) OnActivityRetry(ctx context.Context, info hooks.ActivityRetryInfo) {
	attrs := append(activityAttrs(info.ActivityID, info.ActivityName),
		attribute.Int("selvedge.attempt", info.Attempt),
		attribute.Int("selvedge.max_attempts", info.MaxAttempts),
		attribute.Int64("selvedge.next_delay_ms", info.NextDelay.Milliseconds()),
	)
	h.record(ctx, "activity_retry/"+info.ActivityName, info.InstanceID, info.WorkflowName, 0, codes.Unset, "", info.Error, attrs...)
}

func (h *OTelHooks) OnActivityCacheHit(ctx context.Context, info hooks.ActivityCacheHitInfo) {
	h.record(ctx, "activity_cache_hit/"+info.ActivityName, info.InstanceID, info.WorkflowName, 0, codes.Ok, "cache hit", nil,
		activityAttrs(info.ActivityID, info.ActivityName)...)
}

func (h *OTelHooks) OnTimerStart(ctx context.Context, info hooks.TimerStartInfo) {
	h.record(ctx, "timer_start/"+info.TimerID, info.InstanceID, info.WorkflowName, 0, codes.Unset, "", nil,
		attribute.String("selvedge.timer_id", info.TimerID),
		attribute.String("selvedge.expires_at", info.ExpiresAt.String()))
}

func (h *OTelHooks) OnTimerFired(ctx context.Context, info hooks.TimerFiredInfo) {
	h.record(ctx, "timer/"+info.TimerID, info.InstanceID, info.WorkflowName, 0, codes.Ok, "timer fired", nil,
		attribute.String("selvedge.timer_id", info.TimerID),
		attribute.String("selvedge.fired_at", info.FiredAt.String()))
}

func (h *OTelHooks) OnReplayStart(ctx context.Context, info hooks.ReplayStartInfo) {
	h.record(ctx, "replay_start/"+info.WorkflowName, info.InstanceID, info.WorkflowName, 0, codes.Unset, "", nil,
		attribute.Int("selvedge.history_events", info.HistoryEvents),
		attribute.String("selvedge.resume_activity", info.ResumeActivity))
}

func (h *OTelHooks) OnReplayComplete(ctx context.Context, info hooks.ReplayCompleteInfo) {
	h.record(ctx, "replay/"+info.WorkflowName, info.InstanceID, info.WorkflowName, info.Duration, codes.Ok, "replay completed", nil,
		attribute.Int("selvedge.cache_hits", info.CacheHits),
		attribute.Int("selvedge.new_activities", info.NewActivities))
}

var _ hooks.WorkflowHooks = (*OTelHooks)(nil)
