package selvedge

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
)

// ReceivedEvent represents an event received by a workflow.
type ReceivedEvent[T any] struct {
	// Event metadata
	ID          string         `json:"id"`
	Type        string         `json:"type"`
	Source      string         `json:"source"`
	SpecVersion string         `json:"specversion"`
	Time        *time.Time     `json:"time,omitempty"`
	Extensions  map[string]any `json:"extensions,omitempty"`

	// Event data
	Data T `json:"data"`
}

// WaitEvent suspends the workflow until an event of the specified type is received.
// The event data will be deserialized into type T.
//
// During replay, if the event was already received, this returns immediately.
// Otherwise, the workflow is suspended until an event arrives on the channel.
//
// When the event arrives (published to the channel), the workflow will be resumed.
func WaitEvent[T any](ctx *WorkflowContext, eventType string, opts ...WaitEventOption) (*ReceivedEvent[T], error) {
	options := &waitEventOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Call OnEventWaitStart hook
	if h := ctx.Hooks(); h != nil {
		h.OnEventWaitStart(ctx.Context(), hooks.EventWaitStartInfo{
			InstanceID:   ctx.InstanceID(),
			WorkflowName: ctx.WorkflowName(),
			EventType:    eventType,
			Timeout:      options.timeout,
		})
	}

	// Subscribe to the event type channel (idempotent)
	if err := subscribeEvent(ctx, eventType); err != nil {
		return nil, fmt.Errorf("failed to subscribe to event channel: %w", err)
	}

	// Convert WaitEventOption to receiveEventOption
	var receiveOpts []receiveEventOption
	if options.timeout != nil {
		receiveOpts = append(receiveOpts, withReceiveEventTimeout(*options.timeout))
	}

	// The internal receiver reads the stored event envelope.
	msg, err := receiveEvent[map[string]any](ctx, eventType, receiveOpts...)
	if err != nil {
		return nil, err // SuspendSignal or other errors propagate
	}

	// Convert receivedEventMessage to ReceivedEvent
	return convertToReceivedEvent[T](msg)
}

// convertToReceivedEvent converts a receivedEventMessage containing event envelope data
// to a ReceivedEvent with proper typing.
func convertToReceivedEvent[T any](msg *receivedEventMessage[map[string]any]) (*ReceivedEvent[T], error) {
	if msg == nil {
		return nil, fmt.Errorf("nil message")
	}

	event := &ReceivedEvent[T]{}

	// Extract event envelope metadata from the message data
	if id, ok := msg.Data["id"].(string); ok {
		event.ID = id
	}
	if typ, ok := msg.Data["type"].(string); ok {
		event.Type = typ
	}
	if source, ok := msg.Data["source"].(string); ok {
		event.Source = source
	}
	if specVersion, ok := msg.Data["specversion"].(string); ok {
		event.SpecVersion = specVersion
	}
	switch t := msg.Data["time"].(type) {
	case *time.Time:
		event.Time = t
	case string:
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			event.Time = &parsed
		}
	}
	if extensions, ok := msg.Data["extensions"].(map[string]any); ok {
		event.Extensions = extensions
	}

	// Extract and deserialize the actual event data
	if data, ok := msg.Data["data"]; ok {
		// Re-serialize and deserialize to get proper type
		dataJSON, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize event data: %w", err)
		}
		var typedData T
		if err := json.Unmarshal(dataJSON, &typedData); err != nil {
			return nil, fmt.Errorf("failed to deserialize event data: %w", err)
		}
		event.Data = typedData
	}

	return event, nil
}

// WaitEventOption configures event waiting behavior.
type WaitEventOption func(*waitEventOptions)

type waitEventOptions struct {
	timeout *time.Duration
}

// WithEventTimeout sets a timeout for waiting for an event.
func WithEventTimeout(d time.Duration) WaitEventOption {
	return func(o *waitEventOptions) {
		o.timeout = &d
	}
}

// Sleep suspends the workflow for a specified duration.
// The workflow will resume after the duration has elapsed.
//
// During replay, if the timer already fired, this returns immediately.
// Otherwise, it returns a SuspendSignal that signals the engine to:
// 1. Register a timer subscription
// 2. Update workflow status to "waiting_for_timer"
// 3. Release the workflow lock
//
// When the timer expires, the workflow will be resumed.
func Sleep(ctx *WorkflowContext, duration time.Duration, opts ...SleepOption) error {
	options := &sleepOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Generate timer ID
	timerID := options.timerID
	if timerID == "" {
		timerID = ctx.GenerateActivityID("sleep")
	}

	// Check if result is cached (replay)
	if _, ok := ctx.GetCachedResult(timerID); ok {
		return nil
	}

	// Calculate expiry time (must be calculated on first execution only)
	expiresAt := time.Now().Add(duration)

	// Call OnTimerStart hook
	if h := ctx.Hooks(); h != nil {
		h.OnTimerStart(ctx.Context(), hooks.TimerStartInfo{
			InstanceID:   ctx.InstanceID(),
			WorkflowName: ctx.WorkflowName(),
			TimerID:      timerID,
			ExpiresAt:    expiresAt,
		})
	}

	// Record the timer ID for tracking
	ctx.RecordActivityID(timerID)

	// Return SuspendSignal that the replay engine will handle
	return NewTimerSuspend(ctx.InstanceID(), timerID, timerID, expiresAt)
}

// SleepOption configures sleep behavior.
type SleepOption func(*sleepOptions)

type sleepOptions struct {
	timerID string
}

// WithSleepID sets a custom timer ID for the sleep.
// This is useful for identifying timers in logs and for deterministic replay.
func WithSleepID(id string) SleepOption {
	return func(o *sleepOptions) {
		o.timerID = id
	}
}

// SleepUntil suspends the workflow until a specific time.
func SleepUntil(ctx *WorkflowContext, t time.Time, opts ...SleepOption) error {
	duration := time.Until(t)
	if duration <= 0 {
		// Already past the target time
		return nil
	}
	return Sleep(ctx, duration, opts...)
}
