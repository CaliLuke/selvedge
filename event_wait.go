// Package selvedge provides a durable execution framework for Go.
package selvedge

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// receivedEventMessage represents a message received from a channel.
type receivedEventMessage[T any] struct {
	// Message ID
	ID int64 `json:"id"`

	// Channel name
	ChannelName string `json:"channel_name"`

	// Message data
	Data T `json:"data"`

	// Metadata (optional)
	Metadata map[string]any `json:"metadata,omitempty"`

	// When the message was created
	CreatedAt time.Time `json:"created_at"`
}

// subscribeEvent registers the workflow to wait for an event type.
func subscribeEvent(ctx *WorkflowContext, channelName string) error {
	// Generate activity ID for deterministic replay
	activityID := ctx.GenerateActivityID(fmt.Sprintf("subscribe_%s", channelName))

	// Check if already subscribed (replay)
	if _, ok := ctx.GetCachedResult(activityID); ok {
		return nil
	}

	// Subscribe to channel
	s := ctx.Storage()
	if s == nil {
		return fmt.Errorf("storage not available")
	}

	// Check for mode conflict
	existingMode, err := s.GetChannelMode(ctx.Context(), channelName)
	if err != nil {
		return fmt.Errorf("failed to check channel mode: %w", err)
	}
	if existingMode != "" && existingMode != storage.ChannelModeBroadcast {
		return &eventModeConflictError{
			Channel:       channelName,
			ExistingMode:  string(existingMode),
			RequestedMode: string(storage.ChannelModeBroadcast),
		}
	}

	if err := s.SubscribeToChannel(ctx.Context(), ctx.InstanceID(), channelName, storage.ChannelModeBroadcast); err != nil {
		return fmt.Errorf("failed to subscribe to channel: %w", err)
	}

	// Record for replay
	ctx.SetCachedResult(activityID, true)

	return nil
}

// receiveEventOption configures channel receive behavior.
type receiveEventOption func(*receiveOptions)

type receiveOptions struct {
	timeout *time.Duration
}

// withReceiveEventTimeout sets a timeout for waiting for a message.
func withReceiveEventTimeout(d time.Duration) receiveEventOption {
	return func(o *receiveOptions) {
		o.timeout = &d
	}
}

func receiveEvent[T any](ctx *WorkflowContext, channelName string, opts ...receiveEventOption) (*receivedEventMessage[T], error) {
	options := &receiveOptions{}
	for _, opt := range opts {
		opt(options)
	}

	startTime := time.Now()

	// Generate activity ID for this receive (use original channelName for consistency)
	activityID := ctx.GenerateActivityID(fmt.Sprintf("receive_%s", channelName))

	// Check if result is cached (replay)
	// First try raw JSON to avoid re-serialization
	if rawJSON, ok := ctx.GetCachedResultRaw(activityID); ok && len(rawJSON) > 0 {
		// Decode the recorded Go message envelope.
		var msg receivedEventMessage[T]
		if err := json.Unmarshal(rawJSON, &msg); err == nil && msg.ChannelName != "" {
			return &msg, nil
		}
	}
	// Also check GetCachedResult which handles cached errors
	if result, ok := ctx.GetCachedResult(activityID); ok {
		// Check if it's an error (e.g., timeout)
		if err, isErr := result.(error); isErr {
			// Convert generic timeout error to ChannelMessageTimeoutError
			// so that errors.As works correctly
			if strings.Contains(err.Error(), "timeout waiting for channel message") {
				return nil, &ChannelMessageTimeoutError{
					InstanceID:  ctx.InstanceID(),
					ChannelName: channelName,
				}
			}
			return nil, err
		}
		if msg, ok := result.(*receivedEventMessage[T]); ok {
			return msg, nil
		}
		return nil, fmt.Errorf("cached result type mismatch for channel receive %s", activityID)
	}

	// Verify subscription exists
	s := ctx.Storage()
	if s == nil {
		return nil, fmt.Errorf("storage not available")
	}

	sub, err := s.GetChannelSubscription(ctx.Context(), ctx.InstanceID(), channelName)
	if err != nil || sub == nil {
		return nil, errEventNotSubscribed
	}

	// Check for pending messages before suspending
	pendingMessages, err := s.GetPendingChannelMessagesForInstance(ctx.Context(), ctx.InstanceID(), channelName)
	if err != nil {
		return nil, fmt.Errorf("failed to check pending messages: %w", err)
	}

	if len(pendingMessages) > 0 {
		// Message is already available - process it immediately
		msg := pendingMessages[0]

		// Update cursor for broadcast mode
		if sub.Mode == storage.ChannelModeBroadcast {
			if err := s.UpdateDeliveryCursor(ctx.Context(), ctx.InstanceID(), channelName, msg.ID); err != nil {
				return nil, fmt.Errorf("failed to update cursor: %w", err)
			}
		}

		// Deserialize the message
		var data T
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return nil, fmt.Errorf("failed to deserialize message data: %w", err)
		}

		var metadata map[string]any
		if len(msg.Metadata) > 0 {
			if err := json.Unmarshal(msg.Metadata, &metadata); err != nil {
				return nil, fmt.Errorf("failed to deserialize metadata: %w", err)
			}
		}

		result := &receivedEventMessage[T]{
			ID:          msg.ID,
			ChannelName: channelName,
			Data:        data,
			Metadata:    metadata,
			CreatedAt:   msg.PublishedAt,
		}

		// Record to history for replay (synchronous message processing)
		if err := ctx.RecordActivityResult(activityID, result, nil); err != nil {
			return nil, fmt.Errorf("failed to record activity result: %w", err)
		}

		// Cache for replay
		ctx.SetCachedResult(activityID, result)

		// Call OnEventWaitComplete hook
		if h := ctx.Hooks(); h != nil {
			h.OnEventWaitComplete(ctx.Context(), hooks.EventWaitCompleteInfo{
				InstanceID:   ctx.InstanceID(),
				WorkflowName: ctx.WorkflowName(),
				EventType:    channelName,
				Duration:     time.Since(startTime),
			})
		}

		return result, nil
	}

	// No pending messages - need to wait for message
	// Record the activity ID for tracking
	ctx.RecordActivityID(activityID)

	// Return SuspendSignal that the replay engine will handle
	// Include the activityID so it can be used when recording history on message delivery
	// Match the subscription used by event waiting.
	return nil, NewChannelMessageSuspend(ctx.InstanceID(), channelName, activityID, options.timeout)
}
