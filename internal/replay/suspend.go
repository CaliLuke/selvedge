package replay

import (
	"errors"
	"fmt"
	"time"
)

// ========================================
// SuspendSignal - Unified Workflow Suspension
// ========================================

// SuspendType represents the type of workflow suspension.
type SuspendType int

const (
	// SuspendForTimer indicates the workflow is waiting for a timer to expire.
	SuspendForTimer SuspendType = iota
	// SuspendForSignal indicates the workflow is waiting for a signal.
	SuspendForSignal
)

func (t SuspendType) String() string {
	switch t {
	case SuspendForTimer:
		return "timer"
	case SuspendForSignal:
		return "signal"
	default:
		return "unknown"
	}
}

// SuspendSignal is returned when a workflow needs to suspend execution.
// It implements the error interface for compatibility with Go's error handling,
// but it is NOT an error - it's a control flow signal.
//
// When a workflow function returns a SuspendSignal, callers should propagate it:
//
//	if err := selvedge.Sleep(ctx, time.Hour); err != nil {
//	    return result, err  // Propagate the suspend signal
//	}
//
// The replay engine will detect SuspendSignal and handle the suspension appropriately.
type SuspendSignal struct {
	// Type indicates what the workflow is waiting for
	Type SuspendType

	// InstanceID is the workflow instance being suspended
	InstanceID string

	// For SuspendForTimer
	TimerID   string
	ExpiresAt time.Time

	// For SuspendForSignal (signal waiting) or Timer
	Channel    string // Signal channel name
	ActivityID string // Activity ID for replay matching (used for both signal and timer)
}

func (s *SuspendSignal) Error() string {
	switch s.Type {
	case SuspendForTimer:
		return fmt.Sprintf("workflow suspended: waiting for timer %q until %v", s.TimerID, s.ExpiresAt)
	case SuspendForSignal:
		return fmt.Sprintf("workflow suspended: waiting for signal on %q", s.Channel)
	default:
		return "workflow suspended"
	}
}

// IsSuspendSignal returns true if the error is a SuspendSignal.
func IsSuspendSignal(err error) bool {
	var sig *SuspendSignal
	return errors.As(err, &sig)
}

// AsSuspendSignal extracts the SuspendSignal from an error if present.
// Returns nil if the error is not a SuspendSignal.
func AsSuspendSignal(err error) *SuspendSignal {
	var sig *SuspendSignal
	if errors.As(err, &sig) {
		return sig
	}
	return nil
}

// NewTimerSuspend creates a SuspendSignal for timer waiting.
func NewTimerSuspend(instanceID, timerID, activityID string, expiresAt time.Time) *SuspendSignal {
	return &SuspendSignal{
		Type:       SuspendForTimer,
		InstanceID: instanceID,
		TimerID:    timerID,
		ExpiresAt:  expiresAt,
		ActivityID: activityID,
	}
}

// NewSignalSuspend creates a SuspendSignal for signal waiting.
func NewSignalSuspend(instanceID, channel, activityID string) *SuspendSignal {
	return &SuspendSignal{
		Type:       SuspendForSignal,
		InstanceID: instanceID,
		Channel:    channel,
		ActivityID: activityID,
	}
}
