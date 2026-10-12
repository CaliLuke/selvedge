// Package selvedge provides a durable execution framework for Go.
package selvedge

import (
	"context"
	"errors"
	"fmt"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// TerminalError wraps an error to indicate it should not be retried.
// When an activity returns a TerminalError, the workflow will fail
// without attempting any retries.
type TerminalError struct {
	Err error
}

func (e *TerminalError) Error() string {
	return fmt.Sprintf("terminal error: %v", e.Err)
}

func (e *TerminalError) Unwrap() error {
	return e.Err
}

// NewTerminalError creates a new TerminalError wrapping the given error.
func NewTerminalError(err error) *TerminalError {
	return &TerminalError{Err: err}
}

// NewTerminalErrorf creates a new TerminalError with a formatted message.
func NewTerminalErrorf(format string, args ...any) *TerminalError {
	return &TerminalError{Err: fmt.Errorf(format, args...)}
}

// IsTerminalError returns true if the error is or wraps a TerminalError.
func IsTerminalError(err error) bool {
	var terminalErr *TerminalError
	return errors.As(err, &terminalErr)
}

// RetryExhaustedError indicates that all retry attempts have been exhausted.
type RetryExhaustedError struct {
	ActivityName string
	Attempts     int
	LastErr      error
}

func (e *RetryExhaustedError) Error() string {
	return fmt.Sprintf("retry exhausted for activity %q after %d attempts: %v",
		e.ActivityName, e.Attempts, e.LastErr)
}

func (e *RetryExhaustedError) Unwrap() error {
	return e.LastErr
}

// WorkflowCancelledError indicates that a workflow has been cancelled.
type WorkflowCancelledError struct {
	InstanceID string
	Reason     string
}

func (e *WorkflowCancelledError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("workflow %s cancelled: %s", e.InstanceID, e.Reason)
	}
	return fmt.Sprintf("workflow %s cancelled", e.InstanceID)
}

// Unwrap makes workflow cancellation match context.Canceled.
func (e *WorkflowCancelledError) Unwrap() error { return context.Canceled }

// WorkflowNotFoundError indicates that a workflow instance was not found.
type WorkflowNotFoundError struct {
	InstanceID string
}

func (e *WorkflowNotFoundError) Error() string {
	return fmt.Sprintf("workflow instance %s not found", e.InstanceID)
}

// LockAcquisitionError indicates failure to acquire a workflow lock.
type LockAcquisitionError struct {
	InstanceID string
	WorkerID   string
	Reason     string
}

func (e *LockAcquisitionError) Error() string {
	return fmt.Sprintf("failed to acquire lock for instance %s (worker: %s): %s",
		e.InstanceID, e.WorkerID, e.Reason)
}

// ErrWorkflowAlreadyCompleted indicates an operation on a completed workflow.
var ErrWorkflowAlreadyCompleted = errors.New("workflow already completed")

// ErrWorkflowAlreadyCancelled indicates an operation on a cancelled workflow.
var ErrWorkflowAlreadyCancelled = errors.New("workflow already cancelled")

// ErrWorkflowAlreadyFailed indicates an operation on a failed workflow.
var ErrWorkflowAlreadyFailed = errors.New("workflow already failed")

// ErrInvalidWorkflowState indicates an invalid workflow state transition.
var ErrInvalidWorkflowState = errors.New("invalid workflow state")

// ErrHistoryKeyConflict indicates duplicate claims or conflicting stored commands.
var ErrHistoryKeyConflict = storage.ErrHistoryKeyConflict

// ErrWorkflowAlreadyStarted indicates that an instance ID already exists.
var ErrWorkflowAlreadyStarted = storage.ErrWorkflowAlreadyStarted

// ErrDeterminismViolation indicates non-deterministic behavior during replay.
var ErrDeterminismViolation = replay.ErrDeterminismViolation

// WorkflowPanicError preserves a recovered workflow panic and its stack.
type WorkflowPanicError = replay.WorkflowPanicError

// ApplicationError carries a portable activity or workflow failure.
// Details must be JSON-serializable and is returned as JSON-decoded data.
type ApplicationError struct {
	Type         string
	Message      string
	NonRetryable bool
	Details      any
	Cause        error
}

// Error returns the message, followed by the cause when one is set.
func (e *ApplicationError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// Unwrap returns the cause, so errors.Is and errors.As see through the error.
func (e *ApplicationError) Unwrap() error {
	return e.Cause
}

// NewApplicationError constructs a retryable application failure.
func NewApplicationError(message, errType string, details any, cause error) *ApplicationError {
	return &ApplicationError{Type: errType, Message: message, Details: details, Cause: cause}
}

// NewNonRetryableApplicationError constructs a failure that must not be retried.
func NewNonRetryableApplicationError(message, errType string, details any, cause error) *ApplicationError {
	return &ApplicationError{Type: errType, Message: message, NonRetryable: true, Details: details, Cause: cause}
}

func nonRetryableApplication(err error) bool {
	for err != nil {
		if app, ok := err.(*ApplicationError); ok && app.NonRetryable {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// ErrLeaseLost means the execution abandoned its expired local lease belief.
var ErrLeaseLost = replay.ErrLeaseLost

// ErrFenced means another execution owns the instance.
var ErrFenced = storage.ErrFenced

// NondeterminismError reports a replay command divergence.
type NondeterminismError = replay.NondeterminismError
