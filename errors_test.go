package selvedge

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTerminalError(t *testing.T) {
	tests := []struct {
		name       string
		innerErr   error
		wantMsg    string
		wantUnwrap error
	}{
		{
			name:       "with simple error",
			innerErr:   errors.New("some error"),
			wantMsg:    "terminal error: some error",
			wantUnwrap: errors.New("some error"),
		},
		{
			name:       "with formatted error",
			innerErr:   fmt.Errorf("error code: %d", 42),
			wantMsg:    "terminal error: error code: 42",
			wantUnwrap: fmt.Errorf("error code: %d", 42),
		},
		{
			name:       "with nil error",
			innerErr:   nil,
			wantMsg:    "terminal error: <nil>",
			wantUnwrap: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			termErr := NewTerminalError(tt.innerErr)

			assert.Equal(t, tt.wantMsg, termErr.Error())
			if tt.wantUnwrap != nil {
				assert.Equal(t, tt.wantUnwrap.Error(), termErr.Unwrap().Error())
			} else {
				assert.Nil(t, termErr.Unwrap())
			}
		})
	}
}

func TestNewTerminalErrorf(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		args    []any
		wantMsg string
	}{
		{
			name:    "with string argument",
			format:  "failed: %s",
			args:    []any{"timeout"},
			wantMsg: "terminal error: failed: timeout",
		},
		{
			name:    "with multiple arguments",
			format:  "error at step %d: %s",
			args:    []any{3, "validation failed"},
			wantMsg: "terminal error: error at step 3: validation failed",
		},
		{
			name:    "no arguments",
			format:  "simple error",
			args:    nil,
			wantMsg: "terminal error: simple error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			termErr := NewTerminalErrorf(tt.format, tt.args...)
			assert.Equal(t, tt.wantMsg, termErr.Error())
		})
	}
}

func TestIsTerminalError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "regular error",
			err:  errors.New("regular error"),
			want: false,
		},
		{
			name: "terminal error",
			err:  NewTerminalError(errors.New("inner")),
			want: true,
		},
		{
			name: "wrapped terminal error",
			err:  fmt.Errorf("wrapped: %w", NewTerminalError(errors.New("inner"))),
			want: true,
		},
		{
			name: "double wrapped terminal error",
			err:  fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", NewTerminalError(errors.New("core")))),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsTerminalError(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRetryExhaustedError(t *testing.T) {
	tests := []struct {
		name         string
		activityName string
		attempts     int
		lastErr      error
		wantContains []string
	}{
		{
			name:         "basic case",
			activityName: "send_email",
			attempts:     3,
			lastErr:      errors.New("connection refused"),
			wantContains: []string{"send_email", "3 attempts", "connection refused"},
		},
		{
			name:         "single attempt",
			activityName: "validate",
			attempts:     1,
			lastErr:      errors.New("invalid input"),
			wantContains: []string{"validate", "1 attempts", "invalid input"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &RetryExhaustedError{
				ActivityName: tt.activityName,
				Attempts:     tt.attempts,
				LastErr:      tt.lastErr,
			}

			errMsg := err.Error()
			for _, substr := range tt.wantContains {
				assert.Contains(t, errMsg, substr)
			}

			assert.Equal(t, tt.lastErr, err.Unwrap())
		})
	}
}

func TestRetryExhaustedError_ErrorsIs(t *testing.T) {
	innerErr := errors.New("connection timeout")
	retryErr := &RetryExhaustedError{
		ActivityName: "fetch_data",
		Attempts:     5,
		LastErr:      innerErr,
	}

	assert.True(t, errors.Is(retryErr, innerErr))
}

func TestWorkflowCancelledError(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		reason     string
		wantMsg    string
	}{
		{
			name:       "with reason",
			instanceID: "inst-123",
			reason:     "user requested",
			wantMsg:    "workflow inst-123 cancelled: user requested",
		},
		{
			name:       "without reason",
			instanceID: "inst-456",
			reason:     "",
			wantMsg:    "workflow inst-456 cancelled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &WorkflowCancelledError{
				InstanceID: tt.instanceID,
				Reason:     tt.reason,
			}
			assert.Equal(t, tt.wantMsg, err.Error())
		})
	}
}

func TestWorkflowNotFoundError(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		wantMsg    string
	}{
		{
			name:       "simple ID",
			instanceID: "wf-001",
			wantMsg:    "workflow instance wf-001 not found",
		},
		{
			name:       "UUID style ID",
			instanceID: "550e8400-e29b-41d4-a716-446655440000",
			wantMsg:    "workflow instance 550e8400-e29b-41d4-a716-446655440000 not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &WorkflowNotFoundError{InstanceID: tt.instanceID}
			assert.Equal(t, tt.wantMsg, err.Error())
		})
	}
}

func TestLockAcquisitionError(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		workerID   string
		reason     string
		wantMsg    string
	}{
		{
			name:       "lock held by another worker",
			instanceID: "inst-1",
			workerID:   "worker-a",
			reason:     "lock held by worker-b",
			wantMsg:    "failed to acquire lock for instance inst-1 (worker: worker-a): lock held by worker-b",
		},
		{
			name:       "timeout",
			instanceID: "inst-2",
			workerID:   "worker-c",
			reason:     "timeout after 5s",
			wantMsg:    "failed to acquire lock for instance inst-2 (worker: worker-c): timeout after 5s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &LockAcquisitionError{
				InstanceID: tt.instanceID,
				WorkerID:   tt.workerID,
				Reason:     tt.reason,
			}
			assert.Equal(t, tt.wantMsg, err.Error())
		})
	}
}

func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{
			name:    "ErrWorkflowAlreadyCompleted",
			err:     ErrWorkflowAlreadyCompleted,
			wantMsg: "workflow already completed",
		},
		{
			name:    "ErrWorkflowAlreadyCancelled",
			err:     ErrWorkflowAlreadyCancelled,
			wantMsg: "workflow already cancelled",
		},
		{
			name:    "ErrWorkflowAlreadyFailed",
			err:     ErrWorkflowAlreadyFailed,
			wantMsg: "workflow already failed",
		},
		{
			name:    "ErrInvalidWorkflowState",
			err:     ErrInvalidWorkflowState,
			wantMsg: "invalid workflow state",
		},
		{
			name:    "ErrHistoryKeyConflict",
			err:     ErrHistoryKeyConflict,
			wantMsg: "history key holds another command",
		},
		{
			name:    "ErrDeterminismViolation",
			err:     ErrDeterminismViolation,
			wantMsg: "determinism violation during replay",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantMsg, tt.err.Error())
		})
	}
}

func TestSentinelErrors_ErrorsIs(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wrappedIn error
	}{
		{
			name:      "ErrWorkflowAlreadyCompleted wrapped",
			err:       ErrWorkflowAlreadyCompleted,
			wrappedIn: fmt.Errorf("failed: %w", ErrWorkflowAlreadyCompleted),
		},
		{
			name:      "ErrWorkflowAlreadyCancelled wrapped",
			err:       ErrWorkflowAlreadyCancelled,
			wrappedIn: fmt.Errorf("failed: %w", ErrWorkflowAlreadyCancelled),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, errors.Is(tt.wrappedIn, tt.err))
		})
	}
}
