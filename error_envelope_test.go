package selvedge

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorEnvelopePreservesChains(t *testing.T) {
	application := NewApplicationError("invalid", "Validation", map[string]any{"count": float64(2)}, context.Canceled)
	for _, original := range []error{
		application,
		NewNonRetryableApplicationError("stop", "Stop", nil, nil),
		NewTerminalError(context.DeadlineExceeded),
		&RetryExhaustedError{ActivityName: "activity", Attempts: 4, LastErr: application},
		&WorkflowCancelledError{InstanceID: "instance", Reason: "requested"},
		context.Canceled, context.DeadlineExceeded, ErrDeterminismViolation,
		errors.New("plain"), NewTerminalError(nil),
		&RetryExhaustedError{ActivityName: "empty", Attempts: 1},
	} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%T/wrapped=%v", original, wrapped), func(t *testing.T) {
				before := original
				if wrapped {
					before = fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", before))
				}
				after := decodeError(encodeError(before))
				require.EqualError(t, after, before.Error())
				assertErrorShape(t, before, after)
				require.Equal(t, errors.Is(before, ErrDeterminismViolation), errors.Is(after, ErrDeterminismViolation))
				var expected, actual *RetryExhaustedError
				require.Equal(t, errors.As(before, &expected), errors.As(after, &actual))
				var app *ApplicationError
				if errors.As(after, &app) && app.Details != nil {
					require.Equal(t, map[string]any{"count": float64(2)}, app.Details)
				}
			})
		}
	}
}

func TestErrorEnvelopeUnserializableDetails(t *testing.T) {
	err := decodeError(encodeError(NewApplicationError("invalid", "Validation", make(chan int), context.Canceled)))
	require.Contains(t, err.Error(), "failed to serialize error details")
	require.ErrorIs(t, err, context.Canceled)
}
