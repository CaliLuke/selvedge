package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestErrorEnvelopeSnakeCaseRoundTrip(t *testing.T) {
	for _, original := range []error{
		NewNonRetryableApplicationError("stop", "validation", nil, nil),
		&RetryExhaustedError{ActivityName: "task", Attempts: 3, LastErr: context.Canceled},
		&NondeterminismError{Seq: 2, Key: "key", Recorded: "sleep", Issued: "receive", Reason: "changed"},
		&WorkflowPanicError{Value: "boom", Stack: "stack"},
	} {
		t.Run(fmt.Sprintf("%T", original), func(t *testing.T) {
			data := encodeError(original)
			var wire map[string]any
			require.NoError(t, json.Unmarshal(data, &wire))
			require.Contains(t, wire, "message")
			for _, key := range []string{"Type", "Message", "NonRetryable", "Activity", "Attempts"} {
				require.NotContains(t, wire, key)
			}
			after := decodeError(data)
			require.EqualError(t, after, original.Error())
			switch before := original.(type) {
			case *ApplicationError:
				require.Equal(t, true, wire["non_retryable"])
				require.Equal(t, before.Type, wire["type"])
			case *RetryExhaustedError:
				require.Equal(t, before.ActivityName, wire["activity"])
				require.Equal(t, float64(before.Attempts), wire["attempts"])
			case *NondeterminismError:
				var got *NondeterminismError
				require.ErrorAs(t, after, &got)
				require.Equal(t, before, got)
				require.ErrorIs(t, after, ErrDeterminismViolation)
				require.Contains(t, wire["details"], "seq")
			case *WorkflowPanicError:
				var got *WorkflowPanicError
				require.ErrorAs(t, after, &got)
				require.Equal(t, before, got)
				require.Contains(t, wire["details"], "stack")
			}
		})
	}
}

func TestErrorEnvelopePreservesMultipleCauses(t *testing.T) {
	application := NewApplicationError("invalid", "validation", nil, context.Canceled)
	for _, original := range []error{
		errors.Join(application, context.DeadlineExceeded),
		fmt.Errorf("two causes: %w and %w", application, context.DeadlineExceeded),
		fmt.Errorf("outer: %w", errors.Join(application, errors.Join(context.DeadlineExceeded, ErrHistoryKeyConflict))),
	} {
		after := decodeError(encodeError(original))
		require.EqualError(t, after, original.Error())
		require.ErrorIs(t, after, context.Canceled)
		require.ErrorIs(t, after, context.DeadlineExceeded)
		var got *ApplicationError
		require.ErrorAs(t, after, &got)
		require.Equal(t, application.Type, got.Type)
		require.Equal(t, errors.Is(original, ErrHistoryKeyConflict), errors.Is(after, ErrHistoryKeyConflict))
	}
}

func TestReplayDecodeErrorEnvelopeRoundTrip(t *testing.T) {
	before := &ReplayDecodeError{InstanceID: "instance", Key: "key", GoType: "bool", Err: context.DeadlineExceeded}
	after := decodeError(encodeError(before))
	var got *ReplayDecodeError
	require.ErrorAs(t, after, &got)
	require.EqualError(t, got, before.Error())
	require.Equal(t, before.InstanceID, got.InstanceID)
	require.Equal(t, before.Key, got.Key)
	require.Equal(t, before.GoType, got.GoType)
	require.ErrorIs(t, got, context.DeadlineExceeded)
}

func TestCorruptSignalCancellationEnvelopeFailsDurably(t *testing.T) {
	testCorruptSignalCancellationEnvelopeFailsDurably(t, createTestApp)
}
func testCorruptSignalCancellationEnvelopeFailsDurably(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	w := DefineWorkflow("corrupt_signal", func(ctx *WorkflowContext, _ string) (string, error) {
		_, _ = Receive[string](ctx, "inbox")
		return "caught", nil
	})
	id := seedErrorWorkflow(t, h, w)
	requireRecordedHistory(t, h.app.storage, h.app.ctx, &storage.HistoryEvent{InstanceID: id, ActivityID: "#1", Seq: 1, Command: "receive:inbox", EventType: storage.HistorySignalCancelled, EventData: []byte(`{"v":1,"kind":"application","cause":4}`), DataType: "json"})
	var live *ReplayDecodeError
	require.ErrorAs(t, h.RunOnce(id), &live)
	result, err := GetWorkflowResult[string](h.app.ctx, h.app, id)
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	var recorded *ReplayDecodeError
	require.ErrorAs(t, result.Error, &recorded)
	require.Equal(t, id, recorded.InstanceID)
	require.Equal(t, "#1", recorded.Key)
	require.Equal(t, "selvedge.errorEnvelope", recorded.GoType)
}
