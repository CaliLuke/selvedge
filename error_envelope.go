package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type errorEnvelope struct {
	V            int    `json:"v"`
	Kind         string `json:"kind"`
	Type         string
	Message      string
	NonRetryable bool
	Details      json.RawMessage `json:"details,omitempty"`
	Activity     string
	Attempts     int
	InstanceID   string         `json:"instance_id,omitempty"`
	Reason       string         `json:"reason,omitempty"`
	Cause        *errorEnvelope `json:"cause,omitempty"`
}

// errorCodec bridges the root error types into the replay engine without an import cycle.
type errorCodec struct{}

func (errorCodec) Encode(err error) []byte { return encodeError(err) }

func encodeError(err error) []byte {
	envelope := envelopeForError(err)
	data, marshalErr := json.Marshal(envelope)
	if marshalErr != nil {
		// Only application Details can fail to marshal; envelopeForError handles it.
		panic(marshalErr)
	}
	return data
}

func envelopeForError(err error) *errorEnvelope {
	if err == nil {
		return nil
	}
	ev := &errorEnvelope{V: 1, Kind: "generic", Message: err.Error()}
	// Match the current node, not errors.As/Is: those would discard outer wrappers.
	switch e := err.(type) {
	case *ApplicationError:
		ev.Kind = "application"
		ev.Type = e.Type
		ev.Message = e.Message
		ev.NonRetryable = e.NonRetryable
		details, marshalErr := json.Marshal(e.Details)
		if marshalErr != nil {
			ev.Kind = "generic"
			ev.Message = fmt.Sprintf("%s (failed to serialize error details: %v)", err, marshalErr)
		} else {
			ev.Details = details
		}
	case *TerminalError:
		ev.Kind = "terminal"
	case *RetryExhaustedError:
		ev.Kind = "retry_exhausted"
		ev.Activity = e.ActivityName
		ev.Attempts = e.Attempts
	case *WorkflowCancelledError:
		ev.Kind = "workflow_cancelled"
		ev.InstanceID = e.InstanceID
		ev.Reason = e.Reason
	default:
		switch err {
		case context.Canceled:
			ev.Kind = "canceled"
		case context.DeadlineExceeded:
			ev.Kind = "deadline_exceeded"
		case ErrHistoryKeyConflict:
			ev.Kind = "history_key_conflict"
		case ErrDeterminismViolation:
			ev.Kind = "nondeterminism"
		}
	}
	ev.Cause = envelopeForError(errors.Unwrap(err))
	return ev
}

// recordedError preserves a wrapper's full message and its single %w cause.
type recordedError struct {
	message string
	cause   error
}

func (e *recordedError) Error() string { return e.message }
func (e *recordedError) Unwrap() error { return e.cause }

func decodeError(data []byte) error {
	var ev *errorEnvelope
	if err := json.Unmarshal(data, &ev); err != nil {
		return &ReplayDecodeError{GoType: "selvedge.errorEnvelope", Err: err}
	}
	if ev == nil {
		return &ReplayDecodeError{GoType: "selvedge.errorEnvelope", Err: errors.New("expected an error envelope object")}
	}
	return errorFromEnvelope(ev)
}

func errorFromEnvelope(ev *errorEnvelope) error {
	if ev == nil {
		return nil
	}
	if ev.V != 1 {
		return errors.New(ev.Message)
	}
	cause := errorFromEnvelope(ev.Cause)
	switch ev.Kind {
	case "application":
		var details any
		if len(ev.Details) > 0 {
			if err := json.Unmarshal(ev.Details, &details); err != nil {
				return &ReplayDecodeError{GoType: "selvedge.errorEnvelope", Err: err}
			}
		}
		return &ApplicationError{Type: ev.Type, Message: ev.Message, NonRetryable: ev.NonRetryable, Details: details, Cause: cause}
	case "terminal":
		return &TerminalError{Err: cause}
	case "retry_exhausted":
		return &RetryExhaustedError{ActivityName: ev.Activity, Attempts: ev.Attempts, LastErr: cause}
	case "canceled":
		return context.Canceled
	case "deadline_exceeded":
		return context.DeadlineExceeded
	case "workflow_cancelled":
		return &WorkflowCancelledError{InstanceID: ev.InstanceID, Reason: ev.Reason}
	case "history_key_conflict":
		return ErrHistoryKeyConflict
	case "nondeterminism":
		return ErrDeterminismViolation
	case "generic":
		return &recordedError{message: ev.Message, cause: cause}
	default:
		return errors.New(ev.Message)
	}
}
