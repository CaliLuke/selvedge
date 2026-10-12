package selvedge

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// ReplayDecodeError reports a recorded value that cannot be decoded into the
// Go output type. It is a workflow failure, not a retryable activity failure.
type ReplayDecodeError struct {
	InstanceID string
	Key        string
	GoType     string
	Err        error
}

func (e *ReplayDecodeError) Error() string {
	return fmt.Sprintf("decode recorded value for instance %s key %s into %s: %v", e.InstanceID, e.Key, e.GoType, e.Err)
}

func (e *ReplayDecodeError) Unwrap() error { return e.Err }

func decodeRecorded[O any](event *storage.HistoryEvent) (O, error) {
	var value O
	if err := json.Unmarshal(event.EventData, &value); err != nil {
		var zero O
		return zero, &ReplayDecodeError{
			InstanceID: event.InstanceID, Key: event.ActivityID,
			GoType: reflect.TypeFor[O]().String(), Err: err,
		}
	}
	return value, nil
}

func decodeActivityRecorded[O any](ctx *WorkflowContext, event *storage.HistoryEvent) (O, error) {
	if event.EventType == storage.HistoryActivityFailed {
		var zero O
		return zero, replayRecordedError(ctx, event)
	}
	value, err := decodeRecorded[O](event)
	if err != nil && ctx.execCtx != nil {
		ctx.execCtx.Fail(err)
	}
	return value, err
}

// replayRecordedError attaches row identity to corrupt payloads and uses the
// execution's existing sticky failure mechanism.
func replayRecordedError(ctx *WorkflowContext, event *storage.HistoryEvent) error {
	err := decodeError(event.EventData)
	var decodeErr *ReplayDecodeError
	if errors.As(err, &decodeErr) {
		if decodeErr.InstanceID == "" && decodeErr.Key == "" {
			decodeErr.InstanceID = event.InstanceID
			decodeErr.Key = event.ActivityID
		}
		if ctx.execCtx != nil {
			ctx.execCtx.Fail(err)
		}
	}
	return err
}
