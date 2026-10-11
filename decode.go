package selvedge

import (
	"encoding/json"
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
	value, err := decodeRecorded[O](event)
	if err != nil && ctx.execCtx != nil {
		ctx.execCtx.Fail(err)
	}
	return value, err
}

// Preserve the existing string-only error replay until S2 introduces envelopes.
func replayRecordedError(event *storage.HistoryEvent) error {
	var message string
	_ = json.Unmarshal(event.EventData, &message)
	return fmt.Errorf("%s", message)
}
