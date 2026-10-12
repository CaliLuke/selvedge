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

// decodeActivityOutcome decodes a future without observing its failure yet.
func decodeActivityOutcome[O any](event *storage.HistoryEvent) (O, error) {
	if event.EventType == storage.HistoryActivityFailed || event.EventType == storage.HistoryActivityCancelled {
		var zero O
		return zero, decodeRecordedError(event)
	}
	return decodeRecorded[O](event)
}

func failActivityOutcome(ctx *WorkflowContext, err error) {
	var decodeErr *ReplayDecodeError
	if ctx.execCtx != nil && (isActivityPanic(err) || errors.As(err, &decodeErr)) {
		ctx.execCtx.Fail(err)
	}
}

// decodeRecordedError attaches row identity without making the outcome observed.
func decodeRecordedError(event *storage.HistoryEvent) error {
	err := decodeError(event.EventData)
	var decodeErr *ReplayDecodeError
	if errors.As(err, &decodeErr) && decodeErr.InstanceID == "" && decodeErr.Key == "" {
		decodeErr.InstanceID = event.InstanceID
		decodeErr.Key = event.ActivityID
	}
	return err
}

// replayRecordedError observes a recorded failure and makes corrupt or panic
// outcomes sticky, even when workflow code ignores the returned error.
func replayRecordedError(ctx *WorkflowContext, event *storage.HistoryEvent) error {
	err := decodeRecordedError(event)
	failActivityOutcome(ctx, err)
	return err
}
