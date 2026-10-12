package selvedge

import (
	"context"
	"encoding/json"
	"fmt"
)

// ActivityTaskInfo describes the recorded policy and execution state of an activity.
type ActivityTaskInfo struct {
	Key, Name, Queue, Status string
	Seq                      int64
	Options                  ActivityOptions
	Attempts, Failures       int
	LastError                error
}

// ListActivityTasks returns the instance's activities in reservation order.
func ListActivityTasks(ctx context.Context, app *App, id string) ([]ActivityTaskInfo, error) {
	if app.closed.Load() || app.storage == nil {
		return nil, ErrAppNotStarted
	}
	instance, err := app.storage.GetInstance(ctx, id)
	if err != nil {
		return nil, err
	}
	if instance == nil {
		return nil, &WorkflowNotFoundError{InstanceID: id}
	}
	rows, err := app.storage.ListActivityTasks(ctx, id)
	if err != nil {
		return nil, err
	}
	result := make([]ActivityTaskInfo, 0, len(rows))
	for _, row := range rows {
		var options ActivityOptions
		if err := json.Unmarshal(row.Options, &options); err != nil {
			return nil, fmt.Errorf("decode recorded activity options: %w", err)
		}
		var lastError error
		if len(row.LastError) > 0 {
			lastError = decodeError(row.LastError)
		}
		result = append(result, ActivityTaskInfo{Key: row.ActivityID, Name: row.ActivityName, Seq: row.Seq, Queue: row.Queue, Options: cloneActivityOptions(options), Status: row.Status, Attempts: row.Attempts, Failures: row.Failures, LastError: lastError})
	}
	return result, nil
}
