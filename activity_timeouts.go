package selvedge

import (
	"context"
	"errors"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func (a *App) checkActivityTimeouts(ctx context.Context) error {
	if a.storage == nil {
		return nil
	}
	candidates, err := a.storage.FindActivityTimeouts(ctx, a.config.maxTimersPerBatch)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		task := candidate.Task
		var outcome error = &ActivityTimeoutError{Activity: task.ActivityName, Kind: TimeoutKind(candidate.Kind), Attempt: task.Attempts}
		if candidate.Exhausted {
			outcome = &RetryExhaustedError{ActivityName: task.ActivityName, Attempts: task.Failures + 1, LastErr: outcome}
		}
		inst, err := a.storage.GetInstance(ctx, task.InstanceID)
		if err != nil {
			return err
		}
		if inst == nil {
			continue
		}
		err = a.storage.TimeoutActivityTask(ctx, candidate, encodeError(outcome))
		if errors.Is(err, storage.ErrFenced) {
			continue
		}
		if err != nil {
			return err
		}
		a.wakeDispatcher()
		if a.hooks != nil {
			a.hooks.OnActivityFailed(ctx, hooks.ActivityFailedInfo{InstanceID: task.InstanceID, WorkflowName: inst.WorkflowName, ActivityID: task.ActivityID, ActivityName: task.ActivityName, Attempt: task.Attempts, Error: outcome})
		}
	}
	return nil
}
