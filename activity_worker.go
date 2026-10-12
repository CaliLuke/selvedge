package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// ActivityInfo is immutable metadata for the current durable activity claim.
type ActivityInfo struct {
	InstanceID, WorkflowName, ActivityID, ActivityName string
	Attempt                                            int
}
type activityInfoKey struct{}

// GetActivityInfo returns a copy of claim metadata, or nil outside an activity.
func GetActivityInfo(ctx context.Context) *ActivityInfo {
	info, ok := ctx.Value(activityInfoKey{}).(ActivityInfo)
	if !ok {
		return nil
	}
	return &info
}

// ErrActivityNameConflict rejects different definitions registered under one name.
var ErrActivityNameConflict = errors.New("activity name already registered with another definition")
var errClaimLost = errors.New("activity claim lost")

type activityDefinition interface {
	run(context.Context, ActivityInfo, hooks.WorkflowHooks, []byte) (any, error)
}

func (a *Activity[I, O]) run(ctx context.Context, info ActivityInfo, h hooks.WorkflowHooks, data []byte) (any, error) {
	var input I
	if err := json.Unmarshal(data, &input); err != nil {
		return nil, fmt.Errorf("decode activity input: %w", err)
	}
	return a.executeWithRetry(ctx, info, h, input)
}

// RegisterActivity makes an activity available for recovered tasks on this App.
// Register the same definition pointer again freely; a different definition is a conflict.
func RegisterActivity[I, O any](app *App, a *Activity[I, O]) error {
	app.activitiesMu.Lock()
	defer app.activitiesMu.Unlock()
	if existing, ok := app.activities[a.name]; ok && existing != a {
		return fmt.Errorf("%s: %w", a.name, ErrActivityNameConflict)
	}
	app.activities[a.name] = a
	app.wakeActivityWorkers()
	return nil
}
func (a *App) registeredActivityNames() []string {
	a.activitiesMu.RLock()
	defer a.activitiesMu.RUnlock()
	names := make([]string, 0, len(a.activities))
	for name := range a.activities {
		names = append(names, name)
	}
	return names
}
func (a *App) wakeActivityWorkers() {
	select {
	case a.activityWakeEvent <- struct{}{}:
	default:
	}
}

// runActivityTask is shared by pool and inline execution; the durable claim has committed.
func (a *App) runActivityTask(parent context.Context, s storage.Storage, task *storage.ActivityTask, check func() error) error {
	// An interrupted attempt is retryable work; release with a context independent of shutdown.
	completed := false
	defer func() {
		if !completed {
			releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
			defer releaseCancel()
			if err := s.ReleaseActivityTask(releaseCtx, task.Claim()); err != nil && !errors.Is(err, storage.ErrFenced) {
				slog.Debug("release activity claim", "error", err)
			}
			a.wakeActivityWorkers()
		}
	}()
	a.activitiesMu.RLock()
	definition := a.activities[task.ActivityName]
	a.activitiesMu.RUnlock()
	if definition == nil {
		return fmt.Errorf("unregistered activity %q", task.ActivityName)
	}
	inst, err := s.GetInstance(parent, task.InstanceID)
	if err != nil {
		return err
	}
	if inst == nil {
		return storage.ErrInstanceNotFound
	}
	info := ActivityInfo{InstanceID: task.InstanceID, WorkflowName: inst.WorkflowName, ActivityID: task.ActivityID, ActivityName: task.ActivityName, Attempt: task.Attempts}
	ctx, cancel := context.WithCancelCause(context.WithValue(parent, activityInfoKey{}, info))
	done := make(chan struct{})
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer cancel(nil)
	go a.renewActivityClaim(renewCtx, cancel, s, task.Claim(), check, done)
	stopped := false
	stop := func() {
		if !stopped {
			stopRenew()
			<-done
			stopped = true
		}
	}
	defer stop()

	result, runErr := definition.run(ctx, info, a.hooks, task.InputData)
	if check != nil {
		if err := check(); err != nil {
			cancel(err)
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	var data []byte
	event := storage.HistoryActivityCompleted
	if runErr != nil {
		event = storage.HistoryActivityFailed
		data = encodeError(runErr)
	} else {
		data, err = json.Marshal(result)
		if err != nil {
			event = storage.HistoryActivityFailed
			data = encodeError(fmt.Errorf("serialize activity result: %w", err))
		}
	}
	// Join renewal before completion so it cannot mistake our completed row for claim loss.
	stop()
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	err = s.CompleteActivityTask(parent, task.Claim(), event, data)
	if errors.Is(err, storage.ErrFenced) {
		slog.Debug("activity completion fenced", "instance_id", task.InstanceID, "activity_id", task.ActivityID)
	}
	if err == nil {
		completed = true
		a.wakeDispatcher()
	}
	return err
}
func (a *App) renewActivityClaim(ctx context.Context, cancel context.CancelCauseFunc, s storage.Storage, claim storage.TaskClaim, check func() error, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(a.config.lease.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if check != nil {
				if err := check(); err != nil {
					cancel(err)
					return
				}
			}
			if err := s.RenewActivityTask(ctx, claim, a.config.lease.TTL); err != nil {
				if ctx.Err() != nil {
					return
				}
				cancel(fmt.Errorf("%w: %v", errClaimLost, err))
				return
			}
		}
	}
}

func (a *App) runActivityWorkers() {
	defer a.wg.Done()
	empty := 0
	for a.ctx.Err() == nil {
		count, err := a.dispatchActivityTasks()
		if err != nil {
			slog.Debug("claim activity tasks", "error", err)
		}
		if count == 0 {
			empty++
		} else {
			empty = 0
		}
		delay := addJitterFraction(calculateBackoff(a.config.activityPollInterval, empty, 60*time.Second), 0.3)
		timer := time.NewTimer(delay)
		select {
		case <-a.ctx.Done():
			timer.Stop()
			return
		case <-a.activityWakeEvent:
			timer.Stop()
			empty = 0
		case <-timer.C:
		}
	}
}

func (a *App) dispatchActivityTasks() (int, error) {
	names := a.registeredActivityNames()
	count := 0
	for a.ctx.Err() == nil && a.activitySem.TryAcquire(1) {
		tasks, err := a.storage.ClaimActivityTasks(a.ctx, a.config.workerID+"/"+uuid.NewString(), a.config.lease.TTL, 1, names...)
		if err != nil || len(tasks) == 0 {
			a.activitySem.Release(1)
			return count, err
		}
		task := tasks[0]
		count++
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			defer func() { a.activitySem.Release(1); a.wakeActivityWorkers() }()
			if err := a.runActivityTask(a.ctx, a.storage, task, nil); err != nil {
				slog.Debug("activity attempt stopped", "instance_id", task.InstanceID, "error", err)
			}
		}()
	}
	return count, nil
}
