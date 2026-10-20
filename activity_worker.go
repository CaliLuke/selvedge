package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/retry"
)

// ActivityInfo is immutable metadata for the current durable activity claim.
type ActivityInfo struct {
	InstanceID, WorkflowName, ActivityID, ActivityName string
	Attempt                                            int
	Options                                            ActivityOptions
}
type activityInfoKey struct{}

// GetActivityInfo returns a copy of claim metadata, or nil outside an activity.
func GetActivityInfo(ctx context.Context) *ActivityInfo {
	info, ok := ctx.Value(activityInfoKey{}).(ActivityInfo)
	if !ok {
		return nil
	}
	info.Options = cloneActivityOptions(info.Options)
	return &info
}

// ErrActivityNameConflict rejects different definitions registered under one name.
var ErrActivityNameConflict = errors.New("activity name already registered with another definition")
var errClaimLost = errors.New("activity claim lost")

type activityDefinition interface {
	run(context.Context, []byte) (any, error)
	nonRetryable(error) bool
}

func (a *Activity[I, O]) run(ctx context.Context, data []byte) (any, error) {
	var input I
	if err := json.Unmarshal(data, &input); err != nil {
		return nil, NewTerminalError(fmt.Errorf("decode activity input: %w", err))
	}
	return a.invoke(ctx, input)
}
func (a *Activity[I, O]) nonRetryable(err error) bool {
	if IsTerminalError(err) || nonRetryableApplication(err) || isActivityPanic(err) {
		return true
	}
	if a.options.RetryPolicy != nil {
		for _, target := range a.options.RetryPolicy.NonRetryableErrors {
			if errors.Is(err, target) {
				return true
			}
		}
	}
	return false
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
	var options ActivityOptions
	if err := json.Unmarshal(task.Options, &options); err != nil {
		return fmt.Errorf("decode recorded activity options: %w", err)
	}
	// Storage-level callers may reserve an empty policy. Its budget remains the row's budget.
	if options.RetryPolicy == nil {
		options.RetryPolicy = &retry.Policy{InitialInterval: 100 * time.Millisecond, MaxInterval: 10 * time.Second, Multiplier: 1}
	}
	options.RetryPolicy.MaxAttempts = task.MaxAttempts
	info := ActivityInfo{InstanceID: task.InstanceID, WorkflowName: inst.WorkflowName, ActivityID: task.ActivityID, ActivityName: task.ActivityName, Attempt: task.Attempts, Options: cloneActivityOptions(options)}
	heartbeat := &activityHeartbeat{}
	ctx, cancel := context.WithCancelCause(context.WithValue(context.WithValue(parent, activityInfoKey{}, info), activityHeartbeatKey{}, heartbeat))
	done := make(chan struct{})
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer cancel(nil)
	go a.renewActivityClaim(renewCtx, cancel, s, task, heartbeat, check, done)
	stopped := false
	stop := func() {
		if !stopped {
			stopRenew()
			<-done
			stopped = true
		}
	}
	defer stop()

	started := time.Now()
	attemptCtx := ctx
	timeoutCancel := func() {}
	if options.StartToCloseTimeout > 0 {
		claimStarted := task.ClaimStarted
		if claimStarted.IsZero() {
			claimStarted = started
		}
		attemptCtx, timeoutCancel = context.WithDeadline(ctx, claimStarted.Add(options.StartToCloseTimeout))
	}
	defer timeoutCancel()
	result, runErr := invokeActivityAttempt(attemptCtx, ctx, definition, task.InputData)
	if errors.Is(runErr, context.DeadlineExceeded) && errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
		runErr = &ActivityTimeoutError{Activity: task.ActivityName, Kind: TimeoutStartToClose, Attempt: task.Attempts}
	}
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
	terminal := definition.nonRetryable(runErr)
	if runErr == nil {
		data, err = json.Marshal(result)
		if err != nil {
			runErr = fmt.Errorf("serialize activity result: %w", err)
			terminal = true
		}
	}
	shouldRetry := false
	delay := time.Duration(0)
	if runErr != nil {
		event = storage.HistoryActivityFailed
		if !terminal {
			if task.MaxAttempts > 0 && task.Failures+1 >= task.MaxAttempts {
				runErr = &RetryExhaustedError{ActivityName: task.ActivityName, Attempts: task.Failures + 1, LastErr: runErr}
			} else {
				shouldRetry = true
				delay = options.RetryPolicy.GetDelay(task.Failures + 1)
			}
		}
		data = encodeError(runErr)
	}
	// Join renewal before the terminal write or requeue so our own commit is not mistaken for claim loss.
	stop()
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if shouldRetry {
		err = s.RetryActivityTask(parent, task.Claim(), data, delay)
	} else {
		err = s.CompleteActivityTask(parent, task.Claim(), event, data)
	}
	if errors.Is(err, storage.ErrFenced) {
		cancel(errClaimLost)
	}
	if err == nil {
		completed = true
		if shouldRetry {
			a.wakeActivityWorkers()
			if a.hooks != nil {
				a.hooks.OnActivityRetry(ctx, hooks.ActivityRetryInfo{InstanceID: info.InstanceID, WorkflowName: info.WorkflowName, ActivityID: info.ActivityID, ActivityName: info.ActivityName, Attempt: info.Attempt, MaxAttempts: task.MaxAttempts, NextDelay: delay, Error: runErr})
			}
		} else {
			a.wakeDispatcher()
			if a.hooks != nil {
				if runErr != nil {
					a.hooks.OnActivityFailed(ctx, hooks.ActivityFailedInfo{InstanceID: info.InstanceID, WorkflowName: info.WorkflowName, ActivityID: info.ActivityID, ActivityName: info.ActivityName, Attempt: info.Attempt, Duration: time.Since(started), Error: runErr})
				} else {
					a.hooks.OnActivityComplete(ctx, hooks.ActivityCompleteInfo{InstanceID: info.InstanceID, WorkflowName: info.WorkflowName, ActivityID: info.ActivityID, ActivityName: info.ActivityName, OutputData: result, Duration: time.Since(started)})
				}
			}
		}
	}

	return err
}
func (a *App) renewActivityClaim(ctx context.Context, cancel context.CancelCauseFunc, s storage.Storage, task *storage.ActivityTask, heartbeat *activityHeartbeat, check func() error, done chan<- struct{}) {
	defer close(done)
	interval := a.config.lease.RenewInterval
	if task.HeartbeatTimeoutMS > 0 {
		interval = min(interval, time.Duration(task.HeartbeatTimeoutMS)*time.Millisecond/3)
	}
	interval = max(interval, time.Millisecond)
	ticker := time.NewTicker(interval)
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
			if err := s.RenewActivityTask(ctx, task.Claim(), a.config.lease.TTL, task.HeartbeatTimeoutMS == 0 || heartbeat.progress.Swap(false)); err != nil {
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
		delay := cappedPollDelay(a.config.activityPollInterval, empty, a.config.maxPollInterval)
		if count == 0 {
			due, ok, err := a.storage.NextActivityDue(a.ctx, storage.TaskFilter{Names: a.registeredActivityNames(), Queues: a.config.activityQueues})
			if err == nil && ok && due < delay {
				if due > 0 {
					delay = due
				} else if a.activitySem.TryAcquire(1) {
					// Recheck immediately if the task became due after the empty claim.
					// A full pool instead waits for an attempt's completion wake.
					a.activitySem.Release(1)
					delay = 0
				}
			}
		}
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
		tasks, err := a.storage.ClaimActivityTasks(a.ctx, a.config.workerID+"/"+uuid.NewString(), a.config.lease.TTL, 1, storage.TaskFilter{Names: names, Queues: a.config.activityQueues})
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

// invokeActivityAttempt lets an expired claim stop waiting even if user code ignores cancellation.
// Its buffered result channel also permits late completion without holding a worker slot.
func invokeActivityAttempt(attemptCtx, claimCtx context.Context, definition activityDefinition, data []byte) (any, error) {
	type outcome struct {
		value any
		err   error
	}
	results := make(chan outcome, 1)
	go func() {
		returned := false
		defer func() {
			if value := recover(); value != nil {
				results <- outcome{err: &WorkflowPanicError{Value: fmt.Sprint(value), Stack: string(debug.Stack())}}
				return
			}
			if !returned {
				results <- outcome{err: &WorkflowPanicError{Value: "runtime.Goexit", Stack: string(debug.Stack())}}
			}
		}()
		value, err := definition.run(attemptCtx, data)
		returned = true
		results <- outcome{value, err}
	}()
	select {
	case result := <-results:
		return result.value, result.err
	case <-claimCtx.Done():
		return nil, context.Cause(claimCtx)
	case <-attemptCtx.Done():
		return nil, context.Cause(attemptCtx)
	}
}
