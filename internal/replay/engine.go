package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// ErrDeterminismViolation indicates a changed command at a recorded key.
var ErrDeterminismViolation = errors.New("determinism violation during replay")

// ErrorCodec encodes workflow failures without depending on public error types.
type ErrorCodec interface{ Encode(error) []byte }

// Engine handles workflow execution with deterministic replay.
type Engine struct {
	storage  storage.Storage
	hooks    hooks.WorkflowHooks
	workerID string
	codec    ErrorCodec
	lease    LeaseConfig
	clock    func() Clock
}

// NewEngine creates a new replay engine.
func NewEngine(s storage.Storage, h hooks.WorkflowHooks, workerID string, codec ErrorCodec) *Engine {
	return &Engine{
		storage:  s,
		hooks:    h,
		workerID: workerID,
		codec:    codec,
		lease:    DefaultLeaseConfig(),
	}
}

// WorkflowRunner is a function that executes the workflow logic.
// It receives the context and returns the result or an error.
type WorkflowRunner func(ctx *ExecutionContext) (any, error)

// ExecutionContext provides context for workflow execution.
type ExecutionContext struct {
	ctx        context.Context
	instanceID string
	engine     *Engine
	fence      storage.Fence
	execution  *Execution

	// Activity tracking
	activityCounter map[string]int
	counterMu       sync.Mutex

	// Replay state
	history      []*storage.HistoryEvent
	historyIndex int
	isReplaying  bool

	// Raw recorded rows; values are decoded by their typed caller.
	claimed  map[string]string
	recorded map[string]*storage.HistoryEvent
	fatal    error

	// Replay statistics
	cacheHits     int
	newActivities int
}

// NewExecutionContext creates standalone execution tracking without storage.
func NewExecutionContext(ctx context.Context, instanceID string) *ExecutionContext {
	return &ExecutionContext{ctx: ctx, instanceID: instanceID, activityCounter: make(map[string]int), claimed: make(map[string]string), recorded: make(map[string]*storage.HistoryEvent)}
}

// Claim checks issue uniqueness and the recorded command before any side effect.
func (ec *ExecutionContext) Claim(key, cmd string) (*storage.HistoryEvent, bool, error) {
	if err := ec.Check(); err != nil {
		return nil, false, err
	}
	ec.counterMu.Lock()
	_, duplicate := ec.claimed[key]
	if ec.claimed == nil {
		ec.claimed = make(map[string]string)
	}
	if !duplicate {
		ec.claimed[key] = cmd
	}
	event, found := ec.recorded[key]
	ec.counterMu.Unlock()
	if duplicate {
		ec.Fail(storage.ErrHistoryKeyConflict)
		return nil, false, ec.Fatal()
	}
	if found && event.Command != cmd {
		ec.Fail(ErrDeterminismViolation)
		return nil, false, ec.Fatal()
	}
	if found {
		ec.counterMu.Lock()
		ec.cacheHits++
		ec.counterMu.Unlock()
	}
	return event, found, nil
}

// Reserve records a command with no value, as required before parking a sleep.
func (ec *ExecutionContext) Reserve(key, cmd string) (*storage.HistoryEvent, error) {
	if err := ec.Check(); err != nil {
		return nil, err
	}
	if ec.engine == nil {
		return nil, nil
	}
	event, err := ec.engine.storage.ReserveHistory(ec.ctx, &ec.fence, ec.instanceID, key, cmd)
	if errors.Is(err, storage.ErrHistoryKeyConflict) || errors.Is(err, storage.ErrFenced) || errors.Is(err, ErrLeaseLost) {
		ec.Fail(err)
	}
	return event, err
}

// Context returns the underlying context.Context.
func (ec *ExecutionContext) Context() context.Context {
	return ec.ctx
}

// InstanceID returns the workflow instance ID.
func (ec *ExecutionContext) InstanceID() string {
	return ec.instanceID
}

// IsReplaying returns true if the workflow is being replayed.
func (ec *ExecutionContext) IsReplaying() bool {
	return ec.isReplaying
}

// GenerateActivityID generates a unique activity ID for the given activity name.
func (ec *ExecutionContext) GenerateActivityID(activityName string) string {
	ec.counterMu.Lock()
	defer ec.counterMu.Unlock()

	ec.activityCounter[activityName]++
	return fmt.Sprintf("%s:%d", activityName, ec.activityCounter[activityName])
}

// Lookup retrieves a raw recorded row by its history key.
func (ec *ExecutionContext) Lookup(key string) (*storage.HistoryEvent, bool) {
	ec.counterMu.Lock()
	defer ec.counterMu.Unlock()
	event, ok := ec.recorded[key]
	if ok {
		ec.cacheHits++
	}
	return event, ok
}

// Fail marks an execution as failed even if workflow code catches the error.
func (ec *ExecutionContext) Fail(err error) {
	ec.counterMu.Lock()
	defer ec.counterMu.Unlock()
	if ec.fatal == nil {
		ec.fatal = err
	}
}

// Fatal returns the first failure that must terminate this execution.
func (ec *ExecutionContext) Fatal() error {
	ec.counterMu.Lock()
	defer ec.counterMu.Unlock()
	return ec.fatal
}

// RecordActivityID records that an activity has been executed.
func (ec *ExecutionContext) RecordActivityID(activityID string) {
	// This is called after activity execution to track which activities have run
}

// RecordActivityResultWithContext records an already-encoded activity outcome.
func (ec *ExecutionContext) RecordActivityResultWithContext(ctx context.Context, key string, eventType storage.HistoryEventType, payload []byte) (*storage.HistoryEvent, error) {
	if ec.execution != nil {
		if err := ec.execution.Check(); err != nil {
			ec.Fail(err)
			return nil, err
		}
	}
	ec.counterMu.Lock()
	ec.newActivities++
	cmd := ec.claimed[key]
	ec.counterMu.Unlock()
	if ec.engine == nil {
		return &storage.HistoryEvent{InstanceID: ec.instanceID, ActivityID: key, Command: cmd, EventType: eventType, EventData: payload, DataType: "json"}, nil
	}
	event, err := ec.engine.recordActivityPayload(ctx, ec.instanceID, key, cmd, eventType, payload)
	if errors.Is(err, storage.ErrHistoryKeyConflict) || errors.Is(err, storage.ErrFenced) || errors.Is(err, ErrLeaseLost) {
		ec.Fail(err)
	}
	if err != nil {
		return nil, err
	}
	ec.counterMu.Lock()
	ec.recorded[key] = event
	ec.counterMu.Unlock()
	return event, nil
}

// Storage returns the storage interface for advanced use cases.
func (ec *ExecutionContext) Storage() storage.Storage {
	if ec.engine == nil {
		return nil
	}
	return ec.engine.storage
}

// Hooks returns the workflow hooks for observability.
func (ec *ExecutionContext) Hooks() hooks.WorkflowHooks {
	if ec.engine == nil {
		return nil
	}
	return ec.engine.hooks
}

// Run acquires a fresh execution token and replays one dispatch.
func (e *Engine) Run(ctx context.Context, instanceID string, runner WorkflowRunner) error {
	clk := Clock(monotonicClock{})
	if e.clock != nil {
		clk = e.clock()
	}
	t0 := clk.Now()
	f, ok, err := e.storage.AcquireLease(ctx, instanceID, e.workerID+"/"+uuid.NewString(), e.lease.TTL)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	x := &Execution{fence: f, clk: clk, grantedAt: t0, ttl: e.lease.TTL, margin: e.lease.Margin}
	ctx = storage.ContextWithFence(ctx, f)
	ctx = storage.ContextWithCommitCheck(ctx, x.Check)
	ctx = storage.ContextWithFenceRejection(ctx, x.reject)
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go x.keepAlive(renewCtx, e.storage, e.lease.RenewInterval, done)
	err = x.Check()
	if err == nil {
		err = e.executeWorkflow(ctx, instanceID, runner, f, x)
	}
	cancel()
	<-done
	if ctx.Err() != nil {
		err = errors.Join(err, ErrLeaseLost)
	}
	if !errors.Is(err, storage.ErrFenced) {
		releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		releaseErr := e.storage.ReleaseLease(releaseCtx, f, err != nil)
		cancelRelease()
		if releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
	}
	return err
}

// executeWorkflow executes or replays a workflow.
func (e *Engine) executeWorkflow(ctx context.Context, instanceID string, runner WorkflowRunner, f storage.Fence, x *Execution) error {
	// Load history using iterator (streams from storage in batches)
	iter := storage.NewHistoryIterator(ctx, e.storage, instanceID, &storage.HistoryIteratorOptions{
		BatchSize: 1000,
	})
	defer func() { _ = iter.Close() }()

	// Keep raw rows even when their payload is invalid JSON. The typed caller
	// must fail decoding rather than treating a corrupt result as unrecorded.
	recorded := make(map[string]*storage.HistoryEvent)
	historyCount := 0
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		historyCount++
		recorded[event.ActivityID] = event
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to load history: %w", err)
	}

	// Create execution context
	isReplaying := historyCount > 0
	execCtx := &ExecutionContext{
		ctx:             ctx,
		instanceID:      instanceID,
		engine:          e,
		fence:           f,
		execution:       x,
		activityCounter: make(map[string]int),
		history:         nil, // History is processed via iterator, not stored
		historyIndex:    0,
		isReplaying:     isReplaying,
		recorded:        recorded,
		claimed:         make(map[string]string),
	}

	startTime := time.Now()

	// Call OnReplayStart hook if replaying
	if isReplaying {
		e.hooks.OnReplayStart(ctx, hooks.ReplayStartInfo{
			InstanceID:     instanceID,
			WorkflowName:   "",
			HistoryEvents:  historyCount,
			ResumeActivity: "", // Not tracked in current implementation
		})
	}

	// Call the workflow start hook
	e.hooks.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{
		InstanceID:   instanceID,
		WorkflowName: "",
		StartTime:    startTime,
	})

	// Execute the workflow
	result, err := runner(execCtx)
	if fatal := execCtx.Fatal(); fatal != nil {
		err = fatal
	}

	duration := time.Since(startTime)
	if checkErr := x.Check(); checkErr != nil {
		err = checkErr
	}
	if errors.Is(err, storage.ErrFenced) || errors.Is(err, ErrLeaseLost) || ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}

	if err != nil {
		// Check for SuspendSignal
		if sig := AsSuspendSignal(err); sig != nil {
			return e.handleSuspendSignal(ctx, instanceID, sig)
		}

		// Record workflow failure in history
		eventData := e.codec.Encode(err)
		_, writeErr := e.storage.RecordHistory(ctx, &f, &storage.HistoryEvent{
			InstanceID: instanceID,
			EventType:  storage.HistoryWorkflowFailed,
			ActivityID: "$workflow_failed",
			Command:    "close",
			EventData:  eventData,
			DataType:   "json",
		})

		if writeErr != nil {
			return errors.Join(err, fmt.Errorf("failed to record workflow failure: %w", writeErr))
		}
		// Mark as failed
		if statusErr := e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusFailed, err.Error()); statusErr != nil {
			return errors.Join(err, fmt.Errorf("failed to mark workflow failed: %w", statusErr))
		}

		e.hooks.OnWorkflowFailed(ctx, hooks.WorkflowFailedInfo{
			InstanceID: instanceID,
			Error:      err,
			Duration:   duration,
		})

		return err
	}

	// Success - serialize result and mark as completed
	resultData, err := json.Marshal(result)
	if err != nil {
		if statusErr := e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusFailed, "failed to serialize result"); statusErr != nil {
			return errors.Join(err, statusErr)
		}
		return fmt.Errorf("failed to serialize result: %w", err)
	}

	if err := e.storage.UpdateInstanceOutput(ctx, instanceID, resultData); err != nil {
		return fmt.Errorf("failed to set result: %w", err)
	}

	if err := e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusCompleted, ""); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	// Call OnReplayComplete hook if we were replaying
	if isReplaying {
		e.hooks.OnReplayComplete(ctx, hooks.ReplayCompleteInfo{
			InstanceID:    instanceID,
			WorkflowName:  "",
			CacheHits:     execCtx.cacheHits,
			NewActivities: execCtx.newActivities,
			Duration:      duration,
		})
	}

	e.hooks.OnWorkflowComplete(ctx, hooks.WorkflowCompleteInfo{
		InstanceID: instanceID,
		OutputData: result,
		Duration:   duration,
	})

	return nil
}

// handleSuspendSignal handles all workflow suspension cases.
func (e *Engine) handleSuspendSignal(ctx context.Context, instanceID string, sig *SuspendSignal) error {
	switch sig.Type {
	case SuspendForTimer:
		return e.handleTimerSuspend(ctx, instanceID, sig)
	case SuspendForChannelMessage:
		return e.handleChannelMessageSuspend(ctx, instanceID, sig)
	default:
		return fmt.Errorf("unknown suspend type: %v", sig.Type)
	}
}

// handleTimerSuspend handles the timer suspension case.
func (e *Engine) handleTimerSuspend(ctx context.Context, instanceID string, sig *SuspendSignal) error {
	// Register timer subscription
	timer := &storage.TimerSubscription{
		InstanceID: instanceID,
		TimerID:    sig.TimerID,
		ExpiresAt:  sig.ExpiresAt,
		ActivityID: sig.ActivityID,
	}

	if err := e.storage.RegisterTimerSubscription(ctx, timer); err != nil {
		return fmt.Errorf("failed to register timer subscription: %w", err)
	}

	return nil
}

// handleChannelMessageSuspend handles the channel message suspension case (via WaitEvent).
func (e *Engine) handleChannelMessageSuspend(ctx context.Context, instanceID string, sig *SuspendSignal) error {
	// Register channel receive and set waiting=TRUE
	var timeoutAt *time.Time
	if sig.Timeout != nil {
		t := time.Now().Add(*sig.Timeout)
		timeoutAt = &t
	}
	if err := e.storage.RegisterChannelReceive(ctx, instanceID, sig.Channel, sig.ActivityID, timeoutAt); err != nil {
		return fmt.Errorf("failed to register channel receive: %w", err)
	}

	return nil
}

// RecordActivityResult records an activity result in history.
func (e *Engine) RecordActivityResult(
	ctx context.Context,
	instanceID string,
	activityID string,
	command string,
	result any,
	activityErr error,
) (*storage.HistoryEvent, error) {
	eventType := storage.HistoryActivityCompleted
	var payload []byte
	if activityErr != nil {
		eventType = storage.HistoryActivityFailed
		payload = e.codec.Encode(activityErr)
	} else {
		var err error
		payload, err = json.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize result: %w", err)
		}
	}
	return e.recordActivityPayload(ctx, instanceID, activityID, command, eventType, payload)
}

func (e *Engine) recordActivityPayload(ctx context.Context, instanceID, activityID, command string, eventType storage.HistoryEventType, payload []byte) (*storage.HistoryEvent, error) {
	return e.storage.SetHistoryValueIfNull(ctx, nil, instanceID, activityID, command, eventType, payload)
}

// Fence returns the token for this execution's guarded writes.
func (ec *ExecutionContext) Fence() *storage.Fence { return &ec.fence }
