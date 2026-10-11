package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// ErrorCodec encodes workflow failures without depending on public error types.
type ErrorCodec interface{ Encode(error) []byte }

// Engine handles workflow execution with deterministic replay.
type Engine struct {
	storage  storage.Storage
	hooks    hooks.WorkflowHooks
	workerID string
	codec    ErrorCodec
}

// NewEngine creates a new replay engine.
func NewEngine(s storage.Storage, h hooks.WorkflowHooks, workerID string, codec ErrorCodec) *Engine {
	return &Engine{
		storage:  s,
		hooks:    h,
		workerID: workerID,
		codec:    codec,
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

	// Activity tracking
	activityCounter map[string]int
	counterMu       sync.Mutex

	// Replay state
	history      []*storage.HistoryEvent
	historyIndex int
	isReplaying  bool

	// Raw recorded rows; values are decoded by their typed caller.
	recorded map[string]*storage.HistoryEvent
	fatal    error

	// Replay statistics
	cacheHits     int
	newActivities int
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
	ec.counterMu.Lock()
	ec.newActivities++
	ec.counterMu.Unlock()
	event, err := ec.engine.recordActivityPayload(ctx, ec.instanceID, key, eventType, payload)
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
	return ec.engine.storage
}

// Hooks returns the workflow hooks for observability.
func (ec *ExecutionContext) Hooks() hooks.WorkflowHooks {
	return ec.engine.hooks
}

// StartWorkflow starts a new workflow instance.
func (e *Engine) StartWorkflow(
	ctx context.Context,
	instanceID string,
	workflowName string,
	inputData []byte,
	runner WorkflowRunner,
) error {
	// Try to acquire lock
	acquired, err := e.storage.TryAcquireLock(ctx, instanceID, e.workerID, 300)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		return fmt.Errorf("failed to acquire lock for instance %s", instanceID)
	}
	defer func() { _ = e.storage.ReleaseLock(ctx, instanceID, e.workerID) }()

	// Update status to running
	if err := e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusRunning, ""); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	// Execute the workflow
	return e.executeWorkflow(ctx, instanceID, runner)
}

// ResumeWorkflow resumes a workflow from its history.
func (e *Engine) ResumeWorkflow(
	ctx context.Context,
	instanceID string,
	runner WorkflowRunner,
) error {
	// Try to acquire lock
	acquired, err := e.storage.TryAcquireLock(ctx, instanceID, e.workerID, 300)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !acquired {
		return fmt.Errorf("failed to acquire lock for instance %s", instanceID)
	}
	defer func() { _ = e.storage.ReleaseLock(ctx, instanceID, e.workerID) }()

	// Update status to running
	if err := e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusRunning, ""); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}

	// Execute the workflow
	return e.executeWorkflow(ctx, instanceID, runner)
}

// executeWorkflow executes or replays a workflow.
func (e *Engine) executeWorkflow(ctx context.Context, instanceID string, runner WorkflowRunner) error {
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
		switch event.EventType {
		case storage.HistoryActivityCompleted, storage.HistoryChannelMessageReceived,
			storage.HistoryActivityFailed, storage.HistoryMessageTimeout:
			recorded[event.ActivityID] = event
		}
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
		activityCounter: make(map[string]int),
		history:         nil, // History is processed via iterator, not stored
		historyIndex:    0,
		isReplaying:     isReplaying,
		recorded:        recorded,
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

	if err != nil {
		// Check for SuspendSignal
		if sig := AsSuspendSignal(err); sig != nil {
			return e.handleSuspendSignal(ctx, instanceID, sig)
		}

		// Record workflow failure in history
		eventData := e.codec.Encode(err)
		_ = e.storage.AppendHistory(ctx, &storage.HistoryEvent{
			InstanceID: instanceID,
			EventType:  storage.HistoryWorkflowFailed,
			ActivityID: "$workflow_failed",
			EventData:  eventData,
			DataType:   "json",
		})

		// Mark as failed
		_ = e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusFailed, err.Error())

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
		_ = e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusFailed, "failed to serialize result")
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

	// Update status to waiting
	if err := e.storage.UpdateInstanceStatus(ctx, instanceID, storage.StatusWaitingForTimer, ""); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
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
	if err := e.storage.RegisterChannelReceiveAndReleaseLock(ctx, instanceID, sig.Channel, e.workerID, sig.ActivityID, timeoutAt); err != nil {
		return fmt.Errorf("failed to register channel receive: %w", err)
	}

	return nil
}

// RecordActivityResult records an activity result in history.
func (e *Engine) RecordActivityResult(
	ctx context.Context,
	instanceID string,
	activityID string,
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
	return e.recordActivityPayload(ctx, instanceID, activityID, eventType, payload)
}

func (e *Engine) recordActivityPayload(ctx context.Context, instanceID, activityID string, eventType storage.HistoryEventType, payload []byte) (*storage.HistoryEvent, error) {
	event := &storage.HistoryEvent{InstanceID: instanceID, ActivityID: activityID, EventType: eventType, EventData: payload, DataType: "json"}

	if err := e.storage.AppendHistory(ctx, event); err != nil {
		return nil, err
	}
	// Read the stored bytes, including any normalization by the database.
	// S3's row-returning history API will replace this iterator lookup.
	iter := storage.NewHistoryIterator(ctx, e.storage, instanceID, nil)
	defer func() { _ = iter.Close() }()
	for {
		stored, ok := iter.Next()
		if !ok {
			break
		}
		if stored.ActivityID == activityID {
			return stored, nil
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("recorded activity result missing for %s", activityID)
}
