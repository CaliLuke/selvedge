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

	mu                       sync.Mutex
	bySeq                    map[int64]*storage.HistoryEvent
	byKey                    map[string]*storage.HistoryEvent
	issuedKeys               map[string]bool
	seq, maxRecordedSeq      int64
	replaying                bool
	fatal                    error
	cacheHits, newActivities int
}

// NewExecutionContext creates standalone execution tracking without storage.
func NewExecutionContext(ctx context.Context, instanceID string) *ExecutionContext {
	return &ExecutionContext{ctx: ctx, instanceID: instanceID, bySeq: make(map[int64]*storage.HistoryEvent), byKey: make(map[string]*storage.HistoryEvent), issuedKeys: make(map[string]bool)}
}

// Command identifies one position in the workflow's command history.
type Command struct {
	Seq       int64
	Key       string
	Recorded  *storage.HistoryEvent
	name      string
	execution *ExecutionContext
}

// End ends the caller's command scope. S7b adds coroutine ownership here.
func (c *Command) End() {}

// Issue validates issue order and identity before hooks, user code, or writes.
func (ec *ExecutionContext) Issue(cmd, explicitKey string) (*Command, error) {
	if err := ec.Check(); err != nil {
		return nil, err
	}
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if ec.fatal != nil {
		return nil, ec.fatal
	}
	ec.seq++
	key := explicitKey
	if key == "" {
		key = fmt.Sprintf("#%d", ec.seq)
	}
	if ec.issuedKeys[key] {
		ec.fatal = storage.ErrHistoryKeyConflict
		return nil, ec.fatal
	}
	if ec.issuedKeys == nil {
		ec.issuedKeys = make(map[string]bool)
	}
	ec.issuedKeys[key] = true
	row := ec.bySeq[ec.seq]
	ec.replaying = row != nil && row.EventData != nil
	if row != nil && (row.Command != cmd || row.ActivityID != key) {
		ec.fatal = &NondeterminismError{Seq: ec.seq, Key: key, Recorded: row.Command, Issued: cmd, Reason: "command or key changed at recorded position"}
	} else if row == nil && ec.byKey[key] != nil {
		ec.fatal = &NondeterminismError{Seq: ec.seq, Key: key, Recorded: ec.byKey[key].Command, Issued: cmd, Reason: "key moved from recorded position"}
	}
	if ec.fatal != nil {
		return nil, ec.fatal
	}
	if ec.replaying {
		ec.cacheHits++
	}
	return &Command{Seq: ec.seq, Key: key, Recorded: row, name: cmd, execution: ec}, nil
}

// Position is the most recently issued command position.
func (ec *ExecutionContext) Position() int64 { ec.mu.Lock(); defer ec.mu.Unlock(); return ec.seq }

// Replaying reports whether the current command has a recorded observation.
func (ec *ExecutionContext) Replaying() bool { ec.mu.Lock(); defer ec.mu.Unlock(); return ec.replaying }

func (ec *ExecutionContext) checkLeftover() error {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if ec.fatal != nil {
		return ec.fatal
	}
	if ec.seq < ec.maxRecordedSeq {
		row := ec.bySeq[ec.seq+1]
		if row == nil {
			// S7a can contain gaps after a swallowed empty receive.
			for seq, recorded := range ec.bySeq {
				if seq > ec.seq && (row == nil || seq < row.Seq) {
					row = recorded
				}
			}
		}
		if row != nil {
			ec.fatal = &NondeterminismError{Seq: row.Seq, Key: row.ActivityID, Recorded: row.Command, Reason: "execution stopped before recorded command"}
		}
	}
	return ec.fatal
}

// Reserve persists an activity's command before its side effect.
func (c *Command) Reserve(ctx context.Context) error {
	ec := c.execution
	if err := ec.Check(); err != nil {
		return err
	}
	if ec.engine == nil {
		return nil
	}
	_, err := ec.Storage().ReserveHistory(ctx, ec.Fence(), ec.instanceID, c.Key, c.name, c.Seq)
	if err != nil {
		ec.Fail(err)
	}
	return err
}

// StoreActivityOutcome fills only an already reserved activity command.
func (c *Command) StoreActivityOutcome(ctx context.Context, t storage.HistoryEventType, payload []byte) (*storage.HistoryEvent, error) {
	ec := c.execution
	// Class 3 may finish after another command failed, but not after lease loss.
	if ec.execution != nil {
		if err := ec.execution.Check(); err != nil {
			ec.Fail(err)
			return nil, err
		}
	}
	ec.mu.Lock()
	ec.newActivities++
	ec.mu.Unlock()
	if ec.engine == nil {
		return &storage.HistoryEvent{Seq: c.Seq, InstanceID: ec.instanceID, ActivityID: c.Key, Command: c.name, EventType: t, EventData: payload, DataType: "json"}, nil
	}
	row, err := ec.Storage().SetHistoryValueIfNull(ctx, nil, ec.instanceID, c.Key, c.name, t, payload)
	if err != nil {
		ec.Fail(err)
	}
	return row, err
}

// Context returns the underlying context.Context.
func (ec *ExecutionContext) Context() context.Context {
	return ec.ctx
}

// InstanceID returns the workflow instance ID.
func (ec *ExecutionContext) InstanceID() string {
	return ec.instanceID
}

// Fail marks an execution as failed even if workflow code catches the error.
func (ec *ExecutionContext) Fail(err error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if ec.fatal == nil {
		ec.fatal = err
	}
}

// Fatal returns the first failure that must terminate this execution.
func (ec *ExecutionContext) Fatal() error {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return ec.fatal
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
	if !x.closed && !errors.Is(err, storage.ErrFenced) {
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

	execCtx := NewExecutionContext(ctx, instanceID)
	execCtx.engine, execCtx.fence, execCtx.execution = e, f, x
	historyCount := 0
	for {
		event, ok := iter.Next()
		if !ok {
			break
		}
		if event.Seq == 0 {
			continue
		}
		historyCount++
		execCtx.byKey[event.ActivityID] = event
		execCtx.bySeq[event.Seq] = event
		if event.Seq > execCtx.maxRecordedSeq {
			execCtx.maxRecordedSeq = event.Seq
		}
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to load history: %w", err)
	}
	isReplaying := historyCount > 0

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
	if fatal := execCtx.checkLeftover(); fatal != nil {
		err = fatal
	}

	duration := time.Since(startTime)
	if checkErr := x.Check(); checkErr != nil {
		err = checkErr
	}
	if errors.Is(err, storage.ErrFenced) || errors.Is(err, ErrLeaseLost) || ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}

	if sig := AsSuspendSignal(err); sig != nil {
		return validateSuspendSignal(sig)
	}
	var resultData []byte
	if err == nil {
		resultData, err = json.Marshal(result)
		if err != nil {
			err = fmt.Errorf("failed to serialize result: %w", err)
		}
	}
	status := storage.StatusCompleted
	var closeEvent *storage.HistoryEvent
	var cancelledInstance *storage.WorkflowInstance
	if err != nil {
		status = storage.StatusFailed
		if errors.Is(err, context.Canceled) {
			requested, readErr := e.storage.IsCancelRequested(ctx, instanceID)
			if readErr != nil {
				return errors.Join(err, readErr)
			}
			if requested {
				status = storage.StatusCancelled
				cancelledInstance, readErr = e.storage.GetInstance(ctx, instanceID)
				if readErr != nil {
					return errors.Join(err, readErr)
				}
				if cancelledInstance == nil {
					return errors.Join(err, storage.ErrInstanceNotFound)
				}
			}
		}
		eventType := storage.HistoryWorkflowFailed
		if status == storage.StatusCancelled {
			eventType = storage.HistoryWorkflowCancelled
		}
		closeEvent = &storage.HistoryEvent{InstanceID: instanceID, EventType: eventType, ActivityID: "$workflow_failed", Command: "close", EventData: e.codec.Encode(err), DataType: "json"}
	}
	if closeErr := e.storage.CloseInstance(ctx, f, status, resultData, closeEvent); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	x.closed = true
	if status == storage.StatusCancelled {
		e.hooks.OnWorkflowCancelled(ctx, hooks.WorkflowCancelledInfo{InstanceID: instanceID, WorkflowName: cancelledInstance.WorkflowName, Reason: cancelledInstance.CancelReason, Duration: time.Since(cancelledInstance.StartedAt)})
		return err
	}
	if err != nil {
		e.hooks.OnWorkflowFailed(ctx, hooks.WorkflowFailedInfo{InstanceID: instanceID, Error: err, Duration: duration})
		return err
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

// validateSuspendSignal accepts the supported durable waits.
func validateSuspendSignal(sig *SuspendSignal) error {
	switch sig.Type {
	case SuspendForTimer, SuspendForSignal:
		// Waits are durable already. Run releases the lease without setting runnable.
		return nil
	default:
		return fmt.Errorf("unknown suspend type: %v", sig.Type)
	}
}

// Fence returns the token for this execution's guarded writes.
func (ec *ExecutionContext) Fence() *storage.Fence { return &ec.fence }
