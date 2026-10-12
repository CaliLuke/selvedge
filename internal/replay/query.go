package replay

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// ErrQueryCommand rejects commands and guarded observations in a query handler.
var ErrQueryCommand = errors.New("workflow commands are forbidden in query handlers")

var errQueryForeignGoroutine = errors.New("query stop requested off the workflow coroutine")

// QueryExecution holds a stopped or panic-failed replay and its handlers.
// Close unwinds and joins a parked workflow coroutine, if one remains.
type QueryExecution struct {
	execution *ExecutionContext
	coroutine *coroutine
	// Position is the last recorded command reached by snapshot replay.
	Position int64
	// Stopped means execution reached a command or storage boundary needing live input.
	Stopped bool
	// Result and WorkflowErr retain the runner's actual return values in verify mode.
	Result      any
	WorkflowErr error
}

// ReplayPurpose selects the consumer of a detached snapshot execution.
type ReplayPurpose uint8

const (
	// QueryPurpose preserves handlers at the recorded frontier.
	QueryPurpose ReplayPurpose = iota
	// VerifyPurpose preserves the workflow's result and live error for outcome checking.
	VerifyPurpose
)

// Query replays a detached snapshot without a lease, database access, or hooks.
func (e *Engine) Query(ctx context.Context, id string, runner WorkflowRunner, snapshot *storage.InstanceSnapshot) (*QueryExecution, error) {
	execution, err := e.Replay(ctx, id, runner, snapshot, QueryPurpose)
	if err != nil {
		if execution != nil {
			execution.Close()
		}
		return nil, err
	}
	return execution, nil
}

// Replay is the shared read-only execution path for queries and history verification.
// A returned execution must be closed even when divergence is non-nil.
func (e *Engine) Replay(ctx context.Context, id string, runner WorkflowRunner, snapshot *storage.InstanceSnapshot, purpose ReplayPurpose) (*QueryExecution, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ec := NewExecutionContext(ctx, id)
	ec.queryMode = true
	ec.engine = &Engine{hooks: &hooks.NoOpHooks{}}
	ec.engine.storage = &queryStorage{snapshot: snapshot, execution: ec}
	for _, row := range snapshot.History {
		if row.Seq == 0 {
			continue
		}
		ec.byKey[row.ActivityID] = row
		ec.bySeq[row.Seq] = row
		ec.deliveries[row.ActivityID] = row.DeliveredSeq
		if row.Seq > ec.maxRecordedSeq {
			ec.maxRecordedSeq = row.Seq
		}
	}
	for _, p := range snapshot.Signals {
		ec.signals[p.ID] = p
	}
	replayRunner := runner
	if purpose == QueryPurpose {
		replayRunner = func(ec *ExecutionContext) (any, error) { _, _ = runner(ec); return nil, ec.stopQuery() }
	}
	co := startCoroutine(ec, replayRunner)
	outcome := <-co.outcomes
	q := &QueryExecution{execution: ec, coroutine: co, Position: ec.Position(), Stopped: outcome.wait != nil, Result: outcome.result, WorkflowErr: outcome.err}
	if outcome.wait == nil {
		<-co.exited
		q.coroutine = nil
	}
	if err := ec.checkLeftover(); err != nil {
		return q, err
	}
	if err := ctx.Err(); err != nil {
		return q, err
	}
	if purpose == QueryPurpose && outcome.wait == nil {
		var workflowPanic *WorkflowPanicError
		if !errors.As(outcome.err, &workflowPanic) {
			return q, outcome.err
		}
	}
	return q, nil
}

// Close runs workflow defers with command and registration guards closed.
func (q *QueryExecution) Close() {
	if q.coroutine != nil {
		q.coroutine.closeBlocked(q.execution)
		q.coroutine = nil
	}
}

// Answer invokes the frozen handler before unwinding a parked coroutine.
// A panic-failed replay has already unwound and exited.
func (q *QueryExecution) Answer(name string) (value any, found bool, err error) {
	ec := q.execution
	handler, found := ec.queries[name]
	if !found {
		return nil, false, nil
	}
	ec.inQueryHandler.Store(true)
	defer func() {
		ec.inQueryHandler.Store(false)
		if p := recover(); p != nil {
			if e, ok := p.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("query handler panic: %v", p)
			}
		}
		if fatal := ec.Fatal(); fatal != nil {
			err = fatal
		}
	}()
	value, err = handler()
	return value, true, err
}

// QueryMode identifies snapshot replay; activity definitions must not register in this mode.
func (ec *ExecutionContext) QueryMode() bool { return ec.queryMode }

// SetQueryHandler changes only this execution's handler table, without a command position.
func (ec *ExecutionContext) SetQueryHandler(name string, handler func() (any, error)) error {
	end, err := ec.Enter()
	if err != nil {
		return err
	}
	defer end()
	if ec.coroutine == nil || currentGoroutineID() != ec.coroutine.ownerID {
		err = &NondeterminismError{Seq: ec.Position(), Reason: "query handler registered off the workflow coroutine"}
		ec.Fail(err)
		return err
	}
	if name == "" || len(name) > 255 {
		err = errors.New("query name must contain 1–255 bytes")
		ec.Fail(err)
		return err
	}
	if ec.queries == nil {
		ec.queries = make(map[string]func() (any, error))
	}
	if handler == nil {
		delete(ec.queries, name)
	} else {
		ec.queries[name] = handler
	}
	return nil
}

// A stop never returns on the workflow coroutine. Foreign calls return an error
// and leave a sticky failure for the query caller, without panicking that goroutine.
func (ec *ExecutionContext) stopQuery() error {
	if ec.inQueryHandler.Load() {
		ec.Fail(ErrQueryCommand)
		return ErrQueryCommand
	}
	if ec.coroutine == nil || currentGoroutineID() != ec.coroutine.ownerID {
		ec.Fail(&NondeterminismError{Seq: ec.Position(), Reason: "query stop requested off the workflow coroutine"})
		return errQueryForeignGoroutine
	}
	if ec.closing.Load() {
		runtime.Goexit()
	}
	if err := ec.Yield(Wait{Kind: WaitQueryStop, Seq: ec.Position()}); err != nil {
		return err
	}
	panic("query stop resumed")
}
