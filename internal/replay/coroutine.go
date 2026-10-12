package replay

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"strconv"
)

var errExecutionClosed = errors.New("workflow execution closed")

// WaitKind identifies the durable input needed by a blocked command.
type WaitKind string

const (
	WaitTimer  WaitKind = "timer"
	WaitSignal WaitKind = "signal"
)

// Wait identifies the command that yielded control to the dispatcher.
type Wait struct {
	Seq  int64
	Kind WaitKind
	Key  string
}

// WorkflowPanicError is a recovered workflow panic or unexpected goroutine exit.
type WorkflowPanicError struct{ Value, Stack string }

func (e *WorkflowPanicError) Error() string { return "workflow panic: " + e.Value }

type coroutineOutcome struct {
	result any
	err    error
	wait   *Wait
}

// The unbuffered handoff gives either the dispatcher or the workflow control.
// S7 closes every blocked coroutine; replay starts a new one on the next run.
type coroutine struct {
	outcomes chan coroutineOutcome
	resume   chan struct{}
	exited   chan struct{}
	closeErr error  // Read only after exited closes.
	ownerID  uint64 // Set by the coroutine before calling user code; immutable thereafter.
}

func startCoroutine(ec *ExecutionContext, runner WorkflowRunner) *coroutine {
	c := &coroutine{outcomes: make(chan coroutineOutcome), resume: make(chan struct{}), exited: make(chan struct{})}
	ec.coroutine = c
	go func() {
		defer close(c.exited)
		var outcome coroutineOutcome
		returned := false
		defer func() {
			value := recover()
			ec.running.Store(false)
			if value != nil {
				outcome.err = &WorkflowPanicError{Value: fmt.Sprint(value), Stack: string(debug.Stack())}
			} else if !returned && !ec.closing.Load() {
				outcome.err = &WorkflowPanicError{Value: "runtime.Goexit", Stack: string(debug.Stack())}
			}
			if ec.closing.Load() {
				c.closeErr = outcome.err
				return
			}
			c.outcomes <- outcome
		}()
		c.ownerID = currentGoroutineID()
		ec.running.Store(true)
		outcome.result, outcome.err = runner(ec)
		returned = true
	}()
	return c
}

// Yield hands control to the dispatcher after a command's durable writes.
// S7 never resumes a wait: closing uses Goexit so recovery cannot swallow it.
// A foreign caller gets a sticky nondeterminism error without a handoff.
// S9 can also use the error return for in-process resume.
func (ec *ExecutionContext) Yield(w Wait) error {
	if currentGoroutineID() != ec.coroutine.ownerID {
		err := &NondeterminismError{Seq: w.Seq, Key: w.Key, Reason: "command issued off the workflow coroutine"}
		ec.Fail(err)
		return err
	}
	ec.running.Store(false)
	ec.coroutine.outcomes <- coroutineOutcome{wait: &w}
	<-ec.coroutine.resume
	if ec.closing.Load() {
		runtime.Goexit()
	}
	ec.running.Store(true)
	return nil
}

// Go exposes no goroutine identity API. Keep stack parsing out of Issue: only
// coroutine startup and an actual blocking yield need this ownership check.
func currentGoroutineID() uint64 {
	var buf [64]byte
	header := buf[:runtime.Stack(buf[:], false)]
	const prefix = "goroutine "
	if !bytes.HasPrefix(header, []byte(prefix)) {
		panic("cannot identify workflow goroutine: unexpected runtime stack header")
	}
	idText, _, found := bytes.Cut(header[len(prefix):], []byte(" "))
	id, err := strconv.ParseUint(string(idText), 10, 64)
	if !found || err != nil || id == 0 {
		panic("cannot identify workflow goroutine: invalid runtime goroutine ID")
	}
	return id
}

// closeBlocked waits without a timeout. Workflow defers must not block.
func (c *coroutine) closeBlocked(ec *ExecutionContext) {
	ec.closing.Store(true)
	c.resume <- struct{}{}
	<-c.exited
}
