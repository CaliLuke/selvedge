package selvedge

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// cancelScope is execution-local. Durable requests never mutate this tree.
// Targets are retained only by scopes with a cancel function, in issue order.
type cancelScope struct {
	cancellable bool
	parent      *cancelScope
	attached    bool
	cancelled   bool
	targets     []storage.ScopeTarget
	owner       string
}

func newCancelScope(parent *cancelScope, attached bool) *cancelScope {
	owner := ""
	if parent != nil {
		owner = parent.owner
	} else {
		owner = scopeCoroutine()
	}
	return &cancelScope{parent: parent, attached: attached, owner: owner}
}

// WithCancel creates a child cancellation scope without issuing a command.
// Its cancel function must run on the workflow coroutine, outside predicates.
// The first call records one cancel_scope command, even for an empty scope.
func WithCancel(ctx *WorkflowContext) (scoped *WorkflowContext, cancel func()) {
	child := ctx.WithContext(ctx.Context())
	child.scope = newCancelScope(ctx.scope, ctx.scope.attached)
	child.scope.cancellable = true
	return child, func() { cancelWorkflowScope(child) }
}

// Detached creates a parentless scope that ignores durable workflow cancellation.
// It retains the execution's lease and shutdown context. Use it for cleanup.
func Detached(ctx *WorkflowContext) *WorkflowContext {
	child := ctx.WithContext(ctx.Context())
	child.scope = &cancelScope{owner: ctx.scope.owner}
	return child
}

func (s *cancelScope) locallyCancelled() bool {
	for ; s != nil; s = s.parent {
		if s.cancelled {
			return true
		}
	}
	return false
}
func (s *cancelScope) track(key, command, kind string) {
	target := storage.ScopeTarget{Key: key, Command: command, Kind: kind}
	for ; s != nil; s = s.parent {
		if s.cancellable {
			s.targets = append(s.targets, target)
		}
	}
}

func cancelWorkflowScope(ctx *WorkflowContext) {
	ec := ctx.execCtx
	// Enter handles closing, predicates, and overlapping commands before any
	// scope state is read. The identity check also covers nonblocking foreign calls.
	end, err := ec.Enter()
	if err != nil {
		return
	}
	if ctx.scope.owner != scopeCoroutine() {
		end()
		ec.Fail(&NondeterminismError{Seq: ec.Position() + 1, Issued: "cancel_scope", Reason: "command issued off the workflow coroutine"})
		return
	}
	cancelled := ctx.scope.locallyCancelled()
	end()
	if cancelled {
		return
	}
	cmd, err := ec.Issue("cancel_scope", "")
	if err != nil {
		return
	}
	defer cmd.End()
	if cmd.Recorded == nil {
		data, _ := json.Marshal(struct {
			Targets int `json:"targets"`
		}{len(ctx.scope.targets)})
		_, err = commitObservation(ctx, &storage.ObservationCommit{Seq: cmd.Seq, Record: &storage.HistoryEvent{InstanceID: ctx.InstanceID(), ActivityID: cmd.Key, Seq: cmd.Seq, Command: "cancel_scope", EventType: storage.HistoryScopeCancelled, EventData: data, DataType: "json"}, Cancel: &storage.ScopeCancel{Targets: ctx.scope.targets}})
		if err != nil {
			return
		}
	}
	ctx.scope.cancelled = true
	// Cross-worker and local wakeups happen only after the scope transaction
	// commits. A batch hint is sufficient: each affected child is runnable.
	if cmd.Recorded == nil {
		for _, target := range ctx.scope.targets {
			if target.Kind == "child" {
				if notifier, ok := ctx.app.storage.(interface{ NotifyRunnable(context.Context) }); ok {
					notifier.NotifyRunnable(ctx.Context())
				}
				ctx.app.wakeDispatcher()
				break
			}
		}
	}
}

// scopeCoroutine keeps the nonblocking cancel guard local to scopes. Go has no
// goroutine identity API; use the same runtime stack header as the yield guard.
func scopeCoroutine() string {
	var buf [64]byte
	header := buf[:runtime.Stack(buf[:], false)]
	fields := bytes.Fields(header)
	if len(fields) < 2 || string(fields[0]) != "goroutine" {
		panic("cannot identify workflow coroutine")
	}
	return string(fields[1])
}

func scopeCancellation(ctx *WorkflowContext, scope *cancelScope) (cancelErr, err error) {
	if scope.locallyCancelled() {
		return context.Canceled, nil
	}
	if !scope.attached {
		return nil, nil
	}
	requested, err := ctx.execCtx.Storage().IsCancelRequested(ctx.Context(), ctx.InstanceID())
	if err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	if !requested {
		return nil, nil
	}
	cancellation, err := cancellationAtYield(ctx)
	if err != nil {
		return nil, err
	}
	return cancellation, nil
}

func scopeCancelledEvent(ctx *WorkflowContext, key, command string, seq int64, kind storage.HistoryEventType) *storage.HistoryEvent {
	return &storage.HistoryEvent{InstanceID: ctx.InstanceID(), ActivityID: key, Seq: seq, Command: command, EventType: kind, EventData: encodeError(context.Canceled), DataType: "json"}
}
