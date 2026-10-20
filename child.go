package selvedge

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// ChildOption configures a child workflow start.
type ChildOption func(*childOptions)
type childOptions struct{ id string }

// WithChildID supplies a stable instance ID instead of parentID/historyKey.
func WithChildID(id string) ChildOption { return func(o *childOptions) { o.id = id } }

// Child is a child workflow outcome belonging to its issuing execution.
// Use its methods serially from the workflow coroutine.
type Child[O any] struct {
	exec                       *replay.ExecutionContext
	scope                      *cancelScope
	id, key, command           string
	ready, observed, cancelled bool
	value                      O
	err                        error
}

// StartChild starts a workflow by name. Registration is required only on a worker
// that executes the child. The parent command and child creation commit together.
// A locally cancelled scope records cancellation without creating an instance.
func StartChild[O any](ctx *WorkflowContext, workflow string, input any, opts ...ChildOption) (*Child[O], error) {
	if err := ctx.execCtx.Check(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(input)
	if err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	options := childOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	command := "child:" + workflow
	cmd, err := ctx.execCtx.Issue(command, "")
	if err != nil {
		return nil, err
	}
	defer cmd.End()
	ctx.scope.track(cmd.Key, command, "child")
	id := options.id
	if id == "" {
		id = ctx.InstanceID() + "/" + cmd.Key
	}
	row := cmd.Recorded
	switch {
	case row != nil:
		var reservation storage.ChildReservation
		if err = json.Unmarshal(row.ReservationData, &reservation); err != nil {
			ctx.execCtx.Fail(err)
			return nil, err
		}
		if reservation.ChildID != id {
			err = &NondeterminismError{Seq: cmd.Seq, Key: cmd.Key, Recorded: reservation.ChildID, Issued: id, Reason: "child ID changed"}
			ctx.execCtx.Fail(err)
			return nil, err
		}
	case ctx.scope.locallyCancelled():
		event := scopeCancelledEvent(ctx, cmd.Key, command, cmd.Seq, storage.HistoryChildCancelled)
		event.ReservationData, _ = json.Marshal(storage.ChildReservation{ChildID: id, Workflow: workflow})
		row, err = ctx.execCtx.Storage().RecordHistory(ctx.Context(), ctx.execCtx.Fence(), event)
		if err != nil {
			ctx.execCtx.Fail(err)
			return nil, err
		}
		ctx.execCtx.ApplyDelivery(row.DeliveredSeq, storage.DeliveryBatch{HistoryKeys: []string{cmd.Key}})
	default:
		row, err = ctx.execCtx.Storage().StartChild(ctx.Context(), ctx.execCtx.Fence(), cmd.Seq, cmd.Key, command, &storage.WorkflowInstance{InstanceID: id, WorkflowName: workflow, InputData: data})
		if err != nil {
			ctx.execCtx.Fail(err)
			return nil, err
		}
		ctx.app.wakeDispatcher()
	}
	c := &Child[O]{exec: ctx.execCtx, scope: ctx.scope, id: id, key: cmd.Key, command: command}
	if row.EventData != nil {
		c.readOutcome(row)
	}
	if row.EventType == storage.HistoryChildStartFailed {
		c.failDecode(ctx)
		return nil, c.err
	}
	return c, nil
}

// ID returns the child ID, even when local cancellation prevented its creation.
func (c *Child[O]) ID() string { return c.id }

// Key returns the parent's child command key.
func (c *Child[O]) Key() string { return c.key }

// IsReady reads only the delivered view or an outcome observed by Get. It is
// safe in Await predicates; off-coroutine calls are unsupported and not detected.
func (c *Child[O]) IsReady() bool { return c.observed || c.exec.Delivered(c.key) }

func (c *Child[O]) readOutcome(row *storage.HistoryEvent) {
	switch row.EventType {
	case storage.HistoryChildCompleted:
		c.value, c.err = decodeRecorded[O](row)
	case storage.HistoryChildFailed, storage.HistoryChildCancelled, storage.HistoryChildStartFailed:
		c.err = decodeRecordedError(row)
	default:
		c.err = &ReplayDecodeError{InstanceID: row.InstanceID, Key: row.ActivityID, GoType: "child outcome", Err: fmt.Errorf("unexpected event %s", row.EventType)}
	}
	c.ready = true
}
func (c *Child[O]) failDecode(ctx *WorkflowContext) {
	var decodeErr *ReplayDecodeError
	if errors.As(c.err, &decodeErr) {
		ctx.execCtx.Fail(c.err)
	}
}
func (c *Child[O]) outcome(ctx *WorkflowContext) (O, error) {
	c.observed = true
	c.failDecode(ctx)
	return c.value, c.err
}
func (c *Child[O]) checkExecution(ctx *WorkflowContext) error {
	if c.exec != ctx.execCtx {
		err := &NondeterminismError{Seq: ctx.execCtx.Position(), Key: c.key, Reason: "child belongs to another workflow execution"}
		ctx.execCtx.Fail(err)
		return err
	}
	return nil
}

// Get returns the stored outcome and consumes no command position or delivery
// batch. Cancellation follows the issuing scope, regardless of ctx's scope.
// A stored outcome wins over cancellation.
func (c *Child[O]) Get(ctx *WorkflowContext) (O, error) {
	var zero O
	end, err := ctx.execCtx.Enter()
	if err != nil {
		return zero, err
	}
	defer end()
	if err := c.checkExecution(ctx); err != nil {
		return zero, err
	}
	if c.ready {
		return c.outcome(ctx)
	}
	for {
		if err := ctx.execCtx.Check(); err != nil {
			return zero, err
		}
		row, err := ctx.Storage().GetHistoryEvent(ctx.Context(), ctx.InstanceID(), c.key)
		if err != nil {
			ctx.execCtx.Fail(err)
			return zero, err
		}
		if row != nil && row.EventData != nil {
			c.readOutcome(row)
			return c.outcome(ctx)
		}
		cancellation, err := scopeCancellation(ctx, c.scope)
		if err != nil {
			ctx.execCtx.Fail(err)
			return zero, err
		}
		if cancellation != nil {
			row, err = ctx.execCtx.Storage().CancelChild(ctx.Context(), ctx.execCtx.Fence(), &storage.ChildCancel{ChildKey: c.key, ChildCommand: c.command, ChildID: c.id, EventData: encodeError(cancellation)})
			if err != nil {
				ctx.execCtx.Fail(err)
				return zero, err
			}
			ctx.app.wakeDispatcher()
			c.readOutcome(row)
			return c.outcome(ctx)
		}
		if err := ctx.execCtx.Yield(replay.Wait{Seq: ctx.execCtx.Position(), Kind: replay.WaitChild, Key: c.key}); err != nil {
			return zero, err
		}
	}
}

// Cancel records one child_cancel command per handle, resolves an unfinished
// handle immediately, and requests cancellation of its running child.
func (c *Child[O]) Cancel(ctx *WorkflowContext) error {
	// Even repeated cancels must reject predicate, closing and overlapping use.
	end, err := ctx.execCtx.Enter()
	if err != nil {
		return err
	}
	err = c.checkExecution(ctx)
	end()
	if err != nil {
		return err
	}
	if c.cancelled {
		return nil
	}
	cmd, err := ctx.execCtx.Issue("child_cancel", "")
	if err != nil {
		return err
	}
	defer cmd.End()
	// Older histories lack cancellation target metadata. New commands preserve
	// it so moving a cancellation to another handle cannot silently replay.
	target := storage.ScopeTarget{Key: c.key, Command: c.command, Kind: "child"}
	if cmd.Recorded != nil && cmd.Recorded.ReservationData != nil {
		var recorded storage.ScopeTarget
		if err = json.Unmarshal(cmd.Recorded.ReservationData, &recorded); err == nil && recorded != target {
			err = &NondeterminismError{Seq: cmd.Seq, Key: cmd.Key, Recorded: recorded.Key, Issued: target.Key, Reason: "child cancellation target changed"}
		}
		if err != nil {
			ctx.execCtx.Fail(err)
			return err
		}
	}
	if cmd.Recorded == nil {
		cancellation := &WorkflowCancelledError{InstanceID: ctx.InstanceID(), Reason: "parent cancelled child"}
		record := &storage.HistoryEvent{InstanceID: ctx.InstanceID(), ActivityID: cmd.Key, Seq: cmd.Seq, Command: "child_cancel", EventType: storage.HistoryChildCancelled, EventData: encodeError(cancellation), DataType: "json"}
		record.ReservationData, _ = json.Marshal(target)
		row, err := ctx.execCtx.Storage().CancelChild(ctx.Context(), ctx.execCtx.Fence(), &storage.ChildCancel{Seq: cmd.Seq, Record: record, ChildKey: c.key, ChildCommand: c.command, ChildID: c.id, EventData: encodeError(cancellation)})
		if err != nil {
			ctx.execCtx.Fail(err)
			return err
		}
		// This fill is self-delivered; a result that arrived earlier stays undelivered.
		if row.DeliveredSeq > 0 {
			ctx.execCtx.ApplyDelivery(row.DeliveredSeq, storage.DeliveryBatch{HistoryKeys: []string{c.key}})
		}
		ctx.app.wakeDispatcher()
	}
	c.cancelled = true
	return nil
}
