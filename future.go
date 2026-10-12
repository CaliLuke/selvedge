package selvedge

import (
	"errors"

	"github.com/google/uuid"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// Future is an activity outcome tied to the workflow execution that issued it.
// Use it serially from that workflow's coroutine. Get may be called repeatedly.
type Future[O any] struct {
	scope        *cancelScope
	exec         *replay.ExecutionContext
	key, command string
	ready        bool
	observed     bool
	value        O
	err          error
}

// Key returns the history key assigned when the activity was issued.
func (f *Future[O]) Key() string { return f.key }

func (f *Future[O]) readOutcome(row *storage.HistoryEvent) {
	f.value, f.err = decodeActivityOutcome[O](row)
	f.ready = true
}

func (f *Future[O]) outcome(ctx *WorkflowContext) (O, error) {
	f.observed = true
	failActivityOutcome(ctx, f.err)
	return f.value, f.err
}

// Get returns the stored outcome, or yields until it is available. It consumes
// no command position. A stored outcome takes precedence over cancellation.
func (f *Future[O]) Get(ctx *WorkflowContext) (O, error) {
	var zero O
	end, err := ctx.execCtx.Enter()
	if err != nil {
		return zero, err
	}
	defer end()
	if f.exec != ctx.execCtx {
		err := &NondeterminismError{Seq: ctx.execCtx.Position(), Key: f.key, Reason: "future belongs to another workflow execution"}
		ctx.execCtx.Fail(err)
		return zero, err
	}
	if f.ready {
		return f.outcome(ctx)
	}
	attempted := false
	for {
		if err := ctx.execCtx.Check(); err != nil {
			return zero, err
		}
		row, err := ctx.Storage().GetHistoryEvent(ctx.Context(), ctx.InstanceID(), f.key)
		if err != nil {
			ctx.execCtx.Fail(err)
			return zero, err
		}
		if row != nil && row.EventData != nil {
			f.readOutcome(row)
			return f.outcome(ctx)
		}
		cancellation, err := scopeCancellation(ctx, f.scope)
		if err != nil {
			ctx.execCtx.Fail(err)
			return zero, err
		}
		if cancellation != nil {
			row, err = ctx.execCtx.Storage().CancelActivityAtYield(ctx.Context(), ctx.execCtx.Fence(), ctx.InstanceID(), f.key, f.command, encodeError(cancellation))
			if err != nil {
				ctx.execCtx.Fail(err)
				return zero, err
			}
			f.readOutcome(row)
			return f.outcome(ctx)
		}
		if !attempted {
			task, err := ctx.execCtx.Storage().ClaimActivityTask(ctx.Context(), ctx.InstanceID(), f.key, ctx.app.config.workerID+"/"+uuid.NewString(), ctx.app.config.lease.TTL, ctx.app.config.activityQueues)
			if err != nil {
				ctx.execCtx.Fail(err)
				return zero, err
			}
			attempted = true
			if task != nil {
				err = ctx.app.runActivityTask(ctx.Context(), ctx.execCtx.Storage(), task, ctx.execCtx.CheckLease)
				if err != nil && !errors.Is(err, storage.ErrFenced) && !errors.Is(err, errClaimLost) {
					ctx.execCtx.Fail(err)
					return zero, err
				}
				continue
			}
		}
		if err := ctx.execCtx.Yield(replay.Wait{Seq: ctx.execCtx.Position(), Key: f.key, Kind: replay.WaitActivity}); err != nil {
			return zero, err
		}
	}
}

// IsReady reports a delivered or previously observed outcome. It consumes no
// position and performs no I/O. Use only on the workflow coroutine, including
// Await predicates; off-coroutine calls are unsupported and not detected.
func (f *Future[O]) IsReady() bool { return f.observed || f.exec.Delivered(f.key) }
