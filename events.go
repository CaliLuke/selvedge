package selvedge

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// Sleep suspends the workflow for a specified duration.
// The workflow will resume after the duration has elapsed.
//
// During replay, if the timer already fired, this returns immediately.
// Otherwise, it reserves and arms the timer under the execution fence, then
// yields to the dispatcher. The engine releases the lease and preserves pending input.
//
// When the timer expires, the workflow will be resumed.
func Sleep(ctx *WorkflowContext, duration time.Duration, opts ...SleepOption) error {
	return sleep(ctx, duration, nil, opts...)
}

func sleep(ctx *WorkflowContext, duration time.Duration, until *time.Time, opts ...SleepOption) error {
	if err := ctx.execCtx.Check(); err != nil {
		return err
	}
	options := &sleepOptions{}
	for _, opt := range opts {
		opt(options)
	}

	if options.err != nil {
		ctx.execCtx.Fail(options.err)
		return options.err
	}

	cmd, err := ctx.execCtx.Issue("sleep", options.timerID)
	if err != nil {
		return err
	}
	defer cmd.End()
	ctx.scope.track(cmd.Key, "sleep", "timer")
	timerID := cmd.Key
	event := cmd.Recorded
	if event != nil && event.EventData != nil {
		return sleepOutcome(ctx, event)
	}
	if ctx.scope.locallyCancelled() && event == nil {
		row, err := commitObservation(ctx, &storage.ObservationCommit{Seq: cmd.Seq, DeliverAll: true, Record: scopeCancelledEvent(ctx, cmd.Key, "sleep", cmd.Seq, storage.HistoryTimerCancelled)})
		if err != nil {
			return err
		}
		return sleepOutcome(ctx, row)
	}
	base := workflowTime(ctx)
	if until != nil {
		duration = until.Sub(base)
	}
	expiresAt := base.Add(duration)
	if duration <= 0 && event == nil {
		stored, err := ctx.execCtx.Storage().RecordHistory(ctx.Context(), ctx.execCtx.Fence(), immediateTimerEvent(ctx.InstanceID(), timerID, cmd.Seq, expiresAt))
		if err != nil {
			ctx.execCtx.Fail(err)
			return err
		}
		return sleepOutcome(ctx, stored)
	}
	stored, err := ctx.execCtx.Storage().ReserveTimer(ctx.Context(), ctx.execCtx.Fence(), cmd.Seq, &storage.TimerSubscription{
		InstanceID: ctx.InstanceID(), TimerID: timerID, ActivityID: timerID, ExpiresAt: expiresAt,
	})
	if err != nil {
		ctx.execCtx.Fail(err)
		return err
	}
	if stored.EventData != nil {
		return sleepOutcome(ctx, stored)
	}
	cancellation, err := scopeCancellation(ctx, ctx.scope)
	if err != nil {
		ctx.execCtx.Fail(err)
		return err
	}
	if cancellation != nil {
		stored, err = commitObservation(ctx, &storage.ObservationCommit{Seq: cmd.Seq, DeliverAll: true, Fill: &storage.HistoryFill{Key: timerID, Command: "sleep", EventType: storage.HistoryTimerCancelled, EventData: encodeError(cancellation)}})
		if err != nil {
			ctx.execCtx.Fail(err)
			return err
		}
		return sleepOutcome(ctx, stored)
	}
	var reservation storage.SleepReservation
	if err := json.Unmarshal(stored.ReservationData, &reservation); err != nil {
		ctx.execCtx.Fail(err)
		return err
	}
	expiresAt = reservation.ExpiresAt

	// Call OnTimerStart hook
	if h := ctx.Hooks(); h != nil {
		h.OnTimerStart(ctx.Context(), hooks.TimerStartInfo{
			InstanceID:   ctx.InstanceID(),
			WorkflowName: ctx.WorkflowName(),
			TimerID:      timerID,
			ExpiresAt:    expiresAt,
		})
	}

	return ctx.execCtx.Yield(replay.Wait{Seq: cmd.Seq, Kind: replay.WaitTimer, Key: timerID})
}

func cancellationAtYield(ctx *WorkflowContext) (*WorkflowCancelledError, error) {
	instance, err := ctx.execCtx.Storage().GetInstance(ctx.Context(), ctx.InstanceID())
	if err == nil && instance == nil {
		err = storage.ErrInstanceNotFound
	}
	if err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	return &WorkflowCancelledError{InstanceID: ctx.InstanceID(), Reason: instance.CancelReason}, nil
}

func sleepOutcome(ctx *WorkflowContext, event *storage.HistoryEvent) error {
	switch event.EventType {
	case storage.HistoryTimerExpired:
		_, err := observedTimerExpiry(ctx, event)
		return err
	case storage.HistoryTimerCancelled:
		return replayRecordedError(ctx, event)
	default:
		err := fmt.Errorf("internal timer outcome %s at %s", event.EventType, event.ActivityID)
		ctx.execCtx.Fail(err)
		return err
	}
}

// SleepOption configures sleep behavior.
type SleepOption func(*sleepOptions)

type sleepOptions struct {
	err     error
	timerID string
}

// WithSleepID sets a custom timer ID for the sleep.
// This is useful for identifying timers in logs and for deterministic replay.
// The same prefixes as WithActivityID are reserved; Sleep rejects them with ErrHistoryKeyConflict.
func WithSleepID(id string) SleepOption {
	return func(o *sleepOptions) {
		o.timerID = id
		if err := reservedHistoryIDError("sleep", id); err != nil {
			o.err = err
		}
	}
}

// SleepUntil suspends the workflow until a specific time.
func SleepUntil(ctx *WorkflowContext, t time.Time, opts ...SleepOption) error {
	return sleep(ctx, 0, &t, opts...)
}
