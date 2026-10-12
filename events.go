package selvedge

import (
	"encoding/json"
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
	timerID := cmd.Key
	event := cmd.Recorded
	if event != nil && event.EventData != nil {
		return sleepOutcome(ctx, event)
	}
	if duration <= 0 && event == nil {
		stored, err := ctx.execCtx.Storage().RecordHistory(ctx.Context(), ctx.execCtx.Fence(), &storage.HistoryEvent{Seq: cmd.Seq, InstanceID: ctx.InstanceID(), ActivityID: timerID, Command: "sleep", EventType: storage.HistoryTimerExpired, EventData: []byte(`true`), DataType: "json"})
		if err != nil {
			ctx.execCtx.Fail(err)
			return err
		}
		return sleepOutcome(ctx, stored)
	}
	stored, err := ctx.execCtx.Storage().ReserveTimer(ctx.Context(), ctx.execCtx.Fence(), cmd.Seq, &storage.TimerSubscription{
		InstanceID: ctx.InstanceID(), TimerID: timerID, ActivityID: timerID, ExpiresAt: time.Now().Add(duration),
	})
	if err != nil {
		ctx.execCtx.Fail(err)
		return err
	}
	if stored.EventData != nil {
		return sleepOutcome(ctx, stored)
	}
	requested, err := ctx.execCtx.Storage().IsCancelRequested(ctx.Context(), ctx.InstanceID())
	if err != nil {
		ctx.execCtx.Fail(err)
		return err
	}
	if requested {
		cancellation, err := cancellationAtYield(ctx)
		if err != nil {
			return err
		}
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
	expiresAt := reservation.ExpiresAt

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
	if event.EventType == storage.HistoryTimerCancelled {
		return replayRecordedError(ctx, event)
	}
	return nil
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
	return Sleep(ctx, time.Until(t), opts...)
}
