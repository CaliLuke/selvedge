package selvedge

import (
	"encoding/json"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// Sleep suspends the workflow for a specified duration.
// The workflow will resume after the duration has elapsed.
//
// During replay, if the timer already fired, this returns immediately.
// Otherwise, it reserves and arms the timer under the execution fence, then
// returns a SuspendSignal. The engine releases the lease and preserves pending input.
//
// When the timer expires, the workflow will be resumed.
func Sleep(ctx *WorkflowContext, duration time.Duration, opts ...SleepOption) error {
	if err := ctx.execCtx.Fatal(); err != nil {
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

	// Generate timer ID
	timerID := options.timerID
	if timerID == "" {
		timerID = ctx.GenerateActivityID("sleep")
	}

	// Check if result is cached (replay)
	event, found, err := ctx.execCtx.Claim(timerID, "sleep")
	if err != nil {
		return err
	}
	if found && event.EventData != nil {
		return sleepOutcome(ctx, event)
	}
	stored, err := ctx.Storage().ReserveTimer(ctx.Context(), ctx.execCtx.Fence(), &storage.TimerSubscription{
		InstanceID: ctx.InstanceID(), TimerID: timerID, ActivityID: timerID, ExpiresAt: time.Now().Add(duration),
	})
	if err != nil {
		ctx.execCtx.Fail(err)
		return err
	}
	if stored.EventData != nil {
		return sleepOutcome(ctx, stored)
	}
	requested, err := ctx.Storage().IsCancelRequested(ctx.Context(), ctx.InstanceID())
	if err != nil {
		ctx.execCtx.Fail(err)
		return err
	}
	if requested {
		cancellation, err := cancellationAtYield(ctx)
		if err != nil {
			return err
		}
		stored, err = ctx.Storage().SetHistoryValueIfNull(ctx.Context(), ctx.execCtx.Fence(), ctx.InstanceID(), timerID, "sleep", storage.HistoryTimerCancelled, encodeError(cancellation))
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

	// Record the timer ID for tracking
	ctx.RecordActivityID(timerID)

	// Return SuspendSignal that the replay engine will handle
	return NewTimerSuspend(ctx.InstanceID(), timerID, timerID, expiresAt)
}

func cancellationAtYield(ctx *WorkflowContext) (*WorkflowCancelledError, error) {
	instance, err := ctx.Storage().GetInstance(ctx.Context(), ctx.InstanceID())
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
	duration := time.Until(t)
	if duration <= 0 {
		// Already past the target time
		return nil
	}
	return Sleep(ctx, duration, opts...)
}
