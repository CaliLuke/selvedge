package selvedge

import (
	"context"
	"time"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// ReceiveWithTimeout receives the oldest signal on channel or returns an error
// matching context.DeadlineExceeded. A nonpositive timeout records an immediate
// timeout without consuming signals. Positive waits prefer cancellation, then a
// signal, then the timer fire, and retire a losing timer atomically.
func ReceiveWithTimeout[T any](ctx *WorkflowContext, channel string, timeout time.Duration) (T, error) {
	var zero T
	if err := ctx.execCtx.Check(); err != nil {
		return zero, err
	}
	if channel == "" || len(channel) > 255 {
		ctx.execCtx.Fail(ErrInvalidSignalChannel)
		return zero, ErrInvalidSignalChannel
	}
	var timer *Timer
	if timeout > 0 {
		var err error
		timer, err = NewTimer(ctx, timeout)
		if err != nil {
			return zero, err
		}
	}
	command := "receive_timeout:" + channel
	cmd, err := ctx.execCtx.Issue(command, "")
	if err != nil {
		return zero, err
	}
	defer cmd.End()
	if cmd.Recorded != nil && cmd.Recorded.EventData != nil {
		return timeoutOutcome[T](ctx, cmd.Recorded)
	}
	record := &storage.HistoryEvent{InstanceID: ctx.InstanceID(), ActivityID: cmd.Key, Seq: cmd.Seq, Command: command, EventType: storage.HistorySignalTimeout, EventData: []byte(`true`), DataType: "json"}
	if timeout <= 0 {
		stored, err := ctx.execCtx.Storage().RecordHistory(ctx.Context(), ctx.execCtx.Fence(), record)
		if err != nil {
			ctx.execCtx.Fail(err)
			return zero, err
		}
		return timeoutOutcome[T](ctx, stored)
	}
	commit := &storage.ObservationCommit{Seq: cmd.Seq, DeliverAll: true, Record: record}
	s := ctx.execCtx.Storage()
	// Read monotone conditions in reverse priority. Deciding in priority order
	// then chooses an outcome valid at one point during these reads.
	row, err := s.GetHistoryEvent(ctx.Context(), ctx.InstanceID(), timer.Key())
	if err != nil {
		ctx.execCtx.Fail(err)
		return zero, err
	}
	fired := row != nil && row.EventType == storage.HistoryTimerExpired && row.EventData != nil
	signal, err := s.NextSignal(ctx.Context(), ctx.InstanceID(), channel)
	if err != nil {
		ctx.execCtx.Fail(err)
		return zero, err
	}
	cancellation, err := scopeCancellation(ctx, ctx.scope)
	if err != nil {
		ctx.execCtx.Fail(err)
		return zero, err
	}
	switch {
	case cancellation != nil:
		record.EventType = storage.HistorySignalCancelled
		record.EventData = encodeError(cancellation)
		commit.Retire = &storage.TimerRetire{Key: timer.Key()}
	case signal != nil:
		record.EventType = storage.HistorySignalReceived
		record.EventData = signal.Payload
		commit.Consume = &storage.SignalConsume{ID: signal.ID, Channel: channel}
		commit.Retire = &storage.TimerRetire{Key: timer.Key()}
	case fired:
		// The timeout observation is already prepared above.
	default:
		return zero, ctx.execCtx.Yield(replay.Wait{Seq: cmd.Seq, Kind: replay.WaitSignal, Key: cmd.Key})
	}
	stored, err := commitObservation(ctx, commit)
	if err != nil {
		return zero, err
	}
	return timeoutOutcome[T](ctx, stored)
}
func timeoutOutcome[T any](ctx *WorkflowContext, row *storage.HistoryEvent) (T, error) {
	if row.EventType == storage.HistorySignalTimeout {
		var zero T
		return zero, context.DeadlineExceeded
	}
	return signalOutcome[T](ctx, row)
}
