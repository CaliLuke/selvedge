package selvedge

import "github.com/CaliLuke/selvedge/internal/storage"

// Pending counts delivered, unconsumed signals at the current command position.
// It performs no I/O and is safe inside Await predicates. Like IsReady, it must
// run serially on the workflow coroutine; off-coroutine use is not detected.
func Pending(ctx *WorkflowContext, channel string) int {
	if channel == "" || len(channel) > 255 {
		ctx.execCtx.Fail(ErrInvalidSignalChannel)
		return 0
	}
	return ctx.execCtx.PendingSignals(channel)
}

// ReceiveAsync records the next delivered signal, or records that none was
// available. It never yields, delivers new events, or observes cancellation.
func ReceiveAsync[T any](ctx *WorkflowContext, channel string) (value T, received bool, err error) {
	var zero T
	if err := ctx.execCtx.Check(); err != nil {
		return zero, false, err
	}
	if channel == "" || len(channel) > 255 {
		ctx.execCtx.Fail(ErrInvalidSignalChannel)
		return zero, false, ErrInvalidSignalChannel
	}
	cmd, err := ctx.execCtx.Issue("receive_async:"+channel, "")
	if err != nil {
		return zero, false, err
	}
	defer cmd.End()
	if cmd.Recorded != nil {
		return asyncSignalOutcome[T](ctx, cmd.Recorded)
	}
	obs := &storage.HistoryEvent{InstanceID: ctx.InstanceID(), Seq: cmd.Seq, ActivityID: cmd.Key, Command: "receive_async:" + channel, EventType: storage.HistorySignalNone, DataType: "json"}
	c := &storage.ObservationCommit{Seq: cmd.Seq, Record: obs}
	if id, ok := ctx.execCtx.NextDeliveredSignal(channel); ok {
		obs.EventType = storage.HistorySignalReceived
		c.Consume = &storage.SignalConsume{ID: id, Channel: channel, RequireDeliveredBefore: cmd.Seq}
	}
	row, err := commitObservation(ctx, c)
	if err != nil {
		return zero, false, err
	}
	return asyncSignalOutcome[T](ctx, row)
}
func asyncSignalOutcome[T any](ctx *WorkflowContext, row *storage.HistoryEvent) (value T, received bool, err error) {
	var zero T
	if row.EventType == storage.HistorySignalNone {
		return zero, false, nil
	}
	v, err := signalOutcome[T](ctx, row)
	return v, err == nil, err
}
