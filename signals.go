package selvedge

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// InstanceClosedError reports the terminal status that rejected a signal.
type InstanceClosedError = storage.InstanceClosedError

var ErrInstanceClosed = storage.ErrInstanceClosed
var ErrInstanceNotFound = storage.ErrInstanceNotFound
var ErrInvalidSignalChannel = storage.ErrInvalidSignalChannel

// Signal durably appends a JSON payload to one instance's named inbox and wakes it.
// Signals are accepted from instance creation until closure, including while an
// execution holds the lease. Channel names pass through unchanged (1–255 bytes).
func (a *App) Signal(ctx context.Context, instanceID, channel string, payload any) error {
	if channel == "" || len(channel) > 255 {
		return ErrInvalidSignalChannel
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode signal payload: %w", err)
	}
	if _, err = a.storage.AppendSignal(ctx, instanceID, channel, data); err != nil {
		return err
	}
	a.wakeDispatcher()
	return nil
}

// Receive returns the next signal from this instance's channel in arrival order.
// Replay returns the recorded payload. Cancellation is observed before consuming
// a pending signal. An empty inbox yields to the dispatcher without returning.
func Receive[T any](ctx *WorkflowContext, channel string) (T, error) {
	var zero T
	if err := ctx.execCtx.Check(); err != nil {
		return zero, err
	}
	if channel == "" || len(channel) > 255 {
		ctx.execCtx.Fail(ErrInvalidSignalChannel)
		return zero, ErrInvalidSignalChannel
	}
	command := "receive:" + channel
	cmd, err := ctx.execCtx.Issue(command, "")
	if err != nil {
		return zero, err
	}
	defer cmd.End()
	key, event := cmd.Key, cmd.Recorded
	if event != nil && event.EventData != nil {
		return signalOutcome[T](ctx, event)
	}
	s := ctx.Storage()
	requested, err := s.IsCancelRequested(ctx.Context(), ctx.InstanceID())
	if err != nil {
		ctx.execCtx.Fail(err)
		return zero, err
	}
	if requested {
		cancellation, err := cancellationAtYield(ctx)
		if err != nil {
			return zero, err
		}
		stored, err := s.RecordHistory(ctx.Context(), ctx.execCtx.Fence(), &storage.HistoryEvent{Seq: cmd.Seq, InstanceID: ctx.InstanceID(), ActivityID: key, Command: command, EventType: storage.HistorySignalCancelled, EventData: encodeError(cancellation), DataType: "json"})
		if err != nil {
			ctx.execCtx.Fail(err)
			return zero, err
		}
		return signalOutcome[T](ctx, stored)
	}
	signal, err := s.NextSignal(ctx.Context(), ctx.InstanceID(), channel)
	if err != nil {
		ctx.execCtx.Fail(err)
		return zero, err
	}
	if signal == nil {
		return zero, ctx.execCtx.Yield(replay.Wait{Seq: cmd.Seq, Kind: replay.WaitSignal, Key: key})
	}
	stored, err := s.ConsumeSignal(ctx.Context(), ctx.execCtx.Fence(), signal.ID, &storage.HistoryEvent{Seq: cmd.Seq, InstanceID: ctx.InstanceID(), ActivityID: key, Command: command, EventType: storage.HistorySignalReceived, EventData: signal.Payload, DataType: "json"})
	if err != nil {
		ctx.execCtx.Fail(err)
		return zero, err
	}
	return signalOutcome[T](ctx, stored)
}

func signalOutcome[T any](ctx *WorkflowContext, event *storage.HistoryEvent) (T, error) {
	var zero T
	if event.EventType == storage.HistorySignalCancelled {
		return zero, replayRecordedError(ctx, event)
	}
	value, err := decodeRecorded[T](event)
	if err != nil {
		ctx.execCtx.Fail(err)
	}
	return value, err
}
