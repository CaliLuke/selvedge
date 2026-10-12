package selvedge

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// Timer is a durable timer tied to its issuing workflow execution.
// Use it serially on the workflow coroutine. Get may be called repeatedly.
type Timer struct {
	exec     *replay.ExecutionContext
	key      string
	row      *storage.HistoryEvent
	observed bool
	value    time.Time
	err      error
}

// NewTimer issues a sleep command without yielding. Nonpositive durations expire
// immediately; positive durations keep their first recorded deadline on replay.
func NewTimer(ctx *WorkflowContext, d time.Duration, opts ...SleepOption) (*Timer, error) {
	if err := ctx.execCtx.Check(); err != nil {
		return nil, err
	}
	options := &sleepOptions{}
	for _, opt := range opts {
		opt(options)
	}
	if options.err != nil {
		ctx.execCtx.Fail(options.err)
		return nil, options.err
	}
	cmd, err := ctx.execCtx.Issue("sleep", options.timerID)
	if err != nil {
		return nil, err
	}
	defer cmd.End()
	row := cmd.Recorded
	if row == nil && d <= 0 {
		row, err = ctx.execCtx.Storage().RecordHistory(ctx.Context(), ctx.execCtx.Fence(), immediateTimerEvent(ctx.InstanceID(), cmd.Key, cmd.Seq, workflowTime(ctx).Add(d)))
	} else if row == nil || row.EventData == nil {
		row, err = ctx.execCtx.Storage().ReserveTimer(ctx.Context(), ctx.execCtx.Fence(), cmd.Seq, &storage.TimerSubscription{InstanceID: ctx.InstanceID(), TimerID: cmd.Key, ActivityID: cmd.Key, ExpiresAt: workflowTime(ctx).Add(d)})
	}
	if err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	var expiry storage.SleepReservation
	if row.ReservationData != nil {
		err = json.Unmarshal(row.ReservationData, &expiry)
	} else {
		expiry, err = decodeRecorded[storage.SleepReservation](row)
	}
	if err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	if row.DeliveredSeq != 0 {
		ctx.execCtx.ApplyDelivery(row.DeliveredSeq, storage.DeliveryBatch{HistoryKeys: []string{cmd.Key}})
	}
	if h := ctx.Hooks(); h != nil {
		h.OnTimerStart(ctx.Context(), hooks.TimerStartInfo{InstanceID: ctx.InstanceID(), WorkflowName: ctx.WorkflowName(), TimerID: cmd.Key, ExpiresAt: expiry.ExpiresAt})
	}
	return &Timer{exec: ctx.execCtx, key: cmd.Key, row: row}, nil
}

func immediateTimerEvent(instanceID, key string, seq int64, expiresAt time.Time) *storage.HistoryEvent {
	data, _ := json.Marshal(storage.SleepReservation{ExpiresAt: expiresAt})
	return &storage.HistoryEvent{InstanceID: instanceID, ActivityID: key, Seq: seq, Command: "sleep", EventType: storage.HistoryTimerExpired, EventData: data, DataType: "json"}
}

// Key returns the history key assigned at issue time.
func (t *Timer) Key() string { return t.key }

// IsReady reads only the delivered view or an outcome returned by Get. It is safe
// in Await predicates. Off-coroutine calls are unsupported and not detected.
func (t *Timer) IsReady() bool { return t.observed || t.exec.Delivered(t.key) }

// Get returns the recorded expiry or cancellation without consuming a command
// position or delivering a batch. A stored fire takes precedence over cancellation.
func (t *Timer) Get(ctx *WorkflowContext) (time.Time, error) {
	end, err := ctx.execCtx.Enter()
	if err != nil {
		return time.Time{}, err
	}
	defer end()
	if t.exec != ctx.execCtx {
		err := &NondeterminismError{Seq: ctx.execCtx.Position(), Key: t.key, Reason: "timer belongs to another workflow execution"}
		ctx.execCtx.Fail(err)
		return time.Time{}, err
	}
	if t.observed {
		return t.value, t.err
	}
	for {
		if err := ctx.execCtx.Check(); err != nil {
			return time.Time{}, err
		}
		row := t.row
		if row == nil || row.EventData == nil {
			row, err = ctx.Storage().GetHistoryEvent(ctx.Context(), ctx.InstanceID(), t.key)
		}
		if err != nil {
			ctx.execCtx.Fail(err)
			return time.Time{}, err
		}
		if row != nil && row.EventData != nil {
			return t.outcome(ctx, row)
		}
		requested, err := ctx.execCtx.Storage().IsCancelRequested(ctx.Context(), ctx.InstanceID())
		if err != nil {
			ctx.execCtx.Fail(err)
			return time.Time{}, err
		}
		if requested {
			cancellation, err := cancellationAtYield(ctx)
			if err != nil {
				return time.Time{}, err
			}
			row, err = ctx.execCtx.Storage().SetHistoryValueIfNull(ctx.Context(), ctx.execCtx.Fence(), ctx.InstanceID(), t.key, "sleep", storage.HistoryTimerCancelled, encodeError(cancellation))
			if err != nil {
				ctx.execCtx.Fail(err)
				return time.Time{}, err
			}
			return t.outcome(ctx, row)
		}
		if err := ctx.execCtx.Yield(replay.Wait{Seq: ctx.execCtx.Position(), Kind: replay.WaitTimer, Key: t.key}); err != nil {
			return time.Time{}, err
		}
	}
}

func (t *Timer) outcome(ctx *WorkflowContext, row *storage.HistoryEvent) (time.Time, error) {
	t.observed = true
	switch row.EventType {
	case storage.HistoryTimerExpired:
		t.value, t.err = observedTimerExpiry(ctx, row)
	case storage.HistoryTimerCancelled:
		t.err = replayRecordedError(ctx, row)
	default:
		t.err = fmt.Errorf("internal timer outcome %s at %s", row.EventType, t.key)
		ctx.execCtx.Fail(t.err)
	}
	return t.value, t.err
}

func observedTimerExpiry(ctx *WorkflowContext, row *storage.HistoryEvent) (time.Time, error) {
	value, err := decodeRecorded[storage.SleepReservation](row)
	if err != nil {
		ctx.execCtx.Fail(err)
		return time.Time{}, err
	}
	expiresAt := value.ExpiresAt.UTC()
	ctx.timeFloor.observe(expiresAt)
	return expiresAt, nil
}
