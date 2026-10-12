package selvedge

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// WithClock sets the wall clock used by Now. A nil clock uses time.Now.
// It does not change lease clocks, timer expiry, or the timer poller.
func WithClock(clock func() time.Time) Option {
	return func(c *appConfig) { c.clock = clock }
}

// Now returns recorded workflow time in UTC, without a monotonic reading.
// Each live call writes one fenced history observation; replay returns that
// observation. Time never falls below earlier Now values in this execution.
// On error, Now returns the last observed time and the run fails even if the
// caller ignores the error.
func Now(ctx *WorkflowContext) (value time.Time, err error) {
	defer func() {
		if err != nil {
			ctx.execCtx.Fail(err)
			value = ctx.timeFloor.load()
		}
	}()
	if err := ctx.execCtx.Check(); err != nil {
		return time.Time{}, err
	}
	cmd, err := ctx.execCtx.Issue("now", "")
	if err != nil {
		return time.Time{}, err
	}
	defer cmd.End()
	if cmd.Recorded != nil {
		return nowOutcome(ctx, cmd.Recorded)
	}

	clock := time.Now
	if ctx.app != nil && ctx.app.config.clock != nil {
		clock = ctx.app.config.clock
	}
	value = clock().UTC()
	if floor := ctx.timeFloor.load(); value.Before(floor) {
		value = floor
	}
	data, err := json.Marshal(value)
	if err != nil {
		return time.Time{}, fmt.Errorf("encode workflow time: %w", err)
	}
	stored, err := ctx.execCtx.Storage().RecordHistory(ctx.Context(), ctx.execCtx.Fence(), &storage.HistoryEvent{
		InstanceID: ctx.InstanceID(), Seq: cmd.Seq, ActivityID: cmd.Key,
		Command: "now", EventType: storage.HistoryNowRecorded, EventData: data, DataType: "json",
	})
	if err != nil {
		return time.Time{}, err
	}
	return nowOutcome(ctx, stored)
}

func nowOutcome(ctx *WorkflowContext, event *storage.HistoryEvent) (time.Time, error) {
	var value time.Time
	var encoded string
	var err error
	if event.EventType != storage.HistoryNowRecorded {
		err = fmt.Errorf("expected NowRecorded event, got %q", event.EventType)
	} else if err = json.Unmarshal(event.EventData, &encoded); err == nil {
		value, err = time.Parse(time.RFC3339Nano, encoded)
	}
	if err != nil {
		return time.Time{}, &ReplayDecodeError{
			InstanceID: event.InstanceID, Key: event.ActivityID, GoType: "time.Time", Err: err,
		}
	}
	value = value.UTC()
	ctx.timeFloor.observe(value)
	return value, nil
}

// The pointer is shared by WithContext copies, but recreated on every execution.
// Synchronization also protects error returns from rejected overlapping commands.
type workflowTimeFloor struct {
	mu    sync.Mutex
	value time.Time
}

func (f *workflowTimeFloor) load() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.value
}

func (f *workflowTimeFloor) observe(value time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if value.After(f.value) {
		f.value = value
	}
}
