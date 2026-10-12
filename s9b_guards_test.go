package selvedge

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestTimerGuards(t *testing.T) { testTimerGuards(t, createTestApp) }
func testTimerGuards(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"new", "get", "timeout", "invalid-id", "invalid-channel"} {
		t.Run(kind, func(t *testing.T) {
			h, _, _ := leaseHarness(t, factory)
			w := DefineWorkflow("timer-guard", func(ctx *WorkflowContext, _ string) (string, error) {
				var timer *Timer
				if kind == "get" {
					var err error
					timer, err = NewTimer(ctx, 0)
					if err != nil {
						return "", err
					}
				}
				_ = Await(ctx, func() bool {
					switch kind {
					case "new":
						_, _ = NewTimer(ctx, time.Hour)
					case "get":
						_, _ = timer.Get(ctx)
					case "timeout":
						_, _ = ReceiveWithTimeout[int](ctx, "c", time.Hour)
					case "invalid-id":
						_, _ = NewTimer(ctx, 0, WithSleepID("#1"))
					case "invalid-channel":
						_, _ = ReceiveWithTimeout[int](ctx, "", 0)
					}
					return true
				})
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			var nondet *NondeterminismError
			require.ErrorAs(t, h.RunOnce(id), &nondet)
		})
	}
	for _, kind := range []string{"superseded", "corrupt", "foreign"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			var previous *Timer
			w := DefineWorkflow("timer-invalid", func(ctx *WorkflowContext, _ string) (string, error) {
				timer, err := NewTimer(ctx, time.Hour)
				if err != nil {
					return "", err
				}
				if kind == "foreign" {
					if previous == nil {
						previous = timer
						return "", Sleep(ctx, time.Hour)
					}
					_, _ = previous.Get(ctx)
				} else {
					_, _ = timer.Get(ctx)
				}
				return "ignored", nil
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			if kind != "foreign" {
				typ, data := "TimerSuperseded", `true`
				if kind == "corrupt" {
					typ, data = "TimerExpired", `{"expires_at":123}`
				}
				_, err := fs.DB().Exec(`UPDATE workflow_history SET event_type=$1,event_data=$2 WHERE instance_id=$3 AND activity_id='#1'`, typ, data, id)
				require.NoError(t, err)
			}
			require.NoError(t, fs.MarkRunnable(h.app.ctx, id))
			err := h.RunOnce(id)
			require.Error(t, err)
			switch kind {
			case "foreign":
				var e *NondeterminismError
				require.ErrorAs(t, err, &e)
			case "corrupt":
				var e *ReplayDecodeError
				require.ErrorAs(t, err, &e)
			case "superseded":
				require.True(t, strings.Contains(strings.ToLower(err.Error()), "superseded"))
			}
		})
	}
}
func TestTimerGetFireWinsCancel(t *testing.T) { testTimerGetFireWinsCancel(t, createTestApp) }
func testTimerGetFireWinsCancel(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	fireAt := time.Unix(123, 0).UTC()
	var got time.Time
	w := DefineWorkflow("fire-cancel", func(ctx *WorkflowContext, _ string) (string, error) {
		timer, err := NewTimer(ctx, time.Hour)
		if err != nil {
			return "", err
		}
		got, err = timer.Get(ctx)
		return "done", err
	})
	id := seedErrorWorkflow(t, h, w)
	require.NoError(t, h.RunOnce(id))
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
	fs.Before = func(_ context.Context, method string) error {
		if method == "SetHistoryValueIfNull" {
			return fs.Storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1", ExpiresAt: fireAt})
		}
		return nil
	}
	require.NoError(t, h.RunOnce(id))
	require.Equal(t, fireAt, got)
	row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
	require.NoError(t, err)
	require.Equal(t, storage.HistoryTimerExpired, row.EventType)
	require.Zero(t, row.DeliveredSeq)
	require.False(t, errors.Is(err, context.Canceled))
}
