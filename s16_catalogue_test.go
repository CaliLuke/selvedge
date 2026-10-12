package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReplayHarnessCommandCatalogue(t *testing.T) {
	s16TestReplayHarnessCommandCatalogue(t, createTestApp)
}
func s16TestReplayHarnessCommandCatalogue(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"sync", "async", "reserved", "scope_before", "scope_pending", "root_pending", "root_before", "close_pending", "timers", "timer_cancel", "timeout_signal", "timeout_expired", "timeout_immediate", "receive", "receive_async", "await", "await_precheck", "now", "child_complete", "child_cancel", "failed", "panic"} {
		t.Run(kind, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			var calls atomic.Int32
			a := DefineActivity("A", func(context.Context, string) (string, error) {
				calls.Add(1)
				if kind == "activity_cancel_return" {
					return "", context.Canceled
				}
				return "value", nil
			})
			child := DefineWorkflow("child", func(ctx *WorkflowContext, _ string) (string, error) {
				if kind == "child_cancel" {
					return Receive[string](ctx, "never")
				}
				return "child-value", nil
			})
			RegisterWorkflow(h.app, child)
			w := DefineWorkflow("catalog", func(ctx *WorkflowContext, _ string) (string, error) {
				switch kind {
				case "sync", "root_before":
					return a.Execute(ctx, "")
				case "async", "reserved", "root_pending":
					f, err := a.ExecuteAsync(ctx, "")
					if err != nil {
						return "", err
					}
					return f.Get(ctx)
				case "close_pending":
					_, err := a.ExecuteAsync(ctx, "")
					return "closed", err
				case "scope_before":
					scoped, cancel := WithCancel(ctx)
					cancel()
					_, err := a.Execute(scoped, "")
					if !errors.Is(err, context.Canceled) {
						return "", fmt.Errorf("expected scope cancellation: %w", err)
					}
					return "cancelled", nil
				case "scope_pending":
					scoped, cancel := WithCancel(ctx)
					f, err := a.ExecuteAsync(scoped, "")
					if err != nil {
						return "", err
					}
					cancel()
					_, err = f.Get(ctx)
					if !errors.Is(err, context.Canceled) {
						return "", fmt.Errorf("expected pending cancellation: %w", err)
					}
					return "cancelled", nil
				case "timers":
					if err := Sleep(ctx, 0); err != nil {
						return "", err
					}
					timer, err := NewTimer(ctx, 0)
					if err != nil {
						return "", err
					}
					_, err = timer.Get(ctx)
					return "timers", err
				case "timer_cancel":
					scoped, cancel := WithCancel(ctx)
					timer, err := NewTimer(scoped, time.Hour)
					if err != nil {
						return "", err
					}
					cancel()
					_, err = timer.Get(ctx)
					if !errors.Is(err, context.Canceled) {
						return "", fmt.Errorf("expected timer cancellation: %w", err)
					}
					return "cancelled", nil
				case "timeout_signal":
					return ReceiveWithTimeout[string](ctx, "ch", time.Hour)
				case "timeout_expired", "timeout_immediate":
					d := time.Hour
					if kind == "timeout_immediate" {
						d = 0
					}
					_, err := ReceiveWithTimeout[string](ctx, "ch", d)
					if !errors.Is(err, context.DeadlineExceeded) {
						return "", fmt.Errorf("expected timeout: %w", err)
					}
					return "timeout", nil
				case "receive":
					return Receive[string](ctx, "ch")
				case "receive_async":
					if err := Await(ctx, func() bool { return Pending(ctx, "ch") > 0 }); err != nil {
						return "", err
					}
					value, ok, err := ReceiveAsync[string](ctx, "ch")
					if err != nil {
						return "", err
					}
					if !ok {
						return "", errors.New("missing signal")
					}
					return value, nil
				case "await":
					if err := Await(ctx, func() bool { return Pending(ctx, "ch") > 0 }); err != nil {
						return "", err
					}
					return Receive[string](ctx, "ch")
				case "await_precheck":
					if err := Await(ctx, func() bool { return true }); err != nil {
						return "", err
					}
					return "ready", nil
				case "now":
					value, err := Now(ctx)
					return value.Format(time.RFC3339Nano), err
				case "child_complete", "child_cancel":
					c, err := StartChild[string](ctx, "child", "")
					if err != nil {
						return "", err
					}
					if kind == "child_cancel" {
						if err = c.Cancel(ctx); err != nil {
							return "", err
						}
					}
					return c.Get(ctx)
				case "failed":
					return "", errors.New("recorded failure")
				case "panic":
					panic("recorded panic")
				default:
					return "", errors.New("unhandled catalog case")
				}
			})
			id := seedErrorWorkflow(t, h, w)
			if kind == "reserved" || kind == "root_pending" || kind == "close_pending" || kind == "scope_pending" {
				seedTask(t, h, id)
				held, err := h.ClaimTask(id, "#1")
				require.NoError(t, err)
				require.NotNil(t, held)
			}
			if kind == "receive" || kind == "receive_async" || kind == "await" || kind == "timeout_signal" {
				require.NoError(t, h.app.Signal(h.app.ctx, id, "ch", "signal-value"))
			}
			if kind == "root_before" {
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
			}
			runErr := h.RunOnce(id)
			switch kind {
			case "async":
				require.NoError(t, runErr)
				require.NoError(t, h.RunTasks(id))
				runErr = h.RunOnce(id)
			case "root_pending":
				require.NoError(t, runErr)
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
				runErr = h.RunOnce(id)
			case "timeout_expired":
				require.NoError(t, runErr)
				require.NoError(t, h.app.storage.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "#1"}))
				runErr = h.RunOnce(id)
			case "child_complete":
				require.NoError(t, runErr)
				require.NoError(t, h.RunOnce(id+"/#1"))
				runErr = h.RunOnce(id)
			}
			if kind == "root_before" || kind == "root_pending" || kind == "child_cancel" || kind == "failed" || kind == "panic" {
				require.Error(t, runErr)
			} else {
				require.NoError(t, runErr)
			}
			history, err := ExportHistory(h.app.ctx, h.app, id)
			require.NoError(t, err)
			if kind == "reserved" {
				require.Equal(t, StatusRunning, history.Instance.Status)
				require.Len(t, history.Events, 1)
				require.Nil(t, history.Events[0].Data)
			} else {
				require.NotEqual(t, StatusRunning, history.Instance.Status)
			}
			wantType := map[string]storage.HistoryEventType{"scope_before": storage.HistoryActivityCancelled, "scope_pending": storage.HistoryActivityCancelled, "root_pending": storage.HistoryActivityCancelled, "root_before": storage.HistoryActivityCancelled, "timer_cancel": storage.HistoryTimerCancelled, "timeout_signal": storage.HistoryTimerSuperseded, "timeout_expired": storage.HistoryTimerExpired, "await": storage.HistoryAwaitCompleted, "child_complete": storage.HistoryChildCompleted, "child_cancel": storage.HistoryChildCancelled}
			if want, ok := wantType[kind]; ok {
				found := false
				for _, event := range history.Events {
					if event.EventType == string(want) {
						found = true
					}
				}
				require.True(t, found, "missing %s in exported history", want)
			}
			if kind == "close_pending" {
				require.Nil(t, history.Events[0].Data)
				var status string
				require.NoError(t, h.app.storage.DB().QueryRow(`SELECT status FROM workflow_activity_tasks WHERE instance_id=$1`, id).Scan(&status))
				require.Equal(t, "cancelled", status)
			}
			if kind == "await_precheck" {
				require.Empty(t, history.Events)
			}
			data, err := json.Marshal(history)
			require.NoError(t, err)
			parsed, err := ParseHistory(data)
			require.NoError(t, err)
			require.Equal(t, history, parsed)
			clean := NewApp()
			RegisterWorkflow(clean, w)
			RegisterWorkflow(clean, child)
			before := calls.Load()
			require.NoError(t, ReplayHistory(context.Background(), clean, parsed))
			require.Equal(t, before, calls.Load())
			require.Empty(t, clean.activities)
			if kind == "child_cancel" {
				require.ErrorIs(t, h.RunOnce(id+"/#1"), context.Canceled)
			}
			if kind == "child_complete" || kind == "child_cancel" {
				childHistory, err := ExportHistory(h.app.ctx, h.app, id+"/#1")
				require.NoError(t, err)
				require.Equal(t, id, childHistory.Instance.ParentInstanceID)
				require.Equal(t, "#1", childHistory.Instance.ParentKey)
				require.NoError(t, ReplayHistory(context.Background(), clean, childHistory))
			}
		})
	}
}
