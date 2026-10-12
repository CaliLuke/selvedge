package selvedge

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

type s16Command struct{ command, key string }

func s16Commands(names ...string) []s16Command {
	result := make([]s16Command, len(names))
	for i, name := range names {
		result[i].command = name
	}
	return result
}
func s16Issue(ctx *WorkflowContext, c s16Command) error {
	switch {
	case strings.HasPrefix(c.command, "activity:"):
		a := DefineActivity(strings.TrimPrefix(c.command, "activity:"), func(context.Context, string) (string, error) { panic("replay invoked activity") })
		_, err := a.Execute(ctx, "", WithActivityID(c.key))
		return err
	case c.command == "async":
		a := DefineActivity("B", func(context.Context, string) (string, error) { panic("replay invoked async activity") })
		_, err := a.ExecuteAsync(ctx, "")
		return err
	case c.command == "sleep":
		return Sleep(ctx, time.Hour, WithSleepID(c.key))
	case c.command == "now":
		_, err := Now(ctx)
		return err
	case c.command == "await":
		return Await(ctx, func() bool { return false })
	case c.command == "cancel_scope":
		_, cancel := WithCancel(ctx)
		cancel()
		return ctx.execCtx.Check()
	case c.command == "receive:ch":
		_, err := Receive[string](ctx, "ch")
		return err
	case c.command == "receive_async:ch":
		_, _, err := ReceiveAsync[string](ctx, "ch")
		return err
	case c.command == "receive_timeout:ch":
		_, err := ReceiveWithTimeout[string](ctx, "ch", 0)
		return err
	case c.command == "receive_timeout_long":
		_, err := ReceiveWithTimeout[string](ctx, "ch", time.Hour)
		return err
	case strings.HasPrefix(c.command, "child:"):
		_, err := StartChild[string](ctx, strings.TrimPrefix(c.command, "child:"), "", WithChildID(c.key))
		return err
	default:
		return fmt.Errorf("unknown test command %s", c.command)
	}
}
func TestReplayHarnessDetectsDivergence(t *testing.T) {
	s16TestReplayHarnessDetectsDivergence(t, createTestApp)
}
func s16TestReplayHarnessDetectsDivergence(t *testing.T, factory replayAppFactory) {
	cases := []struct {
		name             string
		recorded, issued []s16Command
		unfilled         bool
	}{
		{"reorder", s16Commands("activity:A", "activity:B"), s16Commands("activity:B", "activity:A"), false},
		{"insert", s16Commands("activity:A"), s16Commands("activity:C", "activity:A"), false},
		{"remove_middle", s16Commands("activity:B", "activity:A", "activity:C"), s16Commands("activity:B", "activity:C"), false},
		{"remove_final_sleep", s16Commands("activity:A", "sleep"), s16Commands("activity:A"), false},
		{"explicit_change", []s16Command{{"activity:A", "old"}}, []s16Command{{"activity:A", "new"}}, false},
		{"explicit_move", []s16Command{{"activity:A", "first"}, {"activity:A", "second"}}, []s16Command{{"activity:A", "second"}}, false},
		{"leftover_on_suspend", s16Commands("sleep", "activity:A"), s16Commands("sleep"), true},
		{"async_activity", s16Commands("activity:A"), s16Commands("async"), false},
		{"none_removed", s16Commands("receive_async:ch"), nil, false},
		{"cancel_scope_at_sleep", s16Commands("sleep"), s16Commands("cancel_scope", "sleep"), false},
		{"async_receive", s16Commands("receive_async:ch"), s16Commands("receive:ch"), false},
		{"timeout_zero_to_receive", s16Commands("receive_timeout:ch"), s16Commands("receive:ch"), false},
		{"timeout_long_to_receive", s16Commands("sleep", "receive_timeout:ch"), s16Commands("receive:ch"), false},
		{"child_workflow", []s16Command{{"child:child", "child-id"}}, []s16Command{{"child:other", "child-id"}}, false},
		{"child_id", []s16Command{{"child:child", "child-id"}}, []s16Command{{"child:child", "other-id"}}, false},
		{"empty_command", s16Commands(""), s16Commands("activity:A"), false},
	}
	cases = slices.Grow(cases, 12)
	for _, command := range []string{"sleep", "receive:ch", "await", "now", "cancel_scope", "child:child"} {
		cases = append(cases, struct {
			name             string
			recorded, issued []s16Command
			unfilled         bool
		}{command + "_at_activity", s16Commands("activity:A"), s16Commands(command), false})
	}
	for _, command := range []string{"now", "await", "cancel_scope"} {
		cases = append(cases, struct {
			name             string
			recorded, issued []s16Command
			unfilled         bool
		}{command + "_removed", s16Commands(command), nil, false})
		cases = append(cases, struct {
			name             string
			recorded, issued []s16Command
			unfilled         bool
		}{command + "_inserted", s16Commands("activity:A"), s16Commands(command, "activity:A"), false})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			w := DefineWorkflow("divergence", func(ctx *WorkflowContext, _ string) (string, error) {
				for _, command := range tc.issued {
					if err := s16Issue(ctx, command); err != nil {
						return "", err
					}
				}
				return "done", nil
			})
			id := seedErrorWorkflow(t, h, w)
			for i, c := range tc.recorded {
				key := c.key
				if key == "" || strings.HasPrefix(c.command, "child:") {
					key = fmt.Sprintf("#%d", i+1)
				}
				event, data := storage.HistoryActivityCompleted, []byte(`"recorded"`)
				reservation := []byte(`{"expires_at":"2099-01-01T00:00:00Z"}`)
				if c.command == "sleep" {
					event = storage.HistoryTimerExpired
					data = []byte(`true`)
				}
				if strings.HasPrefix(c.command, "child:") {
					event = storage.HistoryReserved
					data = nil
					reservation = []byte(fmt.Sprintf(`{"child_id":%q,"workflow":%q}`, c.key, strings.TrimPrefix(c.command, "child:")))
				}
				if tc.unfilled && i == 0 {
					event = storage.HistoryReserved
					data = nil
				}
				_, err := fixtureRecord(h.app.ctx, h.app.storage, &storage.HistoryEvent{InstanceID: id, ActivityID: key, Command: c.command, EventType: event, EventData: data, ReservationData: reservation, DataType: "json"})
				require.NoError(t, err)
			}
			history, err := ExportHistory(h.app.ctx, h.app, id)
			require.NoError(t, err)
			var checked, live *NondeterminismError
			require.ErrorAs(t, ReplayHistory(h.app.ctx, h.app, history), &checked)
			require.ErrorAs(t, h.RunOnce(id), &live)
			require.Equal(t, live, checked, "verification and live replay must identify the exact same divergence")
		})
	}
}
