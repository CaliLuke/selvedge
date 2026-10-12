package selvedge

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestQueryHandlerCommandsRejected(t *testing.T) {
	s14TestQueryHandlerCommandsRejected(t, createTestApp)
}
func s14TestQueryHandlerCommandsRejected(t *testing.T, factory replayAppFactory) {
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "bad", nil })
	actions := map[string]func(*WorkflowContext){
		"issue":           func(c *WorkflowContext) { _, _ = c.execCtx.Issue("now", "") },
		"execute":         func(c *WorkflowContext) { _, _ = a.Execute(c, "") },
		"execute_async":   func(c *WorkflowContext) { _, _ = a.ExecuteAsync(c, "") },
		"get":             func(c *WorkflowContext) { _, _ = (&Future[string]{exec: c.execCtx}).Get(c) },
		"sleep":           func(c *WorkflowContext) { _ = Sleep(c, time.Hour) },
		"sleep_until":     func(c *WorkflowContext) { _ = SleepUntil(c, time.Now()) },
		"new_timer":       func(c *WorkflowContext) { _, _ = NewTimer(c, time.Hour) },
		"timer_get":       func(c *WorkflowContext) { _, _ = (&Timer{exec: c.execCtx}).Get(c) },
		"await":           func(c *WorkflowContext) { _ = Await(c, func() bool { return true }) },
		"receive":         func(c *WorkflowContext) { _, _ = Receive[int](c, "ch") },
		"receive_async":   func(c *WorkflowContext) { _, _, _ = ReceiveAsync[int](c, "ch") },
		"receive_timeout": func(c *WorkflowContext) { _, _ = ReceiveWithTimeout[int](c, "ch", 0) },
		"now":             func(c *WorkflowContext) { _, _ = Now(c) },
		"start_child":     func(c *WorkflowContext) { _, _ = StartChild[string](c, "child", "") },
		"child_get":       func(c *WorkflowContext) { _, _ = (&Child[string]{exec: c.execCtx}).Get(c) },
		"child_cancel":    func(c *WorkflowContext) { _ = (&Child[string]{exec: c.execCtx}).Cancel(c) },
		"cancel_func":     func(c *WorkflowContext) { _, cancel := WithCancel(c); cancel() },
		"registration":    func(c *WorkflowContext) { _ = SetQueryHandler(c, "nested", func() (int, error) { return 1, nil }) },
	}
	for name, action := range actions {
		t.Run(name, func(t *testing.T) {
			h, _ := s14Harness(t, factory)
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = SetQueryHandler(ctx, "bad", func() (bool, error) { action(ctx); return true, nil })
				return "", nil
			})
			before := s14Dump(t, h.app.storage)
			_, err := Query[bool](context.Background(), h.app, id, "bad")
			require.ErrorIs(t, err, ErrQueryCommand)
			require.Equal(t, before, s14Dump(t, h.app.storage))
		})
	}
}

func s14Dump(t *testing.T, s storage.Storage) map[string][]string {
	t.Helper()
	result := map[string][]string{}
	for _, table := range []string{"workflow_instances", "workflow_history", "workflow_signals", "workflow_activity_tasks", "workflow_timer_subscriptions", "workflow_definitions", "system_locks"} {
		rows, err := s.DB().Query(`SELECT * FROM ` + table)
		require.NoError(t, err)
		columns, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			require.NoError(t, rows.Scan(dest...))
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					values[i] = string(b)
				}
			}
			result[table] = append(result[table], fmt.Sprint(values))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sort.Strings(result[table])
	}
	return result
}

type s14Hooks struct {
	hooks.NoOpHooks
	calls int
}

func (h *s14Hooks) OnWorkflowStart(context.Context, hooks.WorkflowStartInfo)       { h.calls++ }
func (h *s14Hooks) OnWorkflowComplete(context.Context, hooks.WorkflowCompleteInfo) { h.calls++ }
func (h *s14Hooks) OnWorkflowFailed(context.Context, hooks.WorkflowFailedInfo)     { h.calls++ }
func (h *s14Hooks) OnReplayStart(context.Context, hooks.ReplayStartInfo)           { h.calls++ }
func (h *s14Hooks) OnActivityStart(context.Context, hooks.ActivityStartInfo)       { h.calls++ }
func (h *s14Hooks) OnActivityCacheHit(context.Context, hooks.ActivityCacheHitInfo) { h.calls++ }
func (h *s14Hooks) OnTimerStart(context.Context, hooks.TimerStartInfo)             { h.calls++ }

func TestQueryWritesNothing(t *testing.T) { s14TestQueryWritesNothing(t, createTestApp) }
func s14TestQueryWritesNothing(t *testing.T, factory replayAppFactory) {
	for _, state := range []string{"unexecuted", "parked", "running_activity", "completed", "failed", "cancelled", "nondeterministic"} {
		t.Run(state, func(t *testing.T) {
			h, fs := s14Harness(t, factory)
			handlerCalls := 0
			a := DefineActivity("query-activity", func(context.Context, string) (string, error) { return "work", nil })
			id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
				_ = SetQueryHandler(ctx, "value", func() (string, error) { handlerCalls++; return "answer", nil })
				switch state {
				case "parked", "unexecuted", "cancelled":
					return Receive[string](ctx, "ch")
				case "running_activity":
					return a.Execute(ctx, "")
				case "failed":
					return "", errors.New("failure")
				case "nondeterministic":
					_, err := Now(ctx)
					return "", err
				default:
					return "done", nil
				}
			})
			switch state {
			case "unexecuted":
			case "running_activity":
				f, ok, err := fs.AcquireLease(context.Background(), id, "held", time.Minute)
				require.NoError(t, err)
				require.True(t, ok)
				_, err = fs.ReserveActivity(context.Background(), &f, 1, "#1", "activity:query-activity", &storage.ActivityTask{InstanceID: id, ActivityName: a.Name(), InputData: []byte(`""`)})
				require.NoError(t, err)
			case "nondeterministic":
				f, ok, err := fs.AcquireLease(context.Background(), id, "held", time.Minute)
				require.NoError(t, err)
				require.True(t, ok)
				_, err = fs.ReserveHistory(context.Background(), &f, id, "#1", "sleep", 1)
				require.NoError(t, err)
			case "failed":
				require.Error(t, h.RunOnce(id))
			case "cancelled":
				_, err := fs.RequestCancel(context.Background(), id, "reason")
				require.NoError(t, err)
				require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			default:
				require.NoError(t, h.RunOnce(id))
			}
			require.Zero(t, handlerCalls, "live executions must never evaluate handlers")
			observed := &s14Hooks{}
			h.app.hooks = observed
			h.app.engine = replay.NewEngine(fs, observed, "s14", errorCodec{})
			before := s14Dump(t, fs)
			statementsAtSnapshot := -1
			fs.After = func(_ context.Context, method string) error {
				if method == "ReadSnapshot" {
					statementsAtSnapshot = len(fs.Statements())
				}
				return nil
			}
			_, err := Query[string](context.Background(), h.app, id, "value")
			if state == "nondeterministic" {
				var nd *NondeterminismError
				require.ErrorAs(t, err, &nd)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, handlerCalls)
			}
			require.GreaterOrEqual(t, statementsAtSnapshot, 0)
			require.Len(t, fs.Statements(), statementsAtSnapshot, "no SQL may follow snapshot release")
			require.Equal(t, before, s14Dump(t, fs))
			require.Zero(t, observed.calls)
			require.Empty(t, h.app.activities)
		})
	}
}

func TestReplayCommandMismatchFailsFast_Query(t *testing.T) {
	s14TestReplayCommandMismatchFailsFastQuery(t, createTestApp)
}
func s14TestReplayCommandMismatchFailsFastQuery(t *testing.T, factory replayAppFactory) {
	h, _ := s14Harness(t, factory)
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		_ = s14RegisterPosition(ctx)
		_, err := Now(ctx)
		return "", err
	})
	f, ok, err := h.app.storage.AcquireLease(context.Background(), id, "fixture", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = h.app.storage.ReserveTimer(context.Background(), &f, 1, &storage.TimerSubscription{InstanceID: id, ActivityID: "#1", TimerID: "#1", ExpiresAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	require.NoError(t, h.app.storage.ReleaseLease(context.Background(), f, true))
	before := s14Dump(t, h.app.storage)
	_, err = Query[int64](context.Background(), h.app, id, "q")
	var nd *NondeterminismError
	require.ErrorAs(t, err, &nd)
	require.Equal(t, before, s14Dump(t, h.app.storage))
	require.ErrorAs(t, h.RunOnce(id), &nd)
	row, err := h.app.storage.GetHistoryEvent(context.Background(), id, "$workflow_failed")
	require.NoError(t, err)
	require.NotNil(t, row)
}

func TestQueryDuringInlineActivity(t *testing.T) { s14TestQueryDuringInlineActivity(t, createTestApp) }
func s14TestQueryDuringInlineActivity(t *testing.T, factory replayAppFactory) {
	h, fs := s14Harness(t, factory)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	a := DefineActivity("inline", func(context.Context, string) (string, error) { close(entered); <-release; return "result", nil })
	id := s14Seed(t, h, func(ctx *WorkflowContext, _ string) (string, error) {
		value := "waiting"
		_ = SetQueryHandler(ctx, "value", func() (string, error) { return value, nil })
		v, err := a.Execute(ctx, "")
		value = v
		return v, err
	})
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	<-entered
	before := s14Dump(t, fs)
	require.Equal(t, "waiting", s14Query[string](t, h, id, "value"))
	require.Equal(t, before, s14Dump(t, fs))
	once.Do(func() { close(release) })
	require.NoError(t, <-done)
	require.Equal(t, "result", s14Query[string](t, h, id, "value"))
}
