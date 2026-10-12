package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReplayHarnessWritesNothing(t *testing.T) {
	s16TestReplayHarnessWritesNothing(t, createTestApp)
}
func s16TestReplayHarnessWritesNothing(t *testing.T, factory replayAppFactory) {
	for _, state := range []string{"unrecorded", "completed", "failed", "cancelled", "reserved", "storage_stop", "defer"} {
		t.Run(state, func(t *testing.T) {
			h, fs := s14Harness(t, factory)
			calls := 0
			a := DefineActivity("A", func(context.Context, string) (string, error) { calls++; return "value", nil })
			var deferErr error
			w := DefineWorkflow("readonly", func(ctx *WorkflowContext, _ string) (string, error) {
				if state == "defer" {
					defer func() { _, deferErr = a.Execute(ctx, "") }()
				}
				switch state {
				case "unrecorded", "defer":
					return a.Execute(ctx, "")
				case "reserved":
					f, err := a.ExecuteAsync(ctx, "")
					if err != nil {
						return "", err
					}
					return f.Get(ctx)
				case "failed":
					return "", errors.New("failed")
				case "cancelled":
					return Receive[string](ctx, "ch")
				case "storage_stop":
					_, err := ctx.execCtx.Storage().ListInstances(ctx.Context(), storage.ListInstancesOptions{})
					return "", err
				default:
					return "done", nil
				}
			})
			id := seedErrorWorkflow(t, h, w)
			if state == "reserved" {
				seedTask(t, h, id)
				held, err := h.ClaimTask(id, "#1")
				require.NoError(t, err)
				require.NotNil(t, held)
			}
			switch state {
			case "completed", "reserved":
				require.NoError(t, h.RunOnce(id))
			case "failed":
				require.Error(t, h.RunOnce(id))
			case "cancelled":
				require.NoError(t, CancelWorkflow(h.app.ctx, h.app, id, "stop"))
				require.ErrorIs(t, h.RunOnce(id), context.Canceled)
			}
			observed := &s14Hooks{}
			h.app.hooks = observed
			h.app.engine = replay.NewEngine(fs, observed, "verify", errorCodec{})
			before := s14Dump(t, fs)
			statementsAtSnapshot, callsAtSnapshot := -1, -1
			fs.After = func(_ context.Context, method string) error {
				if method == "ReadSnapshot" {
					statementsAtSnapshot = len(fs.Statements())
					callsAtSnapshot = len(fs.Calls())
				}
				return nil
			}
			require.NoError(t, ReplayInstance(context.Background(), h.app, id))
			require.GreaterOrEqual(t, statementsAtSnapshot, 0)
			require.Len(t, fs.Statements(), statementsAtSnapshot)
			require.Len(t, fs.Calls(), callsAtSnapshot)
			history, err := ExportHistory(h.app.ctx, h.app, id)
			require.NoError(t, err)
			data, err := json.Marshal(history)
			require.NoError(t, err)
			parsed, err := ParseHistory(data)
			require.NoError(t, err)
			clean := NewApp(WithHooks(observed))
			RegisterWorkflow(clean, w)
			require.NoError(t, ReplayHistory(context.Background(), clean, parsed))
			require.Empty(t, clean.activities)
			require.Nil(t, clean.storage)
			require.Zero(t, calls)
			require.Zero(t, observed.calls)
			require.Equal(t, before, s14Dump(t, fs))
			if state == "defer" {
				require.EqualError(t, deferErr, "workflow execution closed")
			}
		})
	}
}

func TestReplayInstanceFromDatabase(t *testing.T) {
	s16TestReplayInstanceFromDatabase(t, createTestApp)
}
func s16TestReplayInstanceFromDatabase(t *testing.T, factory replayAppFactory) {
	for _, race := range []string{"after_snapshot", "during_commit", "during_snapshot"} {
		t.Run(race, func(t *testing.T) {
			h, fs := s14Harness(t, factory)
			w := DefineWorkflow("snapshot", func(ctx *WorkflowContext, _ string) (string, error) {
				if err := Await(ctx, func() bool { return Pending(ctx, "ch") > 0 }); err != nil {
					return "", err
				}
				return Receive[string](ctx, "ch")
			})
			id := seedErrorWorkflow(t, h, w)
			require.NoError(t, h.RunOnce(id))
			switch race {
			case "during_snapshot":
				require.NoError(t, h.app.Signal(h.app.ctx, id, "ch", "value"))
				reached, release := make(chan struct{}), make(chan struct{})
				var paused atomic.Bool
				storage.WithTestStatementObserver(func(query string, _ []any) {
					if strings.Contains(query, "FROM workflow_history") && paused.CompareAndSwap(false, true) {
						close(reached)
						<-release
					}
				})(fs.Storage)
				type result struct {
					history *RecordedHistory
					err     error
				}
				done := make(chan result, 1)
				go func() { history, err := ExportHistory(h.app.ctx, h.app, id); done <- result{history, err} }()
				select {
				case <-reached:
				case <-time.After(5 * time.Second):
					t.Fatal("snapshot history read not reached")
				}
				runErr := h.RunOnce(id)
				close(release)
				old := <-done
				require.NoError(t, runErr)
				require.NoError(t, old.err)
				require.Equal(t, StatusRunning, old.history.Instance.Status)
				require.Nil(t, old.history.Instance.Output)
				require.Empty(t, old.history.Events)
				require.Empty(t, old.history.Signals)
				require.NoError(t, ReplayHistory(h.app.ctx, h.app, old.history))
			case "after_snapshot":
				fired := false
				fs.After = func(ctx context.Context, method string) error {
					if method == "ReadSnapshot" && !fired {
						fired = true
						_, err := fs.Storage.AppendSignal(ctx, id, "ch", []byte(`"value"`))
						return err
					}
					return nil
				}
				old, err := ExportHistory(h.app.ctx, h.app, id)
				require.NoError(t, err)
				require.Empty(t, old.Signals)
				require.NoError(t, ReplayHistory(h.app.ctx, h.app, old))
				fs.After = nil
				require.NoError(t, h.RunOnce(id))
			default:
				require.NoError(t, h.app.Signal(h.app.ctx, id, "ch", "value"))
				reached, release := make(chan struct{}), make(chan struct{})
				first := true
				fs.Before = func(_ context.Context, method string) error {
					if method == "CommitObservation" && first {
						first = false
						close(reached)
						<-release
					}
					return nil
				}
				done := make(chan error, 1)
				go func() { done <- h.RunOnce(id) }()
				select {
				case <-reached:
				case <-time.After(5 * time.Second):
					t.Fatal("commit not reached")
				}
				err := ReplayInstance(h.app.ctx, h.app, id)
				close(release)
				require.NoError(t, <-done)
				fs.Before = nil
				require.NoError(t, err)
			}
			require.NoError(t, ReplayInstance(h.app.ctx, h.app, id))
			closed, err := ExportHistory(h.app.ctx, h.app, id)
			require.NoError(t, err)
			require.Equal(t, StatusCompleted, closed.Instance.Status)
			require.JSONEq(t, `"value"`, string(closed.Instance.Output))
			require.Len(t, closed.Signals, 1)
			require.Positive(t, closed.Signals[0].ConsumedSeq)
		})
	}
}
