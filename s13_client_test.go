package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func s13AppPair(t *testing.T, factory replayAppFactory) (*App, *App) {
	t.Helper()
	worker, cleanup := factory(t, WithWorkflowResumptionInterval(time.Hour))
	t.Cleanup(cleanup)
	require.NoError(t, worker.Start(context.Background()))
	client := NewApp(WithDatabase(worker.config.databaseURL), WithAutoMigrate(false), WithWorkflowResumptionInterval(time.Hour))
	require.NoError(t, client.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, client.Shutdown(context.Background())) })
	return worker, client
}

func s13Wait[O any](t *testing.T, app *App, id string) *WorkflowResult[O] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := WaitWorkflow[O](ctx, app, id)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEqual(t, "running", result.Status)
	return result
}

func TestStartByNameWithoutRegistration(t *testing.T) { s13StartByName(t, createTestApp) }
func s13StartByName(t *testing.T, factory replayAppFactory) {
	worker, client := s13AppPair(t, factory)
	RegisterWorkflow(worker, DefineWorkflow("s13_echo", func(_ *WorkflowContext, in string) (string, error) { return in + "!", nil }))
	id, err := StartWorkflowByName(context.Background(), client, "s13_echo", "client")
	require.NoError(t, err)
	require.Empty(t, client.workflowRunners)
	require.Equal(t, "client!", s13Wait[string](t, client, id).Output)
}

func TestStartRejectedBeforeInsert(t *testing.T) { s13StartRejected(t, createTestApp) }
func s13StartRejected(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"never_started", "empty", "long", "encode", "cancelled", "generic"} {
		t.Run(kind, func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			fs := faultstore.New(h.app.storage)
			h.app.storage = fs
			app, name, input := h.app, "s13_reject", any("input")
			ctx := context.Background()
			switch kind {
			case "never_started", "generic":
				app = NewApp()
			case "empty":
				name = ""
			case "long":
				name = strings.Repeat("x", 256)
			case "encode":
				input = make(chan int)
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			var id string
			var err error
			if kind == "generic" {
				id, err = StartWorkflow(ctx, app, DefineWorkflow(name, func(*WorkflowContext, string) (string, error) { return "", nil }), "")
			} else {
				id, err = StartWorkflowByName(ctx, app, name, input)
			}
			require.Empty(t, id)
			require.ErrorIs(t, err, ErrWorkflowStartRejected)
			if kind == "never_started" || kind == "generic" {
				require.ErrorIs(t, err, ErrAppNotStarted)
			}
			if kind == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Empty(t, fs.Statements())
			require.Empty(t, fs.Calls())
		})
	}
}

func TestStartWorkflowDuplicateIDRejected_Concurrent(t *testing.T) {
	s13DuplicateConcurrent(t, createTestApp)
}
func s13DuplicateConcurrent(t *testing.T, factory replayAppFactory) {
	worker, client := s13AppPair(t, factory)
	var wg sync.WaitGroup
	var successes atomic.Int32
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app := worker
			if i%2 == 0 {
				app = client
			}
			id, err := StartWorkflowByName(context.Background(), app, "s13_unregistered", i, WithInstanceID("same"))
			if err == nil {
				successes.Add(1)
				if id != "same" {
					errs <- fmt.Errorf("id %q", id)
				}
			} else if id != "" || !errors.Is(err, ErrWorkflowAlreadyStarted) || errors.Is(err, ErrWorkflowStartRejected) {
				errs <- fmt.Errorf("id %q: %w", id, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, successes.Load())
	var count int
	require.NoError(t, client.storage.DB().QueryRow("SELECT count(*) FROM workflow_instances WHERE instance_id='same'").Scan(&count))
	require.Equal(t, 1, count)
}

func TestStartWorkflowDuplicateIDRejected_Closed(t *testing.T) { s13DuplicateClosed(t, createTestApp) }
func s13DuplicateClosed(t *testing.T, factory replayAppFactory) {
	for _, status := range []storage.WorkflowStatus{storage.StatusCompleted, storage.StatusFailed, storage.StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			h := newTypedReplayHarness(t, factory)
			require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: "closed", WorkflowName: "original", Status: status, InputData: []byte(`null`)}))
			before, err := h.app.GetInstance(h.app.ctx, "closed")
			require.NoError(t, err)
			id, err := StartWorkflowByName(h.app.ctx, h.app, "original", "replacement", WithInstanceID("closed"))
			require.Empty(t, id)
			require.ErrorIs(t, err, ErrWorkflowAlreadyStarted)
			require.NotErrorIs(t, err, ErrWorkflowStartRejected)
			after, err := h.app.GetInstance(h.app.ctx, "closed")
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestStartWorkflowDuplicateIDRejected_ChildID(t *testing.T) { s13DuplicateChild(t, createTestApp) }
func s13DuplicateChild(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	parent := DefineWorkflow("s13_parent", func(ctx *WorkflowContext, _ string) (string, error) {
		child, err := StartChild[string](ctx, "s13_child", "")
		if err != nil {
			return "", err
		}
		return child.Get(ctx)
	})
	id := seedErrorWorkflow(t, h, parent)
	require.NoError(t, h.RunOnce(id))
	childID := id + "/#1"
	got, err := StartWorkflowByName(h.app.ctx, h.app, "s13_child", "replacement", WithInstanceID(childID))
	require.Empty(t, got)
	require.ErrorIs(t, err, ErrWorkflowAlreadyStarted)
	require.NotErrorIs(t, err, ErrWorkflowStartRejected)
	require.NoError(t, h.app.Signal(h.app.ctx, childID, "go", "payload"))
	require.NoError(t, CancelWorkflow(h.app.ctx, h.app, childID, "client cancel"))
}

func TestStartWorkflowDuplicateIDRejected_LostAck(t *testing.T) { s13LostAck(t, createTestApp) }
func s13LostAck(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	calls := 0
	RegisterWorkflow(h.app, DefineWorkflow("s13_once", func(*WorkflowContext, string) (string, error) { calls++; return "once", nil }))
	fs := faultstore.New(h.app.storage)
	h.app.storage = fs
	lost := errors.New("lost start acknowledgment")
	fs.After = func(_ context.Context, method string) error {
		if method == "CreateInstance" {
			return lost
		}
		return nil
	}
	id, err := StartWorkflowByName(h.app.ctx, h.app, "s13_once", "", WithInstanceID("lost"))
	require.Empty(t, id)
	require.ErrorIs(t, err, lost)
	require.NotErrorIs(t, err, ErrWorkflowStartRejected)
	fs.After = nil
	id, err = StartWorkflowByName(h.app.ctx, h.app, "s13_once", "", WithInstanceID("lost"))
	require.Empty(t, id)
	require.ErrorIs(t, err, ErrWorkflowAlreadyStarted)
	require.NotErrorIs(t, err, ErrWorkflowStartRejected)
	require.NoError(t, h.RunOnce("lost"))
	require.NoError(t, h.RunOnce("lost"))
	require.Equal(t, 1, calls)
}

func TestByIDErrorsTyped(t *testing.T) { s13ByIDErrors(t, createTestApp) }
func s13ByIDErrors(t *testing.T, factory replayAppFactory) {
	_, client := s13AppPair(t, factory)
	for _, op := range []string{"signal", "cancel", "wait", "get"} {
		t.Run(op, func(t *testing.T) {
			for _, app := range []*App{NewApp(), client} {
				var err error
				switch op {
				case "signal":
					err = app.Signal(context.Background(), "missing", "go", nil)
				case "cancel":
					err = CancelWorkflow(context.Background(), app, "missing", "")
				case "wait":
					_, err = WaitWorkflow[string](context.Background(), app, "missing")
				case "get":
					_, err = GetWorkflowResult[string](context.Background(), app, "missing")
				}
				if app != client {
					require.ErrorIs(t, err, ErrAppNotStarted)
				} else {
					var missing *WorkflowNotFoundError
					require.ErrorAs(t, err, &missing)
					require.Equal(t, "missing", missing.InstanceID)
					require.ErrorIs(t, err, ErrInstanceNotFound)
				}
			}
		})
	}
	for _, status := range []storage.WorkflowStatus{storage.StatusCompleted, storage.StatusFailed, storage.StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			id := string(status)
			require.NoError(t, client.storage.CreateInstance(context.Background(), &storage.WorkflowInstance{InstanceID: id, WorkflowName: "s13_closed", Status: status, InputData: []byte(`null`)}))
			var closed *InstanceClosedError
			err := client.Signal(context.Background(), id, "go", nil)
			require.ErrorAs(t, err, &closed)
			require.Equal(t, status, closed.Status)
			require.ErrorIs(t, err, ErrInstanceClosed)
			err = CancelWorkflow(context.Background(), client, id, "")
			if status == storage.StatusCancelled {
				require.NoError(t, err)
			} else {
				require.ErrorAs(t, err, &closed)
				require.ErrorIs(t, err, ErrInstanceClosed)
				sentinel := ErrWorkflowAlreadyCompleted
				if status == storage.StatusFailed {
					sentinel = ErrWorkflowAlreadyFailed
				}
				require.ErrorIs(t, err, sentinel)
			}
			result := s13Wait[string](t, client, id)
			require.Equal(t, id, result.Status)
			result, err = GetWorkflowResult[string](context.Background(), client, id)
			require.NoError(t, err)
			require.Equal(t, id, result.Status)
		})
	}
}

func TestSignalPayloadEncodeFailureRejected(t *testing.T) { s13SignalEncode(t, createTestApp) }
func s13SignalEncode(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: "encode", WorkflowName: "s13_encode", Status: storage.StatusRunning, InputData: []byte(`null`)}))
	_, err := h.app.storage.DB().Exec("UPDATE workflow_instances SET runnable=FALSE WHERE instance_id='encode'")
	require.NoError(t, err)
	err = h.app.Signal(h.app.ctx, "encode", "go", make(chan int))
	require.ErrorIs(t, err, ErrInvalidSignalPayload)
	instance, err := h.app.GetInstance(h.app.ctx, "encode")
	require.NoError(t, err)
	require.False(t, instance.Runnable)
	var count int
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT count(*) FROM workflow_signals").Scan(&count))
	require.Zero(t, count)
}

func TestWaitWorkflowTerminalOutcomes(t *testing.T) { s13TerminalOutcomes(t, createTestApp) }
func s13TerminalOutcomes(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"completed", "failed", "durable_cancel", "scope_cancel", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			worker, client := s13AppPair(t, factory)
			RegisterWorkflow(worker, DefineWorkflow("s13_outcome", func(ctx *WorkflowContext, _ string) (string, error) {
				switch kind {
				case "completed":
					return "result", nil
				case "failed":
					return "", NewApplicationError("application message", "test", nil, nil)
				case "scope_cancel":
					_, err := Receive[string](ctx, "go")
					if err != nil {
						scope, cancel := WithCancel(ctx)
						cancel()
						return "", Sleep(scope, time.Hour)
					}
					return "", nil
				default:
					return "", Sleep(ctx, time.Hour)
				}
			}))
			id, err := StartWorkflowByName(context.Background(), client, "s13_outcome", "")
			require.NoError(t, err)
			if kind == "durable_cancel" || kind == "scope_cancel" {
				require.NoError(t, CancelWorkflow(context.Background(), client, id, "requested"))
			}
			if kind == "deadline" {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				_, err := WaitWorkflow[string](ctx, client, id)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				return
			}
			result := s13Wait[string](t, client, id)
			switch kind {
			case "completed":
				require.Equal(t, "result", result.Output)
				require.NoError(t, result.Error)
			case "failed":
				var appErr *ApplicationError
				require.ErrorAs(t, result.Error, &appErr)
				require.Equal(t, "application message", appErr.Message)
			default:
				require.Equal(t, "cancelled", result.Status)
				require.ErrorIs(t, result.Error, context.Canceled)
				if kind == "durable_cancel" {
					var cancelled *WorkflowCancelledError
					require.ErrorAs(t, result.Error, &cancelled)
				}
			}
		})
	}
}

// Keep the payload typed throughout ingress, delivery and completion.
type s13Payload struct {
	Sequence int64           `json:"sequence"`
	Data     json.RawMessage `json:"data"`
	Optional *string         `json:"optional"`
}

func TestOutOfProcessTypedSignals(t *testing.T) { s13TypedSignals(t, createTestApp) }
func s13TypedSignals(t *testing.T, factory replayAppFactory) {
	for _, mode := range []string{"receive", "async", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			worker, client := s13AppPair(t, factory)
			RegisterWorkflow(worker, DefineWorkflow("s13_typed", func(ctx *WorkflowContext, _ string) ([]*s13Payload, error) {
				values := make([]*s13Payload, 0, 2)
				for range 2 {
					var value *s13Payload
					var err error
					switch mode {
					case "receive":
						value, err = Receive[*s13Payload](ctx, "payload")
					case "timeout":
						value, err = ReceiveWithTimeout[*s13Payload](ctx, "payload", time.Hour)
					case "async":
						err = Await(ctx, func() bool { return Pending(ctx, "payload") > 0 })
						if err == nil {
							var ok bool
							value, ok, err = ReceiveAsync[*s13Payload](ctx, "payload")
							if !ok && err == nil {
								err = errors.New("pending signal disappeared")
							}
						}
					}
					if err != nil {
						return nil, err
					}
					values = append(values, value)
				}
				return values, nil
			}))
			id, err := StartWorkflowByName(context.Background(), client, "s13_typed", "")
			require.NoError(t, err)
			payload := &s13Payload{Sequence: 9007199254740993, Data: json.RawMessage(`{"nested":[true,3]}`)}
			require.NoError(t, client.Signal(context.Background(), id, "payload", payload))
			require.NoError(t, client.Signal(context.Background(), id, "payload", (*s13Payload)(nil)))
			result := s13Wait[[]*s13Payload](t, client, id)
			require.NoError(t, result.Error)
			require.Equal(t, []*s13Payload{payload, nil}, result.Output)
		})
	}
}

func TestWaitWorkflowOutputDecodeFailure(t *testing.T) { s13WaitDecodeFailure(t, createTestApp) }
func s13WaitDecodeFailure(t *testing.T, factory replayAppFactory) {
	worker, client := s13AppPair(t, factory)
	RegisterWorkflow(worker, DefineWorkflow("s13_decode", func(*WorkflowContext, string) (string, error) { return "text", nil }))
	id, err := StartWorkflowByName(context.Background(), client, "s13_decode", "")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := WaitWorkflow[int](ctx, client, id)
	require.NoError(t, err, "Wait's Go error is reserved for lookup failures")
	require.Equal(t, "completed", result.Status)
	var decode *json.UnmarshalTypeError
	require.ErrorAs(t, result.Error, &decode)
	_, err = GetWorkflowResult[int](ctx, client, id)
	require.ErrorAs(t, err, &decode, "the nonblocking API retains its output decode error")
}

func TestClientAfterFailedStart(t *testing.T) { s13FailedStart(t, createTestApp) }
func s13FailedStart(t *testing.T, factory replayAppFactory) {
	migrations := fstest.MapFS{
		"sqlite/99999999999999_broken.sql":     {Data: []byte("-- migrate:up\nINVALID SQL;\n-- migrate:down\n")},
		"postgresql/99999999999999_broken.sql": {Data: []byte("-- migrate:up\nINVALID SQL;\n-- migrate:down\n")},
	}
	app, cleanup := factory(t, WithAutoMigrate(true), WithMigrationsFS(migrations))
	defer cleanup()
	require.Error(t, app.Start(context.Background()))
	id, err := StartWorkflowByName(context.Background(), app, "never_started", "")
	require.Empty(t, id)
	require.ErrorIs(t, err, ErrWorkflowStartRejected)
	require.ErrorIs(t, err, ErrAppNotStarted)
	require.ErrorIs(t, app.Signal(context.Background(), "missing", "go", nil), ErrAppNotStarted)
	require.ErrorIs(t, CancelWorkflow(context.Background(), app, "missing", ""), ErrAppNotStarted)
}
