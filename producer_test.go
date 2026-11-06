package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

type producerStartHooks struct {
	hooks.NoOpHooks
	starts atomic.Int32
}

func (h *producerStartHooks) OnWorkflowStart(context.Context, hooks.WorkflowStartInfo) {
	h.starts.Add(1)
}

func producerPair(t *testing.T, factory replayAppFactory, opts ...Option) (*Producer, *App) {
	t.Helper()
	worker, cleanup := factory(t, WithWorkflowResumptionInterval(time.Millisecond), WithMaxPollInterval(10*time.Millisecond))
	t.Cleanup(cleanup)
	initCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	options := append([]Option{WithDatabase(worker.config.databaseURL), WithAutoMigrate(false)}, opts...)
	producer, err := OpenProducer(initCtx, options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, producer.Shutdown(context.Background())) })
	return producer, worker
}

func TestProducerValidatesOnlyApplicableConfiguration(t *testing.T) {
	for _, test := range []struct {
		name            string
		option          Option
		producerRejects bool
	}{
		{"worker timer", WithTimerCheckInterval(0), false},
		{"worker resumption", WithWorkflowResumptionInterval(0), false},
		{"listener reconnect", WithNotifyReconnectDelay(0), false},
		{"execution lease", WithLease(0, 0, 0, 0), false},
		{"shutdown timeout", WithShutdownTimeout(0), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Worker validation still rejects these configurations.
			require.Error(t, NewApp(test.option).Start(ctx))
			producer, err := OpenProducer(ctx, WithDatabase(filepath.Join(t.TempDir(), "producer.db")), test.option)
			if test.producerRejects {
				require.Error(t, err)
				require.Nil(t, producer)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, producer.Shutdown(context.Background())) })
			_, err = producer.StartWorkflowByName(ctx, "registered-by-worker", "input")
			require.NoError(t, err)
		})
	}
}

func TestProducerSeparateWorker(t *testing.T) { producerSeparateWorker(t, createTestApp) }
func producerSeparateWorker(t *testing.T, factory replayAppFactory) {
	h := &producerStartHooks{}
	p, worker := producerPair(t, factory, WithHooks(h))
	ctx := context.Background() // producerPair already cancelled its initialization context.
	input := json.RawMessage(" {\n  \"z\":1e2, \"a\":\"<hello>\" } \n")
	id, err := p.StartWorkflowEncoded(ctx, "producer_echo", input, WithInstanceID("stable-id"))
	require.NoError(t, err)
	description, err := p.Describe(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "producer_echo", description.WorkflowName)
	require.Equal(t, input, description.InputData)
	require.Empty(t, description.ParentInstanceID)
	require.Empty(t, description.ParentKey)
	result, err := GetProducerWorkflowResult[json.RawMessage](ctx, p, id)
	require.NoError(t, err)
	require.Equal(t, string(StatusRunning), result.Status)
	var locks int
	require.NoError(t, p.app.storage.DB().QueryRowContext(ctx, "SELECT count(*) FROM system_locks").Scan(&locks))
	require.Zero(t, locks)
	require.Nil(t, p.app.engine)
	require.Nil(t, p.app.notifyListener)
	require.Zero(t, h.starts.Load())

	RegisterWorkflow(worker, DefineWorkflow("producer_echo", func(_ *WorkflowContext, in map[string]any) (string, error) {
		return in["a"].(string), nil
	}))
	require.NoError(t, worker.Start(ctx))
	require.Eventually(t, func() bool {
		result, lookupErr := GetProducerWorkflowResult[string](ctx, p, id)
		return lookupErr == nil && result.Status == string(StatusCompleted) && result.Output == "<hello>"
	}, 5*time.Second, time.Millisecond)
	require.NoError(t, worker.Shutdown(ctx))
	require.NoError(t, worker.Start(ctx))
	_, err = p.StartWorkflowEncoded(ctx, "producer_echo", input, WithInstanceID(id))
	require.ErrorIs(t, err, ErrWorkflowAlreadyStarted)
	require.NotErrorIs(t, err, ErrWorkflowStartRejected)
	resultAfterRestart, err := GetProducerWorkflowResult[string](ctx, p, id)
	require.NoError(t, err)
	require.Equal(t, "<hello>", resultAfterRestart.Output)
	require.Zero(t, h.starts.Load())
}

func TestProducerUncertainSubmission(t *testing.T) { producerUncertainSubmission(t, createTestApp) }
func producerUncertainSubmission(t *testing.T, factory replayAppFactory) {
	p, _ := producerPair(t, factory)
	lost := errors.New("lost acknowledgement")
	faults := faultstore.New(p.app.storage)
	faults.After = func(_ context.Context, method string) error {
		if method == "CreateInstance" {
			return lost
		}
		return nil
	}
	p.app.storage = faults
	id, err := p.StartWorkflowByName(context.Background(), "original", "input", WithInstanceID("uncertain"))
	require.Empty(t, id)
	require.ErrorIs(t, err, lost)
	require.NotErrorIs(t, err, ErrWorkflowStartRejected)
	_, err = p.StartWorkflowByName(context.Background(), "replacement", "different", WithInstanceID("uncertain"))
	require.ErrorIs(t, err, ErrWorkflowAlreadyStarted)
	description, err := p.Describe(context.Background(), "uncertain")
	require.NoError(t, err)
	require.Equal(t, "original", description.WorkflowName)
	require.JSONEq(t, `"input"`, string(description.InputData))
}

func TestProducerRejectsInvalidInput(t *testing.T) { producerRejectsInvalidInput(t, createTestApp) }
func producerRejectsInvalidInput(t *testing.T, factory replayAppFactory) {
	p, _ := producerPair(t, factory)
	for _, input := range []json.RawMessage{nil, json.RawMessage(`{`), json.RawMessage(`{} {}`)} {
		_, err := p.StartWorkflowEncoded(context.Background(), "invalid", input, WithInstanceID("invalid"))
		require.ErrorIs(t, err, ErrWorkflowStartRejected)
	}
	_, err := p.StartWorkflowByName(context.Background(), "", nil)
	require.ErrorIs(t, err, ErrWorkflowStartRejected)
	_, err = p.StartWorkflowByName(context.Background(), "invalid", make(chan int))
	require.ErrorIs(t, err, ErrWorkflowStartRejected)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.StartWorkflowByName(ctx, "invalid", nil)
	require.ErrorIs(t, err, ErrWorkflowStartRejected)
	require.ErrorIs(t, err, context.Canceled)
	_, err = p.Describe(context.Background(), "invalid")
	require.ErrorIs(t, err, ErrInstanceNotFound)
}

type producerBlockingInput struct {
	entered chan struct{}
	release chan struct{}
}

func (b producerBlockingInput) MarshalJSON() ([]byte, error) {
	close(b.entered)
	<-b.release
	return []byte(`"admitted"`), nil
}

type producerCloseStore struct {
	storage.Storage
	closed chan struct{}
	count  atomic.Int32
}

func (s *producerCloseStore) Close() error {
	s.count.Add(1)
	err := s.Storage.Close()
	close(s.closed)
	return err
}

func TestProducerShutdownDrainsOperations(t *testing.T) {
	producerShutdownDrainsOperations(t, createTestApp)
}
func producerShutdownDrainsOperations(t *testing.T, factory replayAppFactory) {
	p, _ := producerPair(t, factory)
	store := &producerCloseStore{Storage: p.app.storage, closed: make(chan struct{})}
	p.app.storage = store
	input := producerBlockingInput{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(input.release) }) })
	admitted := make(chan error, 1)
	go func() {
		_, err := p.StartWorkflowByName(context.Background(), "admitted", input)
		admitted <- err
	}()
	select {
	case <-input.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not reach encoding")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, p.Shutdown(ctx), context.Canceled)
	_, err := p.StartWorkflowByName(context.Background(), "late", nil)
	require.ErrorIs(t, err, ErrProducerClosed)
	require.ErrorIs(t, err, ErrWorkflowStartRejected)
	_, err = p.Describe(context.Background(), "late")
	require.ErrorIs(t, err, ErrProducerClosed)
	_, err = GetProducerWorkflowResult[string](context.Background(), p, "late")
	require.ErrorIs(t, err, ErrProducerClosed)
	require.Zero(t, store.count.Load(), "storage must stay open until admitted work finishes")

	waiters := make(chan error, 8)
	for range 8 {
		go func() { waiters <- p.Shutdown(ctx) }()
	}
	for range 8 {
		require.ErrorIs(t, <-waiters, context.Canceled)
	}
	release.Do(func() { close(input.release) })
	require.NoError(t, <-admitted)
	select {
	case <-store.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish after interrupted waiter")
	}
	require.NoError(t, p.Shutdown(context.Background()))
	require.NoError(t, p.Shutdown(ctx), "completed cleanup wins over a cancelled waiting context")
	require.EqualValues(t, 1, store.count.Load())
}

func TestProducerMigrationOwnership(t *testing.T) {
	producerMigrationOwnership(t, filepath.Join(t.TempDir(), "producer.db"))
}

func producerMigrationOwnership(t *testing.T, path string) {
	t.Helper()
	p, err := OpenProducer(context.Background(), WithDatabase(path), WithAutoMigrate(false))
	require.NoError(t, err, "external migration mode checks connectivity, not schema")
	_, err = p.StartWorkflowByName(context.Background(), "missing_schema", nil)
	require.Error(t, err)
	require.NoError(t, p.Shutdown(context.Background()))
	p, err = OpenProducer(context.Background(), WithDatabase(path))
	require.NoError(t, err)
	defer p.Shutdown(context.Background())
	_, err = p.StartWorkflowByName(context.Background(), "migrated", nil)
	require.NoError(t, err)
}

func TestProducerInitializationCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p, err := OpenProducer(ctx, WithDatabase(filepath.Join(t.TempDir(), "cancelled.db")))
	require.Nil(t, p)
	require.ErrorIs(t, err, context.Canceled)
	p, err = OpenProducer(context.Background(), WithShutdownTimeout(0))
	require.Nil(t, p)
	require.Error(t, err)
}

type producerCancellingFS struct {
	fs.FS
	cancel context.CancelFunc
}

func (f producerCancellingFS) Open(name string) (fs.File, error) {
	f.cancel()
	return f.FS.Open(name)
}

func TestProducerInitializationCancellationDuringMigration(t *testing.T) {
	producerInitializationCancellationDuringMigration(t, createTestApp)
}
func producerInitializationCancellationDuringMigration(t *testing.T, factory replayAppFactory) {
	worker, cleanup := factory(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := OpenProducer(ctx, WithDatabase(worker.config.databaseURL), WithMigrationsFS(producerCancellingFS{EmbeddedMigrationsFS(), cancel}))
	require.Nil(t, p)
	require.ErrorIs(t, err, context.Canceled)
	// A cancelled initializer must release its pool and migration lock.
	p, err = OpenProducer(context.Background(), WithDatabase(worker.config.databaseURL))
	require.NoError(t, err)
	require.NoError(t, p.Shutdown(context.Background()))
}

func TestProducerChildIdentity(t *testing.T) { producerChildIdentity(t, createTestApp) }
func producerChildIdentity(t *testing.T, factory replayAppFactory) {
	p, worker := producerPair(t, factory)
	RegisterWorkflow(worker, DefineWorkflow("producer_parent", func(ctx *WorkflowContext, _ string) (string, error) {
		child, err := StartChild[string](ctx, "producer_child", "child input")
		if err != nil {
			return "", err
		}
		return child.Get(ctx)
	}))
	id, err := p.StartWorkflowByName(context.Background(), "producer_parent", "", WithInstanceID("parent"))
	require.NoError(t, err)
	require.NoError(t, worker.Start(context.Background()))
	childID := id + "/#1"
	require.Eventually(t, func() bool {
		d, lookupErr := p.Describe(context.Background(), childID)
		return lookupErr == nil && d.ParentInstanceID == id && d.ParentKey == "#1" && string(d.InputData) == `"child input"`
	}, 5*time.Second, time.Millisecond)
	_, err = p.StartWorkflowByName(context.Background(), "producer_child", "replacement", WithInstanceID(childID))
	require.ErrorIs(t, err, ErrWorkflowAlreadyStarted)
}

func TestProducerInitializationCancelledDuringConnect(t *testing.T) {
	server, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer server.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := server.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opened := make(chan error, 1)
	go func() {
		p, openErr := OpenProducer(ctx, WithDatabase("postgres://test:test@"+server.Addr().String()+"/test?sslmode=disable"), WithAutoMigrate(false))
		if p != nil {
			_ = p.Shutdown(context.Background())
		}
		opened <- openErr
	}()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("producer never connected")
	}
	cancel()
	select {
	case err := <-opened:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("initialization ignored cancellation")
	}
}
