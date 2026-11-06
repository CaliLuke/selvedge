package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
)

// ErrProducerClosed means the producer is shutting down or has shut down.
var ErrProducerClosed = errors.New("producer closed")

// Producer submits and reads workflows without running them. Its private App
// shares the storage and codecs; it never starts workers, election, maintenance,
// or notification listeners. A Producer cannot register workflow or activity code.
// The zero value is closed; use OpenProducer.
type Producer struct {
	app        *App
	mu         sync.Mutex
	operations sync.WaitGroup
	shutdown   *appShutdown
}

// OpenProducer initializes storage using ctx only for initialization. After a
// successful return, only Shutdown owns its lifetime. Database, migration,
// notification publishing, and shutdown timeout options apply. Worker options
// have no effect; workflow lifecycle hooks (including OnWorkflowStart) never run.
// Client observability can be implemented by wrapping the operations with metrics
// or tracing using their returned errors and IDs.
//
// WithAutoMigrate(false) validates configuration and database connectivity, but
// does not validate schema compatibility. The caller must apply the current
// embedded migrations before using the producer.
func OpenProducer(ctx context.Context, opts ...Option) (*Producer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app := NewApp(opts...)
	if err := app.config.validate(); err != nil {
		return nil, err
	}
	app.hooks = &hooks.NoOpHooks{}
	if err := app.openStorage(ctx, false); err != nil {
		if app.storage != nil {
			_ = app.storage.Close()
		}
		return nil, fmt.Errorf("failed to initialize producer: %w", err)
	}
	return &Producer{app: app}, nil
}

// admit and Shutdown serialize admission so Wait cannot race a new Add.
func (p *Producer) admit() (*App, func(), error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.app == nil || p.shutdown != nil {
		return nil, nil, ErrProducerClosed
	}
	p.operations.Add(1)
	return p.app, p.operations.Done, nil
}

// StartWorkflowByName submits JSON-encoded input without local registration.
// json.RawMessage is encoded by encoding/json, which may compact whitespace and
// escape HTML characters. Use StartWorkflowEncoded to preserve JSON bytes.
// Error classification matches the package-level StartWorkflowByName function.
func (p *Producer) StartWorkflowByName(ctx context.Context, workflow string, input any, opts ...StartOption) (string, error) {
	app, done, err := p.admit()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrWorkflowStartRejected, err)
	}
	defer done()
	return StartWorkflowByName(ctx, app, workflow, input, opts...)
}

// StartWorkflowEncoded submits valid JSON verbatim. Whitespace, key order,
// escapes and number spelling are preserved in storage on both backends. Input
// is copied before persistence; callers must not mutate it during the call.
func (p *Producer) StartWorkflowEncoded(ctx context.Context, workflow string, input json.RawMessage, opts ...StartOption) (string, error) {
	return p.StartWorkflowByName(ctx, workflow, encodedWorkflowInput(input), opts...)
}

type encodedWorkflowInput json.RawMessage

// Describe returns persisted identity, encoded input, status and parent metadata.
// Empty ParentInstanceID and ParentKey identify a root workflow.
func (p *Producer) Describe(ctx context.Context, instanceID string) (*WorkflowDescription, error) {
	app, done, err := p.admit()
	if err != nil {
		return nil, err
	}
	defer done()
	return Describe(ctx, app, instanceID)
}

// GetProducerWorkflowResult reads a typed result without waiting for completion.
// Its status, failure and decode-error semantics match GetWorkflowResult.
func GetProducerWorkflowResult[O any](ctx context.Context, producer *Producer, instanceID string) (*WorkflowResult[O], error) {
	app, done, err := producer.admit()
	if err != nil {
		return nil, err
	}
	defer done()
	return GetWorkflowResult[O](ctx, app, instanceID)
}

// Shutdown rejects new operations, drains admitted calls, and closes storage.
// Operations retain their own contexts: bound them to bound cleanup time.
// Cleanup continues if this caller stops waiting. Concurrent and repeated calls
// wait for the same cleanup and return its result. A wrapped context error or a
// timeout means waiting was interrupted; call again to observe completed cleanup.
func (p *Producer) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.app == nil {
		p.mu.Unlock()
		return nil
	}
	if p.shutdown == nil {
		p.shutdown = &appShutdown{done: make(chan struct{})}
		go p.finishShutdown(p.shutdown)
	}
	shutdown := p.shutdown
	timeout := p.app.config.shutdownTimeout
	p.mu.Unlock()

	// Prefer an already completed cleanup over an interrupted waiting context.
	select {
	case <-shutdown.done:
		return shutdown.err
	default:
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-shutdown.done:
		return shutdown.err
	case <-ctx.Done():
		return fmt.Errorf("producer shutdown interrupted: %w", ctx.Err())
	case <-timer.C:
		return fmt.Errorf("producer shutdown timed out after %v: %w", timeout, context.DeadlineExceeded)
	}
}

func (p *Producer) finishShutdown(shutdown *appShutdown) {
	defer close(shutdown.done)
	p.operations.Wait()
	if err := p.app.storage.Close(); err != nil {
		shutdown.err = fmt.Errorf("failed to close producer storage: %w", err)
	}
}
