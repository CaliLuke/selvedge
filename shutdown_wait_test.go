package selvedge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// Hold the owned storage boundary so interrupted waits cannot complete cleanup.
type shutdownCloseStore struct {
	storage.Storage
	release chan struct{}
	err     error
	closes  atomic.Int32
}

func (s *shutdownCloseStore) NotifyRunnable(context.Context) {}

func (s *shutdownCloseStore) Close() error {
	s.closes.Add(1)
	<-s.release
	return s.err
}

// shutdownHandle isolates each public shutdown method at the storage-close
// boundary, without worker scheduling or database timing influencing the result.
func shutdownHandle(t *testing.T, role string, timeout time.Duration, closeErr error) (func(context.Context) error, func(), *shutdownCloseStore) {
	t.Helper()
	store := &shutdownCloseStore{release: make(chan struct{}), err: closeErr}
	app := NewApp(WithShutdownTimeout(timeout))
	app.storage = store
	app.running = true
	shutdown := app.Shutdown
	if role == "producer" {
		shutdown = (&Producer{app: app}).Shutdown
	}
	var once sync.Once
	release := func() { once.Do(func() { close(store.release) }) }
	t.Cleanup(func() {
		release()
		_ = awaitShutdownResult(t, shutdown)
	})
	return shutdown, release, store
}

// Retry only interrupted waits, never a final cleanup error, even a context error.
func awaitShutdownResult(t *testing.T, shutdown func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		err := shutdown(ctx)
		if !errors.Is(err, ErrShutdownInterrupted) {
			return err
		}
		require.NoError(t, ctx.Err(), "cleanup did not complete")
	}
}

// Context-shaped close failures must remain final results, so retry decisions
// depend on the interruption marker rather than on the underlying context cause.
func TestShutdownDistinguishesInterruptedWaitFromCleanupResult(t *testing.T) {
	for _, role := range []string{"app", "producer"} {
		for _, wait := range []string{"timeout", "cancelled", "deadline"} {
			for _, result := range []struct {
				name string
				err  error
			}{
				{"success", nil},
				{"close cancelled", fmt.Errorf("close failed: %w", context.Canceled)},
				{"close deadline", fmt.Errorf("close failed: %w", context.DeadlineExceeded)},
			} {
				t.Run(role+"/"+wait+"/"+result.name, func(t *testing.T) {
					timeout := 5 * time.Second
					if wait == "timeout" {
						timeout = time.Millisecond
					}
					shutdown, release, store := shutdownHandle(t, role, timeout, result.err)
					ctx := context.Background()
					cause := context.DeadlineExceeded
					switch wait {
					case "cancelled":
						cancelled, cancel := context.WithCancel(ctx)
						cancel()
						ctx, cause = cancelled, context.Canceled
					case "deadline":
						expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
						defer cancel()
						ctx = expired
					}

					err := shutdown(ctx)
					require.ErrorIs(t, err, ErrShutdownInterrupted)
					require.ErrorIs(t, err, cause)
					release()
					err = awaitShutdownResult(t, shutdown)
					require.ErrorIs(t, err, result.err)
					require.NotErrorIs(t, err, ErrShutdownInterrupted)
					require.EqualValues(t, 1, store.closes.Load())

					// Completed cleanup wins even when this new caller cannot wait.
					cancelled, cancel := context.WithCancel(context.Background())
					cancel()
					require.ErrorIs(t, shutdown(cancelled), result.err)
					expired, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
					defer cancelDeadline()
					require.ErrorIs(t, shutdown(expired), result.err)
					require.EqualValues(t, 1, store.closes.Load())
				})
			}
		}
	}
}

// Establish overlapping waiters before cancellation so cancelling one cannot
// silently interrupt another caller already waiting for the cleanup result.
func TestShutdownCallersWaitIndependently(t *testing.T) {
	for _, role := range []string{"app", "producer"} {
		t.Run(role, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				closeErr := errors.New("storage close failed")
				shutdown, release, store := shutdownHandle(t, role, 5*time.Second, closeErr)
				waiting := make(chan error, 1)
				go func() { waiting <- shutdown(context.Background()) }()
				synctest.Wait()
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err := shutdown(ctx)
				require.ErrorIs(t, err, ErrShutdownInterrupted)
				require.ErrorIs(t, err, context.Canceled)
				synctest.Wait()
				select {
				case err := <-waiting:
					t.Fatalf("other caller stopped waiting before cleanup: %v", err)
				default:
				}
				release()
				select {
				case err := <-waiting:
					require.ErrorIs(t, err, closeErr)
					require.NotErrorIs(t, err, ErrShutdownInterrupted)
				case <-time.After(5 * time.Second):
					t.Fatal("other caller did not observe cleanup")
				}
				require.ErrorIs(t, shutdown(ctx), closeErr)
				require.EqualValues(t, 1, store.closes.Load())
			})
		})
	}
}
