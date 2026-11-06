package selvedge

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrShutdownInterrupted means a Shutdown caller stopped waiting before observing
// the cleanup result. Cleanup continues; call Shutdown again to observe its result.
// The returned error also wraps context.Canceled or context.DeadlineExceeded.
// A final cleanup error may wrap a context error without matching this sentinel.
var ErrShutdownInterrupted = errors.New("shutdown wait interrupted")

// appShutdown belongs to one run. Closing done publishes err to all waiters.
type appShutdown struct {
	done chan struct{}
	err  error
}

// wait observes the published cleanup result without owning cleanup's lifetime.
// It checks completion both before blocking and after a wait interruption.
func (s *appShutdown) wait(ctx context.Context, timeout time.Duration) error {
	// A completed cleanup takes precedence over an already cancelled waiter.
	select {
	case <-s.done:
		return s.err
	default:
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var interrupted error
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		interrupted = fmt.Errorf("%w: %w", ErrShutdownInterrupted, ctx.Err())
	case <-timer.C:
		interrupted = fmt.Errorf("%w after %v: %w", ErrShutdownInterrupted, timeout, context.DeadlineExceeded)
	}
	// Completion may have become ready alongside the interruption. Observe it
	// before reporting an interrupted wait; the caller must retry only if unseen.
	select {
	case <-s.done:
		return s.err
	default:
		return interrupted
	}
}
