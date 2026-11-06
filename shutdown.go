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

func (s *appShutdown) wait(ctx context.Context, timeout time.Duration) error {
	// A completed cleanup takes precedence over an already cancelled waiter.
	select {
	case <-s.done:
		return s.err
	default:
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", ErrShutdownInterrupted, ctx.Err())
	case <-timer.C:
		return fmt.Errorf("%w after %v: %w", ErrShutdownInterrupted, timeout, context.DeadlineExceeded)
	}
}
