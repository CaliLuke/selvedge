package replay

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// ErrLeaseLost means the execution can no longer safely use its lease.
var ErrLeaseLost = errors.New("execution lease lost")

// Clock supplies monotonic elapsed time for one execution, independent of store time.
type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}
type monotonicClock struct{}

// Now returns a timestamp containing the process monotonic clock reading.
func (monotonicClock) Now() time.Time { return time.Now() }

// Since measures elapsed time using the monotonic component of t.
func (monotonicClock) Since(t time.Time) time.Duration { return time.Since(t) }

// LeaseConfig controls store grants and the holder's conservative lease budget.
type LeaseConfig struct {
	// TTL is the duration of each store lease grant.
	TTL time.Duration
	// RenewInterval is the time between renewal attempts.
	RenewInterval time.Duration
	// Margin is the minimum remaining budget required to execute; it must be at least MaxSkew.
	Margin time.Duration
	// MaxSkew bounds the assumed skew between holder and store clocks.
	MaxSkew time.Duration
}

// DefaultLeaseConfig returns a 30s grant, 10s renewal interval, 5s margin, and 2s skew bound.
func DefaultLeaseConfig() LeaseConfig {
	return LeaseConfig{30 * time.Second, 10 * time.Second, 5 * time.Second, 2 * time.Second}
}

// Validate rejects budgets that cannot renew safely before the lease margin.
func (c LeaseConfig) Validate() error {
	if c.TTL < time.Millisecond || c.RenewInterval <= 0 || c.Margin < 0 || c.MaxSkew < 0 || c.Margin < c.MaxSkew || c.Margin >= c.TTL || c.RenewInterval >= c.TTL-c.Margin {
		return fmt.Errorf("invalid lease: require positive TTL and renewal interval, Margin >= MaxSkew >= 0, and renewal interval + Margin < TTL")
	}
	return nil
}

// ConfigureLease must be called before running the engine. Each execution gets a clock.
func (e *Engine) ConfigureLease(config LeaseConfig, clock func() Clock) error {
	if err := config.Validate(); err != nil {
		return err
	}
	e.lease = config
	e.clock = clock
	return nil
}

// Execution tracks a conservative monotonic belief, including request latency.
type Execution struct {
	closed      bool // Accessed only by the dispatch goroutine.
	fence       storage.Fence
	clk         Clock
	mu          sync.Mutex
	grantedAt   time.Time
	ttl, margin time.Duration
	lost        error
}

// Check permanently rejects execution once its remaining lease budget is below Margin.
func (x *Execution) Check() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.lost != nil {
		return x.lost
	}
	if x.ttl-x.clk.Since(x.grantedAt) < x.margin {
		x.lost = ErrLeaseLost
	}
	return x.lost
}
func (x *Execution) renew(ctx context.Context, s storage.Storage) {
	t0 := x.clk.Now()
	ok, err := s.RenewLease(ctx, x.fence, x.ttl)
	x.mu.Lock()
	defer x.mu.Unlock()
	if err != nil || !ok {
		if x.lost == nil {
			x.lost = ErrLeaseLost
		}
		return
	}
	if x.lost == nil {
		x.grantedAt = t0
	}
}
func (x *Execution) keepAlive(ctx context.Context, s storage.Storage, interval time.Duration, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			x.renew(ctx, s)
		}
	}
}

// Check stops commands even when workflow code catches an earlier lease failure.
func (ec *ExecutionContext) Check() error {
	if ec.closing.Load() {
		return errExecutionClosed
	}
	if err := ec.Fatal(); err != nil {
		return err
	}
	if ec.execution == nil {
		return nil
	}
	if err := ec.execution.Check(); err != nil {
		ec.Fail(err)
		return err
	}
	if err := ec.ctx.Err(); err != nil {
		ec.Fail(ErrLeaseLost)
		return ErrLeaseLost
	}
	return nil
}

func (x *Execution) reject(err error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.lost == nil {
		x.lost = err
	}
}
