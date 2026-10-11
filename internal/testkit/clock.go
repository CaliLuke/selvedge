// Package testkit provides deterministic execution clocks for tests.
package testkit

import (
	"sync"
	"time"
)

// Clock is a concurrency-safe clock advanced explicitly by the test.
type Clock struct {
	mu      sync.Mutex
	elapsed time.Duration
}

// Now returns the current test time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Unix(0, 0).Add(c.elapsed)
}

// Since returns elapsed test time since t.
func (c *Clock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

// Advance moves test time forward by d.
func (c *Clock) Advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.elapsed += d }
