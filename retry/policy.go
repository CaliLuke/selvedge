// Package retry provides retry policies for activities.
package retry

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

// Policy defines the retry behavior for activities.
type Policy struct {
	// MaxAttempts is the maximum number of attempts (including the first).
	// 0 means no limit.
	MaxAttempts int

	// InitialInterval is the delay before the first retry.
	InitialInterval time.Duration

	// MaxInterval is the maximum delay between retries.
	MaxInterval time.Duration

	// Multiplier is the factor by which the interval increases.
	Multiplier float64

	// RandomizationFactor adds jitter to the delay.
	// A value of 0.5 means the actual delay will be within [delay * 0.5, delay * 1.5].
	RandomizationFactor float64

	// NonRetryableErrors is a list of errors that should not be retried.
	// Errors are matched using errors.Is.
	NonRetryableErrors []error
}

// DefaultPolicy returns a sensible default retry policy.
func DefaultPolicy() *Policy {
	return &Policy{
		MaxAttempts:         3,
		InitialInterval:     100 * time.Millisecond,
		MaxInterval:         10 * time.Second,
		Multiplier:          2.0,
		RandomizationFactor: 0.5,
	}
}

// NoRetry returns a policy that never retries.
func NoRetry() *Policy {
	return &Policy{
		MaxAttempts: 1,
	}
}

// Fixed returns a policy with fixed delay between retries.
func Fixed(maxAttempts int, interval time.Duration) *Policy {
	return &Policy{
		MaxAttempts:     maxAttempts,
		InitialInterval: interval,
		MaxInterval:     interval,
		Multiplier:      1.0,
	}
}

// Exponential returns an exponential backoff policy.
func Exponential(maxAttempts int, initial, maxInterval time.Duration, multiplier float64) *Policy {
	return &Policy{
		MaxAttempts:         maxAttempts,
		InitialInterval:     initial,
		MaxInterval:         maxInterval,
		Multiplier:          multiplier,
		RandomizationFactor: 0.5,
	}
}

// ShouldRetry returns true if the operation should be retried.
func (p *Policy) ShouldRetry(attempts int, err error) bool {
	if p.MaxAttempts > 0 && attempts >= p.MaxAttempts {
		return false
	}

	// Check if error is in the non-retryable list
	for _, nonRetryable := range p.NonRetryableErrors {
		if errors.Is(err, nonRetryable) {
			return false
		}
	}

	return true
}

// GetDelay calculates the delay before the next retry. MaxInterval bounds the
// final delay, including jitter. A nonpositive MaxInterval leaves it uncapped.
func (p *Policy) GetDelay(attempts int) time.Duration {
	if p.InitialInterval <= 0 {
		return 0
	}
	multiplier := p.Multiplier
	if math.IsNaN(multiplier) || multiplier < 1 {
		multiplier = 1
	}
	delay := float64(p.InitialInterval)
	if attempts > 1 {
		delay *= math.Pow(multiplier, float64(attempts-1))
	}
	limit := time.Duration(math.MaxInt64)
	if p.MaxInterval > 0 {
		limit = p.MaxInterval
	}
	delay = min(delay, float64(limit))
	jitter := p.RandomizationFactor
	if math.IsNaN(jitter) {
		jitter = 0
	}
	jitter = max(0, min(1, jitter))
	if jitter != 0 {
		delay *= 1 + jitter*(2*rand.Float64()-1)
	}
	// float64 rounds MaxInt64 upward. Check before converting to avoid an
	// overflow into negative durations, which would turn backoff into a spin.
	if delay >= float64(limit) {
		return limit
	}
	return time.Duration(delay)
}

// Builder provides a fluent API for building policies.
type Builder struct {
	policy *Policy
}

// NewBuilder creates a new policy builder.
func NewBuilder() *Builder {
	return &Builder{
		policy: &Policy{
			MaxAttempts:     3,
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     10 * time.Second,
			Multiplier:      2.0,
		},
	}
}

// WithMaxAttempts sets the maximum number of attempts.
func (b *Builder) WithMaxAttempts(n int) *Builder {
	b.policy.MaxAttempts = n
	return b
}

// WithInitialInterval sets the initial retry interval.
func (b *Builder) WithInitialInterval(d time.Duration) *Builder {
	b.policy.InitialInterval = d
	return b
}

// WithMaxInterval sets the maximum retry interval.
func (b *Builder) WithMaxInterval(d time.Duration) *Builder {
	b.policy.MaxInterval = d
	return b
}

// WithMultiplier sets the backoff multiplier.
func (b *Builder) WithMultiplier(m float64) *Builder {
	b.policy.Multiplier = m
	return b
}

// WithJitter sets the randomization factor.
func (b *Builder) WithJitter(factor float64) *Builder {
	b.policy.RandomizationFactor = factor
	return b
}

// WithNonRetryableErrors sets errors that should not trigger retries.
func (b *Builder) WithNonRetryableErrors(errs ...error) *Builder {
	b.policy.NonRetryableErrors = errs
	return b
}

// Build returns the configured policy.
func (b *Builder) Build() *Policy {
	return b.policy
}
