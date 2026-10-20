package selvedge

import (
	"fmt"
	"math"
	"time"

	"github.com/CaliLuke/selvedge/retry"
)

// ActivityOptions configures the durable execution policy recorded at reservation.
// Non-zero per-call fields override definition defaults. A nil policy inherits.
type ActivityOptions struct {
	Queue                                                         string
	StartToCloseTimeout, ScheduleToStartTimeout, HeartbeatTimeout time.Duration
	RetryPolicy                                                   *retry.Policy
}

// WithActivityDefaults sets the definition's activity execution options.
func WithActivityDefaults[I, O any](options ActivityOptions) ActivityOption[I, O] {
	return func(a *Activity[I, O]) {
		a.options = options
	}
}

// WithActivityOptions overrides non-zero fields for this activity reservation.
func WithActivityOptions(options ActivityOptions) ExecuteOption {
	return func(o *executeOptions) { o.activity = options }
}

func cloneActivityOptions(o ActivityOptions) ActivityOptions {
	if o.RetryPolicy != nil {
		p := *o.RetryPolicy
		p.NonRetryableErrors = nil
		o.RetryPolicy = &p
	}
	return o
}
func resolveActivityOptions(def, call ActivityOptions) (ActivityOptions, error) {
	if err := validateActivityOptions(def, false); err != nil {
		return ActivityOptions{}, err
	}
	if err := validateActivityOptions(call, true); err != nil {
		return ActivityOptions{}, err
	}
	out := cloneActivityOptions(def)
	if call.Queue != "" {
		out.Queue = call.Queue
	}
	if call.StartToCloseTimeout != 0 {
		out.StartToCloseTimeout = call.StartToCloseTimeout
	}
	if call.ScheduleToStartTimeout != 0 {
		out.ScheduleToStartTimeout = call.ScheduleToStartTimeout
	}
	if call.HeartbeatTimeout != 0 {
		out.HeartbeatTimeout = call.HeartbeatTimeout
	}
	if out.RetryPolicy == nil {
		if call.RetryPolicy == nil {
			out.RetryPolicy = retry.DefaultPolicy()
		} else {
			out.RetryPolicy = &retry.Policy{}
		}
	}
	if p := call.RetryPolicy; p != nil {
		dst := out.RetryPolicy
		if p.MaxAttempts != 0 {
			dst.MaxAttempts = p.MaxAttempts
		}
		if p.InitialInterval != 0 {
			dst.InitialInterval = p.InitialInterval
		}
		if p.MaxInterval != 0 {
			dst.MaxInterval = p.MaxInterval
		}
		if p.Multiplier != 0 {
			dst.Multiplier = p.Multiplier
		}
		if p.RandomizationFactor != 0 {
			dst.RandomizationFactor = p.RandomizationFactor
		}
	}
	p := out.RetryPolicy
	if p.InitialInterval == 0 {
		p.InitialInterval = 100 * time.Millisecond
	}
	if p.MaxInterval == 0 {
		p.MaxInterval = p.InitialInterval * 100
		if p.InitialInterval > time.Duration(math.MaxInt64)/100 {
			p.MaxInterval = time.Duration(math.MaxInt64)
		}
	}
	if p.Multiplier < 1 {
		p.Multiplier = 1
	}
	p.RandomizationFactor = max(0, min(1, p.RandomizationFactor))
	if out.StartToCloseTimeout > 0 && out.HeartbeatTimeout > out.StartToCloseTimeout {
		out.HeartbeatTimeout = out.StartToCloseTimeout
	}
	return out, nil
}
func validateActivityOptions(o ActivityOptions, call bool) error {
	if len(o.Queue) > 255 {
		return fmt.Errorf("activity queue exceeds 255 bytes")
	}
	if o.StartToCloseTimeout < 0 || o.ScheduleToStartTimeout < 0 || o.HeartbeatTimeout < 0 {
		return fmt.Errorf("activity timeouts must not be negative")
	}
	if p := o.RetryPolicy; p != nil {
		if p.MaxAttempts < 0 || p.InitialInterval < 0 || p.MaxInterval < 0 {
			return fmt.Errorf("activity retry counts and intervals must not be negative")
		}
		if math.IsNaN(p.Multiplier) || math.IsInf(p.Multiplier, 0) || math.IsNaN(p.RandomizationFactor) || math.IsInf(p.RandomizationFactor, 0) {
			return fmt.Errorf("activity retry factors must be finite numbers")
		}
		if call && len(p.NonRetryableErrors) > 0 {
			return fmt.Errorf("per-call non-retryable errors cannot be recorded")
		}
	}
	return nil
}

func activityMillis(d time.Duration) int64 {
	ms := d.Milliseconds()
	if d%time.Millisecond > 0 {
		ms++
	}
	return ms
}
