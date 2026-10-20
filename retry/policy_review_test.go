package retry

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDelayCapsTheFirstRetry(t *testing.T) {
	policy := Exponential(3, time.Hour, time.Second, 2)
	policy.RandomizationFactor = 0
	require.Equal(t, time.Second, policy.GetDelay(1))
}

func TestDelaySaturatesBeforeDurationOverflow(t *testing.T) {
	policy := Exponential(0, time.Second, time.Duration(math.MaxInt64), 2)
	policy.RandomizationFactor = 0
	require.Equal(t, time.Duration(math.MaxInt64), policy.GetDelay(1000))
}

func TestDelayJitterRespectsMaximum(t *testing.T) {
	policy := Exponential(0, time.Second, time.Second, 2)
	policy.RandomizationFactor = 1
	for range 100 {
		delay := policy.GetDelay(100)
		require.GreaterOrEqual(t, delay, time.Duration(0))
		require.LessOrEqual(t, delay, time.Second)
	}
}
