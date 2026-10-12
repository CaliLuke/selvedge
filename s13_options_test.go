package selvedge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientPollOptions(t *testing.T) {
	app := NewApp(WithWaitPollInterval(17*time.Millisecond), WithMaxPollInterval(25*time.Millisecond))
	require.Equal(t, 17*time.Millisecond, app.config.waitPollInterval)
	require.Equal(t, 25*time.Millisecond, app.config.maxPollInterval)
	require.Equal(t, time.Second, defaultConfig().waitPollInterval)
	require.Equal(t, time.Minute, defaultConfig().maxPollInterval)
}

func TestClientPollJitterStaysWithinCapAndVaries(t *testing.T) {
	defaults := defaultConfig()
	for _, tc := range []struct {
		name  string
		base  time.Duration
		empty int
		cap   time.Duration
	}{
		{"default_idle_dispatcher", defaults.workflowResumptionInterval, 100, defaults.maxPollInterval},
		{"default_idle_activity", defaults.activityPollInterval, 100, defaults.maxPollInterval},
		{"capped_idle", time.Second, 100, time.Second},
		{"base_above_cap", time.Hour, 0, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := make(map[time.Duration]bool)
			for range 100 {
				delay := cappedPollDelay(tc.base, tc.empty, tc.cap)
				require.Positive(t, delay)
				require.LessOrEqual(t, delay, tc.cap)
				seen[delay] = true
			}
			require.Greater(t, len(seen), 1, "idle workers must not all sleep for the cap")
		})
	}
}
