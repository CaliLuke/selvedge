package selvedge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestTimerFireRequiresRunningInstance(t *testing.T) {
	testTimerFireRequiresRunningInstance(t, createTestApp)
}
func testTimerFireRequiresRunningInstance(t *testing.T, factory replayAppFactory) {
	for _, status := range []storage.WorkflowStatus{storage.StatusCompleted, storage.StatusFailed, storage.StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			h, _ := newLeaseStore(t, factory)
			s := h.app.storage
			timer := &storage.TimerSubscription{InstanceID: "lease", TimerID: "pause", ExpiresAt: time.Now()}
			_, err := s.ReserveTimer(h.app.ctx, fixtureFence(t, s, "lease"), 1, timer)
			require.NoError(t, err)
			// Retain a pending reservation and timer after closure to model a stale
			// poller delivery. Closing through CloseInstance would delete the timer.
			_, err = s.DB().Exec("UPDATE workflow_instances SET status=$1,runnable=FALSE WHERE instance_id=$2", status, "lease")
			require.NoError(t, err)
			require.NoError(t, s.FireTimer(h.app.ctx, timer))
			row, err := s.GetHistoryEvent(h.app.ctx, "lease", "pause")
			require.NoError(t, err)
			require.Nil(t, row.EventData, "terminal timer must not acquire a recorded result")
			instance, err := s.GetInstance(h.app.ctx, "lease")
			require.NoError(t, err)
			require.Equal(t, status, instance.Status)
			require.False(t, instance.Runnable)
			var timers int
			require.NoError(t, s.DB().QueryRow("SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1", "lease").Scan(&timers))
			require.Zero(t, timers)
		})
	}
}
