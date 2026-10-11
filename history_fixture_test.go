package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func fixtureRecord(ctx context.Context, s storage.Storage, ev *storage.HistoryEvent) (*storage.HistoryEvent, error) {
	if ev.EventType == storage.HistoryActivityCompleted || ev.EventType == storage.HistoryActivityFailed || ev.EventType == storage.HistoryTimerExpired {
		return s.SetHistoryValueIfNull(ctx, nil, ev.InstanceID, ev.ActivityID, ev.Command, ev.EventType, ev.EventData)
	}
	_, err := s.Conn(ctx).ExecContext(ctx, `INSERT INTO workflow_history (instance_id,activity_id,command,event_type,event_data,data_type) VALUES ($1,$2,$3,$4,$5,$6)`, ev.InstanceID, ev.ActivityID, ev.Command, ev.EventType, string(ev.EventData), ev.DataType)
	if err != nil {
		return nil, err
	}
	return s.GetHistoryEvent(ctx, ev.InstanceID, ev.ActivityID)
}

func fixtureFence(t *testing.T, s storage.Storage, id string) *storage.Fence {
	t.Helper()
	f, ok, err := s.AcquireLease(context.Background(), id, "fixture", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(func() { _ = s.ReleaseLease(context.Background(), f, true) })
	return &f
}

func fixtureTimer(ctx context.Context, s storage.Storage, timer *storage.TimerSubscription) error {
	_, err := s.Conn(ctx).ExecContext(ctx, `INSERT INTO workflow_timer_subscriptions(instance_id,timer_id,expires_at,activity_id) VALUES($1,$2,$3,$4)`, timer.InstanceID, timer.TimerID, timer.ExpiresAt.UTC().Format("2006-01-02 15:04:05"), timer.ActivityID)
	return err
}
