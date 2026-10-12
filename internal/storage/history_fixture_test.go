package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fixtureRecord(ctx context.Context, s Storage, ev *HistoryEvent) (*HistoryEvent, error) {
	if ev.Seq == 0 && !strings.HasPrefix(ev.ActivityID, "$") {
		err := s.Conn(ctx).QueryRowContext(ctx, `SELECT COALESCE((SELECT seq FROM workflow_history WHERE instance_id=$1 AND activity_id=$2),(SELECT COALESCE(MAX(seq),0)+1 FROM workflow_history WHERE instance_id=$1))`, ev.InstanceID, ev.ActivityID).Scan(&ev.Seq)
		if err != nil {
			return nil, err
		}
	}
	instance, err := s.GetInstance(ctx, ev.InstanceID)
	if err != nil {
		return nil, err
	}
	f := Fence{InstanceID: ev.InstanceID, Owner: instance.LeaseOwner, Epoch: instance.LeaseEpoch}
	if f.Owner == "" {
		var ok bool
		f, ok, err = s.AcquireLease(ctx, ev.InstanceID, "fixture", time.Hour)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrFenced
		}
		defer func() { _ = s.ReleaseLease(ctx, f, true) }()
	}
	if ev.EventType == HistoryActivityCompleted || ev.EventType == HistoryActivityFailed || ev.EventType == HistoryTimerExpired {
		_, err := s.ReserveHistory(ctx, &f, ev.InstanceID, ev.ActivityID, ev.Command, ev.Seq)
		if err != nil {
			return nil, err
		}
		return s.SetHistoryValueIfNull(ctx, &f, ev.InstanceID, ev.ActivityID, ev.Command, ev.EventType, ev.EventData)
	}
	return s.RecordHistory(ctx, &f, ev)
}

func fixtureFence(t *testing.T, s Storage, id string) *Fence {
	t.Helper()
	f, ok, err := s.AcquireLease(context.Background(), id, "fixture", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(func() { _ = s.ReleaseLease(context.Background(), f, true) })
	return &f
}
func fixtureContext(t *testing.T, s Storage, ctx context.Context, id string) context.Context {
	return ContextWithFence(ctx, *fixtureFence(t, s, id))
}

func fixtureTimer(ctx context.Context, s Storage, timer *TimerSubscription) error {
	_, err := s.Conn(ctx).ExecContext(ctx, `INSERT INTO workflow_timer_subscriptions(instance_id,timer_id,expires_at,activity_id) VALUES($1,$2,$3,$4)`, timer.InstanceID, timer.TimerID, timer.ExpiresAt.UTC().Format("2006-01-02 15:04:05"), timer.ActivityID)
	return err
}

func fixtureConsume(s Storage, ctx context.Context, f *Fence, id int64, obs *HistoryEvent) (*HistoryEvent, error) {
	c := &ObservationCommit{Seq: 1, DeliverAll: true, Record: obs}
	if obs != nil {
		c.Seq = obs.Seq
		c.Consume = &SignalConsume{ID: id, Channel: strings.TrimPrefix(obs.Command, "receive:")}
	}
	r, err := s.CommitObservation(ctx, f, c)
	if err != nil {
		return nil, err
	}
	return r.Stored, nil
}
