package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimerFireRequiresSubscription(t *testing.T) {
	testTimerFireRequiresSubscription(t, sqliteHistoryFactory)
}
func testTimerFireRequiresSubscription(t *testing.T, factory historyFactory) {
	s, f, _ := taskFixture(t, factory)
	ctx := context.Background()
	timer := &TimerSubscription{InstanceID: "tasks", TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()}
	_, err := s.ReserveTimer(ctx, f, 1, timer)
	require.NoError(t, err)
	require.NoError(t, s.RemoveTimerSubscription(ContextWithFence(ctx, *f), "tasks", "#1"))
	_, err = s.DB().Exec(`UPDATE workflow_instances SET runnable=FALSE WHERE instance_id='tasks'`)
	require.NoError(t, err)
	before, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.NoError(t, s.FireTimer(ctx, timer))
	after, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Equal(t, before, after)
	inst, err := s.GetInstance(ctx, "tasks")
	require.NoError(t, err)
	require.False(t, inst.Runnable)
	// First fire consumes the subscription; a duplicate must not wake again.
	_, err = s.ReserveTimer(ctx, f, 1, timer)
	require.NoError(t, err)
	require.NoError(t, s.FireTimer(ctx, timer))
	_, err = s.DB().Exec(`UPDATE workflow_instances SET runnable=FALSE WHERE instance_id='tasks'`)
	require.NoError(t, err)
	require.NoError(t, s.FireTimer(ctx, timer))
	inst, err = s.GetInstance(ctx, "tasks")
	require.NoError(t, err)
	require.False(t, inst.Runnable)
}

func TestTimerRetireAtomic(t *testing.T) { testTimerRetireAtomic(t, sqliteHistoryFactory) }
func testTimerRetireAtomic(t *testing.T, factory historyFactory) {
	for _, fired := range []bool{false, true} {
		name := "pending"
		if fired {
			name = "fired"
		}
		t.Run(name, func(t *testing.T) {
			s, f, _ := taskFixture(t, factory)
			ctx := context.Background()
			timer := &TimerSubscription{InstanceID: "tasks", TimerID: "#1", ExpiresAt: time.Unix(123, 0).UTC()}
			_, err := s.ReserveTimer(ctx, f, 1, timer)
			require.NoError(t, err)
			if fired {
				require.NoError(t, s.FireTimer(ctx, timer))
			}
			id, err := s.AppendSignal(ctx, "tasks", "c", []byte(`42`))
			require.NoError(t, err)
			c := &ObservationCommit{Seq: 2, DeliverAll: true, Retire: &TimerRetire{Key: "#1"}, Consume: &SignalConsume{ID: id, Channel: "c"}, Record: &HistoryEvent{InstanceID: "tasks", Seq: 2, ActivityID: "#2", Command: "receive_timeout:c", EventType: HistorySignalReceived, EventData: []byte(`42`), DataType: "json"}}
			tx, err := s.BeginTransaction(ctx)
			require.NoError(t, err)
			defer func() { _ = s.RollbackTransaction(tx) }()
			_, err = s.CommitObservation(tx, f, c)
			require.NoError(t, err)
			require.NoError(t, s.RollbackTransaction(tx))
			row, err := s.GetHistoryEvent(ctx, "tasks", "#2")
			require.NoError(t, err)
			require.Nil(t, row)
			sig, err := s.NextSignal(ctx, "tasks", "c")
			require.NoError(t, err)
			require.NotNil(t, sig)
			require.Zero(t, sig.DeliveredSeq)
			stale := *f
			stale.Epoch++
			_, err = s.CommitObservation(ctx, &stale, c)
			require.ErrorIs(t, err, ErrFenced)
			// A later consume failure rolls back retirement and the delivery batch.
			c.Consume.ID = id + 100
			_, err = s.CommitObservation(ctx, f, c)
			require.ErrorIs(t, err, ErrSignalNotAvailable)
			c.Consume.ID = id
			row, err = s.GetHistoryEvent(ctx, "tasks", "#1")
			require.NoError(t, err)
			require.Zero(t, row.DeliveredSeq)
			if !fired {
				require.Nil(t, row.EventData)
				var n int
				require.NoError(t, s.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id='tasks'`).Scan(&n))
				require.Equal(t, 1, n)
			}
			_, err = s.CommitObservation(ctx, f, c)
			require.NoError(t, err)
			row, err = s.GetHistoryEvent(ctx, "tasks", "#1")
			require.NoError(t, err)
			require.EqualValues(t, 2, row.DeliveredSeq)
			if fired {
				require.Equal(t, HistoryTimerExpired, row.EventType)
			} else {
				require.Equal(t, HistoryEventType("TimerSuperseded"), row.EventType)
			}
			var n int
			require.NoError(t, s.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id='tasks'`).Scan(&n))
			require.Zero(t, n)
			_, err = s.CommitObservation(ctx, f, c)
			require.NoError(t, err)
		})
	}
}
