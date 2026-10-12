package storage

import (
	"context"
	"encoding/json"
	"time"
)

// SleepReservation holds immutable command metadata; EventData holds the winning observation.
type SleepReservation struct {
	ExpiresAt time.Time `json:"expires_at"`
}

func reserveTimer(ctx context.Context, s Storage, f *Fence, seq int64, timer *TimerSubscription) (stored *HistoryEvent, err error) {
	if f == nil || f.InstanceID != timer.InstanceID {
		return nil, rejectFence(ctx)
	}
	payload, err := json.Marshal(SleepReservation{ExpiresAt: timer.ExpiresAt})
	if err != nil {
		return nil, err
	}
	err = (fenceStore{s}).withFence(ctx, f, func(conn Executor) error {
		var e error
		stored, e = writeHistory(ctx, conn, &HistoryEvent{Seq: seq, InstanceID: timer.InstanceID, ActivityID: timer.TimerID, Command: "sleep", EventType: HistoryReserved, ReservationData: payload, DataType: "json"}, false)
		if e != nil {
			return e
		}
		if stored.EventData != nil {
			return nil
		}
		var reservation SleepReservation
		if e := json.Unmarshal(stored.ReservationData, &reservation); e != nil {
			return e
		}
		_, e = conn.ExecContext(ctx, `INSERT INTO workflow_timer_subscriptions(instance_id,timer_id,expires_at,activity_id)
   VALUES($1,$2,$3,$4) ON CONFLICT(instance_id,timer_id) DO NOTHING`, timer.InstanceID, timer.TimerID, reservation.ExpiresAt.UTC().Format("2006-01-02 15:04:05.000000"), timer.ActivityID)
		return e
	})
	return stored, err
}

// ReserveTimer reserves a sleep and arms its timer atomically under the execution fence.
func (s *SQLiteStorage) ReserveTimer(ctx context.Context, f *Fence, seq int64, timer *TimerSubscription) (*HistoryEvent, error) {
	return reserveTimer(ctx, s, f, seq, timer)
}

// ReserveTimer reserves a sleep and arms its timer atomically under the execution fence.
func (s *PostgresStorage) ReserveTimer(ctx context.Context, f *Fence, seq int64, timer *TimerSubscription) (*HistoryEvent, error) {
	return reserveTimer(ctx, s, f, seq, timer)
}
