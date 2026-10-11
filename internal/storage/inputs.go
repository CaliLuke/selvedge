package storage

import (
	"context"
	"encoding/json"
	"time"
)

// fireTimer records the poller result, wake, and subscription deletion atomically.
func fireTimer(ctx context.Context, s Storage, timer *TimerSubscription) error {
	err := (fenceStore{s}).withFence(ctx, nil, func(conn Executor) error {
		result, err := conn.ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE instance_id=$1 AND status='running'`, timer.InstanceID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			_, err = conn.ExecContext(ctx, `DELETE FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id=$2`, timer.InstanceID, timer.TimerID)
			return err
		}
		payload, err := json.Marshal(map[string]any{"timer_id": timer.TimerID, "expires_at": timer.ExpiresAt.Format(time.RFC3339Nano), "completed": true})
		if err != nil {
			return err
		}
		_, err = writeHistory(ctx, conn, &HistoryEvent{InstanceID: timer.InstanceID, ActivityID: timer.TimerID, Command: "sleep", EventType: HistoryTimerExpired, EventData: payload, DataType: "json"}, true)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id=$2`, timer.InstanceID, timer.TimerID)
		return err
	})
	if err == nil {
		notifyRunnable(ctx, s, timer.InstanceID)
	}
	return err
}

// FireTimer atomically records a timer result and wake, or discards a terminal instance's timer.
func (s *SQLiteStorage) FireTimer(ctx context.Context, timer *TimerSubscription) error {
	return fireTimer(ctx, s, timer)
}

// FireTimer atomically records a timer result and wake, or discards a terminal instance's timer.
func (s *PostgresStorage) FireTimer(ctx context.Context, timer *TimerSubscription) error {
	return fireTimer(ctx, s, timer)
}
