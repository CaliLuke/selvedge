package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// fireTimer records the poller result, wake, and subscription deletion atomically.
func fireTimer(ctx context.Context, s Storage, timer *TimerSubscription) error {
	woke := false
	err := (fenceStore{s}).withFence(ctx, nil, func(conn Executor) error {
		// All multi-row writers lock the instance before touching subscriptions or history.
		if _, err := conn.ExecContext(ctx, `UPDATE workflow_instances SET lease_epoch=lease_epoch WHERE instance_id=$1`, timer.InstanceID); err != nil {
			return err
		}
		var subscriptionID int64
		err := conn.QueryRowContext(ctx, `DELETE FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id=$2 RETURNING id`, timer.InstanceID, timer.TimerID).Scan(&subscriptionID)
		subscribed := !errors.Is(err, sql.ErrNoRows)
		if err != nil && subscribed {
			return err
		}
		// Preserve reservation validation even for a stale poller snapshot.
		// An absent subscription still cannot fill history or wake the instance.
		row, err := readHistory(ctx, conn, timer.InstanceID, timer.TimerID)
		if err != nil {
			return err
		}
		if row == nil || row.Command != "sleep" {
			var running bool
			err = conn.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_instances WHERE instance_id=$1 AND status='running')`, timer.InstanceID).Scan(&running)
			if err != nil {
				return err
			}
			if running {
				return ErrHistoryKeyConflict
			}
		}

		if !subscribed {
			return nil
		}
		result, err := conn.ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE instance_id=$1 AND status='running'`, timer.InstanceID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		payload, err := json.Marshal(map[string]any{"timer_id": timer.TimerID, "expires_at": timer.ExpiresAt.Format(time.RFC3339Nano), "completed": true})
		if err != nil {
			return err
		}
		_, err = writeHistory(ctx, conn, &HistoryEvent{InstanceID: timer.InstanceID, ActivityID: timer.TimerID, Command: "sleep", EventType: HistoryTimerExpired, EventData: payload, DataType: "json"}, true)
		if err != nil {
			return err
		}
		woke = true
		return nil
	})
	if err == nil && woke {
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
