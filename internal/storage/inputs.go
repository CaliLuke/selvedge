package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
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

// consumeChannel records the observation and advances its cursor together.
func consumeChannel(ctx context.Context, s Storage, f *Fence, channel string, id int64, ev *HistoryEvent) (stored *HistoryEvent, err error) {
	if f == nil || f.InstanceID != ev.InstanceID {
		return nil, rejectFence(ctx)
	}
	err = (fenceStore{s}).withFence(ctx, f, func(conn Executor) error {
		existing, e := readHistory(ctx, conn, ev.InstanceID, ev.ActivityID)
		if e != nil {
			return e
		}
		if existing != nil {
			if existing.Command != ev.Command {
				return ErrHistoryKeyConflict
			}
			stored = existing
			return nil
		}
		stored, e = writeHistory(ctx, conn, ev, false)
		if e != nil {
			return e
		}
		_, e = conn.ExecContext(ctx, `UPDATE channel_delivery_cursors SET last_delivered_id=$1 WHERE instance_id=$2 AND channel=$3`, id, f.InstanceID, channel)
		if e != nil {
			return e
		}
		_, e = conn.ExecContext(ctx, `UPDATE channel_subscriptions SET activity_id=NULL,timeout_at=NULL WHERE instance_id=$1 AND channel=$2`, f.InstanceID, channel)
		return e
	})
	return stored, err
}

// ConsumeChannelMessage records a fenced observation and advances its cursor atomically.
func (s *SQLiteStorage) ConsumeChannelMessage(ctx context.Context, f *Fence, channel string, id int64, ev *HistoryEvent) (*HistoryEvent, error) {
	return consumeChannel(ctx, s, f, channel, id, ev)
}

// ConsumeChannelMessage records a fenced observation and advances its cursor atomically.
func (s *PostgresStorage) ConsumeChannelMessage(ctx context.Context, f *Fence, channel string, id int64, ev *HistoryEvent) (*HistoryEvent, error) {
	return consumeChannel(ctx, s, f, channel, id, ev)
}

func publishChannel(ctx context.Context, s Storage, channel string, data, metadata []byte) (id int64, err error) {
	err = (fenceStore{s}).withFence(ctx, nil, func(conn Executor) error {
		var payload, meta any
		if data != nil {
			payload = string(data)
		}
		if metadata != nil {
			meta = string(metadata)
		}
		if e := conn.QueryRowContext(ctx, `INSERT INTO channel_messages (channel,data,metadata,message_id,data_type) VALUES ($1,$2,$3,$4,'json') RETURNING id`, channel, payload, meta, uuid.NewString()).Scan(&id); e != nil {
			return e
		}
		_, e := conn.ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE status='running' AND instance_id IN (SELECT instance_id FROM channel_subscriptions WHERE channel=$1)`, channel)
		return e
	})
	if err == nil {
		notifyRunnable(ctx, s, "")
	}
	return id, err
}
