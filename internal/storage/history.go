package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// historyStore shares the SQL protocol between dialects, including caller-owned transactions.
// Both drivers support numbered $n placeholders.
type historyStore struct {
	TransactionManager
}

func (s historyStore) get(ctx context.Context, instanceID, key string) (*HistoryEvent, error) {
	return readHistory(ctx, s.Conn(ctx), instanceID, key)
}

func readHistory(ctx context.Context, conn Executor, instanceID, key string) (*HistoryEvent, error) {
	row := conn.QueryRowContext(ctx, `
		SELECT id, instance_id, activity_id, command, event_type, event_data, event_data_binary, data_type, reservation_data, created_at
		FROM workflow_history WHERE instance_id = $1 AND activity_id = $2
	`, instanceID, key)
	var ev HistoryEvent
	var data, reservation sql.NullString
	var created any
	if err := row.Scan(&ev.ID, &ev.InstanceID, &ev.ActivityID, &ev.Command, &ev.EventType, &data, &ev.EventDataBinary, &ev.DataType, &reservation, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if reservation.Valid {
		ev.ReservationData = []byte(reservation.String)
	}
	if data.Valid {
		ev.EventData = []byte(data.String)
	}
	switch value := created.(type) {
	case time.Time:
		ev.CreatedAt = value
	case string:
		var err error
		ev.CreatedAt, err = parseSQLiteTime(value)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected history timestamp type %T", created)
	}
	return &ev, nil
}

func (s historyStore) write(ctx context.Context, f *Fence, ev *HistoryEvent, fill bool) (stored *HistoryEvent, err error) {
	if f != nil && f.InstanceID != ev.InstanceID {
		return nil, rejectFence(ctx)
	}
	err = fenceStore(s).withFence(ctx, f, func(conn Executor) error {
		if fill {
			if _, e := conn.ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE instance_id=$1 AND status='running'`, ev.InstanceID); e != nil {
				return e
			}
		}
		var writeErr error
		stored, writeErr = writeHistory(ctx, conn, ev, fill)
		return writeErr
	})
	if err == nil && fill {
		notifyRunnable(ctx, s.TransactionManager, ev.InstanceID)
	}
	return stored, err
}

func writeHistory(ctx context.Context, conn Executor, ev *HistoryEvent, fill bool) (stored *HistoryEvent, err error) {
	var data any
	if ev.EventData != nil {
		data = string(ev.EventData)
	}
	var id int64
	err = conn.QueryRowContext(ctx, `
		INSERT INTO workflow_history (instance_id, activity_id, command, event_type, event_data, event_data_binary, data_type, reservation_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (instance_id, activity_id) DO NOTHING RETURNING id
	`, ev.InstanceID, ev.ActivityID, ev.Command, ev.EventType, data, ev.EventDataBinary, ev.DataType, nullableJSON(ev.ReservationData)).Scan(&id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if fill {
		_, err = conn.ExecContext(ctx, `
			UPDATE workflow_history SET event_type = $1, event_data = $2, data_type = 'json'
			WHERE instance_id = $3 AND activity_id = $4 AND command = $5 AND event_data IS NULL
	`, ev.EventType, data, ev.InstanceID, ev.ActivityID, ev.Command)
		if err != nil {
			return nil, err
		}
	}
	stored, err = readHistory(ctx, conn, ev.InstanceID, ev.ActivityID)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return nil, fmt.Errorf("history row missing after write: %s", ev.ActivityID)
	}
	if stored.Command != ev.Command {
		return nil, ErrHistoryKeyConflict
	}
	return stored, nil
}

// ReserveHistory reserves a command key with a NULL value.
func (s *SQLiteStorage) ReserveHistory(ctx context.Context, f *Fence, instanceID, key, cmd string) (*HistoryEvent, error) {
	if f == nil {
		return nil, rejectFence(ctx)
	}
	return (historyStore{s}).write(ctx, f, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: HistoryReserved, DataType: "json"}, false)
}

// RecordHistory returns the first stored command and value at the key.
func (s *SQLiteStorage) RecordHistory(ctx context.Context, f *Fence, ev *HistoryEvent) (*HistoryEvent, error) {
	if f == nil {
		return nil, rejectFence(ctx)
	}
	return (historyStore{s}).write(ctx, f, ev, false)
}

// SetHistoryValueIfNull records the first value for a matching command.
func (s *SQLiteStorage) SetHistoryValueIfNull(ctx context.Context, f *Fence, instanceID, key, cmd string, t HistoryEventType, data []byte) (*HistoryEvent, error) {
	if f == nil && t != HistoryActivityCompleted && t != HistoryActivityFailed && t != HistoryTimerExpired {
		return nil, rejectFence(ctx)
	}
	return (historyStore{s}).write(ctx, f, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: t, EventData: data, DataType: "json"}, true)
}

// GetHistoryEvent returns the stored row, or nil when absent.
func (s *SQLiteStorage) GetHistoryEvent(ctx context.Context, instanceID, key string) (*HistoryEvent, error) {
	return (historyStore{s}).get(ctx, instanceID, key)
}

// ReserveHistory reserves a command key with a NULL value.
func (s *PostgresStorage) ReserveHistory(ctx context.Context, f *Fence, instanceID, key, cmd string) (*HistoryEvent, error) {
	if f == nil {
		return nil, rejectFence(ctx)
	}
	return (historyStore{s}).write(ctx, f, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: HistoryReserved, DataType: "json"}, false)
}

// RecordHistory returns the first stored command and value at the key.
func (s *PostgresStorage) RecordHistory(ctx context.Context, f *Fence, ev *HistoryEvent) (*HistoryEvent, error) {
	if f == nil {
		return nil, rejectFence(ctx)
	}
	return (historyStore{s}).write(ctx, f, ev, false)
}

// SetHistoryValueIfNull records the first value for a matching command.
func (s *PostgresStorage) SetHistoryValueIfNull(ctx context.Context, f *Fence, instanceID, key, cmd string, t HistoryEventType, data []byte) (*HistoryEvent, error) {
	if f == nil && t != HistoryActivityCompleted && t != HistoryActivityFailed && t != HistoryTimerExpired {
		return nil, rejectFence(ctx)
	}
	return (historyStore{s}).write(ctx, f, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: t, EventData: data, DataType: "json"}, true)
}

// GetHistoryEvent returns the stored row, or nil when absent.
func (s *PostgresStorage) GetHistoryEvent(ctx context.Context, instanceID, key string) (*HistoryEvent, error) {
	return (historyStore{s}).get(ctx, instanceID, key)
}

func nullableJSON(data []byte) any {
	if data == nil {
		return nil
	}
	return string(data)
}
