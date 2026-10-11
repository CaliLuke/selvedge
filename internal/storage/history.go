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
	row := s.Conn(ctx).QueryRowContext(ctx, `
		SELECT id, instance_id, activity_id, command, event_type, event_data, event_data_binary, data_type, created_at
		FROM workflow_history WHERE instance_id = $1 AND activity_id = $2
	`, instanceID, key)
	var ev HistoryEvent
	var data sql.NullString
	var created any
	if err := row.Scan(&ev.ID, &ev.InstanceID, &ev.ActivityID, &ev.Command, &ev.EventType, &data, &ev.EventDataBinary, &ev.DataType, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
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

func (s historyStore) write(ctx context.Context, ev *HistoryEvent, fill bool) (stored *HistoryEvent, err error) {
	ownTx := !s.InTransaction(ctx)
	if ownTx {
		ctx, err = s.BeginTransaction(ctx)
		if err != nil {
			return nil, err
		}
		defer func() { _ = s.RollbackTransaction(ctx) }()
	}
	var data any
	if ev.EventData != nil {
		data = string(ev.EventData)
	}
	var id int64
	err = s.Conn(ctx).QueryRowContext(ctx, `
		INSERT INTO workflow_history (instance_id, activity_id, command, event_type, event_data, event_data_binary, data_type)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (instance_id, activity_id) DO NOTHING RETURNING id
	`, ev.InstanceID, ev.ActivityID, ev.Command, ev.EventType, data, ev.EventDataBinary, ev.DataType).Scan(&id)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if fill {
		_, err = s.Conn(ctx).ExecContext(ctx, `
			UPDATE workflow_history SET event_type = $1, event_data = $2, data_type = 'json'
			WHERE instance_id = $3 AND activity_id = $4 AND command = $5 AND event_data IS NULL
	`, ev.EventType, data, ev.InstanceID, ev.ActivityID, ev.Command)
		if err != nil {
			return nil, err
		}
	}
	stored, err = s.get(ctx, ev.InstanceID, ev.ActivityID)
	if err != nil {
		return nil, err
	}
	if stored == nil {
		return nil, fmt.Errorf("history row missing after write: %s", ev.ActivityID)
	}
	if stored.Command != ev.Command {
		return nil, ErrHistoryKeyConflict
	}
	if ownTx {
		if err := s.CommitTransaction(ctx); err != nil {
			return nil, err
		}
	}
	return stored, nil
}

// ReserveHistory reserves a command key with a NULL value.
func (s *SQLiteStorage) ReserveHistory(ctx context.Context, _ *Fence, instanceID, key, cmd string) (*HistoryEvent, error) {
	return (historyStore{s}).write(ctx, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: HistoryReserved, DataType: "json"}, false)
}

// RecordHistory returns the first stored command and value at the key.
func (s *SQLiteStorage) RecordHistory(ctx context.Context, _ *Fence, ev *HistoryEvent) (*HistoryEvent, error) {
	return (historyStore{s}).write(ctx, ev, false)
}

// SetHistoryValueIfNull records the first value for a matching command.
func (s *SQLiteStorage) SetHistoryValueIfNull(ctx context.Context, _ *Fence, instanceID, key, cmd string, t HistoryEventType, data []byte) (*HistoryEvent, error) {
	return (historyStore{s}).write(ctx, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: t, EventData: data, DataType: "json"}, true)
}

// GetHistoryEvent returns the stored row, or nil when absent.
func (s *SQLiteStorage) GetHistoryEvent(ctx context.Context, instanceID, key string) (*HistoryEvent, error) {
	return (historyStore{s}).get(ctx, instanceID, key)
}

// ReserveHistory reserves a command key with a NULL value.
func (s *PostgresStorage) ReserveHistory(ctx context.Context, _ *Fence, instanceID, key, cmd string) (*HistoryEvent, error) {
	return (historyStore{s}).write(ctx, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: HistoryReserved, DataType: "json"}, false)
}

// RecordHistory returns the first stored command and value at the key.
func (s *PostgresStorage) RecordHistory(ctx context.Context, _ *Fence, ev *HistoryEvent) (*HistoryEvent, error) {
	return (historyStore{s}).write(ctx, ev, false)
}

// SetHistoryValueIfNull records the first value for a matching command.
func (s *PostgresStorage) SetHistoryValueIfNull(ctx context.Context, _ *Fence, instanceID, key, cmd string, t HistoryEventType, data []byte) (*HistoryEvent, error) {
	return (historyStore{s}).write(ctx, &HistoryEvent{InstanceID: instanceID, ActivityID: key, Command: cmd, EventType: t, EventData: data, DataType: "json"}, true)
}

// GetHistoryEvent returns the stored row, or nil when absent.
func (s *PostgresStorage) GetHistoryEvent(ctx context.Context, instanceID, key string) (*HistoryEvent, error) {
	return (historyStore{s}).get(ctx, instanceID, key)
}
