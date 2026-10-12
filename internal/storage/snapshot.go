package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// InstanceSnapshot is one consistent, detached view of an instance and its replay inputs.
type InstanceSnapshot struct {
	Instance        *WorkflowInstance
	CancelRequested bool
	History         []*HistoryEvent
	Signals         []SignalPosition
}

// SnapshotReader reads replay inputs without acquiring the workflow lease.
type SnapshotReader interface {
	ReadSnapshot(context.Context, string) (*InstanceSnapshot, error)
}

// ReadSnapshot uses a deferred transaction, bypassing the pool's immediate write transactions.
// In-memory SQLite databases (":memory:" with shared cache) cannot use WAL.
// Their snapshots block writers until the read transaction ends.
func (s *SQLiteStorage) ReadSnapshot(ctx context.Context, id string) (*InstanceSnapshot, error) {
	return readSnapshot(ctx, s.db, s.driver, id, "BEGIN")
}

// ReadSnapshot fixes all replay inputs to the same PostgreSQL MVCC snapshot.
func (s *PostgresStorage) ReadSnapshot(ctx context.Context, id string) (*InstanceSnapshot, error) {
	return readSnapshot(ctx, s.db, s.driver, id, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
}

func readSnapshot(ctx context.Context, db *sql.DB, driver Driver, id, begin string) (*InstanceSnapshot, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	observed := observeExecutor(driver, conn)
	if _, err = observed.ExecContext(ctx, begin); err != nil {
		return nil, err
	}
	// Always release the transaction, including on caller cancellation. No callbacks or notifications.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = observed.ExecContext(cleanup, "ROLLBACK")
	}()
	snapshot, err := readSnapshotInstance(ctx, observed, id)
	if err != nil || snapshot == nil {
		return snapshot, err
	}
	snapshot.History, err = readSnapshotHistory(ctx, observed, id)
	if err != nil {
		return nil, err
	}
	snapshot.Signals, err = deliveredSignals(ctx, observed, id)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func readSnapshotInstance(ctx context.Context, conn Executor, id string) (*InstanceSnapshot, error) {
	snapshot := &InstanceSnapshot{Instance: new(WorkflowInstance)}
	inst := snapshot.Instance
	var input, output sql.NullString
	var expiry sql.NullInt64
	var started, updated any
	err := conn.QueryRowContext(ctx, `SELECT instance_id, workflow_name, COALESCE(framework,''), status, input_data, output_data,
 COALESCE(current_activity_id,''), COALESCE(source_hash,''), COALESCE(owner_service,''),
 COALESCE(lease_owner,''), lease_epoch, lease_expires_ms, runnable, started_at, updated_at,
 COALESCE(cancel_reason,''), COALESCE(parent_instance_id,''), COALESCE(parent_key,''), cancel_requested_at IS NOT NULL
 FROM workflow_instances WHERE instance_id=$1`, id).Scan(&inst.InstanceID, &inst.WorkflowName, &inst.Framework, &inst.Status, &input, &output,
		&inst.CurrentActivityID, &inst.SourceHash, &inst.OwnerService, &inst.LeaseOwner, &inst.LeaseEpoch, &expiry, &inst.Runnable,
		&started, &updated, &inst.CancelReason, &inst.ParentInstanceID, &inst.ParentKey, &snapshot.CancelRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if input.Valid {
		inst.InputData = []byte(input.String)
	}
	if output.Valid {
		inst.OutputData = []byte(output.String)
	}
	if expiry.Valid {
		inst.LeaseExpiresMS = &expiry.Int64
	}
	inst.StartedAt, err = snapshotTime(started)
	if err != nil {
		return nil, err
	}
	inst.UpdatedAt, err = snapshotTime(updated)
	return snapshot, err
}

func readSnapshotHistory(ctx context.Context, conn Executor, id string) ([]*HistoryEvent, error) {
	rows, err := conn.QueryContext(ctx, `SELECT id, instance_id, activity_id, COALESCE(seq,0), COALESCE(delivered_seq,0), command,
 event_type, event_data, event_data_binary, data_type, reservation_data, created_at
 FROM workflow_history WHERE instance_id=$1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var history []*HistoryEvent
	for rows.Next() {
		event := new(HistoryEvent)
		var data, reservation sql.NullString
		var created any
		if err := rows.Scan(&event.ID, &event.InstanceID, &event.ActivityID, &event.Seq, &event.DeliveredSeq, &event.Command, &event.EventType, &data, &event.EventDataBinary, &event.DataType, &reservation, &created); err != nil {
			return nil, err
		}
		if data.Valid {
			event.EventData = []byte(data.String)
		}
		if reservation.Valid {
			event.ReservationData = []byte(reservation.String)
		}
		event.CreatedAt, err = snapshotTime(created)
		if err != nil {
			return nil, err
		}
		history = append(history, event)
	}
	return history, rows.Err()
}

func snapshotTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		return parseSQLiteTime(v)
	default:
		return time.Time{}, fmt.Errorf("unexpected snapshot timestamp type %T", value)
	}
}
