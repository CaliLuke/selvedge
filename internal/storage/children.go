package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

const (
	HistoryChildCompleted   HistoryEventType = "ChildCompleted"
	HistoryChildFailed      HistoryEventType = "ChildFailed"
	HistoryChildCancelled   HistoryEventType = "ChildCancelled"
	HistoryChildStartFailed HistoryEventType = "ChildStartFailed"
)

// ChildReservation identifies the immutable child belonging to a command.
type ChildReservation struct {
	ChildID  string `json:"child_id"`
	Workflow string `json:"workflow"`
}

// ChildCancel records an explicit cancel at Seq, or a Get observation at zero.
// Record is the explicit child_cancel command; EventData is the cancellation envelope.
type ChildCancel struct {
	Seq                             int64
	Record                          *HistoryEvent
	ChildKey, ChildCommand, ChildID string
	EventData                       []byte
}

// ChildManager atomically connects child instances to their parent's history.
type ChildManager interface {
	StartChild(context.Context, *Fence, int64, string, string, *WorkflowInstance) (*HistoryEvent, error)
	CancelChild(context.Context, *Fence, *ChildCancel) (*HistoryEvent, error)
}

func startChild(ctx context.Context, s Storage, f *Fence, seq int64, key, cmd string, child *WorkflowInstance) (stored *HistoryEvent, err error) {
	if f == nil {
		return nil, rejectFence(ctx)
	}
	if child == nil || cmd != "child:"+child.WorkflowName {
		return nil, ErrHistoryKeyConflict
	}
	reservation, _ := json.Marshal(ChildReservation{ChildID: child.InstanceID, Workflow: child.WorkflowName})
	err = (fenceStore{s}).withFence(ctx, f, func(conn Executor) error {
		// A matching repeat never re-creates a child, even after a lost acknowledgement.
		existing, e := readHistory(ctx, conn, f.InstanceID, key)
		if e != nil {
			return e
		}
		stored, e = writeHistory(ctx, conn, &HistoryEvent{InstanceID: f.InstanceID, ActivityID: key, Seq: seq, Command: cmd, EventType: HistoryReserved, ReservationData: reservation, DataType: "json"}, false)
		if e != nil {
			return e
		}
		var recorded ChildReservation
		if e := json.Unmarshal(stored.ReservationData, &recorded); e != nil {
			return e
		}
		if recorded.ChildID != child.InstanceID || recorded.Workflow != child.WorkflowName {
			return ErrHistoryKeyConflict
		}
		if existing != nil {
			return nil
		}
		if _, e = conn.ExecContext(ctx, `INSERT INTO workflow_definitions(workflow_name,source_hash,source_code) VALUES($1,$2,'') ON CONFLICT DO NOTHING`, child.WorkflowName, child.SourceHash); e != nil {
			return e
		}
		framework := child.Framework
		if framework == "" {
			framework = "go"
		}
		_, e = conn.ExecContext(ctx, `INSERT INTO workflow_instances(instance_id,workflow_name,source_hash,owner_service,framework,status,input_data,parent_instance_id,parent_key,runnable)
   VALUES($1,$2,$3,$4,$5,'running',$6,$7,$8,TRUE) ON CONFLICT DO NOTHING`, child.InstanceID, child.WorkflowName, child.SourceHash, child.OwnerService, framework, string(child.InputData), f.InstanceID, key)
		if e != nil {
			return e
		}
		var parent, parentKey sql.NullString
		e = conn.QueryRowContext(ctx, `SELECT parent_instance_id,parent_key FROM workflow_instances WHERE instance_id=$1`, child.InstanceID).Scan(&parent, &parentKey)
		if e != nil {
			return e
		}
		if parent.String != f.InstanceID || parentKey.String != key {
			// The storage layer owns this sentinel; keep its envelope compatible with the root codec.
			data := []byte(`{"v":1,"kind":"workflow_already_started","message":"workflow already started"}`)
			_, e = conn.ExecContext(ctx, `UPDATE workflow_history SET event_type=$1,event_data=$2,delivered_seq=CAST($3 AS BIGINT) WHERE instance_id=$4 AND activity_id=$5 AND event_data IS NULL`, HistoryChildStartFailed, string(data), seq, f.InstanceID, key)
			if e != nil {
				return e
			}
			stored, e = readHistory(ctx, conn, f.InstanceID, key)
		}
		return e
	})
	if err == nil && stored.EventType != HistoryChildStartFailed {
		notifyRunnable(ctx, s, child.InstanceID)
	}
	return stored, err
}

func cancelChild(ctx context.Context, s Storage, f *Fence, c *ChildCancel) (stored *HistoryEvent, err error) {
	if f == nil {
		return nil, rejectFence(ctx)
	}
	if c == nil || c.EventData == nil || c.Seq < 0 {
		return nil, ErrHistoryKeyConflict
	}
	if c.Seq > 0 && (c.Record == nil || c.Record.Seq != c.Seq || c.Record.InstanceID != f.InstanceID || c.Record.Command != "child_cancel" || c.Record.EventData == nil) {
		return nil, ErrHistoryKeyConflict
	}
	err = (fenceStore{s}).withFence(ctx, f, func(conn Executor) error {
		var e error
		stored, e = readHistory(ctx, conn, f.InstanceID, c.ChildKey)
		if e != nil {
			return e
		}
		if stored == nil || stored.Command != c.ChildCommand {
			return ErrHistoryKeyConflict
		}
		var reservation ChildReservation
		if e := json.Unmarshal(stored.ReservationData, &reservation); e != nil {
			return e
		}
		if reservation.ChildID != c.ChildID {
			return ErrHistoryKeyConflict
		}
		if c.Seq > 0 {
			if _, e = writeHistory(ctx, conn, c.Record, false); e != nil {
				return e
			}
		} else if stored.EventData != nil {
			return nil
		} // A result beats a cancel observed by Get.
		// A foreign ID whose start failed must never receive this parent's cancel.
		if stored.EventType != HistoryChildStartFailed {
			_, e = conn.ExecContext(ctx, `UPDATE workflow_instances SET
    cancel_reason=CASE WHEN cancel_requested_at IS NULL THEN 'parent cancelled child' ELSE cancel_reason END,
    cancel_requested_at=COALESCE(cancel_requested_at,CURRENT_TIMESTAMP),runnable=TRUE
    WHERE instance_id=$1 AND parent_instance_id=$2 AND parent_key=$3 AND status='running'`, c.ChildID, f.InstanceID, c.ChildKey)
			if e != nil {
				return e
			}
		}
		_, e = conn.ExecContext(ctx, `UPDATE workflow_history SET event_type=$1,event_data=$2,data_type='json',delivered_seq=NULLIF(CAST($3 AS BIGINT),0)
   WHERE instance_id=$4 AND activity_id=$5 AND command=$6 AND event_data IS NULL`, HistoryChildCancelled, string(c.EventData), c.Seq, f.InstanceID, c.ChildKey, c.ChildCommand)
		if e != nil {
			return e
		}
		stored, e = readHistory(ctx, conn, f.InstanceID, c.ChildKey)
		return e
	})
	if err == nil {
		notifyRunnable(ctx, s, c.ChildID)
	}
	return stored, err
}

// StartChild reserves the parent command and creates its child in one fenced transaction.
func (s *SQLiteStorage) StartChild(ctx context.Context, f *Fence, seq int64, key, cmd string, child *WorkflowInstance) (*HistoryEvent, error) {
	return startChild(ctx, s, f, seq, key, cmd, child)
}

// StartChild reserves the parent command and creates its child in one fenced transaction.
func (s *PostgresStorage) StartChild(ctx context.Context, f *Fence, seq int64, key, cmd string, child *WorkflowInstance) (*HistoryEvent, error) {
	return startChild(ctx, s, f, seq, key, cmd, child)
}

// CancelChild resolves a handle and requests cancellation atomically.
func (s *SQLiteStorage) CancelChild(ctx context.Context, f *Fence, c *ChildCancel) (*HistoryEvent, error) {
	return cancelChild(ctx, s, f, c)
}

// CancelChild resolves a handle and requests cancellation atomically.
func (s *PostgresStorage) CancelChild(ctx context.Context, f *Fence, c *ChildCancel) (*HistoryEvent, error) {
	return cancelChild(ctx, s, f, c)
}

// childParent reads immutable linkage before acquiring transaction locks.
func childParent(ctx context.Context, s Storage, id string) (parent, key string, err error) {
	err = s.Conn(ctx).QueryRowContext(ctx, `SELECT COALESCE(parent_instance_id,''),COALESCE(parent_key,'') FROM workflow_instances WHERE instance_id=$1`, id).Scan(&parent, &key)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrInstanceNotFound
	}
	return
}

func closeChildren(ctx context.Context, conn Executor, id string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `UPDATE workflow_instances SET cancel_requested_at=COALESCE(cancel_requested_at,CURRENT_TIMESTAMP),
  cancel_reason=COALESCE(cancel_reason,'parent closed'),runnable=TRUE WHERE parent_instance_id=$1 AND status='running' RETURNING instance_id`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func fillChildResult(ctx context.Context, conn Executor, parent, key string, status WorkflowStatus, output []byte, event *HistoryEvent) (bool, error) {
	if parent == "" {
		return false, nil
	}
	typ, data := HistoryChildCompleted, output
	if status != StatusCompleted {
		typ = HistoryChildFailed
		if status == StatusCancelled {
			typ = HistoryChildCancelled
		}
		if event == nil {
			return false, ErrHistoryKeyConflict
		}
		data = event.EventData
	}
	if data == nil {
		data = []byte(`null`)
	}
	result, err := conn.ExecContext(ctx, `UPDATE workflow_history SET event_type=$1,event_data=$2,data_type='json'
  WHERE instance_id=$3 AND activity_id=$4 AND event_data IS NULL
  AND EXISTS(SELECT 1 FROM workflow_instances WHERE instance_id=$3 AND status='running')`, typ, string(data), parent, key)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	_, err = conn.ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE instance_id=$1 AND status='running'`, parent)
	return err == nil, err
}
