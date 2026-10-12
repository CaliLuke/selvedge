package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrInstanceNotFound indicates that an instance ID does not exist.
var ErrInstanceNotFound = errors.New("workflow instance not found")

// ErrInstanceClosed indicates that a close cannot change a terminal instance.
var ErrInstanceClosed = errors.New("workflow instance already closed")

func requestCancel(ctx context.Context, s Storage, id, reason string) (status WorkflowStatus, err error) {
	err = (fenceStore{s}).withFence(ctx, nil, func(conn Executor) error {
		err := conn.QueryRowContext(ctx, `UPDATE workflow_instances SET
   cancel_reason=CASE WHEN cancel_requested_at IS NULL THEN $1 ELSE cancel_reason END,
   cancel_requested_at=COALESCE(cancel_requested_at,CURRENT_TIMESTAMP), runnable=TRUE
   WHERE instance_id=$2 AND status='running' RETURNING status`, reason, id).Scan(&status)
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		err = conn.QueryRowContext(ctx, `SELECT status FROM workflow_instances WHERE instance_id=$1`, id).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInstanceNotFound
		}
		return err
	})
	if err == nil && status == StatusRunning {
		notifyRunnable(ctx, s, id)
	}
	return status, err
}
func isCancelRequested(ctx context.Context, s Storage, id string) (bool, error) {
	var requested bool
	err := s.Conn(ctx).QueryRowContext(ctx, `SELECT cancel_requested_at IS NOT NULL FROM workflow_instances WHERE instance_id=$1`, id).Scan(&requested)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrInstanceNotFound
	}
	return requested, err
}
func closeInstance(ctx context.Context, s Storage, f Fence, status WorkflowStatus, output []byte, event *HistoryEvent) error {
	if status != StatusCompleted && status != StatusFailed && status != StatusCancelled {
		return fmt.Errorf("invalid close status %q", status)
	}
	if event != nil && (event.InstanceID != f.InstanceID || event.Command != "close") {
		return ErrHistoryKeyConflict
	}
	parent, key, err := childParent(ctx, s, f.InstanceID)
	if err != nil {
		return err
	}
	var children []string
	var parentWoke bool
	err = (fenceStore{s}).withFence(ctx, &f, func(conn Executor) error {
		if event != nil {
			if _, err := writeHistory(ctx, conn, event, false); err != nil {
				return err
			}
		}
		result, err := conn.ExecContext(ctx, `UPDATE workflow_instances SET status=$1,output_data=$2,
   lease_owner=NULL,lease_expires_ms=NULL,runnable=FALSE,updated_at=CURRENT_TIMESTAMP
   WHERE instance_id=$3 AND status='running' AND lease_owner=$4 AND lease_epoch=$5`, status, nullableJSON(output), f.InstanceID, f.Owner, f.Epoch)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrInstanceClosed
		}
		_, err = conn.ExecContext(ctx, `UPDATE workflow_activity_tasks SET status='cancelled',updated_at=CURRENT_TIMESTAMP WHERE instance_id=$1 AND status IN ('queued','running')`, f.InstanceID)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM workflow_timer_subscriptions WHERE instance_id=$1`, f.InstanceID)
		if err != nil {
			return err
		}
		children, err = closeChildren(ctx, conn, f.InstanceID)
		if err != nil {
			return err
		}
		parentWoke, err = fillChildResult(ctx, conn, parent, key, status, output, event)
		return err
	}, parent)
	if err == nil {
		notifyClosed(ctx, s, f.InstanceID)
		for _, id := range children {
			notifyRunnable(ctx, s, id)
		}
		if parentWoke {
			notifyRunnable(ctx, s, parent)
		}
	}
	return err
}

// RequestCancel requests cancellation without acquiring an execution lease.
func (s *SQLiteStorage) RequestCancel(ctx context.Context, id, reason string) (WorkflowStatus, error) {
	return requestCancel(ctx, s, id, reason)
}

// RequestCancel requests cancellation without acquiring an execution lease.
func (s *PostgresStorage) RequestCancel(ctx context.Context, id, reason string) (WorkflowStatus, error) {
	return requestCancel(ctx, s, id, reason)
}

// IsCancelRequested reads the durable cancellation request.
func (s *SQLiteStorage) IsCancelRequested(ctx context.Context, id string) (bool, error) {
	return isCancelRequested(ctx, s, id)
}

// IsCancelRequested reads the durable cancellation request.
func (s *PostgresStorage) IsCancelRequested(ctx context.Context, id string) (bool, error) {
	return isCancelRequested(ctx, s, id)
}

// CloseInstance commits a terminal status, result, close event, and timer cleanup together.
func (s *SQLiteStorage) CloseInstance(ctx context.Context, f Fence, status WorkflowStatus, output []byte, event *HistoryEvent) error {
	return closeInstance(ctx, s, f, status, output, event)
}

// CloseInstance commits a terminal status, result, close event, and timer cleanup together.
func (s *PostgresStorage) CloseInstance(ctx context.Context, f Fence, status WorkflowStatus, output []byte, event *HistoryEvent) error {
	return closeInstance(ctx, s, f, status, output, event)
}
