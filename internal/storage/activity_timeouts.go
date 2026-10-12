package storage

import (
	"context"
	"database/sql"
	"errors"
)

func exhaustedActivitySQL(now string) string {
	return `(status='running' AND claim_expires_ms<` + now + ` AND max_attempts>0 AND failures+1>=max_attempts)`
}
func activityTimeoutKindSQL(now string) string {
	return `CASE WHEN status='queued' AND schedule_deadline_ms<` + now + ` THEN 'schedule_to_start' WHEN ` + exhaustedActivitySQL(now) + ` THEN CASE WHEN claim_expires_ms=attempt_deadline_ms THEN 'start_to_close' ELSE 'heartbeat' END WHEN status='running' AND claim_expires_ms<` + now + ` AND schedule_to_start_ms>0 AND claim_expires_ms+schedule_to_start_ms<` + now + ` THEN 'schedule_to_start' ELSE '' END`
}
func (s taskStore) FindActivityTimeouts(ctx context.Context, limit int) ([]*ActivityTimeout, error) {
	if limit <= 0 {
		limit = 100
	}
	now := s.driver.NowMillisExpr()
	kind := activityTimeoutKindSQL(now)
	rows, err := s.Conn(ctx).QueryContext(ctx, `SELECT `+taskColumns+` FROM workflow_activity_tasks WHERE (`+kind+`)<>'' ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	candidates := make([]*ActivityTimeout, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		c := &ActivityTimeout{Task: task, Kind: "schedule_to_start"}
		if task.Status == "running" && task.MaxAttempts > 0 && task.Failures+1 >= task.MaxAttempts {
			c.Exhausted = true
			c.Kind = "heartbeat"
			if task.AttemptDeadlineMS != nil && *task.AttemptDeadlineMS == *task.ClaimExpiresMS {
				c.Kind = "start_to_close"
			}
		}
		candidates = append(candidates, c)
	}
	return candidates, rows.Err()
}
func (s taskStore) TimeoutActivityTask(ctx context.Context, c *ActivityTimeout, data []byte) error {
	if c == nil || c.Task == nil {
		return ErrFenced
	}
	task := c.Task
	ctx = ContextWithFenceRejection(ctx, func(error) {})
	err := (fenceStore{s.Storage}).withFence(ctx, nil, func(conn Executor) error {
		if err := checkTaskWrite(conn.ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE instance_id=$1 AND status='running'`, task.InstanceID)); err != nil {
			return err
		}
		now := s.driver.NowMillisExpr()
		var name string
		err := conn.QueryRowContext(ctx, `UPDATE workflow_activity_tasks SET failures=failures+CASE WHEN `+exhaustedActivitySQL(now)+` THEN 1 ELSE 0 END,status='completed',updated_at=CURRENT_TIMESTAMP WHERE id=$1 AND instance_id=$2 AND activity_id=$3 AND claim_epoch=$4 AND status=$5 AND (`+activityTimeoutKindSQL(now)+`)=$6 AND (`+exhaustedActivitySQL(now)+`)=$7 RETURNING activity_name`, task.ID, task.InstanceID, task.ActivityID, task.ClaimEpoch, task.Status, c.Kind, c.Exhausted).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrFenced
		}
		if err != nil {
			return err
		}
		_, err = writeHistory(ctx, conn, &HistoryEvent{InstanceID: task.InstanceID, ActivityID: task.ActivityID, Command: "activity:" + name, EventType: HistoryActivityFailed, EventData: data, DataType: "json"}, true)
		return err
	})
	if err == nil {
		notifyRunnable(ctx, s.Storage, task.InstanceID)
	}
	return err
}
