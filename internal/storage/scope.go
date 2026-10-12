package storage

import (
	"context"
	"errors"
)

// ScopeTarget identifies an already issued activity, timer, or child.
type ScopeTarget struct{ Key, Command, Kind string }
type ScopeCancel struct{ Targets []ScopeTarget }

// cancelScopeTargets runs only inside commitObservation's instance-row fence.
// Filled outcomes retain their value and delivery position; only new cancel
// observations are delivered at the cancel command's position.
func cancelScopeTargets(ctx context.Context, conn Executor, id string, seq int64, c *ScopeCancel) error {
	const data = `{"v":1,"kind":"canceled","message":"context canceled"}`
	for _, target := range c.Targets {
		var typ HistoryEventType
		switch target.Kind {
		case "activity":
			typ = HistoryActivityCancelled
		case "timer":
			typ = HistoryTimerCancelled
		case "child":
			typ = HistoryChildCancelled
		default:
			return errors.New("invalid scope target kind")
		}
		row, err := readHistory(ctx, conn, id, target.Key)
		if err != nil {
			return err
		}
		if row == nil || row.Command != target.Command || row.Seq >= seq {
			return ErrHistoryKeyConflict
		}
		if _, err = conn.ExecContext(ctx, `UPDATE workflow_history SET event_type=$1,event_data=$2,data_type='json',delivered_seq=CAST($3 AS BIGINT) WHERE instance_id=$4 AND activity_id=$5 AND command=$6 AND event_data IS NULL`, typ, data, seq, id, target.Key, target.Command); err != nil {
			return err
		}
		switch target.Kind {
		case "activity":
			_, err = conn.ExecContext(ctx, `UPDATE workflow_activity_tasks SET status='cancelled',updated_at=CURRENT_TIMESTAMP WHERE instance_id=$1 AND activity_id=$2 AND status IN ('queued','running')`, id, target.Key)
		case "child":
			err = requestScopeChildCancel(ctx, conn, id, target.Key)
		case "timer":
			_, err = conn.ExecContext(ctx, `DELETE FROM workflow_timer_subscriptions WHERE instance_id=$1 AND timer_id=$2`, id, target.Key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// The parent fence is held before this child write. The unique parent/key link
// excludes foreign IDs and cancelled-at-issue handles with no child instance.
func requestScopeChildCancel(ctx context.Context, conn Executor, parent, key string) error {
	_, err := conn.ExecContext(ctx, `UPDATE workflow_instances SET
    cancel_reason=CASE WHEN cancel_requested_at IS NULL THEN 'parent cancelled child' ELSE cancel_reason END,
    cancel_requested_at=COALESCE(cancel_requested_at,CURRENT_TIMESTAMP),runnable=TRUE
    WHERE parent_instance_id=$1 AND parent_key=$2 AND status='running'`, parent, key)
	return err
}
