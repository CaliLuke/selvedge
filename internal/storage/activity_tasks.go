package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CaliLuke/selvedge/internal/notify"
)

// ActivityTask is the durable input and current claim for one activity.
type ActivityTask struct {
	ID                     int64
	InstanceID, ActivityID string
	Seq                    int64
	ActivityName           string
	InputData              []byte
	Queue                  string
	Options                []byte
	MaxAttempts, Failures  int
	NotBeforeMS            int64
	LastError              []byte
	Status                 string
	Attempts               int
	ClaimOwner             string
	ClaimEpoch             int64
	ClaimExpiresMS         *int64
}

// TaskFilter restricts claims to registered names and optionally served queues.
// Nil Queues serves all queues; an empty non-nil slice serves none.
type TaskFilter struct{ Names, Queues []string }

// TaskClaim identifies one activity attempt independently of the workflow lease.
type TaskClaim struct {
	TaskID                        int64
	InstanceID, ActivityID, Owner string
	Epoch                         int64
}

// Claim returns the attempt token stored on this task.
func (t *ActivityTask) Claim() TaskClaim {
	return TaskClaim{t.ID, t.InstanceID, t.ActivityID, t.ClaimOwner, t.ClaimEpoch}
}

// ActivityTaskManager persists and fences durable activity attempts.
type ActivityTaskManager interface {
	CancelActivityAtYield(context.Context, *Fence, string, string, string, []byte) (*HistoryEvent, error)
	ReserveActivity(context.Context, *Fence, int64, string, string, *ActivityTask) (*HistoryEvent, error)
	// ClaimActivityTasks requires registered names; a nil or empty filter claims nothing.
	ClaimActivityTasks(context.Context, string, time.Duration, int, TaskFilter) ([]*ActivityTask, error)
	ClaimActivityTask(context.Context, string, string, string, time.Duration, []string) (*ActivityTask, error)
	RenewActivityTask(context.Context, TaskClaim, time.Duration, bool) error
	CompleteActivityTask(context.Context, TaskClaim, HistoryEventType, []byte) error
	ReleaseActivityTask(context.Context, TaskClaim) error
	RetryActivityTask(context.Context, TaskClaim, []byte, time.Duration) error
	NextActivityDue(context.Context, TaskFilter) (time.Duration, bool, error)
	ListActivityTasks(context.Context, string) ([]*ActivityTask, error)
}

type taskStore struct {
	Storage
	driver Driver
}

func (s taskStore) ReserveActivity(ctx context.Context, f *Fence, seq int64, key, cmd string, task *ActivityTask) (stored *HistoryEvent, err error) {
	if f == nil || f.InstanceID != task.InstanceID {
		return nil, rejectFence(ctx)
	}
	if cmd != "activity:"+task.ActivityName {
		return nil, ErrHistoryKeyConflict
	}
	err = (fenceStore{s.Storage}).withFence(ctx, f, func(conn Executor) error {
		var e error
		stored, e = writeHistory(ctx, conn, &HistoryEvent{InstanceID: task.InstanceID, ActivityID: key, Seq: seq, Command: cmd, EventType: HistoryReserved, DataType: "json"}, false)
		if e != nil || stored.EventData != nil {
			return e
		}
		options := task.Options
		if len(options) == 0 {
			options = []byte(`{}`)
		}
		_, e = conn.ExecContext(ctx, `INSERT INTO workflow_activity_tasks(instance_id,activity_id,seq,activity_name,input_data,queue,options,max_attempts,not_before_ms) VALUES($1,$2,CAST($3 AS BIGINT),$4,$5,$6,$7,$8,`+s.driver.NowMillisExpr()+`) ON CONFLICT DO NOTHING`, task.InstanceID, key, seq, task.ActivityName, string(task.InputData), task.Queue, string(options), task.MaxAttempts)
		return e
	})
	if err == nil {
		notifyActivityTasks(ctx, s.Storage)
	}
	return stored, err
}

const taskColumns = "id,instance_id,activity_id,seq,activity_name,input_data,status,attempts,COALESCE(claim_owner,''),claim_epoch,claim_expires_ms,queue,options,max_attempts,failures,not_before_ms,last_error"

func scanTask(row interface{ Scan(...any) error }) (*ActivityTask, error) {
	task := new(ActivityTask)
	err := row.Scan(&task.ID, &task.InstanceID, &task.ActivityID, &task.Seq, &task.ActivityName, &task.InputData, &task.Status, &task.Attempts, &task.ClaimOwner, &task.ClaimEpoch, &task.ClaimExpiresMS, &task.Queue, &task.Options, &task.MaxAttempts, &task.Failures, &task.NotBeforeMS, &task.LastError)
	return task, err
}

func (s taskStore) claim(ctx context.Context, owner string, ttl time.Duration, limit int, filter string, args []any) ([]*ActivityTask, error) {
	if owner == "" || leaseMillis(ttl) <= 0 {
		return nil, fmt.Errorf("activity claim requires owner and positive TTL")
	}
	if limit <= 0 {
		limit = 100
	}
	now := s.driver.NowMillisExpr()
	eligible := `((status='queued' AND not_before_ms <= ` + now + `) OR (status='running' AND claim_expires_ms < ` + now + ` AND (max_attempts=0 OR failures+1 < max_attempts)))`
	// The outer predicate is rechecked after concurrent UPDATEs acquire the row lock.
	query := `UPDATE workflow_activity_tasks SET failures=failures+CASE WHEN status='running' THEN 1 ELSE 0 END,status='running',claim_owner=$1,claim_epoch=claim_epoch+1,attempts=attempts+1,claim_expires_ms=` + now + `+$2,updated_at=CURRENT_TIMESTAMP WHERE id IN (SELECT id FROM workflow_activity_tasks WHERE ` + eligible + filter + ` ORDER BY id LIMIT $3) AND ` + eligible + ` RETURNING ` + taskColumns
	params := make([]any, 0, 3+len(args))
	params = append(params, owner, leaseMillis(ttl), limit)
	params = append(params, args...)
	rows, err := s.Conn(ctx).QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tasks []*ActivityTask
	for rows.Next() {
		task, e := scanTask(rows)
		if e != nil {
			return nil, e
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// taskFilterSQL appends parameterized name and queue restrictions.
func taskFilterSQL(filter TaskFilter, first int) (clause string, args []any) {
	if len(filter.Names) == 0 {
		return " AND FALSE", nil
	}

	for _, entry := range []struct {
		column string
		values []string
	}{{"activity_name", filter.Names}, {"queue", filter.Queues}} {
		if entry.values == nil {
			continue
		}
		if len(entry.values) == 0 {
			clause += " AND FALSE"
			continue
		}
		slots := make([]string, len(entry.values))
		for i, value := range entry.values {
			slots[i] = fmt.Sprintf("$%d", first+len(args))
			args = append(args, value)
		}
		clause += " AND " + entry.column + " IN (" + strings.Join(slots, ",") + ")"
	}
	return clause, args
}
func (s taskStore) ClaimActivityTasks(ctx context.Context, owner string, ttl time.Duration, limit int, filter TaskFilter) ([]*ActivityTask, error) {
	query, args := taskFilterSQL(filter, 4)
	return s.claim(ctx, owner, ttl, limit, query, args)
}
func (s taskStore) ClaimActivityTask(ctx context.Context, id, key, owner string, ttl time.Duration, queues []string) (*ActivityTask, error) {
	// The issuing workflow registers its definition before an inline Get.
	query := " AND instance_id=$4 AND activity_id=$5"
	args := []any{id, key}
	if queues != nil {
		if len(queues) == 0 {
			return nil, nil
		}
		slots := make([]string, len(queues))
		for i, q := range queues {
			slots[i] = fmt.Sprintf("$%d", i+6)
			args = append(args, q)
		}
		query += " AND queue IN (" + strings.Join(slots, ",") + ")"
	}
	tasks, err := s.claim(ctx, owner, ttl, 1, query, args)
	if err != nil || len(tasks) == 0 {
		return nil, err
	}
	return tasks[0], nil
}

const claimWhere = ` WHERE id=$1 AND instance_id=$2 AND activity_id=$3 AND claim_owner=$4 AND claim_epoch=$5 AND status='running'`

func claimArgs(c TaskClaim) []any {
	return []any{c.TaskID, c.InstanceID, c.ActivityID, c.Owner, c.Epoch}
}
func checkTaskWrite(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrFenced
	}
	return nil
}
func (s taskStore) RenewActivityTask(ctx context.Context, c TaskClaim, ttl time.Duration, extend bool) error {
	if leaseMillis(ttl) <= 0 {
		return fmt.Errorf("activity claim TTL must be positive")
	}
	now := s.driver.NowMillisExpr()
	if !extend {
		return checkTaskWrite(s.Conn(ctx).ExecContext(ctx, `UPDATE workflow_activity_tasks SET claim_epoch=claim_epoch`+claimWhere+` AND claim_expires_ms >= `+now, claimArgs(c)...))
	}
	args := append(claimArgs(c), leaseMillis(ttl))
	return checkTaskWrite(s.Conn(ctx).ExecContext(ctx, `UPDATE workflow_activity_tasks SET claim_expires_ms=`+now+`+$6,updated_at=CURRENT_TIMESTAMP`+claimWhere+` AND claim_expires_ms >= `+now, args...))
}
func (s taskStore) ReleaseActivityTask(ctx context.Context, c TaskClaim) error {
	err := checkTaskWrite(s.Conn(ctx).ExecContext(ctx, `UPDATE workflow_activity_tasks SET status='queued',claim_owner=NULL,claim_expires_ms=NULL,not_before_ms=`+s.driver.NowMillisExpr()+`,updated_at=CURRENT_TIMESTAMP`+claimWhere, claimArgs(c)...))
	if err == nil {
		notifyActivityTasks(ctx, s.Storage)
	}
	return err
}
func (s taskStore) CompleteActivityTask(ctx context.Context, c TaskClaim, event HistoryEventType, data []byte) error {
	// Attempt fencing is independent of the workflow's fence rejection callback.
	ctx = ContextWithFenceRejection(ctx, func(error) {})
	if event != HistoryActivityCompleted && event != HistoryActivityFailed {
		return ErrHistoryKeyConflict
	}
	err := (fenceStore{s.Storage}).withFence(ctx, nil, func(conn Executor) error {
		// All writes touching both instance and tasks lock the instance first,
		// as ReserveActivity and CloseInstance do through their workflow fence.
		// A rejected claim rolls back this wake along with the rest of the write.
		_, err := conn.ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE instance_id=$1 AND status='running'`, c.InstanceID)
		if err != nil {
			return err
		}
		var name string
		err = conn.QueryRowContext(ctx, `UPDATE workflow_activity_tasks SET status='completed',updated_at=CURRENT_TIMESTAMP`+claimWhere+` RETURNING activity_name`, claimArgs(c)...).Scan(&name)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrFenced
		}
		if err != nil {
			return err
		}
		_, err = writeHistory(ctx, conn, &HistoryEvent{InstanceID: c.InstanceID, ActivityID: c.ActivityID, Command: "activity:" + name, EventType: event, EventData: data, DataType: "json"}, true)
		return err
	})
	if err == nil {
		notifyRunnable(ctx, s.Storage, c.InstanceID)
	}
	return err
}

func notifyActivityTasks(ctx context.Context, s Storage) {
	pg, ok := s.(*PostgresStorage)
	if !ok || !pg.notifyEnabled {
		return
	}
	send := func() error {
		_, err := pg.db.ExecContext(ctx, "SELECT pg_notify($1, $2)", string(notify.ChannelActivityTask), "")
		return err
	}
	if s.InTransaction(ctx) {
		_ = s.RegisterPostCommitCallback(ctx, send)
	} else {
		_ = send()
	}
}

// ReserveActivity delegates to the durable task store.
func (s *SQLiteStorage) ReserveActivity(ctx context.Context, f *Fence, seq int64, key, cmd string, task *ActivityTask) (*HistoryEvent, error) {
	return (taskStore{s, s.driver}).ReserveActivity(ctx, f, seq, key, cmd, task)
}

// ClaimActivityTasks delegates to the durable task store.
func (s *SQLiteStorage) ClaimActivityTasks(ctx context.Context, owner string, ttl time.Duration, limit int, filter TaskFilter) ([]*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTasks(ctx, owner, ttl, limit, filter)
}

// ClaimActivityTask delegates to the durable task store.
func (s *SQLiteStorage) ClaimActivityTask(ctx context.Context, id, key, owner string, ttl time.Duration, queues []string) (*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTask(ctx, id, key, owner, ttl, queues)
}

// RenewActivityTask delegates to the durable task store.
func (s *SQLiteStorage) RenewActivityTask(ctx context.Context, claim TaskClaim, ttl time.Duration, extend bool) error {
	return (taskStore{s, s.driver}).RenewActivityTask(ctx, claim, ttl, extend)
}

// CompleteActivityTask delegates to the durable task store.
func (s *SQLiteStorage) CompleteActivityTask(ctx context.Context, claim TaskClaim, event HistoryEventType, data []byte) error {
	return (taskStore{s, s.driver}).CompleteActivityTask(ctx, claim, event, data)
}

// ReleaseActivityTask delegates to the durable task store.
func (s *SQLiteStorage) ReleaseActivityTask(ctx context.Context, claim TaskClaim) error {
	return (taskStore{s, s.driver}).ReleaseActivityTask(ctx, claim)
}

// ReserveActivity delegates to the durable task store.
func (s *PostgresStorage) ReserveActivity(ctx context.Context, f *Fence, seq int64, key, cmd string, task *ActivityTask) (*HistoryEvent, error) {
	return (taskStore{s, s.driver}).ReserveActivity(ctx, f, seq, key, cmd, task)
}

// ClaimActivityTasks delegates to the durable task store.
func (s *PostgresStorage) ClaimActivityTasks(ctx context.Context, owner string, ttl time.Duration, limit int, filter TaskFilter) ([]*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTasks(ctx, owner, ttl, limit, filter)
}

// ClaimActivityTask delegates to the durable task store.
func (s *PostgresStorage) ClaimActivityTask(ctx context.Context, id, key, owner string, ttl time.Duration, queues []string) (*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTask(ctx, id, key, owner, ttl, queues)
}

// RenewActivityTask delegates to the durable task store.
func (s *PostgresStorage) RenewActivityTask(ctx context.Context, claim TaskClaim, ttl time.Duration, extend bool) error {
	return (taskStore{s, s.driver}).RenewActivityTask(ctx, claim, ttl, extend)
}

// CompleteActivityTask delegates to the durable task store.
func (s *PostgresStorage) CompleteActivityTask(ctx context.Context, claim TaskClaim, event HistoryEventType, data []byte) error {
	return (taskStore{s, s.driver}).CompleteActivityTask(ctx, claim, event, data)
}

// ReleaseActivityTask delegates to the durable task store.
func (s *PostgresStorage) ReleaseActivityTask(ctx context.Context, claim TaskClaim) error {
	return (taskStore{s, s.driver}).ReleaseActivityTask(ctx, claim)
}

// CancelActivityAtYield records cancellation and cancels the associated task atomically.
func (s *SQLiteStorage) CancelActivityAtYield(ctx context.Context, f *Fence, id, key, cmd string, data []byte) (*HistoryEvent, error) {
	return (taskStore{s, s.driver}).CancelActivityAtYield(ctx, f, id, key, cmd, data)
}

// CancelActivityAtYield records cancellation and cancels the associated task atomically.
func (s *PostgresStorage) CancelActivityAtYield(ctx context.Context, f *Fence, id, key, cmd string, data []byte) (*HistoryEvent, error) {
	return (taskStore{s, s.driver}).CancelActivityAtYield(ctx, f, id, key, cmd, data)
}

func (s taskStore) CancelActivityAtYield(ctx context.Context, f *Fence, id, key, cmd string, data []byte) (stored *HistoryEvent, err error) {
	if f == nil || f.InstanceID != id {
		return nil, rejectFence(ctx)
	}
	// withFence locks the instance before either history or tasks, matching completion and close.
	err = (fenceStore{s.Storage}).withFence(ctx, f, func(conn Executor) error {
		var e error
		stored, e = writeHistory(ctx, conn, &HistoryEvent{InstanceID: id, ActivityID: key, Command: cmd, EventType: HistoryActivityCancelled, EventData: data, DataType: "json"}, true)
		if e != nil {
			return e
		}
		// A previously stored result wins without changing its task.
		if stored.EventType != HistoryActivityCancelled {
			return nil
		}
		_, e = conn.ExecContext(ctx, `UPDATE workflow_activity_tasks SET status='cancelled',updated_at=CURRENT_TIMESTAMP WHERE instance_id=$1 AND activity_id=$2 AND status IN ('queued','running')`, id, key)
		return e
	})
	return stored, err
}

// RetryActivityTask durably schedules another attempt.
func (s *SQLiteStorage) RetryActivityTask(ctx context.Context, c TaskClaim, data []byte, delay time.Duration) error {
	return (taskStore{s, s.driver}).RetryActivityTask(ctx, c, data, delay)
}

// NextActivityDue returns the delay until the next eligible queued task.
func (s *SQLiteStorage) NextActivityDue(ctx context.Context, f TaskFilter) (time.Duration, bool, error) {
	return (taskStore{s, s.driver}).NextActivityDue(ctx, f)
}

// ListActivityTasks returns tasks in reservation order.
func (s *SQLiteStorage) ListActivityTasks(ctx context.Context, id string) ([]*ActivityTask, error) {
	return (taskStore{s, s.driver}).ListActivityTasks(ctx, id)
}

// RetryActivityTask durably schedules another attempt.
func (s *PostgresStorage) RetryActivityTask(ctx context.Context, c TaskClaim, data []byte, delay time.Duration) error {
	return (taskStore{s, s.driver}).RetryActivityTask(ctx, c, data, delay)
}

// NextActivityDue returns the delay until the next eligible queued task.
func (s *PostgresStorage) NextActivityDue(ctx context.Context, f TaskFilter) (time.Duration, bool, error) {
	return (taskStore{s, s.driver}).NextActivityDue(ctx, f)
}

// ListActivityTasks returns tasks in reservation order.
func (s *PostgresStorage) ListActivityTasks(ctx context.Context, id string) ([]*ActivityTask, error) {
	return (taskStore{s, s.driver}).ListActivityTasks(ctx, id)
}

func (s taskStore) RetryActivityTask(ctx context.Context, c TaskClaim, data []byte, delay time.Duration) error {
	if delay < 0 {
		return fmt.Errorf("activity retry delay must not be negative")
	}
	args := append(claimArgs(c), string(data), leaseMillis(delay))
	err := checkTaskWrite(s.Conn(ctx).ExecContext(ctx, `UPDATE workflow_activity_tasks SET status='queued',claim_owner=NULL,claim_expires_ms=NULL,failures=failures+1,last_error=$6,not_before_ms=`+s.driver.NowMillisExpr()+`+$7,updated_at=CURRENT_TIMESTAMP`+claimWhere, args...))
	if err == nil {
		notifyActivityTasks(ctx, s.Storage)
	}
	return err
}
func (s taskStore) NextActivityDue(ctx context.Context, f TaskFilter) (time.Duration, bool, error) {
	filter, args := taskFilterSQL(f, 1)
	var delta sql.NullInt64
	err := s.Conn(ctx).QueryRowContext(ctx, `SELECT MIN(not_before_ms)-`+s.driver.NowMillisExpr()+` FROM workflow_activity_tasks WHERE status='queued'`+filter, args...).Scan(&delta)
	if err != nil || !delta.Valid {
		return 0, false, err
	}
	if delta.Int64 < 0 {
		delta.Int64 = 0
	}
	return time.Duration(delta.Int64) * time.Millisecond, true, nil
}
func (s taskStore) ListActivityTasks(ctx context.Context, id string) ([]*ActivityTask, error) {
	rows, err := s.Conn(ctx).QueryContext(ctx, `SELECT `+taskColumns+` FROM workflow_activity_tasks WHERE instance_id=$1 ORDER BY seq`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	tasks := make([]*ActivityTask, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}
