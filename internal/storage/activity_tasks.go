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
	Status                 string
	Attempts               int
	ClaimOwner             string
	ClaimEpoch             int64
	ClaimExpiresMS         *int64
}

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
	ReserveActivity(context.Context, *Fence, int64, string, string, *ActivityTask) (*HistoryEvent, error)
	// ClaimActivityTasks requires registered names; a nil or empty filter claims nothing.
	ClaimActivityTasks(context.Context, string, time.Duration, int, ...string) ([]*ActivityTask, error)
	ClaimActivityTask(context.Context, string, string, string, time.Duration) (*ActivityTask, error)
	RenewActivityTask(context.Context, TaskClaim, time.Duration) error
	CompleteActivityTask(context.Context, TaskClaim, HistoryEventType, []byte) error
	ReleaseActivityTask(context.Context, TaskClaim) error
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
		_, e = conn.ExecContext(ctx, `INSERT INTO workflow_activity_tasks(instance_id,activity_id,seq,activity_name,input_data) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, task.InstanceID, key, seq, task.ActivityName, string(task.InputData))
		return e
	})
	if err == nil {
		notifyActivityTasks(ctx, s.Storage)
	}
	return stored, err
}

const taskColumns = "id,instance_id,activity_id,seq,activity_name,input_data,status,attempts,claim_owner,claim_epoch,claim_expires_ms"

func scanTask(row interface{ Scan(...any) error }) (*ActivityTask, error) {
	task := new(ActivityTask)
	err := row.Scan(&task.ID, &task.InstanceID, &task.ActivityID, &task.Seq, &task.ActivityName, &task.InputData, &task.Status, &task.Attempts, &task.ClaimOwner, &task.ClaimEpoch, &task.ClaimExpiresMS)
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
	eligible := `(status='queued' OR (status='running' AND claim_expires_ms < ` + now + `))`
	// The outer predicate is rechecked after concurrent UPDATEs acquire the row lock.
	query := `UPDATE workflow_activity_tasks SET status='running',claim_owner=$1,claim_epoch=claim_epoch+1,attempts=attempts+1,claim_expires_ms=` + now + `+$2,updated_at=CURRENT_TIMESTAMP WHERE id IN (SELECT id FROM workflow_activity_tasks WHERE ` + eligible + filter + ` ORDER BY id LIMIT $3) AND ` + eligible + ` RETURNING ` + taskColumns
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

func (s taskStore) ClaimActivityTasks(ctx context.Context, owner string, ttl time.Duration, limit int, names ...string) ([]*ActivityTask, error) {
	if len(names) == 0 {
		return nil, nil
	}
	args := make([]any, len(names))
	slots := make([]string, len(names))
	for i, name := range names {
		args[i] = name
		slots[i] = fmt.Sprintf("$%d", i+4)
	}
	return s.claim(ctx, owner, ttl, limit, " AND activity_name IN ("+strings.Join(slots, ",")+")", args)
}
func (s taskStore) ClaimActivityTask(ctx context.Context, id, key, owner string, ttl time.Duration) (*ActivityTask, error) {
	tasks, err := s.claim(ctx, owner, ttl, 1, " AND instance_id=$4 AND activity_id=$5", []any{id, key})
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
func (s taskStore) RenewActivityTask(ctx context.Context, c TaskClaim, ttl time.Duration) error {
	if leaseMillis(ttl) <= 0 {
		return fmt.Errorf("activity claim TTL must be positive")
	}
	now := s.driver.NowMillisExpr()
	args := append(claimArgs(c), leaseMillis(ttl))
	return checkTaskWrite(s.Conn(ctx).ExecContext(ctx, `UPDATE workflow_activity_tasks SET claim_expires_ms=`+now+`+$6,updated_at=CURRENT_TIMESTAMP`+claimWhere+` AND claim_expires_ms >= `+now, args...))
}
func (s taskStore) ReleaseActivityTask(ctx context.Context, c TaskClaim) error {
	err := checkTaskWrite(s.Conn(ctx).ExecContext(ctx, `UPDATE workflow_activity_tasks SET status='queued',claim_owner=NULL,claim_expires_ms=NULL,updated_at=CURRENT_TIMESTAMP`+claimWhere, claimArgs(c)...))
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
func (s *SQLiteStorage) ClaimActivityTasks(ctx context.Context, owner string, ttl time.Duration, limit int, names ...string) ([]*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTasks(ctx, owner, ttl, limit, names...)
}

// ClaimActivityTask delegates to the durable task store.
func (s *SQLiteStorage) ClaimActivityTask(ctx context.Context, id, key, owner string, ttl time.Duration) (*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTask(ctx, id, key, owner, ttl)
}

// RenewActivityTask delegates to the durable task store.
func (s *SQLiteStorage) RenewActivityTask(ctx context.Context, claim TaskClaim, ttl time.Duration) error {
	return (taskStore{s, s.driver}).RenewActivityTask(ctx, claim, ttl)
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
func (s *PostgresStorage) ClaimActivityTasks(ctx context.Context, owner string, ttl time.Duration, limit int, names ...string) ([]*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTasks(ctx, owner, ttl, limit, names...)
}

// ClaimActivityTask delegates to the durable task store.
func (s *PostgresStorage) ClaimActivityTask(ctx context.Context, id, key, owner string, ttl time.Duration) (*ActivityTask, error) {
	return (taskStore{s, s.driver}).ClaimActivityTask(ctx, id, key, owner, ttl)
}

// RenewActivityTask delegates to the durable task store.
func (s *PostgresStorage) RenewActivityTask(ctx context.Context, claim TaskClaim, ttl time.Duration) error {
	return (taskStore{s, s.driver}).RenewActivityTask(ctx, claim, ttl)
}

// CompleteActivityTask delegates to the durable task store.
func (s *PostgresStorage) CompleteActivityTask(ctx context.Context, claim TaskClaim, event HistoryEventType, data []byte) error {
	return (taskStore{s, s.driver}).CompleteActivityTask(ctx, claim, event, data)
}

// ReleaseActivityTask delegates to the durable task store.
func (s *PostgresStorage) ReleaseActivityTask(ctx context.Context, claim TaskClaim) error {
	return (taskStore{s, s.driver}).ReleaseActivityTask(ctx, claim)
}
