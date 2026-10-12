package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidSignalChannel = errors.New("signal channel must contain 1 to 255 bytes")
	ErrSignalNotAvailable   = errors.New("signal is absent, consumed, or belongs to another instance or channel")
)

// Status errors retain identity across the storage and public APIs.
var (
	ErrWorkflowAlreadyCompleted = errors.New("workflow already completed")
	ErrWorkflowAlreadyFailed    = errors.New("workflow already failed")
	ErrWorkflowAlreadyCancelled = errors.New("workflow already cancelled")
)

// InstanceClosedError reports the status that prevented signal ingress.
type InstanceClosedError struct {
	InstanceID string
	Status     WorkflowStatus
}

func (e *InstanceClosedError) Error() string {
	return fmt.Sprintf("workflow instance %q is closed (status %s)", e.InstanceID, e.Status)
}

func (e *InstanceClosedError) Unwrap() error { return ErrInstanceClosed }

// Is also matches the sentinel for this terminal status.
func (e *InstanceClosedError) Is(target error) bool {
	switch e.Status {
	case StatusCompleted:
		return target == ErrWorkflowAlreadyCompleted
	case StatusFailed:
		return target == ErrWorkflowAlreadyFailed
	case StatusCancelled:
		return target == ErrWorkflowAlreadyCancelled
	}
	return false
}

// Signal is one durable inbox entry. An empty ConsumedKey denotes a pending entry.
type Signal struct {
	ID                        int64
	InstanceID, Channel       string
	Payload                   []byte
	SentAt                    time.Time
	ConsumedKey               string
	DeliveredSeq, ConsumedSeq int64
}

// SignalManager persists and consumes the per-instance signal inbox.
type SignalManager interface {
	AppendSignal(ctx context.Context, instanceID, channel string, payload []byte) (int64, error)
	// NextSignal returns nil, nil when this channel has no unconsumed signal.
	NextSignal(ctx context.Context, instanceID, channel string) (*Signal, error)
}

// Both dialects support numbered parameters and RETURNING. Only ingress locking
// differs: PostgreSQL locks the instance before checking status; SQLite's insert
// takes the writer lock before reading status.
type signalStore struct {
	Storage
	driver   Driver
	postgres bool
}

// signalTransaction reuses a guarded connection without starting another transaction.
type signalTransaction struct {
	Storage
	executor Executor
}

func (s signalTransaction) Conn(context.Context) Executor      { return s.executor }
func (s signalTransaction) InTransaction(context.Context) bool { return true }

func (s signalStore) append(ctx context.Context, instanceID, channel string, payload []byte) (int64, error) {
	if channel == "" || len(channel) > 255 {
		return 0, ErrInvalidSignalChannel
	}
	var id int64
	err := s.transaction(ctx, func(ctx context.Context, conn Executor) error {
		if s.postgres {
			var status WorkflowStatus
			err := conn.QueryRowContext(ctx, "SELECT status FROM workflow_instances WHERE instance_id = $1 FOR UPDATE", instanceID).Scan(&status)
			if err := signalInstanceError(instanceID, status, err); err != nil {
				return err
			}
		}
		err := conn.QueryRowContext(ctx, `
			INSERT INTO workflow_signals (instance_id, channel, payload)
			SELECT instance_id, $2, $3 FROM workflow_instances
			WHERE instance_id = $1 AND status = 'running' RETURNING id
		`, instanceID, channel, string(payload)).Scan(&id)
		if err == nil {
			return (leaseStore{Storage: signalTransaction{s.Storage, conn}}).MarkRunnable(ctx, instanceID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var status WorkflowStatus
		err = conn.QueryRowContext(ctx, "SELECT status FROM workflow_instances WHERE instance_id = $1", instanceID).Scan(&status)
		return signalInstanceError(instanceID, status, err)
	})
	if err != nil {
		return 0, err
	}
	notifyRunnable(ctx, s.Storage, instanceID)
	return id, nil
}

func signalInstanceError(instanceID string, status WorkflowStatus, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrInstanceNotFound
	}
	if err != nil {
		return err
	}
	if status != StatusRunning {
		return &InstanceClosedError{InstanceID: instanceID, Status: status}
	}
	return nil
}

func (s signalStore) next(ctx context.Context, instanceID, channel string) (*Signal, error) {
	var signal Signal
	var payload string
	var sent any
	err := s.Conn(ctx).QueryRowContext(ctx, `
		SELECT id, instance_id, channel, payload, sent_at, COALESCE(delivered_seq,0), COALESCE(consumed_seq,0) FROM workflow_signals
		WHERE instance_id = $1 AND channel = $2 AND consumed_key IS NULL
		ORDER BY id LIMIT 1
	`, instanceID, channel).Scan(&signal.ID, &signal.InstanceID, &signal.Channel, &payload, &sent, &signal.DeliveredSeq, &signal.ConsumedSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	signal.Payload = []byte(payload)
	switch value := sent.(type) {
	case time.Time:
		signal.SentAt = value
	case string:
		signal.SentAt, err = parseSQLiteTime(value)
	default:
		err = fmt.Errorf("unexpected signal timestamp type %T", sent)
	}
	if err != nil {
		return nil, err
	}
	return &signal, nil
}

// Savepoints keep failed operations atomic even if the caller owns and later
// commits the surrounding transaction. Storage transactions are used serially.
func (s signalStore) transaction(ctx context.Context, fn func(context.Context, Executor) error) error {
	if !s.InTransaction(ctx) {
		return s.ownTransaction(ctx, fn)
	}
	conn := s.Conn(ctx)
	if _, err := conn.ExecContext(ctx, "SAVEPOINT s6_signal"); err != nil {
		return err
	}
	err := fn(ctx, conn)
	if err == nil {
		_, err = conn.ExecContext(ctx, "RELEASE SAVEPOINT s6_signal")
	}
	if err == nil {
		return nil
	}
	// Cancellation must not leave partial changes available to an outer commit.
	cleanupCtx := context.WithoutCancel(ctx)
	_, rollbackErr := conn.ExecContext(cleanupCtx, "ROLLBACK TO SAVEPOINT s6_signal")
	if rollbackErr != nil {
		return errors.Join(err, rollbackErr, s.RollbackTransaction(ctx))
	}
	_, releaseErr := conn.ExecContext(cleanupCtx, "RELEASE SAVEPOINT s6_signal")
	return errors.Join(err, releaseErr)
}

// Retain the connection through COMMIT and its failure cleanup. SQLite can leave
// a transaction open after a failed COMMIT (e.g. a deferred constraint), whereas
// database/sql marks a *sql.Tx done even on that error and cannot roll it back.
func (s signalStore) ownTransaction(ctx context.Context, fn func(context.Context, Executor) error) (err error) {
	conn, err := s.DB().Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	begin := "BEGIN IMMEDIATE"
	if s.postgres {
		begin = "BEGIN"
	}
	if _, err = conn.ExecContext(ctx, begin); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, rollbackErr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
			if rollbackErr != nil {
				// Never return a connection with an uncertain transaction to the pool.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
			err = errors.Join(err, rollbackErr)
		}
	}()
	if err := fn(ctx, observeExecutor(s.driver, conn)); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	committed = err == nil
	return err
}
