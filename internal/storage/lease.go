package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrFenced means a write lacks the current instance lease token.
var ErrFenced = errors.New("execution fenced out")

// LeaseManager grants a distinct token to each execution using the store clock.
type LeaseManager interface {
	// AcquireLease grants a fresh epoch to a runnable unowned instance or an instance with an expired lease.
	AcquireLease(context.Context, string, string, time.Duration) (Fence, bool, error)
	// RenewLease extends an unexpired matching lease using the store clock.
	RenewLease(context.Context, Fence, time.Duration) (bool, error)
	// ReleaseLease clears a matching lease and preserves pending input; runnable also requests another dispatch.
	ReleaseLease(context.Context, Fence, bool) error
	// MarkRunnable marks a running instance for dispatch without changing ownership.
	MarkRunnable(context.Context, string) error
	// FindDispatchable lists runnable unowned instances and instances with expired leases.
	FindDispatchable(context.Context, int) ([]string, error)
}

type leaseStore struct {
	Storage
	driver Driver
}

// AcquireLease grants a fresh epoch to a runnable unowned instance or an instance with an expired lease.
func (s leaseStore) AcquireLease(ctx context.Context, id, owner string, ttl time.Duration) (Fence, bool, error) {
	f := Fence{InstanceID: id, Owner: owner}
	if owner == "" || leaseMillis(ttl) <= 0 {
		return f, false, fmt.Errorf("lease requires an owner and positive millisecond TTL")
	}
	now := s.driver.NowMillisExpr()
	err := observeExecutor(s.driver, s.DB()).QueryRowContext(ctx, `UPDATE workflow_instances SET lease_owner=$1, lease_epoch=lease_epoch+1,
 lease_expires_ms=`+now+`+$2, runnable=FALSE
 WHERE instance_id=$3 AND status='running' AND
 ((lease_owner IS NULL AND runnable) OR (lease_owner IS NOT NULL AND lease_expires_ms < `+now+`)) RETURNING lease_epoch`, owner, leaseMillis(ttl), id).Scan(&f.Epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return f, false, nil
	}
	return f, err == nil, err
}

// RenewLease extends an unexpired matching lease using the store clock.
func (s leaseStore) RenewLease(ctx context.Context, f Fence, ttl time.Duration) (bool, error) {
	if leaseMillis(ttl) <= 0 {
		return false, fmt.Errorf("lease TTL must be positive")
	}
	now := s.driver.NowMillisExpr()
	result, err := observeExecutor(s.driver, s.DB()).ExecContext(ctx, `UPDATE workflow_instances SET lease_expires_ms=`+now+`+$1
 WHERE instance_id=$2 AND lease_owner=$3 AND lease_epoch=$4 AND lease_expires_ms >= `+now, leaseMillis(ttl), f.InstanceID, f.Owner, f.Epoch)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// ReleaseLease clears a matching lease and preserves pending input; runnable also requests another dispatch.
func (s leaseStore) ReleaseLease(ctx context.Context, f Fence, runnable bool) error {
	result, err := observeExecutor(s.driver, s.DB()).ExecContext(ctx, `UPDATE workflow_instances SET lease_owner=NULL, lease_expires_ms=NULL,
 runnable=CASE WHEN $1 AND status='running' THEN TRUE ELSE runnable END
 WHERE instance_id=$2 AND lease_owner=$3 AND lease_epoch=$4`, runnable, f.InstanceID, f.Owner, f.Epoch)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		return ErrFenced
	}
	return err
}

// MarkRunnable marks a running instance for dispatch without changing ownership.
func (s leaseStore) MarkRunnable(ctx context.Context, id string) error {
	_, err := s.Conn(ctx).ExecContext(ctx, `UPDATE workflow_instances SET runnable=TRUE WHERE instance_id=$1 AND status='running'`, id)
	if err == nil {
		notifyRunnable(ctx, s.Storage, id)
	}
	return err
}

// FindDispatchable lists runnable unowned instances and instances with expired leases.
func (s leaseStore) FindDispatchable(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := observeExecutor(s.driver, s.DB()).QueryContext(ctx, `SELECT instance_id FROM workflow_instances WHERE status='running' AND
 ((lease_owner IS NULL AND runnable) OR (lease_owner IS NOT NULL AND lease_expires_ms < `+s.driver.NowMillisExpr()+`)) ORDER BY started_at, instance_id LIMIT $1`, limit)
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

// AcquireLease grants a fresh epoch to a runnable unowned instance or an instance with an expired lease.
func (s *SQLiteStorage) AcquireLease(ctx context.Context, id, owner string, ttl time.Duration) (Fence, bool, error) {
	return (leaseStore{s, s.driver}).AcquireLease(ctx, id, owner, ttl)
}

// RenewLease extends an unexpired matching lease using the store clock.
func (s *SQLiteStorage) RenewLease(ctx context.Context, f Fence, ttl time.Duration) (bool, error) {
	return (leaseStore{s, s.driver}).RenewLease(ctx, f, ttl)
}

// ReleaseLease clears a matching lease and preserves pending input; runnable also requests another dispatch.
func (s *SQLiteStorage) ReleaseLease(ctx context.Context, f Fence, runnable bool) error {
	return (leaseStore{s, s.driver}).ReleaseLease(ctx, f, runnable)
}

// MarkRunnable marks a running instance for dispatch without changing ownership.
func (s *SQLiteStorage) MarkRunnable(ctx context.Context, id string) error {
	return (leaseStore{s, s.driver}).MarkRunnable(ctx, id)
}

// FindDispatchable lists runnable unowned instances and instances with expired leases.
func (s *SQLiteStorage) FindDispatchable(ctx context.Context, limit int) ([]string, error) {
	return (leaseStore{s, s.driver}).FindDispatchable(ctx, limit)
}

// AcquireLease grants a fresh epoch to a runnable unowned instance or an instance with an expired lease.
func (s *PostgresStorage) AcquireLease(ctx context.Context, id, owner string, ttl time.Duration) (Fence, bool, error) {
	return (leaseStore{s, s.driver}).AcquireLease(ctx, id, owner, ttl)
}

// RenewLease extends an unexpired matching lease using the store clock.
func (s *PostgresStorage) RenewLease(ctx context.Context, f Fence, ttl time.Duration) (bool, error) {
	return (leaseStore{s, s.driver}).RenewLease(ctx, f, ttl)
}

// ReleaseLease clears a matching lease and preserves pending input; runnable also requests another dispatch.
func (s *PostgresStorage) ReleaseLease(ctx context.Context, f Fence, runnable bool) error {
	err := (leaseStore{s, s.driver}).ReleaseLease(ctx, f, runnable)
	// Abandon retries use polling; notifying here would bypass dispatcher backoff.
	// Suspend releases still notify so pending input can run promptly.
	if err == nil && !runnable {
		s.sendRunnableNotify(ctx, f.InstanceID)
	}
	return err
}

// MarkRunnable marks a running instance for dispatch without changing ownership.
func (s *PostgresStorage) MarkRunnable(ctx context.Context, id string) error {
	return (leaseStore{s, s.driver}).MarkRunnable(ctx, id)
}

// FindDispatchable lists runnable unowned instances and instances with expired leases.
func (s *PostgresStorage) FindDispatchable(ctx context.Context, limit int) ([]string, error) {
	return (leaseStore{s, s.driver}).FindDispatchable(ctx, limit)
}

// The store's millisecond grant must never be shorter than the holder's budget.
func leaseMillis(ttl time.Duration) int64 {
	ms := ttl.Milliseconds()
	if ttl%time.Millisecond > 0 {
		ms++
	}
	return ms
}

// notifyRunnable is a no-op for backends without cross-worker notifications.
func notifyRunnable(ctx context.Context, s TransactionManager, id string) {
	if notifier, ok := s.(interface{ notifyRunnable(context.Context, string) }); ok {
		notifier.notifyRunnable(ctx, id)
	}
}
