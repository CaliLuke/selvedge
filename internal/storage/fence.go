package storage

import (
	"context"
	"errors"
)

type fenceKey struct{}

// ContextWithFence carries the execution token for status and subscription writes.
func ContextWithFence(ctx context.Context, f Fence) context.Context {
	return context.WithValue(ctx, fenceKey{}, f)
}
func contextFence(ctx context.Context, id string) *Fence {
	f, ok := ctx.Value(fenceKey{}).(Fence)
	if !ok || f.InstanceID != id {
		return &Fence{}
	}
	return &f
}

type fenceStore struct{ TransactionManager }

// withFence holds the guard row lock until the guarded write commits.
// A nil fence is used only by explicitly classified worker/poller writes.
func (s fenceStore) withFence(ctx context.Context, f *Fence, fn func(Executor) error) (err error) {
	defer func() {
		if errors.Is(err, ErrFenced) {
			if reject, ok := ctx.Value(fenceRejectionKey{}).(func(error)); ok {
				reject(err)
			}
		}
	}()
	if f != nil {
		if check, ok := ctx.Value(commitCheckKey{}).(func() error); ok {
			if err := check(); err != nil {
				return err
			}
		}
	}
	own := !s.InTransaction(ctx)
	if own {
		var err error
		ctx, err = s.BeginTransaction(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = s.RollbackTransaction(ctx) }()
	}
	conn := s.Conn(ctx)
	if f != nil {
		result, err := conn.ExecContext(ctx, `UPDATE workflow_instances SET lease_epoch=lease_epoch WHERE instance_id=$1 AND lease_owner=$2 AND lease_epoch=$3`, f.InstanceID, f.Owner, f.Epoch)
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
	}
	if err := fn(conn); err != nil {
		return err
	}
	if own {
		return s.CommitTransaction(ctx)
	}
	return nil
}

type commitCheckKey struct{}

// ContextWithCommitCheck checks the holder's monotonic belief before guarded commits.
func ContextWithCommitCheck(ctx context.Context, check func() error) context.Context {
	return context.WithValue(ctx, commitCheckKey{}, check)
}

type fenceRejectionKey struct{}

// ContextWithFenceRejection makes rejected commits fatal to their execution.
func ContextWithFenceRejection(ctx context.Context, reject func(error)) context.Context {
	return context.WithValue(ctx, fenceRejectionKey{}, reject)
}

func rejectFence(ctx context.Context) error {
	if reject, ok := ctx.Value(fenceRejectionKey{}).(func(error)); ok {
		reject(ErrFenced)
	}
	return ErrFenced
}
