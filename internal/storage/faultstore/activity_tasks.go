package faultstore

import (
	"context"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// ReserveActivity delegates with fault hooks.
func (s *Store) ReserveActivity(ctx context.Context, f *storage.Fence, seq int64, key, cmd string, task *storage.ActivityTask) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "ReserveActivity"); err != nil {
		return nil, err
	}
	value, err := s.Storage.ReserveActivity(ctx, f, seq, key, cmd, task)
	return value, s.after(ctx, "ReserveActivity", err)
}

// ClaimActivityTasks delegates with fault hooks.
func (s *Store) ClaimActivityTasks(ctx context.Context, owner string, ttl time.Duration, limit int, names ...string) ([]*storage.ActivityTask, error) {
	if err := s.before(ctx, "ClaimActivityTasks"); err != nil {
		return nil, err
	}
	value, err := s.Storage.ClaimActivityTasks(ctx, owner, ttl, limit, names...)
	return value, s.after(ctx, "ClaimActivityTasks", err)
}

// ClaimActivityTask delegates with fault hooks.
func (s *Store) ClaimActivityTask(ctx context.Context, id, key, owner string, ttl time.Duration) (*storage.ActivityTask, error) {
	if err := s.before(ctx, "ClaimActivityTask"); err != nil {
		return nil, err
	}
	value, err := s.Storage.ClaimActivityTask(ctx, id, key, owner, ttl)
	return value, s.after(ctx, "ClaimActivityTask", err)
}

// RenewActivityTask delegates with fault hooks.
func (s *Store) RenewActivityTask(ctx context.Context, claim storage.TaskClaim, ttl time.Duration) error {
	if err := s.before(ctx, "RenewActivityTask"); err != nil {
		return err
	}
	err := s.Storage.RenewActivityTask(ctx, claim, ttl)
	return s.after(ctx, "RenewActivityTask", err)
}

// CompleteActivityTask delegates with fault hooks.
func (s *Store) CompleteActivityTask(ctx context.Context, claim storage.TaskClaim, event storage.HistoryEventType, data []byte) error {
	if err := s.before(ctx, "CompleteActivityTask"); err != nil {
		return err
	}
	err := s.Storage.CompleteActivityTask(ctx, claim, event, data)
	return s.after(ctx, "CompleteActivityTask", err)
}

// ReleaseActivityTask delegates with fault hooks.
func (s *Store) ReleaseActivityTask(ctx context.Context, claim storage.TaskClaim) error {
	if err := s.before(ctx, "ReleaseActivityTask"); err != nil {
		return err
	}
	err := s.Storage.ReleaseActivityTask(ctx, claim)
	return s.after(ctx, "ReleaseActivityTask", err)
}
