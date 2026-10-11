package faultstore

import (
	"context"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// AcquireLease delegates to storage with before/after fault hooks and call recording.
func (s *Store) AcquireLease(ctx context.Context, id, owner string, ttl time.Duration) (storage.Fence, bool, error) {
	if err := s.before(ctx, "AcquireLease"); err != nil {
		return storage.Fence{}, false, err
	}
	v0, v1, err := s.Storage.AcquireLease(ctx, id, owner, ttl)
	return v0, v1, s.after(ctx, "AcquireLease", err)
}

// RenewLease delegates to storage with before/after fault hooks and call recording.
func (s *Store) RenewLease(ctx context.Context, f storage.Fence, ttl time.Duration) (bool, error) {
	if err := s.before(ctx, "RenewLease"); err != nil {
		return false, err
	}
	v0, err := s.Storage.RenewLease(ctx, f, ttl)
	return v0, s.after(ctx, "RenewLease", err)
}

// ReleaseLease delegates to storage with before/after fault hooks and call recording.
func (s *Store) ReleaseLease(ctx context.Context, f storage.Fence, runnable bool) error {
	if err := s.before(ctx, "ReleaseLease"); err != nil {
		return err
	}
	err := s.Storage.ReleaseLease(ctx, f, runnable)
	return s.after(ctx, "ReleaseLease", err)
}

// MarkRunnable delegates to storage with before/after fault hooks and call recording.
func (s *Store) MarkRunnable(ctx context.Context, id string) error {
	if err := s.before(ctx, "MarkRunnable"); err != nil {
		return err
	}
	err := s.Storage.MarkRunnable(ctx, id)
	return s.after(ctx, "MarkRunnable", err)
}

// CreateInstance delegates to storage with before/after fault hooks and call recording.
func (s *Store) CreateInstance(ctx context.Context, inst *storage.WorkflowInstance) error {
	if err := s.before(ctx, "CreateInstance"); err != nil {
		return err
	}
	err := s.Storage.CreateInstance(ctx, inst)
	return s.after(ctx, "CreateInstance", err)
}

// ReserveHistory delegates to storage with before/after fault hooks and call recording.
func (s *Store) ReserveHistory(ctx context.Context, f *storage.Fence, id, key, cmd string) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "ReserveHistory"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.ReserveHistory(ctx, f, id, key, cmd)
	return v0, s.after(ctx, "ReserveHistory", err)
}

// RecordHistory delegates to storage with before/after fault hooks and call recording.
func (s *Store) RecordHistory(ctx context.Context, f *storage.Fence, ev *storage.HistoryEvent) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "RecordHistory"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.RecordHistory(ctx, f, ev)
	return v0, s.after(ctx, "RecordHistory", err)
}

// SetHistoryValueIfNull delegates to storage with before/after fault hooks and call recording.
func (s *Store) SetHistoryValueIfNull(ctx context.Context, f *storage.Fence, id, key, cmd string, t storage.HistoryEventType, data []byte) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "SetHistoryValueIfNull"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.SetHistoryValueIfNull(ctx, f, id, key, cmd, t, data)
	return v0, s.after(ctx, "SetHistoryValueIfNull", err)
}

// UpdateInstanceActivity delegates to storage with before/after fault hooks and call recording.
func (s *Store) UpdateInstanceActivity(ctx context.Context, id, key string) error {
	if err := s.before(ctx, "UpdateInstanceActivity"); err != nil {
		return err
	}
	err := s.Storage.UpdateInstanceActivity(ctx, id, key)
	return s.after(ctx, "UpdateInstanceActivity", err)
}

// RegisterTimerSubscription delegates to storage with before/after fault hooks and call recording.
func (s *Store) RegisterTimerSubscription(ctx context.Context, sub *storage.TimerSubscription) error {
	if err := s.before(ctx, "RegisterTimerSubscription"); err != nil {
		return err
	}
	err := s.Storage.RegisterTimerSubscription(ctx, sub)
	return s.after(ctx, "RegisterTimerSubscription", err)
}

// RemoveTimerSubscription delegates to storage with before/after fault hooks and call recording.
func (s *Store) RemoveTimerSubscription(ctx context.Context, id, key string) error {
	if err := s.before(ctx, "RemoveTimerSubscription"); err != nil {
		return err
	}
	err := s.Storage.RemoveTimerSubscription(ctx, id, key)
	return s.after(ctx, "RemoveTimerSubscription", err)
}

// FireTimer delegates to storage with before/after fault hooks and call recording.
func (s *Store) FireTimer(ctx context.Context, sub *storage.TimerSubscription) error {
	if err := s.before(ctx, "FireTimer"); err != nil {
		return err
	}
	err := s.Storage.FireTimer(ctx, sub)
	return s.after(ctx, "FireTimer", err)
}

// SubscribeToChannel delegates to storage with before/after fault hooks and call recording.
func (s *Store) SubscribeToChannel(ctx context.Context, id, channel string, mode storage.ChannelMode) error {
	if err := s.before(ctx, "SubscribeToChannel"); err != nil {
		return err
	}
	err := s.Storage.SubscribeToChannel(ctx, id, channel, mode)
	return s.after(ctx, "SubscribeToChannel", err)
}

// RegisterChannelReceive delegates to storage with before/after fault hooks and call recording.
func (s *Store) RegisterChannelReceive(ctx context.Context, id, channel, key string, timeout *time.Time) error {
	if err := s.before(ctx, "RegisterChannelReceive"); err != nil {
		return err
	}
	err := s.Storage.RegisterChannelReceive(ctx, id, channel, key, timeout)
	return s.after(ctx, "RegisterChannelReceive", err)
}

// UpdateDeliveryCursor delegates to storage with before/after fault hooks and call recording.
func (s *Store) UpdateDeliveryCursor(ctx context.Context, id, channel string, msg int64) error {
	if err := s.before(ctx, "UpdateDeliveryCursor"); err != nil {
		return err
	}
	err := s.Storage.UpdateDeliveryCursor(ctx, id, channel, msg)
	return s.after(ctx, "UpdateDeliveryCursor", err)
}

// ClearChannelWaitingState delegates to storage with before/after fault hooks and call recording.
func (s *Store) ClearChannelWaitingState(ctx context.Context, id, channel string) error {
	if err := s.before(ctx, "ClearChannelWaitingState"); err != nil {
		return err
	}
	err := s.Storage.ClearChannelWaitingState(ctx, id, channel)
	return s.after(ctx, "ClearChannelWaitingState", err)
}

// ConsumeChannelMessage delegates to storage with before/after fault hooks and call recording.
func (s *Store) ConsumeChannelMessage(ctx context.Context, f *storage.Fence, channel string, id int64, ev *storage.HistoryEvent) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "ConsumeChannelMessage"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.ConsumeChannelMessage(ctx, f, channel, id, ev)
	return v0, s.after(ctx, "ConsumeChannelMessage", err)
}

// PublishToChannel delegates to storage with before/after fault hooks and call recording.
func (s *Store) PublishToChannel(ctx context.Context, channel string, data, metadata []byte) (int64, error) {
	if err := s.before(ctx, "PublishToChannel"); err != nil {
		return 0, err
	}
	v0, err := s.Storage.PublishToChannel(ctx, channel, data, metadata)
	return v0, s.after(ctx, "PublishToChannel", err)
}

// CommitTransaction delegates to storage with before/after fault hooks and call recording.
func (s *Store) CommitTransaction(ctx context.Context) error {
	if err := s.before(ctx, "CommitTransaction"); err != nil {
		return err
	}
	err := s.Storage.CommitTransaction(ctx)
	return s.after(ctx, "CommitTransaction", err)
}

// Initialize delegates to storage with before/after fault hooks and call recording.
func (s *Store) Initialize(ctx context.Context) error {
	if err := s.before(ctx, "Initialize"); err != nil {
		return err
	}
	err := s.Storage.Initialize(ctx)
	return s.after(ctx, "Initialize", err)
}

// BeginTransaction delegates to storage with before/after fault hooks and call recording.
func (s *Store) BeginTransaction(ctx context.Context) (context.Context, error) {
	if err := s.before(ctx, "BeginTransaction"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.BeginTransaction(ctx)
	return v0, s.after(ctx, "BeginTransaction", err)
}

// RollbackTransaction delegates to storage with before/after fault hooks and call recording.
func (s *Store) RollbackTransaction(ctx context.Context) error {
	if err := s.before(ctx, "RollbackTransaction"); err != nil {
		return err
	}
	err := s.Storage.RollbackTransaction(ctx)
	return s.after(ctx, "RollbackTransaction", err)
}

// RegisterPostCommitCallback delegates to storage with before/after fault hooks and call recording.
func (s *Store) RegisterPostCommitCallback(ctx context.Context, cb func() error) error {
	if err := s.before(ctx, "RegisterPostCommitCallback"); err != nil {
		return err
	}
	err := s.Storage.RegisterPostCommitCallback(ctx, cb)
	return s.after(ctx, "RegisterPostCommitCallback", err)
}

// GetInstance delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetInstance(ctx context.Context, id string) (*storage.WorkflowInstance, error) {
	if err := s.before(ctx, "GetInstance"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.GetInstance(ctx, id)
	return v0, s.after(ctx, "GetInstance", err)
}

// ListInstances delegates to storage with before/after fault hooks and call recording.
func (s *Store) ListInstances(ctx context.Context, opts storage.ListInstancesOptions) (*storage.PaginationResult, error) {
	if err := s.before(ctx, "ListInstances"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.ListInstances(ctx, opts)
	return v0, s.after(ctx, "ListInstances", err)
}

// FindDispatchable delegates to storage with before/after fault hooks and call recording.
func (s *Store) FindDispatchable(ctx context.Context, limit int) ([]string, error) {
	if err := s.before(ctx, "FindDispatchable"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.FindDispatchable(ctx, limit)
	return v0, s.after(ctx, "FindDispatchable", err)
}

// GetHistoryEvent delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetHistoryEvent(ctx context.Context, id, key string) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "GetHistoryEvent"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.GetHistoryEvent(ctx, id, key)
	return v0, s.after(ctx, "GetHistoryEvent", err)
}

// GetHistoryPaginated delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetHistoryPaginated(ctx context.Context, id string, afterID int64, limit int) ([]*storage.HistoryEvent, bool, error) {
	if err := s.before(ctx, "GetHistoryPaginated"); err != nil {
		return nil, false, err
	}
	v0, v1, err := s.Storage.GetHistoryPaginated(ctx, id, afterID, limit)
	return v0, v1, s.after(ctx, "GetHistoryPaginated", err)
}

// GetHistoryCount delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetHistoryCount(ctx context.Context, id string) (int64, error) {
	if err := s.before(ctx, "GetHistoryCount"); err != nil {
		return 0, err
	}
	v0, err := s.Storage.GetHistoryCount(ctx, id)
	return v0, s.after(ctx, "GetHistoryCount", err)
}

// FindExpiredTimers delegates to storage with before/after fault hooks and call recording.
func (s *Store) FindExpiredTimers(ctx context.Context, limit int) ([]*storage.TimerSubscription, error) {
	if err := s.before(ctx, "FindExpiredTimers"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.FindExpiredTimers(ctx, limit)
	return v0, s.after(ctx, "FindExpiredTimers", err)
}

// GetChannelSubscription delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetChannelSubscription(ctx context.Context, id, channel string) (*storage.ChannelSubscription, error) {
	if err := s.before(ctx, "GetChannelSubscription"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.GetChannelSubscription(ctx, id, channel)
	return v0, s.after(ctx, "GetChannelSubscription", err)
}

// GetChannelMode delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetChannelMode(ctx context.Context, channel string) (storage.ChannelMode, error) {
	if err := s.before(ctx, "GetChannelMode"); err != nil {
		return "", err
	}
	v0, err := s.Storage.GetChannelMode(ctx, channel)
	return v0, s.after(ctx, "GetChannelMode", err)
}

// GetPendingChannelMessagesForInstance delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetPendingChannelMessagesForInstance(ctx context.Context, id, channel string) ([]*storage.ChannelMessage, error) {
	if err := s.before(ctx, "GetPendingChannelMessagesForInstance"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.GetPendingChannelMessagesForInstance(ctx, id, channel)
	return v0, s.after(ctx, "GetPendingChannelMessagesForInstance", err)
}

// GetChannelSubscribersWaiting delegates to storage with before/after fault hooks and call recording.
func (s *Store) GetChannelSubscribersWaiting(ctx context.Context, channel string) ([]*storage.ChannelSubscription, error) {
	if err := s.before(ctx, "GetChannelSubscribersWaiting"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.GetChannelSubscribersWaiting(ctx, channel)
	return v0, s.after(ctx, "GetChannelSubscribersWaiting", err)
}

// FindExpiredChannelSubscriptions delegates to storage with before/after fault hooks and call recording.
func (s *Store) FindExpiredChannelSubscriptions(ctx context.Context, limit int) ([]*storage.ChannelSubscription, error) {
	if err := s.before(ctx, "FindExpiredChannelSubscriptions"); err != nil {
		return nil, err
	}
	v0, err := s.Storage.FindExpiredChannelSubscriptions(ctx, limit)
	return v0, s.after(ctx, "FindExpiredChannelSubscriptions", err)
}

// CleanupOldChannelMessages delegates to storage with before/after fault hooks and call recording.
func (s *Store) CleanupOldChannelMessages(ctx context.Context, age time.Duration) error {
	if err := s.before(ctx, "CleanupOldChannelMessages"); err != nil {
		return err
	}
	err := s.Storage.CleanupOldChannelMessages(ctx, age)
	return s.after(ctx, "CleanupOldChannelMessages", err)
}

// TryAcquireSystemLock delegates to storage with before/after fault hooks and call recording.
func (s *Store) TryAcquireSystemLock(ctx context.Context, name, owner string, ttl time.Duration) (bool, error) {
	if err := s.before(ctx, "TryAcquireSystemLock"); err != nil {
		return false, err
	}
	v0, err := s.Storage.TryAcquireSystemLock(ctx, name, owner, ttl)
	return v0, s.after(ctx, "TryAcquireSystemLock", err)
}

// ReleaseSystemLock delegates to storage with before/after fault hooks and call recording.
func (s *Store) ReleaseSystemLock(ctx context.Context, name, owner string) error {
	if err := s.before(ctx, "ReleaseSystemLock"); err != nil {
		return err
	}
	err := s.Storage.ReleaseSystemLock(ctx, name, owner)
	return s.after(ctx, "ReleaseSystemLock", err)
}

// CleanupExpiredSystemLocks delegates to storage with before/after fault hooks and call recording.
func (s *Store) CleanupExpiredSystemLocks(ctx context.Context) error {
	if err := s.before(ctx, "CleanupExpiredSystemLocks"); err != nil {
		return err
	}
	err := s.Storage.CleanupExpiredSystemLocks(ctx)
	return s.after(ctx, "CleanupExpiredSystemLocks", err)
}

// RequestCancel delegates with fault injection.
func (s *Store) RequestCancel(ctx context.Context, id, reason string) (storage.WorkflowStatus, error) {
	if err := s.before(ctx, "RequestCancel"); err != nil {
		return "", err
	}
	v, err := s.Storage.RequestCancel(ctx, id, reason)
	return v, s.after(ctx, "RequestCancel", err)
}

// IsCancelRequested delegates with fault injection.
func (s *Store) IsCancelRequested(ctx context.Context, id string) (bool, error) {
	if err := s.before(ctx, "IsCancelRequested"); err != nil {
		return false, err
	}
	v, err := s.Storage.IsCancelRequested(ctx, id)
	return v, s.after(ctx, "IsCancelRequested", err)
}

// CloseInstance delegates with fault injection.
func (s *Store) CloseInstance(ctx context.Context, f storage.Fence, status storage.WorkflowStatus, output []byte, event *storage.HistoryEvent) error {
	if err := s.before(ctx, "CloseInstance"); err != nil {
		return err
	}
	return s.after(ctx, "CloseInstance", s.Storage.CloseInstance(ctx, f, status, output, event))
}

// ReserveTimer delegates with fault injection.
func (s *Store) ReserveTimer(ctx context.Context, f *storage.Fence, timer *storage.TimerSubscription) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "ReserveTimer"); err != nil {
		return nil, err
	}
	v, err := s.Storage.ReserveTimer(ctx, f, timer)
	return v, s.after(ctx, "ReserveTimer", err)
}
