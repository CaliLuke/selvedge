package replay

// Explicit denials complete queryStorage. Only foreign calls and query handlers return errors; coroutine stops do not return.

import (
	"context"
	"database/sql"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func (s *queryStorage) Initialize(ctx context.Context) error {
	return s.stop()
}

func (s *queryStorage) NotifyRunnable(context.Context) {
	_ = s.stop()
}

func (s *queryStorage) Close() error {
	return s.stop()
}

func (s *queryStorage) DB() *sql.DB {
	_ = s.stop()
	return nil
}

func (s *queryStorage) BeginTransaction(ctx context.Context) (context.Context, error) {
	return nil, s.stop()
}

func (s *queryStorage) CommitTransaction(ctx context.Context) error {
	return s.stop()
}

func (s *queryStorage) RollbackTransaction(ctx context.Context) error {
	return s.stop()
}

func (s *queryStorage) InTransaction(ctx context.Context) bool {
	_ = s.stop()
	return false
}

func (s *queryStorage) Conn(ctx context.Context) storage.Executor {
	_ = s.stop()
	return nil
}

func (s *queryStorage) RegisterPostCommitCallback(ctx context.Context, cb func() error) error {
	return s.stop()
}

func (s *queryStorage) CreateInstance(ctx context.Context, instance *storage.WorkflowInstance) error {
	return s.stop()
}

func (s *queryStorage) RequestCancel(ctx context.Context, instanceID, reason string) (storage.WorkflowStatus, error) {
	return "", s.stop()
}

func (s *queryStorage) CloseInstance(ctx context.Context, f storage.Fence, status storage.WorkflowStatus, output []byte, closeEvent *storage.HistoryEvent) error {
	return s.stop()
}

func (s *queryStorage) UpdateInstanceActivity(ctx context.Context, instanceID, activityID string) error {
	return s.stop()
}

func (s *queryStorage) ListInstances(ctx context.Context, opts storage.ListInstancesOptions) (*storage.PaginationResult, error) {
	return nil, s.stop()
}

func (s *queryStorage) AcquireLease(context.Context, string, string, time.Duration) (storage.Fence, bool, error) {
	return storage.Fence{}, false, s.stop()
}

func (s *queryStorage) RenewLease(context.Context, storage.Fence, time.Duration) (bool, error) {
	return false, s.stop()
}

func (s *queryStorage) ReleaseLease(context.Context, storage.Fence, bool) error {
	return s.stop()
}

func (s *queryStorage) MarkRunnable(context.Context, string) error {
	return s.stop()
}

func (s *queryStorage) FindDispatchable(context.Context, int, ...string) ([]string, error) {
	return nil, s.stop()
}

func (s *queryStorage) RecordHistory(ctx context.Context, f *storage.Fence, event *storage.HistoryEvent) (*storage.HistoryEvent, error) {
	return nil, s.stop()
}

func (s *queryStorage) SetHistoryValueIfNull(ctx context.Context, f *storage.Fence, instanceID, key, cmd string, t storage.HistoryEventType, data []byte) (*storage.HistoryEvent, error) {
	return nil, s.stop()
}

func (s *queryStorage) GetHistoryCount(ctx context.Context, instanceID string) (int64, error) {
	return 0, s.stop()
}

func (s *queryStorage) ArrivedUndelivered(context.Context, string) (*storage.DeliveryBatch, error) {
	return nil, s.stop()
}

func (s *queryStorage) CommitObservation(context.Context, *storage.Fence, *storage.ObservationCommit) (*storage.ObservationResult, error) {
	return nil, s.stop()
}

func (s *queryStorage) CancelActivityAtYield(context.Context, *storage.Fence, string, string, string, []byte) (*storage.HistoryEvent, error) {
	return nil, s.stop()
}

func (s *queryStorage) ClaimActivityTasks(context.Context, string, time.Duration, int, storage.TaskFilter) ([]*storage.ActivityTask, error) {
	return nil, s.stop()
}

func (s *queryStorage) ClaimActivityTask(context.Context, string, string, string, time.Duration, []string) (*storage.ActivityTask, error) {
	return nil, s.stop()
}

func (s *queryStorage) RenewActivityTask(context.Context, storage.TaskClaim, time.Duration, bool) error {
	return s.stop()
}

func (s *queryStorage) CompleteActivityTask(context.Context, storage.TaskClaim, storage.HistoryEventType, []byte) error {
	return s.stop()
}

func (s *queryStorage) ReleaseActivityTask(context.Context, storage.TaskClaim) error {
	return s.stop()
}

func (s *queryStorage) StartChild(context.Context, *storage.Fence, int64, string, string, *storage.WorkflowInstance) (*storage.HistoryEvent, error) {
	return nil, s.stop()
}

func (s *queryStorage) CancelChild(context.Context, *storage.Fence, *storage.ChildCancel) (*storage.HistoryEvent, error) {
	return nil, s.stop()
}

func (s *queryStorage) ReadSnapshot(context.Context, string) (*storage.InstanceSnapshot, error) {
	return nil, s.stop()
}

func (s *queryStorage) FireTimer(context.Context, *storage.TimerSubscription) error {
	return s.stop()
}

func (s *queryStorage) FindExpiredTimers(ctx context.Context, limit int) ([]*storage.TimerSubscription, error) {
	return nil, s.stop()
}

func (s *queryStorage) RemoveTimerSubscription(ctx context.Context, instanceID, timerID string) error {
	return s.stop()
}

func (s *queryStorage) AppendSignal(ctx context.Context, instanceID, channel string, payload []byte) (int64, error) {
	return 0, s.stop()
}

func (s *queryStorage) NextSignal(ctx context.Context, instanceID, channel string) (*storage.Signal, error) {
	return nil, s.stop()
}

func (s *queryStorage) TryAcquireSystemLock(ctx context.Context, lockName, workerID string, ttl time.Duration) (bool, error) {
	return false, s.stop()
}

func (s *queryStorage) ReleaseSystemLock(ctx context.Context, lockName, workerID string) error {
	return s.stop()
}

func (s *queryStorage) CleanupExpiredSystemLocks(ctx context.Context) error {
	return s.stop()
}

func (s *queryStorage) RetryActivityTask(context.Context, storage.TaskClaim, []byte, time.Duration) error {
	return s.stop()
}
func (s *queryStorage) NextActivityDue(context.Context, storage.TaskFilter) (time.Duration, bool, error) {
	return 0, false, s.stop()
}
func (s *queryStorage) ListActivityTasks(context.Context, string) ([]*storage.ActivityTask, error) {
	return nil, s.stop()
}
