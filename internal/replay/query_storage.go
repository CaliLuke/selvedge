package replay

import (
	"context"
	"slices"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// There is deliberately no embedded live Storage. Unlisted methods stop the replay.
// Implementing Storage explicitly makes newly added methods a compile-time error.
type queryStorage struct {
	snapshot  *storage.InstanceSnapshot
	execution *ExecutionContext
}

var _ storage.Storage = (*queryStorage)(nil)

func (s *queryStorage) stop() error { return s.execution.stopQuery() }
func (s *queryStorage) check(ctx context.Context, id string) error {
	if id != s.snapshot.Instance.InstanceID {
		return s.stop()
	}
	if err := ctx.Err(); err != nil {
		s.execution.Fail(err)
		return err
	}
	return nil
}
func (s *queryStorage) GetInstance(ctx context.Context, id string) (*storage.WorkflowInstance, error) {
	if err := s.check(ctx, id); err != nil {
		return nil, err
	}
	inst := *s.snapshot.Instance
	inst.InputData = slices.Clone(inst.InputData)
	inst.OutputData = slices.Clone(inst.OutputData)
	if inst.LeaseExpiresMS != nil {
		expiry := *inst.LeaseExpiresMS
		inst.LeaseExpiresMS = &expiry
	}
	return &inst, nil
}
func (s *queryStorage) IsCancelRequested(ctx context.Context, id string) (bool, error) {
	if err := s.check(ctx, id); err != nil {
		return false, err
	}
	return s.snapshot.CancelRequested, nil
}
func cloneQueryHistory(row *storage.HistoryEvent) *storage.HistoryEvent {
	if row == nil {
		return nil
	}
	cloned := *row
	cloned.EventData = slices.Clone(row.EventData)
	cloned.EventDataBinary = slices.Clone(row.EventDataBinary)
	cloned.ReservationData = slices.Clone(row.ReservationData)
	return &cloned
}
func (s *queryStorage) GetHistoryEvent(ctx context.Context, id, key string) (*storage.HistoryEvent, error) {
	if err := s.check(ctx, id); err != nil {
		return nil, err
	}
	return cloneQueryHistory(s.execution.byKey[key]), nil
}
func (s *queryStorage) GetHistoryPaginated(ctx context.Context, id string, afterID int64, limit int) ([]*storage.HistoryEvent, bool, error) {
	if err := s.check(ctx, id); err != nil {
		return nil, false, err
	}
	if limit <= 0 {
		limit = 1000
	}
	var rows []*storage.HistoryEvent
	for _, row := range s.snapshot.History {
		if row.ID > afterID {
			if len(rows) == limit {
				return rows, true, nil
			}
			rows = append(rows, cloneQueryHistory(row))
		}
	}
	return rows, false, nil
}
func (s *queryStorage) DeliveredSignals(ctx context.Context, id string) ([]storage.SignalPosition, error) {
	if err := s.check(ctx, id); err != nil {
		return nil, err
	}
	return slices.Clone(s.snapshot.Signals), nil
}
func (s *queryStorage) ReserveHistory(ctx context.Context, _ *storage.Fence, id, key, command string, seq int64) (*storage.HistoryEvent, error) {
	row, err := s.GetHistoryEvent(ctx, id, key)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, s.stop()
	}
	if row.Command != command || row.Seq != seq {
		s.execution.Fail(storage.ErrHistoryKeyConflict)
		return nil, storage.ErrHistoryKeyConflict
	}
	return row, nil
}
func (s *queryStorage) ReserveTimer(ctx context.Context, f *storage.Fence, seq int64, timer *storage.TimerSubscription) (*storage.HistoryEvent, error) {
	return s.ReserveHistory(ctx, f, timer.InstanceID, timer.TimerID, "sleep", seq)
}
func (s *queryStorage) ReserveActivity(ctx context.Context, f *storage.Fence, seq int64, key, command string, task *storage.ActivityTask) (*storage.HistoryEvent, error) {
	return s.ReserveHistory(ctx, f, task.InstanceID, key, command, seq)
}
