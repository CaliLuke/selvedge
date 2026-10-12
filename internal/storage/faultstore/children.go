package faultstore

import (
	"context"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// StartChild delegates with fault hooks.
func (s *Store) StartChild(ctx context.Context, f *storage.Fence, seq int64, key, cmd string, child *storage.WorkflowInstance) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "StartChild"); err != nil {
		return nil, err
	}
	value, err := s.Storage.StartChild(ctx, f, seq, key, cmd, child)
	return value, s.after(ctx, "StartChild", err)
}

// CancelChild delegates with fault hooks.
func (s *Store) CancelChild(ctx context.Context, f *storage.Fence, c *storage.ChildCancel) (*storage.HistoryEvent, error) {
	if err := s.before(ctx, "CancelChild"); err != nil {
		return nil, err
	}
	value, err := s.Storage.CancelChild(ctx, f, c)
	return value, s.after(ctx, "CancelChild", err)
}
