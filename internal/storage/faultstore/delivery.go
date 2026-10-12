package faultstore

import (
	"context"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func (s *Store) ArrivedUndelivered(ctx context.Context, id string) (*storage.DeliveryBatch, error) {
	if err := s.before(ctx, "ArrivedUndelivered"); err != nil {
		return nil, err
	}
	v, err := s.Storage.ArrivedUndelivered(ctx, id)
	return v, s.after(ctx, "ArrivedUndelivered", err)
}
func (s *Store) DeliveredSignals(ctx context.Context, id string) ([]storage.SignalPosition, error) {
	if err := s.before(ctx, "DeliveredSignals"); err != nil {
		return nil, err
	}
	v, err := s.Storage.DeliveredSignals(ctx, id)
	return v, s.after(ctx, "DeliveredSignals", err)
}
func (s *Store) CommitObservation(ctx context.Context, f *storage.Fence, c *storage.ObservationCommit) (*storage.ObservationResult, error) {
	if err := s.before(ctx, "CommitObservation"); err != nil {
		return nil, err
	}
	v, err := s.Storage.CommitObservation(ctx, f, c)
	return v, s.after(ctx, "CommitObservation", err)
}
