package faultstore

import (
	"context"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// ReadSnapshot exposes a boundary after which a query must perform no storage I/O.
func (s *Store) ReadSnapshot(ctx context.Context, id string) (*storage.InstanceSnapshot, error) {
	if err := s.before(ctx, "ReadSnapshot"); err != nil {
		return nil, err
	}
	snapshot, err := s.Storage.ReadSnapshot(ctx, id)
	return snapshot, s.after(ctx, "ReadSnapshot", err)
}
