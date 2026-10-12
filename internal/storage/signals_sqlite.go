package storage

import "context"

var _ SignalManager = (*SQLiteStorage)(nil)

// AppendSignal buffers a signal only while the instance is running.
func (s *SQLiteStorage) AppendSignal(ctx context.Context, instanceID, channel string, payload []byte) (int64, error) {
	return (signalStore{Storage: s, driver: s.driver}).append(ctx, instanceID, channel, payload)
}

// NextSignal returns the oldest unconsumed signal for this instance and channel.
func (s *SQLiteStorage) NextSignal(ctx context.Context, instanceID, channel string) (*Signal, error) {
	return (signalStore{Storage: s, driver: s.driver}).next(ctx, instanceID, channel)
}
