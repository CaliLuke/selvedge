package storage

import "context"

var _ SignalManager = (*PostgresStorage)(nil)

// AppendSignal buffers a signal only while the instance is running.
func (s *PostgresStorage) AppendSignal(ctx context.Context, instanceID, channel string, payload []byte) (int64, error) {
	return (signalStore{Storage: s, driver: s.driver, postgres: true}).append(ctx, instanceID, channel, payload)
}

// NextSignal returns the oldest unconsumed signal for this instance and channel.
func (s *PostgresStorage) NextSignal(ctx context.Context, instanceID, channel string) (*Signal, error) {
	return (signalStore{Storage: s, driver: s.driver, postgres: true}).next(ctx, instanceID, channel)
}
