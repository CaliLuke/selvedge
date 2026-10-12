//go:build integration

package storage

import "testing"

func TestPostgresCommitObservationScopeCancelAtomic(t *testing.T) {
	testCommitObservationScopeCancelAtomic(t, postgresHistoryFactory)
}

func TestPostgresScopeCancelUpdatesTaskTimestamp(t *testing.T) {
	testScopeCancelUpdatesTaskTimestamp(t, postgresHistoryFactory)
}
