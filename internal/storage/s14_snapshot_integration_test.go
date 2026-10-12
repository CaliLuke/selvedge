//go:build integration

package storage

import "testing"

func TestPostgresReadSnapshotIsolated(t *testing.T) {
	s14TestReadSnapshotIsolated(t, postgresHistoryFactory)
}
