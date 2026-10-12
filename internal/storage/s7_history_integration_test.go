//go:build integration

package storage

import "testing"

func postgresHistoryFactory(t *testing.T) Storage {
	s, cleanup := setupPostgresContainer(t)
	t.Cleanup(cleanup)
	return s
}
func TestPostgresClassThreeFillRequiresReservation(t *testing.T) {
	testClassThreeFillRequiresReservation(t, postgresHistoryFactory)
}
func TestPostgresHistorySeqConstraints(t *testing.T) {
	testHistorySeqConstraints(t, postgresHistoryFactory)
}
