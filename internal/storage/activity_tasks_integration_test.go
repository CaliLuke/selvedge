//go:build integration

package storage

import "testing"

func TestPostgresReserveActivityAtomic(t *testing.T) {
	testReserveActivityAtomic(t, postgresHistoryFactory)
}
func TestPostgresActivityTaskClaimFencing(t *testing.T) {
	testActivityTaskClaimFencing(t, postgresHistoryFactory)
}
func TestPostgresActivityClaimExpiryIsStrict(t *testing.T) {
	testActivityClaimExpiryIsStrict(t, postgresHistoryFactory)
}
func TestPostgresActivityFillRequiresClaim(t *testing.T) {
	testActivityFillRequiresClaim(t, postgresHistoryFactory)
}
func TestPostgresCloseCancelsInFlightTasks(t *testing.T) {
	testCloseCancelsInFlightTasks(t, postgresHistoryFactory)
}
