//go:build integration

package storage

import "testing"

func TestPostgresCommitObservationAtomic(t *testing.T) {
	testCommitObservationAtomic(t, postgresHistoryFactory)
}
func TestPostgresDeliveryConstraints(t *testing.T) {
	testDeliveryConstraints(t, postgresHistoryFactory)
}
func TestPostgresDeliveredOnce(t *testing.T) { testDeliveredOnce(t, postgresHistoryFactory) }

func TestPostgresDeliverySequencesAreInt64(t *testing.T) {
	testDeliverySequencesAreInt64(t, postgresHistoryFactory)
}
