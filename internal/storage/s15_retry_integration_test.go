//go:build integration

package storage

import "testing"

func TestPostgresRetryActivityTaskFencing(t *testing.T) {
	testRetryActivityTaskFencing(t, postgresHistoryFactory)
}
func TestPostgresClaimEligibility(t *testing.T) { testClaimEligibility(t, postgresHistoryFactory) }

func TestPostgresRetryBackoffRoundsUp(t *testing.T) {
	testRetryBackoffRoundsUp(t, postgresHistoryFactory)
}
