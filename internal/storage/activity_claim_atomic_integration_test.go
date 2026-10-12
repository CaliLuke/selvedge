//go:build integration

package storage

import "testing"

func TestPostgresActivityClaimAcquisitionAtomic(t *testing.T) {
	testActivityClaimAcquisitionAtomic(t, postgresHistoryFactory)
}
func TestPostgresActivityClaimCallerTransaction(t *testing.T) {
	testActivityClaimCallerTransaction(t, postgresHistoryFactory)
}

func TestPostgresActivityClaimCommittedHandoff(t *testing.T) {
	testActivityClaimCommittedHandoff(t, postgresHistoryFactory)
}
func TestPostgresActivityClaimRollbackFailure(t *testing.T) {
	testActivityClaimRollbackFailure(t, postgresHistoryFactory)
}
