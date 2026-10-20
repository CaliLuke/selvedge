//go:build integration

package storage

import "testing"

func TestPostgresTransactionContextDoesNotRedirectAnotherDatabase(t *testing.T) {
	testTransactionContextDoesNotRedirectAnotherDatabase(t, postgresHistoryFactory)
}
