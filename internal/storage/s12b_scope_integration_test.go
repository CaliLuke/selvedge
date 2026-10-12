//go:build integration

package storage

import "testing"

func TestPostgresScopeCancelChildrenAtomic(t *testing.T) {
	testScopeCancelChildrenAtomic(t, postgresHistoryFactory)
}
