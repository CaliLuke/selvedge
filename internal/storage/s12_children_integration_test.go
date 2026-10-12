//go:build integration

package storage

import "testing"

func TestPostgresStartChildAtomic(t *testing.T) { testStartChildAtomic(t, postgresHistoryFactory) }
func TestPostgresChildResultFillOnClose(t *testing.T) {
	testChildResultFillOnClose(t, postgresHistoryFactory)
}
func TestPostgresParentCloseRequestsChildCancel(t *testing.T) {
	testParentCloseRequestsChildCancel(t, postgresHistoryFactory)
}
func TestPostgresParentChildLockOrder(t *testing.T) {
	testParentChildLockOrder(t, postgresHistoryFactory)
}
