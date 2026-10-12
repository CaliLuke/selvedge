//go:build integration

package selvedge

import "testing"

func TestPostgresCancelScopeCancelsChildren(t *testing.T) {
	testCancelScopeCancelsChildren(t, createTestAppWithPostgres)
}
func TestPostgresStartChildInCancelledScope(t *testing.T) {
	testStartChildInCancelledScope(t, createTestAppWithPostgres)
}
func TestPostgresDetachedChildGetIgnoresParentCancel(t *testing.T) {
	testDetachedChildGetIgnoresParentCancel(t, createTestAppWithPostgres)
}
func TestPostgresCancelScopeChildrenStoredResultWins(t *testing.T) {
	testCancelScopeChildrenStoredResultWins(t, createTestAppWithPostgres)
}

func TestPostgresReplayDeterministicAfterCrashChildScopes(t *testing.T) {
	testReplayDeterministicAfterCrashChildScopes(t, createTestAppWithPostgres)
}
