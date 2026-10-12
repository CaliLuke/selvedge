//go:build integration

package selvedge

import "testing"

func TestPostgresCancelScopeTimer(t *testing.T) { testCancelScopeTimer(t, createTestAppWithPostgres) }
func TestPostgresCommandsInCancelledScopeRecordAtIssue(t *testing.T) {
	testCommandsInCancelledScopeRecordAtIssue(t, createTestAppWithPostgres)
}
func TestPostgresDetachedCleanupRunsAfterCancel(t *testing.T) {
	testDetachedCleanupRunsAfterCancel(t, createTestAppWithPostgres)
}
func TestPostgresNestedScopesAndDetached(t *testing.T) {
	testNestedScopesAndDetached(t, createTestAppWithPostgres)
}
func TestPostgresScopeCancelErrorTyped(t *testing.T) {
	testScopeCancelErrorTyped(t, createTestAppWithPostgres)
}
func TestPostgresCancelFuncGuards(t *testing.T) { testCancelFuncGuards(t, createTestAppWithPostgres) }
func TestPostgresFutureCancellationFollowsIssuingScope(t *testing.T) {
	testFutureCancellationFollowsIssuingScope(t, createTestAppWithPostgres)
}
func TestPostgresCancelEndsBlockedWaitsScopes(t *testing.T) {
	testCancelEndsBlockedWaitsScopes(t, createTestAppWithPostgres)
}
func TestPostgresReplayCommandMismatchFailsFastScope(t *testing.T) {
	testReplayCommandMismatchFailsFastScope(t, createTestAppWithPostgres)
}

func TestPostgresCancelScopeCancelsInFlightActivities(t *testing.T) {
	testCancelScopeCancelsInFlightActivities(t, createTestAppWithPostgres)
}

func TestPostgresCancelScopeStoredValueWins(t *testing.T) {
	testCancelScopeStoredValueWins(t, createTestAppWithPostgres)
}

func TestPostgresStalledExecutionCommitRejectedScope(t *testing.T) {
	testStalledExecutionCommitRejectedScope(t, createTestAppWithPostgres)
}

func TestPostgresReplayDeterministicAfterCrashScope(t *testing.T) {
	testReplayDeterministicAfterCrashScope(t, createTestAppWithPostgres)
}

func TestPostgresScopeCancelPreservesTimerStartOnReplay(t *testing.T) {
	testScopeCancelPreservesTimerStartOnReplay(t, createTestAppWithPostgres)
}

func TestPostgresScopeCancelTracksSleep(t *testing.T) {
	testScopeCancelTracksSleep(t, createTestAppWithPostgres)
}

func TestPostgresScopeTargetsOnlyCancellableScopes(t *testing.T) {
	testScopeTargetsOnlyCancellableScopes(t, createTestAppWithPostgres)
}
