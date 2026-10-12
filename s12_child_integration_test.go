//go:build integration

package selvedge

import "testing"

func TestPostgresChildResultDelivery(t *testing.T) {
	testChildResultDelivery(t, createTestAppWithPostgres)
}
func TestPostgresChildIsReadyReadsDeliveredView(t *testing.T) {
	testChildIsReadyReadsDeliveredView(t, createTestAppWithPostgres)
}
func TestPostgresChildCancelCommand(t *testing.T) {
	testChildCancelCommand(t, createTestAppWithPostgres)
}
func TestPostgresCancelEndsBlockedWaitsChild(t *testing.T) {
	testCancelEndsBlockedWaitsChild(t, createTestAppWithPostgres)
}
func TestPostgresChildStartAtomic(t *testing.T) { testChildStartAtomic(t, createTestAppWithPostgres) }
func TestPostgresParentCloseCancelsRunningChild(t *testing.T) {
	testParentCloseCancelsRunningChild(t, createTestAppWithPostgres)
}
func TestPostgresReplayCommandMismatchFailsFastChild(t *testing.T) {
	testReplayCommandMismatchFailsFastChild(t, createTestAppWithPostgres)
}
func TestPostgresStalledExecutionCommitRejectedChild(t *testing.T) {
	testStalledExecutionCommitRejectedChild(t, createTestAppWithPostgres)
}
func TestPostgresChildRunsWithoutParentRegistration(t *testing.T) {
	testChildRunsWithoutParentRegistration(t, createTestAppWithPostgres)
}
func TestPostgresChildGuards(t *testing.T) { testChildGuards(t, createTestAppWithPostgres) }
func TestPostgresReplayDeterministicAfterCrashChild(t *testing.T) {
	testReplayDeterministicAfterCrashChild(t, createTestAppWithPostgres)
}

func TestPostgresChildGetRequestsCancelWhileParentRunning(t *testing.T) {
	testChildGetRequestsCancelWhileParentRunning(t, createTestAppWithPostgres)
}
