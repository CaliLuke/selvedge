//go:build integration

package selvedge

import "testing"

func TestPostgresIsReadyReadsDeliveredView(t *testing.T) {
	testIsReadyReadsDeliveredView(t, createTestAppWithPostgres)
}
func TestPostgresPendingReadsDeliveredView(t *testing.T) {
	testPendingReadsDeliveredView(t, createTestAppWithPostgres)
}
func TestPostgresPendingUsesConsumptionPositions(t *testing.T) {
	testPendingUsesConsumptionPositions(t, createTestAppWithPostgres)
}
func TestPostgresIsReadyAfterReplayedWake(t *testing.T) {
	testIsReadyAfterReplayedWake(t, createTestAppWithPostgres)
}
func TestPostgresGetDoesNotDeliver(t *testing.T) { testGetDoesNotDeliver(t, createTestAppWithPostgres) }
func TestPostgresReceiveDeliversBatch(t *testing.T) {
	testReceiveDeliversBatch(t, createTestAppWithPostgres)
}
func TestPostgresAwaitImmediatelyTrueWritesNothing(t *testing.T) {
	testAwaitImmediatelyTrueWritesNothing(t, createTestAppWithPostgres)
}
func TestPostgresAwaitDecisionRecorded(t *testing.T) {
	testAwaitDecisionRecorded(t, createTestAppWithPostgres)
}
func TestPostgresAwaitPredicateCommandFailsRun(t *testing.T) {
	testAwaitPredicateCommandFailsRun(t, createTestAppWithPostgres)
}
func TestPostgresAwaitParallelFutures(t *testing.T) {
	testAwaitParallelFutures(t, createTestAppWithPostgres)
}
func TestPostgresCancelEndsBlockedWaitsAwait(t *testing.T) {
	testCancelEndsBlockedWaitsAwait(t, createTestAppWithPostgres)
}

func TestPostgresPendingAndReceiveAsyncAgree(t *testing.T) {
	testPendingAndReceiveAsyncAgree(t, createTestAppWithPostgres)
}

func TestPostgresReplayCommandMismatchFailsFastAwait(t *testing.T) {
	testReplayCommandMismatchFailsFastAwait(t, createTestAppWithPostgres)
}

func TestPostgresStalledExecutionCommitRejectedAwait(t *testing.T) {
	testStalledExecutionCommitRejectedAwait(t, createTestAppWithPostgres)
}

func TestPostgresReplayDeterministicAfterCrashDelivery(t *testing.T) {
	testReplayDeterministicAfterCrashDelivery(t, createTestAppWithPostgres)
}

func TestPostgresAwaitPredicateBeforeCancel(t *testing.T) {
	testAwaitPredicateBeforeCancel(t, createTestAppWithPostgres)
}
