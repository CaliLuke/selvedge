//go:build integration

package selvedge

import "testing"

func TestPostgresFutureKeysFollowIssueOrder(t *testing.T) {
	testFutureKeysFollowIssueOrder(t, createTestAppWithPostgres)
}
func TestPostgresAsyncActivitiesRunConcurrently(t *testing.T) {
	testAsyncActivitiesRunConcurrently(t, createTestAppWithPostgres)
}
func TestPostgresGetYieldsUntilResult(t *testing.T) {
	testGetYieldsUntilResult(t, createTestAppWithPostgres)
}
func TestPostgresFutureGetIsRepeatable(t *testing.T) {
	testFutureGetIsRepeatable(t, createTestAppWithPostgres)
}
func TestPostgresGetOffCoroutineFailsRun(t *testing.T) {
	testGetOffCoroutineFailsRun(t, createTestAppWithPostgres)
}
func TestPostgresRecordedActivityNotRescheduledOnReplayAsync(t *testing.T) {
	testRecordedActivityNotRescheduledOnReplayAsync(t, createTestAppWithPostgres)
}
func TestPostgresHistoryKeyWrittenOnceAsync(t *testing.T) {
	testHistoryKeyWrittenOnceAsync(t, createTestAppWithPostgres)
}
func TestPostgresReplayCommandMismatchFailsFastAsync(t *testing.T) {
	testReplayCommandMismatchFailsFastAsync(t, createTestAppWithPostgres)
}
func TestPostgresUnawaitedFutureClosesNormally(t *testing.T) {
	testUnawaitedFutureClosesNormally(t, createTestAppWithPostgres)
}
func TestPostgresCancelWhileAwaitingActivityRecordsCanceled(t *testing.T) {
	testCancelWhileAwaitingActivityRecordsCanceled(t, createTestAppWithPostgres)
}
func TestPostgresReplayDeterministicAfterCrashAsync(t *testing.T) {
	testReplayDeterministicAfterCrashAsync(t, createTestAppWithPostgres)
}
func TestPostgresAsyncValidationIsSticky(t *testing.T) {
	testAsyncValidationIsSticky(t, createTestAppWithPostgres)
}
func TestPostgresFutureDecodeFailureIsSticky(t *testing.T) {
	testFutureDecodeFailureIsSticky(t, createTestAppWithPostgres)
}
func TestPostgresFutureClosingRejectsCommands(t *testing.T) {
	testFutureClosingRejectsCommands(t, createTestAppWithPostgres)
}
func TestPostgresSyncAsyncShareCommand(t *testing.T) {
	testSyncAsyncShareCommand(t, createTestAppWithPostgres)
}
func TestPostgresFutureMissingTaskDoesNotRun(t *testing.T) {
	testFutureMissingTaskDoesNotRun(t, createTestAppWithPostgres)
}

func TestPostgresFutureGetWaitUsesCurrentPosition(t *testing.T) {
	testFutureGetWaitUsesCurrentPosition(t, createTestAppWithPostgres)
}
func TestPostgresFutureFailureSurfacesAtGetLiveAndReplay(t *testing.T) {
	testFutureFailureSurfacesAtGetLiveAndReplay(t, createTestAppWithPostgres)
}
