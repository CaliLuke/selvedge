//go:build integration

package selvedge

import "testing"

func TestNowRecordedOnce_Postgres(t *testing.T) { testNowRecordedOnce(t, createTestAppWithPostgres) }

func TestNowMonotonic_Postgres(t *testing.T) { testNowMonotonic(t, createTestAppWithPostgres) }

func TestNowReplayValueIdentical_Postgres(t *testing.T) {
	testNowReplayValueIdentical(t, createTestAppWithPostgres)
}

func TestNowOffCoroutineAndClosing_Postgres(t *testing.T) {
	testNowOffCoroutineAndClosing(t, createTestAppWithPostgres)
}

func TestNowErrorsReturnFloorAndStaySticky_Postgres(t *testing.T) {
	testNowErrorsReturnFloorAndStaySticky(t, createTestAppWithPostgres)
}

func TestReplayCommandMismatchFailsFast_Now_Postgres(t *testing.T) {
	testReplayCommandMismatchFailsFastNow(t, createTestAppWithPostgres)
}

func TestStalledExecutionCommitRejected_Now_Postgres(t *testing.T) {
	testStalledExecutionCommitRejectedNow(t, createTestAppWithPostgres)
}

func TestReplayDeterministicAfterCrash_DeadlinePassesBetweenRuns_Postgres(t *testing.T) {
	testReplayDeterministicAfterCrashDeadlinePassesBetweenRuns(t, createTestAppWithPostgres)
}

func TestNowUsesStoredValueOnConflict_Postgres(t *testing.T) {
	testNowUsesStoredValueOnConflict(t, createTestAppWithPostgres)
}
