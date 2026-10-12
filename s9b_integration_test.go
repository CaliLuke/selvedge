//go:build integration

package selvedge

import "testing"

func TestPostgresNewTimerFuture(t *testing.T) { testNewTimerFuture(t, createTestAppWithPostgres) }
func TestPostgresCancelEndsBlockedWaitsTimers(t *testing.T) {
	testCancelEndsBlockedWaitsTimers(t, createTestAppWithPostgres)
}
func TestPostgresTimerReplayCompatibility(t *testing.T) {
	testTimerReplayCompatibility(t, createTestAppWithPostgres)
}
func TestPostgresReceiveWithTimeoutNonPositiveIsRecorded(t *testing.T) {
	testReceiveWithTimeoutNonPositiveIsRecorded(t, createTestAppWithPostgres)
}
func TestPostgresReceiveWithTimeoutLosingTimerDoesNotWake(t *testing.T) {
	testReceiveWithTimeoutLosingTimerDoesNotWake(t, createTestAppWithPostgres)
}
func TestPostgresReceiveWithTimeoutTimesOut(t *testing.T) {
	testReceiveWithTimeoutTimesOut(t, createTestAppWithPostgres)
}
func TestPostgresReplayCommandMismatchFailsFastTimeout(t *testing.T) {
	testReplayCommandMismatchFailsFastTimeout(t, createTestAppWithPostgres)
}
func TestPostgresReplayDeterministicAfterCrashTimeout(t *testing.T) {
	testReplayDeterministicAfterCrashTimeout(t, createTestAppWithPostgres)
}
func TestPostgresTimerGuards(t *testing.T) { testTimerGuards(t, createTestAppWithPostgres) }
func TestPostgresTimerGetFireWinsCancel(t *testing.T) {
	testTimerGetFireWinsCancel(t, createTestAppWithPostgres)
}

func TestPostgresReceiveWithTimeoutCancelPriority(t *testing.T) {
	testReceiveWithTimeoutCancelPriority(t, createTestAppWithPostgres)
}
func TestPostgresSleepRejectsSupersededTimeout(t *testing.T) {
	testSleepRejectsSupersededTimeout(t, createTestAppWithPostgres)
}
func TestPostgresReceiveWithTimeoutConsistentReads(t *testing.T) {
	testReceiveWithTimeoutConsistentReads(t, createTestAppWithPostgres)
}
