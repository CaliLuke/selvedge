//go:build integration

package selvedge

import "testing"

func TestCancelWhileSleepingStaysCancelledIntegration(t *testing.T) {
	testCancelWhileSleepingStaysCancelled(t, createTestAppWithPostgres)
}
func TestWakeCancelRacesTimerFireIntegration(t *testing.T) {
	testWakeCancelRacesTimerFire(t, createTestAppWithPostgres)
}
func TestTimerFireRecordedOnceIntegration(t *testing.T) {
	testTimerFireRecordedOnce(t, createTestAppWithPostgres)
}
func TestStaleExecutionCannotOverwriteClosedStatusIntegration(t *testing.T) {
	testStaleExecutionCannotOverwriteClosedStatus(t, createTestAppWithPostgres)
}
func TestCancelLeasedInstanceSucceedsIntegration(t *testing.T) {
	testCancelLeasedInstanceSucceeds(t, createTestAppWithPostgres)
}
func TestCancelOutcomesTypedIntegration(t *testing.T) {
	testCancelOutcomesTyped(t, createTestAppWithPostgres)
}
func TestAbandonedRunIsRedispatchedPendingCancelIntegration(t *testing.T) {
	testAbandonedRunIsRedispatchedPendingCancel(t, createTestAppWithPostgres)
}
func TestCaughtCancelCompletesIntegration(t *testing.T) {
	testCaughtCancelCompletes(t, createTestAppWithPostgres)
}
func TestReceiveObservesCancelIntegration(t *testing.T) {
	testReceiveObservesCancel(t, createTestAppWithPostgres)
}
func TestStickyFatalStopsYieldsIntegration(t *testing.T) {
	testStickyFatalStopsYields(t, createTestAppWithPostgres)
}
func TestSleepReservationAtomicAndExpiryStableIntegration(t *testing.T) {
	testSleepReservationAtomicAndExpiryStable(t, createTestAppWithPostgres)
}

func TestCancelObservationReplaysIntegration(t *testing.T) {
	testCancelObservationReplays(t, createTestAppWithPostgres)
}
func TestCloseInstanceAtomicIntegration(t *testing.T) {
	testCloseInstanceAtomic(t, createTestAppWithPostgres)
}

func TestCancelHookAfterCloseIntegration(t *testing.T) {
	testCancelHookAfterClose(t, createTestAppWithPostgres)
}
