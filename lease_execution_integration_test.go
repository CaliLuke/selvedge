//go:build integration

package selvedge

import "testing"

func TestLeaseCheckStopsBeforeTakeoverPostgres(t *testing.T) {
	testLeaseCheckStopsBeforeTakeover(t, createTestAppWithPostgres)
}
func TestTwoHandlersOneProcessDoNotShareLeasePostgres(t *testing.T) {
	testTwoHandlersOneProcessDoNotShareLease(t, createTestAppWithPostgres)
}
func TestInputDuringSuspendIsNotLostPostgres(t *testing.T) {
	testInputDuringSuspendIsNotLost(t, createTestAppWithPostgres)
}
func TestAbandonedRunIsRedispatchedPostgres(t *testing.T) {
	testAbandonedRunIsRedispatched(t, createTestAppWithPostgres)
}
func TestTakeoverAfterCrashCompletesPostgres(t *testing.T) {
	testTakeoverAfterCrashCompletes(t, createTestAppWithPostgres)
}
func TestRunsTerminateWithoutFaultsPostgres(t *testing.T) {
	testRunsTerminateWithoutFaults(t, createTestAppWithPostgres)
}
func TestLeaseRenewedDuringLongActivityPostgres(t *testing.T) {
	testLeaseRenewedDuringLongActivity(t, createTestAppWithPostgres)
}
func TestLeaseRequestLatencyCountsAgainstBudgetPostgres(t *testing.T) {
	testLeaseRequestLatencyCountsAgainstBudget(t, createTestAppWithPostgres)
}
func TestNewWorkflowWaitsForDispatcherPostgres(t *testing.T) {
	testNewWorkflowWaitsForDispatcher(t, createTestAppWithPostgres)
}
func TestLeaderLeasePreservesMillisecondsPostgres(t *testing.T) {
	testLeaderLeasePreservesMilliseconds(t, createTestAppWithPostgres)
}
func TestSwallowedFenceRejectionStopsExecutionPostgres(t *testing.T) {
	testSwallowedFenceRejectionStopsExecution(t, createTestAppWithPostgres)
}
func TestReplayDeterministicAfterCrashPostgres(t *testing.T) {
	testReplayDeterministicAfterCrash(t, createTestAppWithPostgres)
}
func TestMessageDuringSuspendIsNotLostPostgres(t *testing.T) {
	testMessageDuringSuspendIsNotLost(t, createTestAppWithPostgres)
}
func TestShutdownAbandonsLeasePostgres(t *testing.T) {
	testShutdownAbandonsLease(t, createTestAppWithPostgres)
}
func TestDispatcherAdmitsWorkDuringLongExecutionPostgres(t *testing.T) {
	testDispatcherAdmitsWorkDuringLongExecution(t, createTestAppWithPostgres)
}
