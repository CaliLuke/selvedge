//go:build integration

package selvedge

import "testing"

func TestStartByNameWithoutRegistration_Postgres(t *testing.T) {
	s13StartByName(t, createTestAppWithPostgres)
}
func TestStartRejectedBeforeInsert_Postgres(t *testing.T) {
	s13StartRejected(t, createTestAppWithPostgres)
}
func TestStartWorkflowDuplicateIDRejected_Concurrent_Postgres(t *testing.T) {
	s13DuplicateConcurrent(t, createTestAppWithPostgres)
}
func TestStartWorkflowDuplicateIDRejected_Closed_Postgres(t *testing.T) {
	s13DuplicateClosed(t, createTestAppWithPostgres)
}
func TestStartWorkflowDuplicateIDRejected_ChildID_Postgres(t *testing.T) {
	s13DuplicateChild(t, createTestAppWithPostgres)
}
func TestStartWorkflowDuplicateIDRejected_LostAck_Postgres(t *testing.T) {
	s13LostAck(t, createTestAppWithPostgres)
}
func TestByIDErrorsTyped_Postgres(t *testing.T) { s13ByIDErrors(t, createTestAppWithPostgres) }
func TestSignalPayloadEncodeFailureRejected_Postgres(t *testing.T) {
	s13SignalEncode(t, createTestAppWithPostgres)
}
func TestWaitWorkflowTerminalOutcomes_Postgres(t *testing.T) {
	s13TerminalOutcomes(t, createTestAppWithPostgres)
}
func TestOutOfProcessTypedSignals_Postgres(t *testing.T) {
	s13TypedSignals(t, createTestAppWithPostgres)
}
func TestSignalBeforeFirstReceiveIsDelivered_FromClientApp_Postgres(t *testing.T) {
	s13SignalBeforeReceive(t, createTestAppWithPostgres)
}
func TestInputDuringSuspendIsNotLost_FromClientApp_Postgres(t *testing.T) {
	s13DuringSuspend(t, createTestAppWithPostgres)
}
func TestPendingAndReceiveAsyncAgree_FromClientApp_Postgres(t *testing.T) {
	s13PendingAsync(t, createTestAppWithPostgres)
}
func TestCrossAppWakeSameProcess_Postgres(t *testing.T) {
	s13CrossAppWake(t, createTestAppWithPostgres)
}
func TestDurableRestartHandleStaysUsable_Postgres(t *testing.T) {
	s13Restart(t, createTestAppWithPostgres)
}
func TestConcurrentSendersKeepChannelOrder_Postgres(t *testing.T) {
	s13ConcurrentSenders(t, createTestAppWithPostgres)
}
func TestChannelsHaveSeparateCursors_LoomChannels_Postgres(t *testing.T) {
	s13SeparateChannels(t, createTestAppWithPostgres)
}
func TestCancelWhileSleepingStaysCancelled_FromClientApp_Postgres(t *testing.T) {
	s13CancelBlocked(t, createTestAppWithPostgres, []string{"sleep", "receive", "child"})
}
func TestCancelEndsBlockedWaits_FromClientApp_Postgres(t *testing.T) {
	s13CancelBlocked(t, createTestAppWithPostgres, []string{"await", "timer", "timeout"})
}
func TestWaitWorkflowOutputDecodeFailure_Postgres(t *testing.T) {
	s13WaitDecodeFailure(t, createTestAppWithPostgres)
}
func TestClientAfterFailedStart_Postgres(t *testing.T) { s13FailedStart(t, createTestAppWithPostgres) }
