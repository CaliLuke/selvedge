//go:build integration

package selvedge

import "testing"

func TestSignalBeforeFirstReceiveIsDelivered_Postgres(t *testing.T) {
	testSignalBeforeFirstReceiveIsDelivered(t, createTestAppWithPostgres)
}
func TestChannelsHaveSeparateCursors_Postgres(t *testing.T) {
	testChannelsHaveSeparateCursors(t, createTestAppWithPostgres)
}
func TestSignalReplayReadsHistory_Postgres(t *testing.T) {
	testSignalReplayReadsHistory(t, createTestAppWithPostgres)
}
func TestConsumeAtomicWithRecord_Postgres(t *testing.T) {
	testConsumeAtomicWithRecord(t, createTestAppWithPostgres)
}
func TestSignalToClosedOrUnknownInstanceTyped_Postgres(t *testing.T) {
	testSignalToClosedOrUnknownInstanceTyped(t, createTestAppWithPostgres)
}
func TestCancelWhileReceivingStaysCancelled_Postgres(t *testing.T) {
	testCancelWhileReceivingStaysCancelled(t, createTestAppWithPostgres)
}
func TestSignalInputDuringSuspendIsNotLost_Postgres(t *testing.T) {
	testSignalInputDuringSuspendIsNotLost(t, createTestAppWithPostgres)
}
func TestAbandonedRunWithSignalAndCancelIsRedispatched_Postgres(t *testing.T) {
	testAbandonedRunWithSignalAndCancelIsRedispatched(t, createTestAppWithPostgres)
}
func TestSignalsNotLostRandomized_Postgres(t *testing.T) {
	testSignalsNotLostRandomized(t, createTestAppWithPostgres)
}
func TestTypedSignalPayload_Postgres(t *testing.T) {
	testTypedSignalPayload(t, createTestAppWithPostgres)
}

func TestEmptySignalInboxParks_Postgres(t *testing.T) {
	testEmptySignalInboxParks(t, createTestAppWithPostgres)
}
