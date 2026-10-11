//go:build integration

package selvedge

import "testing"

func TestHistoryKeyWrittenOnce_Postgres(t *testing.T) {
	testHistoryKeyWrittenOnce(t, createTestAppWithPostgres)
}
func TestReplayCommandMismatchFailsFast_Postgres(t *testing.T) {
	testReplayCommandMismatchFailsFast(t, createTestAppWithPostgres)
}
func TestHistoryWriteClassesStorageConformance_Postgres(t *testing.T) {
	testHistoryWriteClassesStorageConformance(t, createTestAppWithPostgres)
}
func TestStartWorkflowDuplicateIDRejected_Postgres(t *testing.T) {
	testStartWorkflowDuplicateIDRejected(t, createTestAppWithPostgres)
}
func TestReservedActivityIDsRejected_Postgres(t *testing.T) {
	testReservedActivityIDsRejected(t, createTestAppWithPostgres)
}
func TestWorkflowFailureWriteErrorReported_Postgres(t *testing.T) {
	testWorkflowFailureWriteErrorReported(t, createTestAppWithPostgres)
}
func TestHistoryConcurrentFirstWriterWins_Postgres(t *testing.T) {
	testHistoryConcurrentFirstWriterWins(t, createTestAppWithPostgres)
}
func TestDuplicateClaimPreservesOriginalCommand_Postgres(t *testing.T) {
	testDuplicateClaimPreservesOriginalCommand(t, createTestAppWithPostgres)
}
func TestSleepReservationReplaysUntilFilled_Postgres(t *testing.T) {
	testSleepReservationReplaysUntilFilled(t, createTestAppWithPostgres)
}
