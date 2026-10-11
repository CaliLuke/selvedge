//go:build integration

package selvedge

import "testing"

func TestLeaseExpiryIsStrictPostgres(t *testing.T) {
	testLeaseExpiryIsStrict(t, createTestAppWithPostgres)
}
func TestExpiredLeaseRecoveredWithoutFlagPostgres(t *testing.T) {
	testExpiredLeaseRecoveredWithoutFlag(t, createTestAppWithPostgres)
}
func TestLeaseRenewalRequiresUnexpiredLeasePostgres(t *testing.T) {
	testLeaseRenewalRequiresUnexpiredLease(t, createTestAppWithPostgres)
}
func TestLeaseReleasePreservesInputPostgres(t *testing.T) {
	testLeaseReleasePreservesInput(t, createTestAppWithPostgres)
}
func TestStalledExecutionCommitRejectedPostgres(t *testing.T) {
	testStalledExecutionCommitRejected(t, createTestAppWithPostgres)
}
func TestCommandWritesRequireFencePostgres(t *testing.T) {
	testCommandWritesRequireFence(t, createTestAppWithPostgres)
}
func TestTimerFireRollsBackAsOneWritePostgres(t *testing.T) {
	testTimerFireRollsBackAsOneWrite(t, createTestAppWithPostgres)
}
func TestLeaseTTLDoesNotRoundDownPostgres(t *testing.T) {
	testLeaseTTLDoesNotRoundDown(t, createTestAppWithPostgres)
}
