//go:build integration

package selvedge

import "testing"

func TestSwallowedYieldDoesNotRunLaterCommands_Postgres(t *testing.T) {
	testSwallowedYieldDoesNotRunLaterCommands(t, createTestAppWithPostgres)
}
func TestDeferredCommandAfterYieldIsRejected_Postgres(t *testing.T) {
	testDeferredCommandAfterYieldIsRejected(t, createTestAppWithPostgres)
}
func TestCommandOffCoroutineFailsRun_Postgres(t *testing.T) {
	testCommandOffCoroutineFailsRun(t, createTestAppWithPostgres)
}
func TestWorkflowPanicFailsRun_Postgres(t *testing.T) {
	testWorkflowPanicFailsRun(t, createTestAppWithPostgres)
}
func TestCoroutineExitsBeforeLeaseRelease_Postgres(t *testing.T) {
	testCoroutineExitsBeforeLeaseRelease(t, createTestAppWithPostgres)
}

func TestCoroutineAbnormalExitFailsRun_Postgres(t *testing.T) {
	testCoroutineAbnormalExitFailsRun(t, createTestAppWithPostgres)
}
