//go:build integration

package selvedge

import "testing"

func TestPostgresActivityStartsOnlyFromTaskRow(t *testing.T) {
	testActivityStartsOnlyFromTaskRow(t, createTestAppWithPostgres)
}
func TestPostgresFutureGetReadsHistory(t *testing.T) {
	testFutureGetReadsHistory(t, createTestAppWithPostgres)
}
func TestPostgresActivityResultDuringSuspendIsNotLost(t *testing.T) {
	testActivityResultDuringSuspendIsNotLost(t, createTestAppWithPostgres)
}
func TestPostgresActivityPanicOnWorkerFailsRun(t *testing.T) {
	testActivityPanicOnWorkerFailsRun(t, createTestAppWithPostgres)
}
func TestPostgresActivityRegistryConflict(t *testing.T) {
	testActivityRegistryConflict(t, createTestAppWithPostgres)
}
func TestPostgresActivityShutdownReleasesClaim(t *testing.T) {
	testActivityShutdownReleasesClaim(t, createTestAppWithPostgres)
}

func TestPostgresActivityPoolHonorsLimitAndNames(t *testing.T) {
	testActivityPoolHonorsLimitAndNames(t, createTestAppWithPostgres)
}

func TestPostgresActivityShutdownBeforeStartReleasesClaim(t *testing.T) {
	testActivityShutdownBeforeStartReleasesClaim(t, createTestAppWithPostgres)
}

func TestCloseCancelsInFlightTasks_Postgres(t *testing.T) {
	testCloseCancelsInFlightTasks(t, createTestAppWithPostgres)
}
