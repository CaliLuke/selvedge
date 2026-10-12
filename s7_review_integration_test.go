//go:build integration

package selvedge

import "testing"

func TestLeftoverHistoryGapFailsWithoutPanic_Postgres(t *testing.T) {
	testLeftoverHistoryGapFailsWithoutPanic(t, createTestAppWithPostgres)
}
func TestUnfilledActivityRecoveryRunsLive_Postgres(t *testing.T) {
	testUnfilledActivityRecoveryRunsLive(t, createTestAppWithPostgres)
}
