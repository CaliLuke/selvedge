//go:build integration

package selvedge

import "testing"

func TestTimerFiresAfterNowAndNowAfterFire_Postgres(t *testing.T) {
	testTimerFiresAfterNowAndNowAfterFire(t, createTestAppWithPostgres)
}
