//go:build integration

package selvedge

import "testing"

func TestCoroutineForeignYieldFailsRun_Postgres(t *testing.T) {
	testCoroutineForeignYieldFailsRun(t, createTestAppWithPostgres)
}

func TestCoroutineActivityPanicFailsWithoutRetry_Postgres(t *testing.T) {
	testCoroutineActivityPanicFailsWithoutRetry(t, createTestAppWithPostgres)
}
