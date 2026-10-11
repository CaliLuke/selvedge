//go:build integration

package selvedge

import "testing"

func TestTypedReplayStructResult_Postgres(t *testing.T) {
	testTypedReplayStructResult(t, createTestAppWithPostgres)
}
func TestTypedReplayInt64Precision_Postgres(t *testing.T) {
	testTypedReplayInt64Precision(t, createTestAppWithPostgres)
}
func TestLiveValueEqualsRecordedValue_Postgres(t *testing.T) {
	testLiveValueEqualsRecordedValue(t, createTestAppWithPostgres)
}
func TestReplayDecodeErrorFailsRun_Postgres(t *testing.T) {
	testReplayDecodeErrorFailsRun(t, createTestAppWithPostgres)
}
func TestRecordedActivityNotRescheduledOnReplay_Postgres(t *testing.T) {
	testRecordedActivityNotRescheduledOnReplay(t, createTestAppWithPostgres)
}

func TestLiveDecodeErrorFailsRunEvenWhenCaught_Postgres(t *testing.T) {
	testLiveDecodeErrorFailsRunEvenWhenCaught(t, createTestAppWithPostgres)
}
