//go:build integration

package selvedge

import "testing"

func TestPostgresReplayDeterministicAfterCrashActivityTimeout(t *testing.T) {
	testReplayDeterministicAfterCrashActivityTimeout(t, createTestAppWithPostgres)
}
func TestPostgresActivityTimeoutLeaderSweep(t *testing.T) {
	testActivityTimeoutLeaderSweep(t, createTestAppWithPostgres)
}

func TestPostgresReplayHeartbeatLapseDifferentResults(t *testing.T) {
	testFutureGetReadsHistoryWithHeartbeat(t, createTestAppWithPostgres, true)
}
