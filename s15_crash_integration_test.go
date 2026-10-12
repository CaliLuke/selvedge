//go:build integration

package selvedge

import "testing"

func TestPostgresReplayDeterministicAfterCrashRetry(t *testing.T) {
	testReplayDeterministicAfterCrashRetry(t, createTestAppWithPostgres)
}
