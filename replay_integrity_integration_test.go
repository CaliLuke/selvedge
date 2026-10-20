//go:build integration

package selvedge

import "testing"

func TestPostgresReplayRejectsChangedChildCancellationTarget(t *testing.T) {
	testReplayRejectsChangedChildCancellationTarget(t, createTestAppWithPostgres)
}

func TestPostgresReplayRejectsChangedScopeCancellationTargets(t *testing.T) {
	testReplayRejectsChangedScopeCancellationTargets(t, createTestAppWithPostgres)
}
