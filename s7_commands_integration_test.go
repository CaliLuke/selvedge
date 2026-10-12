//go:build integration

package selvedge

import "testing"

func TestPostgresKeysFollowIssueOrderAcrossCommands(t *testing.T) {
	testKeysFollowIssueOrderAcrossCommands(t, createTestAppWithPostgres)
}
func TestPostgresSleepUntilPastDeadlineKeepsCommandOrder(t *testing.T) {
	testSleepUntilPastDeadlineKeepsCommandOrder(t, createTestAppWithPostgres)
}
func TestPostgresIsReplayIsPerPosition(t *testing.T) {
	testIsReplayIsPerPosition(t, createTestAppWithPostgres)
}
