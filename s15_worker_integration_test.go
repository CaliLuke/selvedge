//go:build integration

package selvedge

import "testing"

func TestPostgresActivityAttemptNumbers(t *testing.T) {
	testActivityAttemptNumbers(t, createTestAppWithPostgres)
}
func TestPostgresActivityQueues(t *testing.T) { testActivityQueues(t, createTestAppWithPostgres) }
func TestPostgresRetryBackoffHonoured(t *testing.T) {
	testRetryBackoffHonoured(t, createTestAppWithPostgres)
}

func TestPostgresRetrySurvivesAppRestart(t *testing.T) {
	testRetrySurvivesAppRestart(t, createTestAppWithPostgres)
}
