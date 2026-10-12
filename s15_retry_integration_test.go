//go:build integration

package selvedge

import "testing"

func TestPostgresRetryIsDurable(t *testing.T) { testRetryIsDurable(t, createTestAppWithPostgres) }
func TestPostgresRecordedOptionsDriveRetries(t *testing.T) {
	testRecordedOptionsDriveRetries(t, createTestAppWithPostgres)
}
func TestPostgresRetryExhaustedRecorded(t *testing.T) {
	testRetryExhaustedRecorded(t, createTestAppWithPostgres)
}
func TestPostgresNonRetryableStopsRetries(t *testing.T) {
	testNonRetryableStopsRetries(t, createTestAppWithPostgres)
}
func TestPostgresRetryStopsOnCancel(t *testing.T) {
	testRetryStopsOnCancel(t, createTestAppWithPostgres)
}
