//go:build integration

package selvedge

import "testing"

func TestPostgresHistoryExportRoundTrip(t *testing.T) {
	s16TestHistoryExportRoundTrip(t, createTestAppWithPostgres)
}
func TestPostgresReplayHarnessPreservesWorkflowError(t *testing.T) {
	s16TestReplayHarnessPreservesWorkflowError(t, createTestAppWithPostgres)
}
func TestPostgresReplayHarnessRunningInstance(t *testing.T) {
	s16TestReplayHarnessRunningInstance(t, createTestAppWithPostgres)
}
func TestPostgresReplayHarnessOutcomeMismatch(t *testing.T) {
	s16TestReplayHarnessOutcomeMismatch(t, createTestAppWithPostgres)
}
func TestPostgresReplayHarnessDetectsDivergence(t *testing.T) {
	s16TestReplayHarnessDetectsDivergence(t, createTestAppWithPostgres)
}
func TestPostgresReplayHarnessCommandCatalogue(t *testing.T) {
	s16TestReplayHarnessCommandCatalogue(t, createTestAppWithPostgres)
}
func TestPostgresReplayHarnessWritesNothing(t *testing.T) {
	s16TestReplayHarnessWritesNothing(t, createTestAppWithPostgres)
}
func TestPostgresReplayInstanceFromDatabase(t *testing.T) {
	s16TestReplayInstanceFromDatabase(t, createTestAppWithPostgres)
}
func TestPostgresHistoryExportPreservesMissingValues(t *testing.T) {
	s16TestHistoryExportPreservesMissingValues(t, createTestAppWithPostgres)
}
func TestPostgresHistoryExportRetainsEventFields(t *testing.T) {
	s16TestHistoryExportRetainsEventFields(t, createTestAppWithPostgres)
}
