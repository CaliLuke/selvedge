//go:build integration

package selvedge

import "testing"

func TestActivityErrorTypePreservedOnReplay_Postgres(t *testing.T) {
	testActivityErrorTypePreservedOnReplay(t, createTestAppWithPostgres)
}
func TestFailedActivityNotRerunOnReplay_Postgres(t *testing.T) {
	testFailedActivityNotRerunOnReplay(t, createTestAppWithPostgres)
}
func TestNonRetryableApplicationErrorNotRetried_Postgres(t *testing.T) {
	testNonRetryableApplicationErrorNotRetried(t, createTestAppWithPostgres)
}
func TestWorkflowResultErrorTyped_Postgres(t *testing.T) {
	testWorkflowResultErrorTyped(t, createTestAppWithPostgres)
}
func TestErrorEnvelopeUnknownKindDecodesGeneric_Postgres(t *testing.T) {
	testErrorEnvelopeUnknownKindDecodesGeneric(t, createTestAppWithPostgres)
}
func TestCorruptFailureFailsRunEvenWhenCaught_Postgres(t *testing.T) {
	testCorruptFailureFailsRunEvenWhenCaught(t, createTestAppWithPostgres)
}
func TestLiveActivityUsesStoredOutcome_Postgres(t *testing.T) {
	testLiveActivityUsesStoredOutcome(t, createTestAppWithPostgres)
}

func TestLiveCorruptFailureFailsRunEvenWhenCaught_Postgres(t *testing.T) {
	testLiveCorruptFailureFailsRunEvenWhenCaught(t, createTestAppWithPostgres)
}
