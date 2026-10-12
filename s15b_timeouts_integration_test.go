//go:build integration

package selvedge

import "testing"

func TestPostgresScheduleToStartTimeout(t *testing.T) {
	testScheduleToStartTimeout(t, createTestAppWithPostgres)
}
func TestPostgresRecordHeartbeatAPI(t *testing.T) {
	testRecordHeartbeatAPI(t, createTestAppWithPostgres)
}
func TestPostgresHeartbeatTimeoutStartsNewAttempt(t *testing.T) {
	testHeartbeatTimeoutStartsNewAttempt(t, createTestAppWithPostgres)
}
func TestPostgresStartToCloseTimeout(t *testing.T) {
	testStartToCloseTimeout(t, createTestAppWithPostgres)
}
func TestPostgresActivityTimeoutRecordedOnce(t *testing.T) {
	testActivityTimeoutRecordedOnce(t, createTestAppWithPostgres)
}
func TestPostgresActivityHeartbeatKeepsClaim(t *testing.T) {
	testActivityHeartbeatKeepsClaim(t, createTestAppWithPostgres)
}
func TestPostgresActivityGoexitDoesNotHangClaim(t *testing.T) {
	testActivityGoexitDoesNotHangClaim(t, createTestAppWithPostgres)
}

func TestPostgresActivityDecoderPanicIsRecorded(t *testing.T) {
	testActivityDecoderPanicIsRecorded(t, createTestAppWithPostgres)
}

func TestPostgresCooperativeStartToCloseTimeout(t *testing.T) {
	testCooperativeStartToCloseTimeout(t, createTestAppWithPostgres)
}
func TestPostgresExhaustedTimeoutFencesLateCompletion(t *testing.T) {
	testExhaustedTimeoutFencesLateCompletion(t, createTestAppWithPostgres)
}

func TestPostgresPoolWaitDoesNotConsumeExecutionTimeout(t *testing.T) {
	testPoolWaitDoesNotConsumeExecutionTimeout(t, createTestAppWithPostgres)
}
