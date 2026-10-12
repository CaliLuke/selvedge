//go:build integration

package selvedge

import "testing"

func TestCorruptSignalCancellationEnvelopeFailsDurablyPostgres(t *testing.T) {
	testCorruptSignalCancellationEnvelopeFailsDurably(t, createTestAppWithPostgres)
}
func TestTimerFireRequiresRunningInstancePostgres(t *testing.T) {
	testTimerFireRequiresRunningInstance(t, createTestAppWithPostgres)
}
