//go:build integration

package storage

import "testing"

func TestPostgresTimerFireRequiresSubscription(t *testing.T) {
	testTimerFireRequiresSubscription(t, postgresHistoryFactory)
}
func TestPostgresTimerRetireAtomic(t *testing.T) { testTimerRetireAtomic(t, postgresHistoryFactory) }
