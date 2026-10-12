//go:build integration

package storage

import "testing"

func TestPostgresCancelActivityAtYieldAtomic(t *testing.T) {
	testCancelActivityAtYieldAtomic(t, postgresHistoryFactory)
}
func TestPostgresCancelActivityAtYieldVersusCompletion(t *testing.T) {
	testCancelActivityAtYieldVersusCompletion(t, postgresHistoryFactory)
}
