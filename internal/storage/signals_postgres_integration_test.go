//go:build integration

package storage

import "testing"

func TestPostgresSignalInbox(t *testing.T) {
	s, cleanup := setupPostgresContainer(t)
	t.Cleanup(cleanup)
	testSignalInbox(t, s)
	t.Run("CommitFailure", func(t *testing.T) { testSignalCommitFailure(t, s) })
}
