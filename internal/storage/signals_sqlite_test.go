package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteSignalInbox(t *testing.T) {
	s, err := NewSQLiteStorage(filepath.Join(t.TempDir(), "signals.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, InitializeTestSchema(context.Background(), s))
	testSignalInbox(t, s)
}

func TestSQLiteSignalCommitFailure(t *testing.T) {
	s, err := NewSQLiteStorage(filepath.Join(t.TempDir(), "commit.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	// PRAGMA foreign_keys is connection-local. Pin this fault test to one
	// connection; the shared inbox suite above retains the normal pool.
	s.DB().SetMaxOpenConns(1)
	_, err = s.DB().ExecContext(context.Background(), "PRAGMA foreign_keys = ON")
	require.NoError(t, err)
	require.NoError(t, InitializeTestSchema(context.Background(), s))
	testSignalCommitFailure(t, s)
}
