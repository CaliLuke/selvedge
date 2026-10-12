package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalNotifyKeyedByDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bus.db")
	s, err := NewSQLiteStorage(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.NoError(t, InitializeTestSchema(context.Background(), s))
	s13LocalNotify(t, s, path, "file:"+path)
}

func s13LocalNotify(t *testing.T, s Storage, key, alias string) {
	t.Helper()
	events := make(chan LocalNotification, 8)
	off := SubscribeLocal(alias, func(n LocalNotification) { events <- n })
	defer off()
	wrong := make(chan LocalNotification, 1)
	offWrong := SubscribeLocal(key+"other", func(n LocalNotification) { wrong <- n })
	defer offWrong()
	s.NotifyRunnable(context.Background())
	select {
	case n := <-events:
		require.False(t, n.Closed)
	case <-time.After(time.Second):
		t.Fatal("same database not notified")
	}
	select {
	case n := <-wrong:
		t.Fatalf("other database notified: %+v", n)
	default:
	}
	ctx, err := s.BeginTransaction(context.Background())
	require.NoError(t, err)
	s.NotifyRunnable(ctx)
	select {
	case n := <-events:
		t.Fatalf("notification before commit: %+v", n)
	default:
	}
	require.NoError(t, s.RollbackTransaction(ctx))
	select {
	case n := <-events:
		t.Fatalf("notification after rollback: %+v", n)
	default:
	}
	ctx, err = s.BeginTransaction(context.Background())
	require.NoError(t, err)
	s.NotifyRunnable(ctx)
	require.NoError(t, s.CommitTransaction(ctx))
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("commit did not notify")
	}
	off()
	s.NotifyRunnable(context.Background())
	select {
	case n := <-events:
		t.Fatalf("notification after unsubscribe: %+v", n)
	default:
	}
}

func TestLocalNotifyMemoryExcluded(t *testing.T) {
	for _, dsn := range []string{":memory:", "file::memory:?cache=shared", "file:mem?mode=memory&cache=shared"} {
		t.Run(dsn, func(t *testing.T) {
			off := SubscribeLocal(dsn, func(LocalNotification) { t.Error("memory database shared a bus") })
			defer off()
			require.Empty(t, localNotifyKey(dsn))
		})
	}
}
