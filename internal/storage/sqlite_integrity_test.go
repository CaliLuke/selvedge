package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSQLiteMemoryStoresAreIndependent(t *testing.T) {
	first, err := NewSQLiteStorage(":memory:")
	require.NoError(t, err)
	defer first.Close()
	second, err := NewSQLiteStorage(":memory:")
	require.NoError(t, err)
	defer second.Close()
	_, err = first.DB().Exec("CREATE TABLE private_data(value TEXT)")
	require.NoError(t, err)
	var count int
	require.NoError(t, second.DB().QueryRow("SELECT count(*) FROM sqlite_master WHERE name='private_data'").Scan(&count))
	require.Zero(t, count, "independent in-memory stores must not expose each other's data")

	// Pool connections within one store must still see the same database.
	held, err := first.DB().Conn(context.Background())
	require.NoError(t, err)
	defer held.Close()
	require.NoError(t, first.DB().QueryRow("SELECT count(*) FROM private_data").Scan(&count))
}

func TestSQLitePreservesURIOptions(t *testing.T) {
	store, err := NewSQLiteStorage("file:" + filepath.Join(t.TempDir(), "uri.db") + "?mode=rwc&cache=private")
	require.NoError(t, err)
	defer store.Close()
	var timeout int
	require.NoError(t, store.DB().QueryRow("PRAGMA busy_timeout").Scan(&timeout))
	require.Equal(t, 5000, timeout)
}

func TestSQLiteEnforcesForeignKeysOnEveryConnection(t *testing.T) {
	store, err := NewSQLiteStorage(filepath.Join(t.TempDir(), "integrity.db"))
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, InitializeTestSchema(context.Background(), store))
	held, err := store.DB().Conn(context.Background())
	require.NoError(t, err)
	defer held.Close()
	// Holding the migrated connection forces the pool to open a second one.
	_, err = store.DB().Exec("INSERT INTO workflow_signals(instance_id,channel,payload) VALUES('missing','updates','null')")
	require.ErrorContains(t, err, "FOREIGN KEY constraint failed")
}

func TestSQLiteTimersRespectSubsecondDeadline(t *testing.T) {
	store, fence, _ := taskFixture(t, sqliteHistoryFactory)
	ctx := context.Background()
	_, err := store.DB().Exec("CREATE TABLE selvedge_test_clock(now_ms BIGINT NOT NULL)")
	require.NoError(t, err)
	_, err = store.DB().Exec("INSERT INTO selvedge_test_clock VALUES(123100)")
	require.NoError(t, err)
	WithTestStoreClock()(store)
	timer := &TimerSubscription{InstanceID: "tasks", TimerID: "#1", ExpiresAt: time.Unix(123, 900500000).UTC()}
	_, err = store.ReserveTimer(ctx, fence, 1, timer)
	require.NoError(t, err)
	expired, err := store.FindExpiredTimers(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, expired)
	_, err = store.DB().Exec("UPDATE selvedge_test_clock SET now_ms=123900")
	require.NoError(t, err)
	expired, err = store.FindExpiredTimers(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, expired, "microsecond deadlines must not round down")
	_, err = store.DB().Exec("UPDATE selvedge_test_clock SET now_ms=123901")
	require.NoError(t, err)
	expired, err = store.FindExpiredTimers(ctx, 10)
	require.NoError(t, err)
	require.Len(t, expired, 1)
	require.Equal(t, timer.ExpiresAt, expired[0].ExpiresAt)
}
