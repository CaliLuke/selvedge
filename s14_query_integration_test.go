//go:build integration

package selvedge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresQueryExecutionBoundaries(t *testing.T) {
	s14TestQueryExecutionBoundaries(t, s14PostgresFactory(t))
}

func TestPostgresQueryReadsDeliveredView(t *testing.T) {
	s14TestQueryReadsDeliveredView(t, s14PostgresFactory(t))
}

func TestPostgresQueryEnginetestShapes(t *testing.T) {
	s14TestQueryEnginetestShapes(t, s14PostgresFactory(t))
}

func TestPostgresQuerySnapshotConsistent(t *testing.T) {
	s14TestQuerySnapshotConsistent(t, s14PostgresFactory(t))
}

func TestPostgresPendingAndReceiveAsyncAgree_Query(t *testing.T) {
	s14TestPendingAndReceiveAsyncAgreeQuery(t, s14PostgresFactory(t))
}

func TestPostgresReplayDeterministicAfterCrash_Queries(t *testing.T) {
	s14TestReplayDeterministicAfterCrashQueries(t, s14PostgresFactory(t))
}

func TestPostgresQueryHandlerLifecycle(t *testing.T) {
	s14TestQueryHandlerLifecycle(t, s14PostgresFactory(t))
}

func TestPostgresQueryStopsAtFirstLiveAction(t *testing.T) {
	s14TestQueryStopsAtFirstLiveAction(t, s14PostgresFactory(t))
}

func TestPostgresQueryResultJSONRoundTrip(t *testing.T) {
	s14TestQueryResultJSONRoundTrip(t, s14PostgresFactory(t))
}

func TestPostgresQueryFromSecondApp(t *testing.T) {
	s14TestQueryFromSecondApp(t, s14PostgresFactory(t))
}

func TestPostgresQueryHandlerCommandsRejected(t *testing.T) {
	s14TestQueryHandlerCommandsRejected(t, s14PostgresFactory(t))
}

func TestPostgresQueryWritesNothing(t *testing.T) {
	s14TestQueryWritesNothing(t, s14PostgresFactory(t))
}

func TestPostgresReplayCommandMismatchFailsFast_Query(t *testing.T) {
	s14TestReplayCommandMismatchFailsFastQuery(t, s14PostgresFactory(t))
}

func TestPostgresQueryDuringInlineActivity(t *testing.T) {
	s14TestQueryDuringInlineActivity(t, s14PostgresFactory(t))
}

func TestPostgresQueryPreservesReplayFailure(t *testing.T) {
	s14TestQueryPreservesReplayFailure(t, s14PostgresFactory(t))
}

func TestPostgresQuerySnapshotReservations(t *testing.T) {
	s14TestQuerySnapshotReservations(t, s14PostgresFactory(t))
}

func TestPostgresQueryClosingStorageStop(t *testing.T) {
	s14TestQueryClosingStorageStop(t, s14PostgresFactory(t))
}

func TestPostgresQueryForeignGoroutineStops(t *testing.T) {
	s14TestQueryForeignGoroutineStops(t, s14PostgresFactory(t))
}
func TestPostgresQueryPanicFailedInstance(t *testing.T) {
	s14TestQueryPanicFailedInstance(t, s14PostgresFactory(t))
}
func TestPostgresQueryLifecycleRowsExcludedFromReplay(t *testing.T) {
	s14TestQueryLifecycleRowsExcludedFromReplay(t, s14PostgresFactory(t))
}

func TestPostgresQueryFactoryResetsNewTables(t *testing.T) {
	factory := s14PostgresFactory(t)
	var schema string
	var migrations int
	t.Run("populate", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		db := h.app.storage.DB()
		require.NoError(t, db.QueryRow(`SELECT current_schema()`).Scan(&schema))
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&migrations))
		require.Positive(t, migrations)
		_, err := db.Exec(`CREATE TABLE "later table" (id bigint)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO "later table" VALUES (1)`)
		require.NoError(t, err)
	})
	t.Run("reuse", func(t *testing.T) {
		h, _ := s14Harness(t, factory)
		db := h.app.storage.DB()
		var reused string
		var rows, retainedMigrations int
		require.NoError(t, db.QueryRow(`SELECT current_schema()`).Scan(&reused))
		require.Equal(t, schema, reused)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM "later table"`).Scan(&rows))
		require.Zero(t, rows, "new tables must be emptied before schema reuse")
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&retainedMigrations))
		require.Equal(t, migrations, retainedMigrations)
	})
}
