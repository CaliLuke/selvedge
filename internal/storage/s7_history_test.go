package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type historyFactory func(*testing.T) Storage

func sqliteHistoryFactory(t *testing.T) Storage {
	s, err := NewSQLiteStorage(filepath.Join(t.TempDir(), "history.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, InitializeTestSchema(context.Background(), s))
	return s
}
func TestClassThreeFillRequiresReservation(t *testing.T) {
	testClassThreeFillRequiresReservation(t, sqliteHistoryFactory)
}
func testClassThreeFillRequiresReservation(t *testing.T, factory historyFactory) {
	t.Run("timer_in_caller_transaction", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()
		require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "timer", WorkflowName: "test", Status: StatusRunning}))
		fixtureFence(t, s, "timer")
		tx, err := s.BeginTransaction(ctx)
		require.NoError(t, err)
		err = s.FireTimer(tx, &TimerSubscription{InstanceID: "timer", TimerID: "missing"})
		require.ErrorIs(t, err, ErrHistoryKeyConflict)
		require.NoError(t, s.CommitTransaction(tx))
		row, err := s.GetInstance(ctx, "timer")
		require.NoError(t, err)
		require.False(t, row.Runnable, "a rejected fill must not persist a wake")
		count, err := s.GetHistoryCount(ctx, "timer")
		require.NoError(t, err)
		require.Zero(t, count)
	})

	for _, event := range []HistoryEventType{HistoryActivityCompleted, HistoryActivityFailed, HistoryTimerExpired} {
		t.Run(string(event), func(t *testing.T) {
			s := factory(t)
			ctx := context.Background()
			require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "unreserved", WorkflowName: "test", Status: StatusRunning}))
			_, err := s.SetHistoryValueIfNull(ctx, nil, "unreserved", "missing", "activity:A", event, []byte(`true`))
			if event == HistoryTimerExpired {
				require.ErrorIs(t, err, ErrHistoryKeyConflict)
			} else {
				require.ErrorIs(t, err, ErrFenced)
			}
			row, err := s.GetHistoryEvent(ctx, "unreserved", "missing")
			require.NoError(t, err)
			require.Nil(t, row)
		})
	}
}
func TestHistorySeqConstraints(t *testing.T) { testHistorySeqConstraints(t, sqliteHistoryFactory) }
func testHistorySeqConstraints(t *testing.T, factory historyFactory) {
	s := factory(t)
	ctx := context.Background()
	require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "seq", WorkflowName: "test", Status: StatusRunning}))
	f := fixtureFence(t, s, "seq")
	t.Run("occupied_reservation", func(t *testing.T) {
		_, err := s.ReserveHistory(ctx, f, "seq", "first", "activity:A", 1)
		require.NoError(t, err)
		_, err = s.ReserveHistory(ctx, f, "seq", "other", "activity:B", 1)
		require.ErrorIs(t, err, ErrHistoryKeyConflict)
		_, err = s.ReserveHistory(ctx, f, "seq", "first", "activity:A", 2)
		require.ErrorIs(t, err, ErrHistoryKeyConflict)
		row, err := s.GetHistoryEvent(ctx, "seq", "other")
		require.NoError(t, err)
		require.Nil(t, row)
	})
	t.Run("occupied_receive", func(t *testing.T) {
		id, err := s.AppendSignal(ctx, "seq", "messages", []byte(`true`))
		require.NoError(t, err)
		_, err = s.ConsumeSignal(ctx, f, id, &HistoryEvent{Seq: 1, InstanceID: "seq", ActivityID: "receive", Command: "receive:messages", EventType: HistorySignalReceived, EventData: []byte(`true`), DataType: "json"})
		require.ErrorIs(t, err, ErrHistoryKeyConflict)
		signal, err := s.NextSignal(ctx, "seq", "messages")
		require.NoError(t, err)
		require.NotNil(t, signal)
	})
	t.Run("null_seq_check", func(t *testing.T) {
		_, err := s.DB().Exec("INSERT INTO workflow_history(instance_id,activity_id,command,event_type,data_type) VALUES('seq','invalid','sleep','reserved','json')")
		require.Error(t, err)
	})
}
