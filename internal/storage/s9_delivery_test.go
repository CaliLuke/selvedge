package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func deliveryFixture(t *testing.T, factory historyFactory) (Storage, *Fence, int64) {
	t.Helper()
	s, f, task := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.ReserveActivity(ctx, f, 1, "#1", "activity:A", task)
	require.NoError(t, err)
	claim, err := s.ClaimActivityTask(ctx, "tasks", "#1", "worker", time.Hour)
	require.NoError(t, err)
	require.NoError(t, s.CompleteActivityTask(ctx, claim.Claim(), HistoryActivityCompleted, []byte(`1`)))
	id, err := s.AppendSignal(ctx, "tasks", "c", []byte(`2`))
	require.NoError(t, err)
	return s, f, id
}
func awaitObservation(seq int64, key string) *HistoryEvent {
	return &HistoryEvent{InstanceID: "tasks", Seq: seq, ActivityID: key, Command: "await", EventType: HistoryAwaitCompleted, EventData: []byte(`{"history":1,"signals":1}`), DataType: "json"}
}
func TestCommitObservationAtomic(t *testing.T) { testCommitObservationAtomic(t, sqliteHistoryFactory) }
func testCommitObservationAtomic(t *testing.T, factory historyFactory) {
	s, f, id := deliveryFixture(t, factory)
	ctx := context.Background()
	batch, err := s.ArrivedUndelivered(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, []string{"#1"}, batch.HistoryKeys)
	require.Equal(t, []SignalRef{{ID: id, Channel: "c"}}, batch.Signals)
	commit := &ObservationCommit{Seq: 2, Batch: batch, Record: awaitObservation(2, "#2")}
	stale := *f
	stale.Epoch++
	_, err = s.CommitObservation(ctx, &stale, commit)
	require.ErrorIs(t, err, ErrFenced)
	_, err = s.CommitObservation(ctx, nil, commit)
	require.ErrorIs(t, err, ErrFenced)
	tx, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = s.CommitObservation(tx, f, commit)
	require.NoError(t, err)
	require.NoError(t, s.RollbackTransaction(tx))
	got, err := s.ArrivedUndelivered(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, batch, got)
	conflict := *commit
	conflict.Record = awaitObservation(2, "#1")
	_, err = s.CommitObservation(ctx, f, &conflict)
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	got, err = s.ArrivedUndelivered(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, batch, got)
	bad := *batch
	bad.HistoryKeys = []string{"#1", "missing"}
	conflict = *commit
	conflict.Batch = &bad
	_, err = s.CommitObservation(ctx, f, &conflict)
	require.Error(t, err)
	got, err = s.ArrivedUndelivered(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, batch, got)
	result, err := s.CommitObservation(ctx, f, commit)
	require.NoError(t, err)
	require.EqualValues(t, 2, result.Stored.DeliveredSeq)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.EqualValues(t, 2, row.DeliveredSeq)
	signals, err := s.DeliveredSignals(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, []SignalPosition{{ID: id, Channel: "c", DeliveredSeq: 2}}, signals)
}
func TestDeliveredOnce(t *testing.T) { testDeliveredOnce(t, sqliteHistoryFactory) }
func testDeliveredOnce(t *testing.T, factory historyFactory) {
	s, f, id := deliveryFixture(t, factory)
	ctx := context.Background()
	c := &ObservationCommit{Seq: 2, DeliverAll: true, Record: awaitObservation(2, "#2")}
	first, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	late, err := s.AppendSignal(ctx, "tasks", "c", []byte(`3`))
	require.NoError(t, err)
	repeat, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, first, repeat)
	batch, err := s.ArrivedUndelivered(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, []SignalRef{{ID: late, Channel: "c"}}, batch.Signals)
	obs := &HistoryEvent{InstanceID: "tasks", Seq: 3, ActivityID: "#3", Command: "receive_async:c", EventType: HistorySignalReceived, EventData: []byte(`2`), DataType: "json"}
	c = &ObservationCommit{Seq: 3, Record: obs, Consume: &SignalConsume{ID: late, Channel: "c", RequireDeliveredBefore: 3}}
	_, err = s.CommitObservation(ctx, f, c)
	require.ErrorIs(t, err, ErrSignalNotAvailable)
	c.Consume.ID = id
	result, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, []byte(`2`), result.Stored.EventData)
	positions, err := s.DeliveredSignals(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, []SignalPosition{{ID: id, Channel: "c", DeliveredSeq: 2, ConsumedSeq: 3}}, positions)
	_, err = s.CommitObservation(ctx, f, &ObservationCommit{Seq: 4, DeliverAll: true, Record: awaitObservation(4, "#4")})
	require.NoError(t, err)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.EqualValues(t, 2, row.DeliveredSeq)
}
func TestDeliveryConstraints(t *testing.T) { testDeliveryConstraints(t, sqliteHistoryFactory) }
func testDeliveryConstraints(t *testing.T, factory historyFactory) {
	for _, query := range []string{
		`UPDATE workflow_history SET delivered_seq=1,event_data=NULL WHERE activity_id='#1'`,
		`UPDATE workflow_history SET delivered_seq=1,seq=2 WHERE activity_id='#1'`,
		`UPDATE workflow_signals SET consumed_key='x'`,
		`UPDATE workflow_signals SET consumed_seq=2`,
		`UPDATE workflow_signals SET consumed_key='x',consumed_seq=2`,
		`UPDATE workflow_signals SET consumed_key='x',consumed_seq=2,delivered_seq=3`,
	} {
		t.Run(query, func(t *testing.T) {
			s, _, _ := deliveryFixture(t, factory)
			// Prove the delivery columns exist before testing their constraints.
			var n int
			require.NoError(t, s.DB().QueryRow(`SELECT COUNT(*) FROM workflow_history WHERE delivered_seq IS NULL`).Scan(&n))
			_, err := s.DB().Exec(query)
			require.Error(t, err)
		})
	}
	s, f, _ := deliveryFixture(t, factory)
	row, err := s.RecordHistory(context.Background(), f, awaitObservation(2, "#2"))
	require.NoError(t, err)
	require.EqualValues(t, 2, row.DeliveredSeq)
	rows, _, err := s.GetHistoryPaginated(context.Background(), "tasks", 0, 10)
	require.NoError(t, err)
	require.EqualValues(t, 2, rows[1].DeliveredSeq)
}

func TestDeliverySequencesAreInt64(t *testing.T) {
	testDeliverySequencesAreInt64(t, sqliteHistoryFactory)
}
func testDeliverySequencesAreInt64(t *testing.T, factory historyFactory) {
	s, f, id := deliveryFixture(t, factory)
	ctx := context.Background()
	const seq int64 = 1 << 32
	c := &ObservationCommit{Seq: seq, DeliverAll: true, Record: awaitObservation(seq, "large-await")}
	first, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, seq, first.Stored.DeliveredSeq)
	repeated, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, first, repeated)
	row, err := s.GetHistoryEvent(ctx, "tasks", "#1")
	require.NoError(t, err)
	require.Equal(t, seq, row.DeliveredSeq)
	observation := &HistoryEvent{InstanceID: "tasks", Seq: seq + 1, ActivityID: "large-receive", Command: "receive_async:c", EventType: HistorySignalReceived, DataType: "json"}
	_, err = s.CommitObservation(ctx, f, &ObservationCommit{Seq: seq + 1, Record: observation, Consume: &SignalConsume{ID: id, Channel: "c", RequireDeliveredBefore: seq + 1}})
	require.NoError(t, err)
	positions, err := s.DeliveredSignals(ctx, "tasks")
	require.NoError(t, err)
	require.Equal(t, []SignalPosition{{ID: id, Channel: "c", DeliveredSeq: seq, ConsumedSeq: seq + 1}}, positions)
}
