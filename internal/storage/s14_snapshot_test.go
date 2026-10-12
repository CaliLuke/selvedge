package storage

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadSnapshotIsolated(t *testing.T) { s14TestReadSnapshotIsolated(t, sqliteHistoryFactory) }
func s14TestReadSnapshotIsolated(t *testing.T, factory historyFactory) {
	s := factory(t)
	ctx := context.Background()
	require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "snapshot", WorkflowName: "snapshot", Status: StatusRunning}))
	f := fixtureFence(t, s, "snapshot")
	timer := &TimerSubscription{InstanceID: "snapshot", TimerID: "#1", ActivityID: "#1", ExpiresAt: time.Date(2030, 1, 1, 0, 0, 0, 123, time.UTC)}
	reserved, err := s.ReserveTimer(ctx, f, 1, timer)
	require.NoError(t, err)
	sig, err := s.AppendSignal(ctx, "snapshot", "ch", []byte(`1`))
	require.NoError(t, err)
	// Pause before the history read, after the instance read established the snapshot.
	reached, release := make(chan struct{}), make(chan struct{})
	var paused atomic.Bool
	WithTestStatementObserver(func(q string, _ []any) {
		if strings.Contains(q, "FROM workflow_history") && paused.CompareAndSwap(false, true) {
			close(reached)
			<-release
		}
	})(s)
	type result struct {
		snapshot *InstanceSnapshot
		err      error
	}
	done := make(chan result, 1)
	go func() { v, e := s.ReadSnapshot(ctx, "snapshot"); done <- result{v, e} }()
	select {
	case <-reached:
	case r := <-done:
		require.NoError(t, r.err)
		t.Fatal("snapshot did not read history")
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot stalled before read")
	}
	defer close(release)
	// A concurrent writer must be able to finish while the snapshot is open.
	_, err = s.RequestCancel(ctx, "snapshot", "after snapshot")
	require.NoError(t, err)
	_, err = s.CommitObservation(ctx, f, &ObservationCommit{Seq: 2, DeliverAll: true, Consume: &SignalConsume{ID: sig, Channel: "ch"}, Record: &HistoryEvent{InstanceID: "snapshot", ActivityID: "#2", Seq: 2, Command: "receive:ch", EventType: HistorySignalReceived, EventData: []byte(`1`), DataType: "json"}})
	require.NoError(t, err)
	release <- struct{}{}
	r := <-done
	require.NoError(t, r.err)
	require.NotNil(t, r.snapshot)
	require.False(t, r.snapshot.CancelRequested)
	require.Empty(t, r.snapshot.Instance.CancelReason)
	require.Len(t, r.snapshot.History, 1)
	require.Equal(t, reserved.ReservationData, r.snapshot.History[0].ReservationData)
	require.Empty(t, r.snapshot.Signals)
	later, err := s.ReadSnapshot(ctx, "snapshot")
	require.NoError(t, err)
	require.True(t, later.CancelRequested)
	require.Len(t, later.History, 2)
	require.Len(t, later.Signals, 1)
	require.EqualValues(t, 2, later.Signals[0].DeliveredSeq)
	require.EqualValues(t, 2, later.Signals[0].ConsumedSeq)
	missing, err := s.ReadSnapshot(ctx, "missing")
	require.NoError(t, err)
	require.Nil(t, missing)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.ReadSnapshot(cancelled, "snapshot")
	require.ErrorIs(t, err, context.Canceled)
}
