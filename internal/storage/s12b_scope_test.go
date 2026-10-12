package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScopeCancelChildrenAtomic(t *testing.T) {
	testScopeCancelChildrenAtomic(t, sqliteHistoryFactory)
}
func testScopeCancelChildrenAtomic(t *testing.T, factory historyFactory) {
	s, f, _ := taskFixture(t, factory)
	ctx := context.Background()
	_, err := s.StartChild(ctx, f, 1, "#1", "child:child", &WorkflowInstance{InstanceID: "child", WorkflowName: "child", InputData: []byte(`null`)})
	require.NoError(t, err)
	c := &ObservationCommit{Seq: 2, Record: &HistoryEvent{InstanceID: f.InstanceID, ActivityID: "#2", Seq: 2, Command: "cancel_scope", EventType: HistoryScopeCancelled, EventData: []byte(`{"targets":1}`)}, Cancel: &ScopeCancel{Targets: []ScopeTarget{{Key: "#1", Command: "child:child", Kind: "child"}}}}
	untouched := func() {
		t.Helper()
		row, e := s.GetHistoryEvent(ctx, f.InstanceID, "#1")
		require.NoError(t, e)
		require.Nil(t, row.EventData)
		requested, e := s.IsCancelRequested(ctx, "child")
		require.NoError(t, e)
		require.False(t, requested)
	}
	tx, err := s.BeginTransaction(ctx)
	require.NoError(t, err)
	_, err = s.CommitObservation(tx, f, c)
	require.NoError(t, err)
	require.NoError(t, s.RollbackTransaction(tx))
	untouched()
	bad := *c
	targets := append([]ScopeTarget{}, c.Cancel.Targets...)
	targets = append(targets, ScopeTarget{Key: "missing", Command: "child:child", Kind: "child"})
	bad.Cancel = &ScopeCancel{Targets: targets}
	_, err = s.CommitObservation(ctx, f, &bad)
	require.ErrorIs(t, err, ErrHistoryKeyConflict)
	untouched()
	stale := *f
	stale.Epoch++
	_, err = s.CommitObservation(ctx, &stale, c)
	require.ErrorIs(t, err, ErrFenced)
	untouched()
	result, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, []string{"#1", "#2"}, result.Delivered.HistoryKeys)
	again, err := s.CommitObservation(ctx, f, c)
	require.NoError(t, err)
	require.Equal(t, result, again)
	row, err := s.GetHistoryEvent(ctx, f.InstanceID, "#1")
	require.NoError(t, err)
	require.Equal(t, HistoryChildCancelled, row.EventType)
	require.EqualValues(t, 2, row.DeliveredSeq)
	require.JSONEq(t, `{"v":1,"kind":"canceled","message":"context canceled"}`, string(row.EventData))
	requested, err := s.IsCancelRequested(ctx, "child")
	require.NoError(t, err)
	require.True(t, requested)
	child, err := s.GetInstance(ctx, "child")
	require.NoError(t, err)
	require.True(t, child.Runnable)
}
