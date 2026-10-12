package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func childFixture(t *testing.T, factory historyFactory) (Storage, *Fence) {
	t.Helper()
	s := factory(t)
	require.NoError(t, s.CreateInstance(context.Background(), &WorkflowInstance{InstanceID: "parent", WorkflowName: "parent", Status: StatusRunning}))
	return s, fixtureFence(t, s, "parent")
}
func startTestChild(t *testing.T, s Storage, f *Fence, id string) *HistoryEvent {
	t.Helper()
	row, err := s.StartChild(context.Background(), f, 1, "#1", "child:child", &WorkflowInstance{InstanceID: id, WorkflowName: "child", InputData: []byte(`"input"`)})
	require.NoError(t, err)
	require.NotNil(t, row)
	return row
}
func TestStartChildAtomic(t *testing.T) { testStartChildAtomic(t, sqliteHistoryFactory) }
func testStartChildAtomic(t *testing.T, factory historyFactory) {
	t.Run("rollback_repeat_conflict", func(t *testing.T) {
		s, f := childFixture(t, factory)
		ctx := context.Background()
		tx, err := s.BeginTransaction(ctx)
		require.NoError(t, err)
		_, err = s.StartChild(tx, f, 1, "#1", "child:child", &WorkflowInstance{InstanceID: "child", WorkflowName: "child", InputData: []byte(`null`)})
		require.NoError(t, err)
		require.NoError(t, s.RollbackTransaction(tx))
		row, err := s.GetHistoryEvent(ctx, "parent", "#1")
		require.NoError(t, err)
		require.Nil(t, row)
		inst, err := s.GetInstance(ctx, "child")
		require.NoError(t, err)
		require.Nil(t, inst)
		first := startTestChild(t, s, f, "child")
		second := startTestChild(t, s, f, "child")
		require.Equal(t, first.ID, second.ID)
		require.Nil(t, second.EventData)
		require.JSONEq(t, `{"child_id":"child","workflow":"child"}`, string(second.ReservationData))
		inst, err = s.GetInstance(ctx, "child")
		require.NoError(t, err)
		require.Equal(t, "parent", inst.ParentInstanceID)
		require.Equal(t, "#1", inst.ParentKey)
		require.True(t, inst.Runnable)
		_, err = s.StartChild(ctx, f, 1, "other", "child:child", &WorkflowInstance{InstanceID: "other", WorkflowName: "child", InputData: []byte(`null`)})
		require.ErrorIs(t, err, ErrHistoryKeyConflict)
		inst, err = s.GetInstance(ctx, "other")
		require.NoError(t, err)
		require.Nil(t, inst)
		var n int
		require.NoError(t, s.DB().QueryRow(`SELECT COUNT(*) FROM workflow_instances WHERE parent_instance_id='parent'`).Scan(&n))
		require.Equal(t, 1, n)
	})
	t.Run("foreign_id", func(t *testing.T) {
		s, f := childFixture(t, factory)
		ctx := context.Background()
		require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "foreign", WorkflowName: "other", Status: StatusRunning}))
		row := startTestChild(t, s, f, "foreign")
		require.Equal(t, HistoryChildStartFailed, row.EventType)
		require.EqualValues(t, 1, row.DeliveredSeq)
		require.Contains(t, string(row.EventData), "workflow_already_started")
		require.Equal(t, row.EventData, startTestChild(t, s, f, "foreign").EventData)
	})
	t.Run("schema", func(t *testing.T) {
		s, _ := childFixture(t, factory)
		// A successful paired update distinguishes the CHECK from a missing column.
		_, err := s.DB().Exec(`UPDATE workflow_instances SET parent_instance_id='parent',parent_key='#1' WHERE instance_id='parent'`)
		require.NoError(t, err)
		_, err = s.DB().Exec(`UPDATE workflow_instances SET parent_instance_id=NULL,parent_key=NULL WHERE instance_id='parent'`)
		require.NoError(t, err)
		_, err = s.DB().Exec(`UPDATE workflow_instances SET parent_key='orphan' WHERE instance_id='parent'`)
		require.Error(t, err)
		if _, ok := s.(*PostgresStorage); ok {
			_, err = s.DB().Exec(`UPDATE workflow_instances SET parent_instance_id='missing',parent_key='#1' WHERE instance_id='parent'`)
			require.Error(t, err)
		}
	})
}
func TestChildResultFillOnClose(t *testing.T) { testChildResultFillOnClose(t, sqliteHistoryFactory) }
func testChildResultFillOnClose(t *testing.T, factory historyFactory) {
	for _, status := range []WorkflowStatus{StatusCompleted, StatusFailed, StatusCancelled} {
		for _, parentClosed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed_%t", status, parentClosed), func(t *testing.T) {
				s, f := childFixture(t, factory)
				ctx := context.Background()
				startTestChild(t, s, f, "child")
				cf := fixtureFence(t, s, "child")
				if parentClosed {
					require.NoError(t, s.CloseInstance(ctx, *f, StatusCompleted, []byte(`null`), nil))
				}
				var event *HistoryEvent
				data := []byte(`"output"`)
				typ := HistoryChildCompleted
				if status != StatusCompleted {
					data = []byte(`{"v":1,"kind":"generic","message":"child error"}`)
					typ = HistoryChildFailed
					if status == StatusCancelled {
						typ = HistoryChildCancelled
					}
					event = &HistoryEvent{InstanceID: "child", ActivityID: "$workflow_failed", Command: "close", EventType: HistoryWorkflowFailed, EventData: data}
				}
				require.NoError(t, s.CloseInstance(ctx, *cf, status, []byte(`"output"`), event))
				row, err := s.GetHistoryEvent(ctx, "parent", "#1")
				require.NoError(t, err)
				parent, err := s.GetInstance(ctx, "parent")
				require.NoError(t, err)
				if parentClosed {
					require.Nil(t, row.EventData)
					require.False(t, parent.Runnable)
				} else {
					require.Equal(t, typ, row.EventType)
					require.JSONEq(t, string(data), string(row.EventData))
					require.Zero(t, row.DeliveredSeq)
					require.True(t, parent.Runnable)
				}
			})
		}
	}
}
func TestParentCloseRequestsChildCancel(t *testing.T) {
	testParentCloseRequestsChildCancel(t, sqliteHistoryFactory)
}
func testParentCloseRequestsChildCancel(t *testing.T, factory historyFactory) {
	for _, status := range []WorkflowStatus{StatusCompleted, StatusFailed, StatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			s, f := childFixture(t, factory)
			ctx := context.Background()
			startTestChild(t, s, f, "child")
			fixtureFence(t, s, "child")
			_, err := s.StartChild(ctx, f, 2, "#2", "child:child", &WorkflowInstance{InstanceID: "finished", WorkflowName: "child", InputData: []byte(`null`)})
			require.NoError(t, err)
			finishedFence := fixtureFence(t, s, "finished")
			require.NoError(t, s.CloseInstance(ctx, *finishedFence, StatusCompleted, []byte(`"finished"`), nil))
			finishedBefore, err := s.GetInstance(ctx, "finished")
			require.NoError(t, err)
			tx, err := s.BeginTransaction(ctx)
			require.NoError(t, err)
			require.NoError(t, s.CloseInstance(tx, *f, status, []byte(`null`), nil))
			require.NoError(t, s.RollbackTransaction(tx))
			requested, err := s.IsCancelRequested(ctx, "child")
			require.NoError(t, err)
			require.False(t, requested)
			require.NoError(t, s.CloseInstance(ctx, *f, status, []byte(`null`), nil))
			requested, err = s.IsCancelRequested(ctx, "child")
			require.NoError(t, err)
			require.True(t, requested)
			child, err := s.GetInstance(ctx, "child")
			require.NoError(t, err)
			require.True(t, child.Runnable)
			require.Equal(t, "parent closed", child.CancelReason)
			finishedAfter, err := s.GetInstance(ctx, "finished")
			require.NoError(t, err)
			require.Equal(t, finishedBefore, finishedAfter, "closed children must be untouched")
		})
	}
}
func TestParentChildLockOrder(t *testing.T) { testParentChildLockOrder(t, sqliteHistoryFactory) }
func testParentChildLockOrder(t *testing.T, factory historyFactory) {
	s := factory(t)
	for i := range 50 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		id := fmt.Sprintf("parent-%d", i)
		cid := id + "/child"
		require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: id, WorkflowName: "parent", Status: StatusRunning}))
		f := fixtureFence(t, s, id)
		startTestChild(t, s, f, cid)
		cf := fixtureFence(t, s, cid)
		start := make(chan struct{})
		done := make(chan error, 2)
		go func() { <-start; done <- s.CloseInstance(ctx, *f, StatusCompleted, []byte(`null`), nil) }()
		go func() { <-start; done <- s.CloseInstance(ctx, *cf, StatusCompleted, []byte(`null`), nil) }()
		close(start)
		require.NoError(t, <-done)
		require.NoError(t, <-done)
		cancel()
	}
}
