package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestSQLiteStorage(t *testing.T) {
	// Create a temporary database
	tmpFile, err := os.CreateTemp("", "selvedge-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	// Create storage
	storage, err := NewSQLiteStorage(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Initialize the storage (create tables) using test helper
	if err := InitializeTestSchema(ctx, storage); err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	t.Run("CreateAndGetInstance", func(t *testing.T) {
		instance := &WorkflowInstance{
			InstanceID:   "test-instance-1",
			WorkflowName: "test-workflow",
			Status:       StatusRunning,
			InputData:    []byte(`{"name":"test"}`),
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		if err := storage.CreateInstance(ctx, instance); err != nil {
			t.Fatalf("failed to create instance: %v", err)
		}

		got, err := storage.GetInstance(ctx, "test-instance-1")
		if err != nil {
			t.Fatalf("failed to get instance: %v", err)
		}

		if got == nil {
			t.Fatal("expected instance, got nil")
		}

		if got.InstanceID != instance.InstanceID {
			t.Errorf("expected instance_id %s, got %s", instance.InstanceID, got.InstanceID)
		}

		if got.WorkflowName != instance.WorkflowName {
			t.Errorf("expected workflow_name %s, got %s", instance.WorkflowName, got.WorkflowName)
		}

		if got.Status != StatusRunning {
			t.Errorf("expected status %s, got %s", StatusRunning, got.Status)
		}
	})

	t.Run("RequestCancel", func(t *testing.T) {
		_, err := storage.RequestCancel(ctx, "test-instance-1", "test")
		if err != nil {
			t.Fatalf("failed to update status: %v", err)
		}

		got, err := storage.GetInstance(ctx, "test-instance-1")
		if err != nil {
			t.Fatalf("failed to get instance: %v", err)
		}

		if got.Status != StatusRunning {
			t.Errorf("expected status %s, got %s", StatusRunning, got.Status)
		}
	})

	t.Run("AcquireLease", func(t *testing.T) {
		f, acquired, err := storage.AcquireLease(ctx, "test-instance-1", "worker-1", time.Second)
		if err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}
		if !acquired {
			t.Error("expected to acquire lock")
		}

		// Try to acquire the same lock with a different worker
		_, acquired2, err := storage.AcquireLease(ctx, "test-instance-1", "worker-2", time.Second)
		if err != nil {
			t.Fatalf("failed to try acquire lock: %v", err)
		}
		if acquired2 {
			t.Error("should not acquire lock held by another worker")
		}

		// Release the lock
		err = storage.ReleaseLease(ctx, f, true)
		if err != nil {
			t.Fatalf("failed to release lock: %v", err)
		}

		// Now worker-2 should be able to acquire
		f2, acquired3, err := storage.AcquireLease(ctx, "test-instance-1", "worker-2", time.Second)
		if err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}
		if !acquired3 {
			t.Error("expected to acquire released lock")
		}
		_ = storage.ReleaseLease(ctx, f2, true)
		_ = storage.ReleaseLease(ctx, f2, true)
	})

	t.Run("AppendAndGetHistory", func(t *testing.T) {
		historyEvent := &HistoryEvent{
			InstanceID: "test-instance-1",
			ActivityID: "activity:1",
			EventType:  HistoryActivityCompleted,
			EventData:  []byte(`{"result":"success"}`),
			DataType:   "json",
		}
		_, err := fixtureRecord(ctx, storage, historyEvent)
		if err != nil {
			t.Fatalf("failed to append history: %v", err)
		}

		history, hasMore, err := storage.GetHistoryPaginated(ctx, "test-instance-1", 0, 100)
		if err != nil {
			t.Fatalf("failed to get history: %v", err)
		}
		if hasMore {
			t.Error("expected hasMore=false for small result set")
		}

		if len(history) != 1 {
			t.Fatalf("expected 1 history event, got %d", len(history))
		}

		if history[0].ActivityID != "activity:1" {
			t.Errorf("expected activity_id 'activity:1', got '%s'", history[0].ActivityID)
		}

		if history[0].EventType != HistoryActivityCompleted {
			t.Errorf("expected event_type %s, got %s", HistoryActivityCompleted, history[0].EventType)
		}
	})

	t.Run("TimerSubscription", func(t *testing.T) {
		expiresAt := time.Now().Add(-time.Minute) // Already expired
		timer := &TimerSubscription{
			InstanceID: "test-instance-1",
			TimerID:    "timer:1",
			ExpiresAt:  expiresAt,
			ActivityID: "timer:1",
		}
		err := fixtureTimer(ctx, storage, timer)
		if err != nil {
			t.Fatalf("failed to register timer subscription: %v", err)
		}

		timers, err := storage.FindExpiredTimers(ctx, 100)
		if err != nil {
			t.Fatalf("failed to find expired timers: %v", err)
		}

		if len(timers) != 1 {
			t.Fatalf("expected 1 expired timer, got %d", len(timers))
		}

		if timers[0].TimerID != "timer:1" {
			t.Errorf("expected timer_id 'timer:1', got '%s'", timers[0].TimerID)
		}
	})

	t.Run("GetNonExistentInstance", func(t *testing.T) {
		got, err := storage.GetInstance(ctx, "non-existent")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for non-existent instance, got %+v", got)
		}
	})

	t.Run("PostCommitCallbackExecuted", func(t *testing.T) {
		callbackExecuted := false

		// Begin transaction
		txCtx, err := storage.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}

		// Verify we are in transaction
		if !storage.InTransaction(txCtx) {
			t.Fatal("expected to be in transaction")
		}

		// Register callback
		err = storage.RegisterPostCommitCallback(txCtx, func() error {
			callbackExecuted = true
			return nil
		})
		if err != nil {
			t.Fatalf("failed to register callback: %v", err)
		}

		// Callback should not be executed yet
		if callbackExecuted {
			t.Error("callback should not be executed before commit")
		}

		// Commit transaction
		err = storage.CommitTransaction(txCtx)
		if err != nil {
			t.Fatalf("failed to commit transaction: %v", err)
		}

		// Callback should be executed after commit
		if !callbackExecuted {
			t.Error("callback should be executed after commit")
		}
	})

	t.Run("PostCommitCallbackNotExecutedOnRollback", func(t *testing.T) {
		callbackExecuted := false

		// Begin transaction
		txCtx, err := storage.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}

		// Register callback
		err = storage.RegisterPostCommitCallback(txCtx, func() error {
			callbackExecuted = true
			return nil
		})
		if err != nil {
			t.Fatalf("failed to register callback: %v", err)
		}

		// Rollback transaction
		err = storage.RollbackTransaction(txCtx)
		if err != nil {
			t.Fatalf("failed to rollback transaction: %v", err)
		}

		// Callback should NOT be executed after rollback
		if callbackExecuted {
			t.Error("callback should NOT be executed after rollback")
		}
	})

	t.Run("RegisterPostCommitCallbackOutsideTransaction", func(t *testing.T) {
		// Try to register callback outside of transaction
		err := storage.RegisterPostCommitCallback(ctx, func() error {
			return nil
		})
		if err == nil {
			t.Error("expected error when registering callback outside transaction")
		}
	})

	t.Run("MultiplePostCommitCallbacks", func(t *testing.T) {
		executionOrder := []int{}

		// Begin transaction
		txCtx, err := storage.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("failed to begin transaction: %v", err)
		}

		// Register multiple callbacks
		for i := 1; i <= 3; i++ {
			idx := i // capture
			err = storage.RegisterPostCommitCallback(txCtx, func() error {
				executionOrder = append(executionOrder, idx)
				return nil
			})
			if err != nil {
				t.Fatalf("failed to register callback %d: %v", i, err)
			}
		}

		// Commit transaction
		err = storage.CommitTransaction(txCtx)
		if err != nil {
			t.Fatalf("failed to commit transaction: %v", err)
		}

		// All callbacks should be executed in order
		if len(executionOrder) != 3 {
			t.Errorf("expected 3 callbacks executed, got %d", len(executionOrder))
		}
		for i, v := range executionOrder {
			if v != i+1 {
				t.Errorf("expected callback %d to execute at position %d, got %d", i+1, i, v)
			}
		}
	})
}

func TestSQLiteStorage_InputFilters(t *testing.T) {
	// Create a temporary database
	tmpFile, err := os.CreateTemp("", "selvedge-input-filter-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	_ = tmpFile.Close()
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	// Create storage
	storage, err := NewSQLiteStorage(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer func() { _ = storage.Close() }()

	ctx := context.Background()

	// Initialize the storage using test helper
	if err := InitializeTestSchema(ctx, storage); err != nil {
		t.Fatalf("failed to initialize storage: %v", err)
	}

	// Create test instances with various input data
	instances := []struct {
		id    string
		input string
	}{
		{"filter-test-1", `{"customer":{"id":"cust_1","tier":"gold"},"amount":100}`},
		{"filter-test-2", `{"customer":{"id":"cust_2","tier":"silver"},"amount":200}`},
		{"filter-test-3", `{"customer":{"id":"cust_1","tier":"silver"},"amount":150}`},
		{"filter-test-4", `{"customer":{"id":"cust_3","tier":"gold"},"amount":300}`},
		{"filter-test-5", `{"status":"active","count":5}`},
	}

	for _, inst := range instances {
		instance := &WorkflowInstance{
			InstanceID:   inst.id,
			WorkflowName: "filter_test_workflow",
			Status:       StatusRunning,
			InputData:    []byte(inst.input),
			StartedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}
		if err := storage.CreateInstance(ctx, instance); err != nil {
			t.Fatalf("failed to create instance %s: %v", inst.id, err)
		}
	}

	t.Run("single string filter", func(t *testing.T) {
		result, err := storage.ListInstances(ctx, ListInstancesOptions{
			InputFilters: map[string]any{
				"customer.id": "cust_1",
			},
		})
		if err != nil {
			t.Fatalf("ListInstances with input filter failed: %v", err)
		}
		if len(result.Instances) != 2 {
			t.Errorf("expected 2 instances with customer.id=cust_1, got %d", len(result.Instances))
		}
	})

	t.Run("multiple filters AND", func(t *testing.T) {
		result, err := storage.ListInstances(ctx, ListInstancesOptions{
			InputFilters: map[string]any{
				"customer.id":   "cust_1",
				"customer.tier": "gold",
			},
		})
		if err != nil {
			t.Fatalf("ListInstances with multiple input filters failed: %v", err)
		}
		if len(result.Instances) != 1 {
			t.Errorf("expected 1 instance matching both filters, got %d", len(result.Instances))
		}
		if len(result.Instances) > 0 && result.Instances[0].InstanceID != "filter-test-1" {
			t.Errorf("expected instance filter-test-1, got %s", result.Instances[0].InstanceID)
		}
	})

	t.Run("numeric filter", func(t *testing.T) {
		result, err := storage.ListInstances(ctx, ListInstancesOptions{
			InputFilters: map[string]any{
				"amount": 200,
			},
		})
		if err != nil {
			t.Fatalf("ListInstances with numeric filter failed: %v", err)
		}
		if len(result.Instances) != 1 {
			t.Errorf("expected 1 instance with amount=200, got %d", len(result.Instances))
		}
		if len(result.Instances) > 0 && result.Instances[0].InstanceID != "filter-test-2" {
			t.Errorf("expected instance filter-test-2, got %s", result.Instances[0].InstanceID)
		}
	})

	t.Run("no matching results", func(t *testing.T) {
		result, err := storage.ListInstances(ctx, ListInstancesOptions{
			InputFilters: map[string]any{
				"customer.id": "nonexistent",
			},
		})
		if err != nil {
			t.Fatalf("ListInstances with non-matching filter failed: %v", err)
		}
		if len(result.Instances) != 0 {
			t.Errorf("expected 0 instances, got %d", len(result.Instances))
		}
	})

	t.Run("combined with status filter", func(t *testing.T) {
		// Update one instance to completed (filter-test-1 is gold)
		if err := storage.CloseInstance(ctx, *contextFence(fixtureContext(t, storage, ctx, "filter-test-1"), "filter-test-1"), StatusCompleted, nil, nil); err != nil {
			t.Fatalf("failed to update status: %v", err)
		}

		result, err := storage.ListInstances(ctx, ListInstancesOptions{
			StatusFilter: StatusRunning,
			InputFilters: map[string]any{
				"customer.tier": "gold",
			},
		})
		if err != nil {
			t.Fatalf("ListInstances with combined filters failed: %v", err)
		}
		// filter-test-1 is gold but now completed, filter-test-4 is gold and still running
		if len(result.Instances) != 1 {
			t.Errorf("expected 1 running gold-tier instance, got %d", len(result.Instances))
		}
		if len(result.Instances) > 0 && result.Instances[0].InstanceID != "filter-test-4" {
			t.Errorf("expected instance filter-test-4, got %s", result.Instances[0].InstanceID)
		}
	})

	t.Run("invalid path rejected", func(t *testing.T) {
		_, err := storage.ListInstances(ctx, ListInstancesOptions{
			InputFilters: map[string]any{
				"'; DROP TABLE workflow_instances; --": "test",
			},
		})
		if err == nil {
			t.Error("expected error for invalid JSON path")
		}
	})

	t.Run("top-level field", func(t *testing.T) {
		result, err := storage.ListInstances(ctx, ListInstancesOptions{
			InputFilters: map[string]any{
				"status": "active",
			},
		})
		if err != nil {
			t.Fatalf("ListInstances with top-level filter failed: %v", err)
		}
		if len(result.Instances) != 1 {
			t.Errorf("expected 1 instance with status=active, got %d", len(result.Instances))
		}
		if len(result.Instances) > 0 && result.Instances[0].InstanceID != "filter-test-5" {
			t.Errorf("expected instance filter-test-5, got %s", result.Instances[0].InstanceID)
		}
	})
}
