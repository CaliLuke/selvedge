//go:build integration

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgresContainer(t *testing.T) (*PostgresStorage, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("selvedge_test"),
		postgres.WithUsername("selvedge"),
		postgres.WithPassword("selvedge"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute)),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v", err)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("failed to get connection string: %v", err)
	}

	store, err := NewPostgresStorage(connStr)
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("failed to create PostgreSQL storage: %v", err)
	}

	// Apply the embedded schema instead of Initialize() (which is a no-op)
	if err := InitializeTestSchemaPostgres(ctx, store); err != nil {
		store.Close()
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("failed to initialize test schema: %v", err)
	}

	cleanup := func() {
		store.Close()
		testcontainers.CleanupContainer(t, container)
	}

	return store, cleanup
}

func cleanupPostgresTables(t *testing.T, store *PostgresStorage) {
	t.Helper()
	ctx := context.Background()

	tables := []string{
		"workflow_timer_subscriptions",
		"workflow_history",
		"workflow_instances",
		"system_locks",
	}

	for _, table := range tables {
		if _, err := store.db.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Logf("Warning: failed to clean table %s: %v", table, err)
		}
	}
}

func TestPostgresIntegration_Initialize(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	// InitializeTestSchemaPostgres should be idempotent
	ctx := context.Background()
	if err := InitializeTestSchemaPostgres(ctx, store); err != nil {
		t.Fatalf("Second InitializeTestSchemaPostgres failed: %v", err)
	}
}

func TestPostgresIntegration_CreateAndGetInstance(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	instance := &WorkflowInstance{
		InstanceID:   "test-instance-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		InputData:    []byte(`{"key": "value"}`),
		StartedAt:    now,
		UpdatedAt:    now,
	}

	if err := store.CreateInstance(ctx, instance); err != nil {
		t.Fatalf("CreateInstance failed: %v", err)
	}

	got, err := store.GetInstance(ctx, "test-instance-1")
	if err != nil {
		t.Fatalf("GetInstance failed: %v", err)
	}

	if got == nil {
		t.Fatal("GetInstance returned nil")
	}

	if got.InstanceID != instance.InstanceID {
		t.Errorf("InstanceID = %q, want %q", got.InstanceID, instance.InstanceID)
	}
	if got.WorkflowName != instance.WorkflowName {
		t.Errorf("WorkflowName = %q, want %q", got.WorkflowName, instance.WorkflowName)
	}
	if got.Status != instance.Status {
		t.Errorf("Status = %q, want %q", got.Status, instance.Status)
	}
	if string(got.InputData) != string(instance.InputData) {
		t.Errorf("InputData = %q, want %q", string(got.InputData), string(instance.InputData))
	}
}

func TestPostgresIntegration_RequestCancel(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	instance := &WorkflowInstance{
		InstanceID:   "test-status-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(ctx, instance)

	if _, err := store.RequestCancel(ctx, "test-status-1", "test"); err != nil {
		t.Fatalf("RequestCancel failed: %v", err)
	}

	got, _ := store.GetInstance(ctx, "test-status-1")
	if got.Status != StatusRunning {
		t.Errorf("Status = %q, want %q", got.Status, StatusRunning)
	}
}

func TestPostgresIntegration_LockOperations(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	instance := &WorkflowInstance{
		InstanceID:   "test-lock-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(ctx, instance)

	// Acquire lock
	f, acquired, err := store.AcquireLease(ctx, "test-lock-1", "worker-1", time.Second)
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	if !acquired {
		t.Error("Expected to acquire lock")
	}

	// Another worker cannot acquire lock
	_, acquired2, err := store.AcquireLease(ctx, "test-lock-1", "worker-2", time.Second)
	if err != nil {
		t.Fatalf("AcquireLease (worker-2) failed: %v", err)
	}
	if acquired2 {
		t.Error("Worker-2 should not acquire lock")
	}

	// A live lease cannot be re-entered, even with the same owner.
	_, acquired3, err := store.AcquireLease(ctx, "test-lock-1", "worker-1", time.Second)
	if err != nil {
		t.Fatalf("AcquireLease (re-entrant) failed: %v", err)
	}
	if acquired3 {
		t.Error("Same owner must not re-acquire a live lease")
	}

	// Release lock
	if err := store.ReleaseLease(ctx, f, true); err != nil {
		t.Fatalf("ReleaseLease failed: %v", err)
	}

	// Now worker-2 can acquire
	_, acquired4, err := store.AcquireLease(ctx, "test-lock-1", "worker-2", time.Second)
	if err != nil {
		t.Fatalf("AcquireLease after release failed: %v", err)
	}
	if !acquired4 {
		t.Error("Worker-2 should acquire lock after release")
	}
}

func TestPostgresIntegration_HistoryOperations(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	instance := &WorkflowInstance{
		InstanceID:   "test-history-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(ctx, instance)

	event := &HistoryEvent{
		InstanceID: "test-history-1",
		ActivityID: "activity:1",
		EventType:  HistoryActivityCompleted,
		EventData:  []byte(`{"result": "ok"}`),
		DataType:   "json",
	}

	if _, err := fixtureRecord(ctx, store, event); err != nil {
		t.Fatalf("RecordHistory failed: %v", err)
	}

	history, hasMore, err := store.GetHistoryPaginated(ctx, "test-history-1", 0, 100)
	if err != nil {
		t.Fatalf("GetHistoryPaginated failed: %v", err)
	}
	if hasMore {
		t.Error("expected hasMore=false for small result set")
	}

	if len(history) != 1 {
		t.Fatalf("len(history) = %d, want 1", len(history))
	}

	if history[0].ActivityID != "activity:1" {
		t.Errorf("ActivityID = %q, want %q", history[0].ActivityID, "activity:1")
	}
}

func TestPostgresIntegration_TimerSubscription(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	instance := &WorkflowInstance{
		InstanceID:   "test-timer-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(ctx, instance)

	sub := &TimerSubscription{
		InstanceID: "test-timer-1",
		TimerID:    "timer-1",
		ExpiresAt:  time.Now().Add(-1 * time.Hour), // Past time
		ActivityID: "activity:1",
	}

	if _, err := store.ReserveTimer(ctx, fixtureFence(t, store, sub.InstanceID), 1, sub); err != nil {
		t.Fatalf("timer fixture failed: %v", err)
	}

	timers, err := store.FindExpiredTimers(ctx, 100)
	if err != nil {
		t.Fatalf("FindExpiredTimers failed: %v", err)
	}

	if len(timers) != 1 {
		t.Fatalf("len(timers) = %d, want 1", len(timers))
	}

	if err := store.FireTimer(ctx, sub); err != nil {
		t.Fatalf("RemoveTimerSubscription failed: %v", err)
	}
}

func TestPostgresIntegration_Transaction(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	// Begin transaction
	txCtx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	if !store.InTransaction(txCtx) {
		t.Error("InTransaction should return true")
	}

	instance := &WorkflowInstance{
		InstanceID:   "test-tx-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(txCtx, instance)

	// Rollback
	store.RollbackTransaction(txCtx)

	// Instance should not exist
	got, _ := store.GetInstance(ctx, "test-tx-1")
	if got != nil {
		t.Error("Instance should not exist after rollback")
	}
}

func TestPostgresIntegration_TransactionCommit(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	txCtx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	instance := &WorkflowInstance{
		InstanceID:   "test-tx-commit-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(txCtx, instance)

	if err := store.CommitTransaction(txCtx); err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	got, _ := store.GetInstance(ctx, "test-tx-commit-1")
	if got == nil {
		t.Error("Instance should exist after commit")
	}
}

func TestPostgresIntegration_ListInstances(t *testing.T) {
	store, cleanup := setupPostgresContainer(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	// Create multiple instances
	for i := 1; i <= 5; i++ {
		instance := &WorkflowInstance{
			InstanceID:   fmt.Sprintf("list-test-%d", i),
			WorkflowName: "test_workflow",
			Status:       StatusRunning,
			StartedAt:    now.Add(time.Duration(i) * time.Second),
			UpdatedAt:    now.Add(time.Duration(i) * time.Second),
		}
		store.CreateInstance(ctx, instance)
	}

	// List with limit
	result, err := store.ListInstances(ctx, ListInstancesOptions{Limit: 3})
	if err != nil {
		t.Fatalf("ListInstances failed: %v", err)
	}

	if len(result.Instances) != 3 {
		t.Errorf("len(instances) = %d, want 3", len(result.Instances))
	}

	if result.NextPageToken == "" {
		t.Error("Expected next page token")
	}

	// Get next page
	result2, err := store.ListInstances(ctx, ListInstancesOptions{
		Limit:     3,
		PageToken: result.NextPageToken,
	})
	if err != nil {
		t.Fatalf("ListInstances (page 2) failed: %v", err)
	}

	if len(result2.Instances) != 2 {
		t.Errorf("len(instances) page 2 = %d, want 2", len(result2.Instances))
	}
}
