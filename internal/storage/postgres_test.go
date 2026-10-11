//go:build postgres

package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

// Test PostgreSQL with:
//   docker compose up -d
//   go test -tags=postgres -v ./internal/storage/...
//
// Or set SELVEDGE_TEST_POSTGRES_URL environment variable:
//   export SELVEDGE_TEST_POSTGRES_URL="postgres://selvedge:selvedge@localhost:5432/selvedge_test?sslmode=disable"

func getTestPostgresURL() string {
	url := os.Getenv("SELVEDGE_TEST_POSTGRES_URL")
	if url == "" {
		url = "postgres://selvedge:selvedge@localhost:5432/selvedge_test?sslmode=disable"
	}
	return url
}

func setupPostgresStorage(t *testing.T) *PostgresStorage {
	t.Helper()

	store, err := NewPostgresStorage(getTestPostgresURL())
	if err != nil {
		t.Fatalf("Failed to create PostgreSQL storage: %v", err)
	}

	ctx := context.Background()
	if err := InitializeTestSchemaPostgres(ctx, store); err != nil {
		store.Close()
		t.Fatalf("Failed to initialize test schema: %v", err)
	}

	// Clean up tables for test isolation
	cleanupTables(t, store)

	return store
}

func cleanupTables(t *testing.T, store *PostgresStorage) {
	t.Helper()
	ctx := context.Background()

	// Order matters due to foreign key constraints
	tables := []string{
		"channel_delivery_cursors",
		"channel_messages",
		"channel_subscriptions",
		"workflow_timer_subscriptions",
		"workflow_history",
		"workflow_instances",
		"workflow_definitions",
		"system_locks",
	}

	for _, table := range tables {
		if _, err := store.db.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			t.Logf("Warning: failed to clean table %s: %v", table, err)
		}
	}
}

func TestPostgresStorage_Initialize(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

	// Initialize should be idempotent
	ctx := context.Background()
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Second Initialize failed: %v", err)
	}
}

func TestPostgresStorage_CreateAndGetInstance(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

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

	// Retrieve the instance
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

func TestPostgresStorage_RequestCancel(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

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

func TestPostgresStorage_LockOperations(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

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

func TestPostgresStorage_HistoryOperations(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// Create instance first
	instance := &WorkflowInstance{
		InstanceID:   "test-history-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(ctx, instance)

	// Append history
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

	// Get history
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

func TestPostgresStorage_TimerSubscription(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// Create instance
	instance := &WorkflowInstance{
		InstanceID:   "test-timer-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(ctx, instance)

	// Register timer (already expired)
	sub := &TimerSubscription{
		InstanceID: "test-timer-1",
		TimerID:    "timer-1",
		ExpiresAt:  time.Now().Add(-1 * time.Hour), // Past time
		ActivityID: "activity:1",
	}

	if err := store.RegisterTimerSubscription(fixtureContext(t, store, ctx, "test-timer-1"), sub); err != nil {
		t.Fatalf("RegisterTimerSubscription failed: %v", err)
	}

	// Find expired timers
	timers, err := store.FindExpiredTimers(ctx, 100)
	if err != nil {
		t.Fatalf("FindExpiredTimers failed: %v", err)
	}

	if len(timers) != 1 {
		t.Fatalf("len(timers) = %d, want 1", len(timers))
	}

	// Remove timer
	if err := store.FireTimer(ctx, sub); err != nil {
		t.Fatalf("RemoveTimerSubscription failed: %v", err)
	}
}

func TestPostgresStorage_Transaction(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// Begin transaction
	txCtx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Verify in transaction
	if !store.InTransaction(txCtx) {
		t.Error("InTransaction should return true")
	}

	// Create instance in transaction
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

func TestPostgresStorage_TransactionCommit(t *testing.T) {
	store := setupPostgresStorage(t)
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	// Begin transaction
	txCtx, err := store.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Create instance in transaction
	instance := &WorkflowInstance{
		InstanceID:   "test-tx-commit-1",
		WorkflowName: "test_workflow",
		Status:       StatusRunning,
		StartedAt:    now,
		UpdatedAt:    now,
	}
	store.CreateInstance(txCtx, instance)

	// Commit
	if err := store.CommitTransaction(txCtx); err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Instance should exist
	got, _ := store.GetInstance(ctx, "test-tx-commit-1")
	if got == nil {
		t.Error("Instance should exist after commit")
	}
}
