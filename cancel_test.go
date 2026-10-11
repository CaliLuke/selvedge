package selvedge_test

import (
	"context"
	"testing"
	"time"

	"github.com/CaliLuke/selvedge"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestCancelWorkflow(t *testing.T) {
	// Create a temporary SQLite database
	app := selvedge.NewApp(
		selvedge.WithDatabase(":memory:"),
		selvedge.WithWorkerID("test-worker"),
	)

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start app: %v", err)
	}
	defer func() { _ = app.Shutdown(ctx) }()

	// Initialize test schema
	if err := storage.InitializeTestSchemaForStorage(ctx, app.Storage()); err != nil {
		t.Fatalf("Failed to initialize test schema: %v", err)
	}

	// Create a workflow instance manually
	instance := &storage.WorkflowInstance{
		InstanceID:   "test-cancel-123",
		WorkflowName: "test_workflow",
		Status:       storage.StatusRunning,
		InputData:    []byte(`{"item_id": "ITEM-001"}`),
		StartedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	if err := app.Storage().CreateInstance(ctx, instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Cancel the workflow
	err := selvedge.CancelWorkflow(ctx, app, "test-cancel-123", "user request")
	if err != nil {
		t.Fatalf("Failed to cancel workflow: %v", err)
	}

	// A request remains running until the workflow observes it at a yield.
	result, err := app.GetInstance(ctx, "test-cancel-123")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}
	if result.Status != storage.StatusRunning {
		t.Errorf("Expected status 'running', got '%s'", result.Status)
	}
	requested, err := app.Storage().IsCancelRequested(ctx, "test-cancel-123")
	if err != nil || !requested {
		t.Fatalf("expected durable cancellation request: requested=%v err=%v", requested, err)
	}
}

func TestCancelCompletedWorkflow(t *testing.T) {
	app := selvedge.NewApp(
		selvedge.WithDatabase(":memory:"),
		selvedge.WithWorkerID("test-worker"),
	)

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start app: %v", err)
	}
	defer func() { _ = app.Shutdown(ctx) }()

	// Initialize test schema
	if err := storage.InitializeTestSchemaForStorage(ctx, app.Storage()); err != nil {
		t.Fatalf("Failed to initialize test schema: %v", err)
	}

	// Create a completed workflow instance
	instance := &storage.WorkflowInstance{
		InstanceID:   "completed-123",
		WorkflowName: "test_workflow",
		Status:       storage.StatusCompleted,
		InputData:    []byte(`{}`),
		StartedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	if err := app.Storage().CreateInstance(ctx, instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Try to cancel - should fail
	err := selvedge.CancelWorkflow(ctx, app, "completed-123", "user request")
	if err == nil {
		t.Error("Expected error when canceling completed workflow")
	}
}

func TestCancelIdempotent(t *testing.T) {
	app := selvedge.NewApp(
		selvedge.WithDatabase(":memory:"),
		selvedge.WithWorkerID("test-worker"),
	)

	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Failed to start app: %v", err)
	}
	defer func() { _ = app.Shutdown(ctx) }()

	// Initialize test schema
	if err := storage.InitializeTestSchemaForStorage(ctx, app.Storage()); err != nil {
		t.Fatalf("Failed to initialize test schema: %v", err)
	}

	// Create an already cancelled workflow instance
	instance := &storage.WorkflowInstance{
		InstanceID:   "cancelled-123",
		WorkflowName: "test_workflow",
		Status:       storage.StatusCancelled,
		InputData:    []byte(`{}`),
		StartedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	if err := app.Storage().CreateInstance(ctx, instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Cancel again - should succeed (idempotent)
	err := selvedge.CancelWorkflow(ctx, app, "cancelled-123", "user request")
	if err != nil {
		t.Errorf("Expected no error when canceling already canceled workflow, got: %v", err)
	}
}
