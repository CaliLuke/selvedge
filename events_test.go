package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"
)

// TestWorkflowWithSleep tests sleep functionality.
func TestWorkflowWithSleep(t *testing.T) {
	// Create app with pre-initialized test database
	app, cleanup := createTestApp(t, WithTimerCheckInterval(100*time.Millisecond))
	defer cleanup()

	// Sleep duration is generous enough that loaded CI runners can still
	// observe the active workflow before the timer fires.
	const sleepDuration = 2 * time.Second

	sleepWorkflow := DefineWorkflow[string, string](
		"sleep_workflow",
		func(ctx *WorkflowContext, input string) (string, error) {
			if err := Sleep(ctx, sleepDuration); err != nil {
				return "", err
			}
			return "completed after sleep", nil
		},
	)

	// Register workflow
	RegisterWorkflow(app, sleepWorkflow)

	// Start the app
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("failed to start app: %v", err)
	}
	defer func() { _ = app.Shutdown(ctx) }()

	// Start the workflow
	instanceID, err := StartWorkflow(ctx, app, sleepWorkflow, "test")
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}

	// Poll until the instance is visible as running.
	if !waitForStatus(t, ctx, app, instanceID, storage.StatusRunning, sleepDuration) {
		instance, _ := app.GetInstance(ctx, instanceID)
		t.Fatalf("expected status 'running', got '%s'", instance.Status)
	}

	// Poll until workflow completes (timer fires + resumption + commit).
	if !waitForStatus(t, ctx, app, instanceID, storage.StatusCompleted, sleepDuration+3*time.Second) {
		instance, _ := app.GetInstance(ctx, instanceID)
		t.Fatalf("expected status 'completed', got '%s'", instance.Status)
	}

	result, err := GetWorkflowResult[string](ctx, app, instanceID)
	if err != nil {
		t.Fatalf("failed to get result: %v", err)
	}
	if result.Output != "completed after sleep" {
		t.Errorf("expected output 'completed after sleep', got '%s'", result.Output)
	}
}

// waitForStatus polls for the given workflow status until the timeout elapses.
// Returns true when the status is observed, false otherwise.
func waitForStatus(t *testing.T, ctx context.Context, app *App, instanceID string, want storage.WorkflowStatus, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		instance, err := app.GetInstance(ctx, instanceID)
		if err == nil && instance != nil && instance.Status == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
