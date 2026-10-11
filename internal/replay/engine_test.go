package replay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func setupTestStorage(t *testing.T) storage.Storage {
	store, err := storage.NewSQLiteStorage(":memory:")
	require.NoError(t, err)
	require.NoError(t, storage.InitializeTestSchema(context.Background(), store))
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestNewEngine(t *testing.T) {
	tests := []struct {
		name     string
		workerID string
	}{
		{
			name:     "with worker ID",
			workerID: "worker-1",
		},
		{
			name:     "with UUID worker ID",
			workerID: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "with empty worker ID",
			workerID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStorage(t)
			hooks := &hooks.NoOpHooks{}

			engine := NewEngine(store, hooks, tt.workerID)

			require.NotNil(t, engine)
			assert.Equal(t, store, engine.storage)
			assert.Equal(t, hooks, engine.hooks)
			assert.Equal(t, tt.workerID, engine.workerID)
		})
	}
}

func TestExecutionContext_Context(t *testing.T) {
	ctx := context.Background()
	execCtx := &ExecutionContext{
		ctx:             ctx,
		instanceID:      "inst-1",
		activityCounter: make(map[string]int),
		recorded:        make(map[string]*storage.HistoryEvent),
	}

	assert.Equal(t, ctx, execCtx.Context())
}

func TestExecutionContext_InstanceID(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
	}{
		{
			name:       "simple ID",
			instanceID: "inst-123",
		},
		{
			name:       "UUID ID",
			instanceID: "550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execCtx := &ExecutionContext{
				ctx:             context.Background(),
				instanceID:      tt.instanceID,
				activityCounter: make(map[string]int),
				recorded:        make(map[string]*storage.HistoryEvent),
			}

			assert.Equal(t, tt.instanceID, execCtx.InstanceID())
		})
	}
}

func TestExecutionContext_IsReplaying(t *testing.T) {
	tests := []struct {
		name        string
		isReplaying bool
	}{
		{
			name:        "replaying true",
			isReplaying: true,
		},
		{
			name:        "replaying false",
			isReplaying: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execCtx := &ExecutionContext{
				ctx:             context.Background(),
				instanceID:      "inst-1",
				isReplaying:     tt.isReplaying,
				activityCounter: make(map[string]int),
				recorded:        make(map[string]*storage.HistoryEvent),
			}

			assert.Equal(t, tt.isReplaying, execCtx.IsReplaying())
		})
	}
}

func TestExecutionContext_GenerateActivityID(t *testing.T) {
	tests := []struct {
		name        string
		calls       []string
		wantResults []string
	}{
		{
			name:        "sequential same activity",
			calls:       []string{"activity_a", "activity_a", "activity_a"},
			wantResults: []string{"activity_a:1", "activity_a:2", "activity_a:3"},
		},
		{
			name:        "different activities",
			calls:       []string{"func_a", "func_b", "func_c"},
			wantResults: []string{"func_a:1", "func_b:1", "func_c:1"},
		},
		{
			name:        "mixed activities",
			calls:       []string{"a", "b", "a", "b", "a"},
			wantResults: []string{"a:1", "b:1", "a:2", "b:2", "a:3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			execCtx := &ExecutionContext{
				ctx:             context.Background(),
				instanceID:      "inst-1",
				activityCounter: make(map[string]int),
				recorded:        make(map[string]*storage.HistoryEvent),
			}

			var results []string
			for _, call := range tt.calls {
				results = append(results, execCtx.GenerateActivityID(call))
			}

			assert.Equal(t, tt.wantResults, results)
		})
	}
}

func TestExecutionContext_GenerateActivityID_Concurrency(t *testing.T) {
	execCtx := &ExecutionContext{
		ctx:             context.Background(),
		instanceID:      "inst-1",
		activityCounter: make(map[string]int),
		recorded:        make(map[string]*storage.HistoryEvent),
	}

	const goroutines = 50
	const callsPerGoroutine = 20

	var wg sync.WaitGroup
	results := make(chan string, goroutines*callsPerGoroutine)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				id := execCtx.GenerateActivityID("concurrent")
				results <- id
			}
		}()
	}

	wg.Wait()
	close(results)

	seen := make(map[string]bool)
	for id := range results {
		if seen[id] {
			t.Errorf("duplicate activity ID: %s", id)
		}
		seen[id] = true
	}

	assert.Len(t, seen, goroutines*callsPerGoroutine)
}

func TestExecutionContext_Storage(t *testing.T) {
	store := setupTestStorage(t)
	engine := NewEngine(store, &hooks.NoOpHooks{}, "worker-1")

	execCtx := &ExecutionContext{
		ctx:             context.Background(),
		instanceID:      "inst-1",
		engine:          engine,
		activityCounter: make(map[string]int),
		recorded:        make(map[string]*storage.HistoryEvent),
	}

	assert.Equal(t, store, execCtx.Storage())
}

func TestEngine_StartWorkflow(t *testing.T) {
	tests := []struct {
		name         string
		runnerResult any
		runnerErr    error
		wantStatus   storage.WorkflowStatus
		wantErr      bool
	}{
		{
			name:         "success",
			runnerResult: "completed result",
			runnerErr:    nil,
			wantStatus:   storage.StatusCompleted,
			wantErr:      false,
		},
		{
			name:         "failure",
			runnerResult: nil,
			runnerErr:    errors.New("workflow error"),
			wantStatus:   storage.StatusFailed,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStorage(t)
			engine := NewEngine(store, &hooks.NoOpHooks{}, "worker-1")

			instance := &storage.WorkflowInstance{
				InstanceID:   "inst-" + tt.name,
				WorkflowName: "test_workflow",
				Status:       storage.StatusRunning,
			}
			require.NoError(t, store.CreateInstance(context.Background(), instance))

			runner := func(execCtx *ExecutionContext) (any, error) {
				return tt.runnerResult, tt.runnerErr
			}

			err := engine.StartWorkflow(context.Background(), instance.InstanceID, "test_workflow", nil, runner)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			gotInstance, err := store.GetInstance(context.Background(), instance.InstanceID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, gotInstance.Status)
		})
	}
}

func TestEngine_StartWorkflow_TimerSuspend(t *testing.T) {
	store := setupTestStorage(t)
	engine := NewEngine(store, &hooks.NoOpHooks{}, "worker-1")

	instance := &storage.WorkflowInstance{
		InstanceID:   "inst-timer",
		WorkflowName: "test_workflow",
		Status:       storage.StatusRunning,
	}
	require.NoError(t, store.CreateInstance(context.Background(), instance))

	expiresAt := time.Now().Add(1 * time.Hour)
	runner := func(execCtx *ExecutionContext) (any, error) {
		return nil, NewTimerSuspend(execCtx.InstanceID(), "timer-1", "timer-1", expiresAt)
	}

	err := engine.StartWorkflow(context.Background(), instance.InstanceID, "test_workflow", nil, runner)
	require.NoError(t, err)

	gotInstance, err := store.GetInstance(context.Background(), instance.InstanceID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusWaitingForTimer, gotInstance.Status)
}

func TestEngine_RecordActivityResult(t *testing.T) {
	tests := []struct {
		name        string
		activityID  string
		result      any
		activityErr error
		wantEvent   storage.HistoryEventType
	}{
		{
			name:        "success result",
			activityID:  "act:1",
			result:      "success",
			activityErr: nil,
			wantEvent:   storage.HistoryActivityCompleted,
		},
		{
			name:        "error result",
			activityID:  "act:2",
			result:      nil,
			activityErr: errors.New("activity failed"),
			wantEvent:   storage.HistoryActivityFailed,
		},
		{
			name:        "struct result",
			activityID:  "act:3",
			result:      struct{ Name string }{Name: "test"},
			activityErr: nil,
			wantEvent:   storage.HistoryActivityCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := setupTestStorage(t)
			engine := NewEngine(store, &hooks.NoOpHooks{}, "worker-1")

			instance := &storage.WorkflowInstance{
				InstanceID:   "inst-" + tt.name,
				WorkflowName: "test_workflow",
				Status:       storage.StatusRunning,
			}
			require.NoError(t, store.CreateInstance(context.Background(), instance))

			stored, err := engine.RecordActivityResult(context.Background(), instance.InstanceID, tt.activityID, tt.result, tt.activityErr)
			require.NoError(t, err)

			history, _, err := store.GetHistoryPaginated(context.Background(), instance.InstanceID, 0, 10)
			require.NoError(t, err)
			require.Len(t, history, 1)
			assert.Equal(t, tt.wantEvent, history[0].EventType)
			assert.Equal(t, tt.activityID, history[0].ActivityID)
			assert.Equal(t, history[0], stored)
		})
	}
}

func TestExecutionContext_Lookup(t *testing.T) {
	completed := &storage.HistoryEvent{ActivityID: "ok", EventType: storage.HistoryActivityCompleted, EventData: []byte(`{"n":9007199254740993}`)}
	corrupt := &storage.HistoryEvent{ActivityID: "corrupt", EventType: storage.HistoryActivityCompleted, EventData: []byte(`{`)}
	failed := &storage.HistoryEvent{ActivityID: "failed", EventType: storage.HistoryActivityFailed, EventData: []byte(`"failed"`)}
	ec := &ExecutionContext{recorded: map[string]*storage.HistoryEvent{"ok": completed, "corrupt": corrupt, "failed": failed}}
	for _, event := range []*storage.HistoryEvent{completed, corrupt, failed} {
		got, found := ec.Lookup(event.ActivityID)
		require.True(t, found)
		require.Same(t, event, got)
	}
	got, found := ec.Lookup("missing")
	require.False(t, found)
	require.Nil(t, got)
	require.Equal(t, 3, ec.cacheHits)
}
