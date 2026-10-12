package replay

import (
	"context"
	"encoding/json"
	"errors"
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

			engine := NewEngine(store, hooks, tt.workerID, testErrorCodec{})

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
		ctx:        ctx,
		instanceID: "inst-1",
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
				ctx:        context.Background(),
				instanceID: tt.instanceID,
			}

			assert.Equal(t, tt.instanceID, execCtx.InstanceID())
		})
	}
}

func TestExecutionContext_Storage(t *testing.T) {
	store := setupTestStorage(t)
	engine := NewEngine(store, &hooks.NoOpHooks{}, "worker-1", testErrorCodec{})

	execCtx := &ExecutionContext{
		ctx:        context.Background(),
		instanceID: "inst-1",
		engine:     engine,
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
			engine := NewEngine(store, &hooks.NoOpHooks{}, "worker-1", testErrorCodec{})

			instance := &storage.WorkflowInstance{
				InstanceID:   "inst-" + tt.name,
				WorkflowName: "test_workflow",
				Status:       storage.StatusRunning,
			}
			require.NoError(t, store.CreateInstance(context.Background(), instance))

			runner := func(execCtx *ExecutionContext) (any, error) {
				return tt.runnerResult, tt.runnerErr
			}

			err := engine.Run(context.Background(), instance.InstanceID, runner)

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
	engine := NewEngine(store, &hooks.NoOpHooks{}, "worker-1", testErrorCodec{})

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

	err := engine.Run(context.Background(), instance.InstanceID, runner)
	require.NoError(t, err)

	gotInstance, err := store.GetInstance(context.Background(), instance.InstanceID)
	require.NoError(t, err)
	assert.Equal(t, storage.StatusRunning, gotInstance.Status)
}

// The engine only encodes opaque error payloads; root tests exercise the full codec.
type testErrorCodec struct{}

func (testErrorCodec) Encode(err error) []byte {
	data, _ := json.Marshal(map[string]any{"v": 1, "kind": "generic", "Message": err.Error()})
	return data
}
