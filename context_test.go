package selvedge

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testContextKey is a custom type for context keys in tests.
type testContextKey string

func TestNewWorkflowContext(t *testing.T) {
	tests := []struct {
		name         string
		instanceID   string
		workerID     string
		workflowName string
		isReplaying  bool
	}{
		{
			name:         "new execution without history",
			instanceID:   "inst-001",
			workerID:     "worker-1",
			workflowName: "test_workflow",
			isReplaying:  false,
		},
		{
			name:         "new execution with empty history",
			instanceID:   "inst-002",
			workerID:     "worker-2",
			workflowName: "test_workflow",
			isReplaying:  false,
		},
		{
			name:         "replay context",
			instanceID:   "inst-003",
			workerID:     "worker-3",
			workflowName: "test_workflow",
			isReplaying:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			wfCtx := NewWorkflowContext(ctx, tt.instanceID, tt.workerID, tt.workflowName, tt.isReplaying)

			require.NotNil(t, wfCtx)
			assert.Equal(t, tt.instanceID, wfCtx.InstanceID())
			assert.Equal(t, tt.workerID, wfCtx.WorkerID())
			assert.Equal(t, tt.workflowName, wfCtx.WorkflowName())
			assert.Equal(t, tt.isReplaying, wfCtx.IsReplaying())
			assert.NotNil(t, wfCtx.Context())
		})
	}
}

func TestWorkflowContext_Getters(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "test-instance", "test-worker", "test-workflow", false)

	tests := []struct {
		name     string
		getter   func() any
		expected any
	}{
		{
			name:     "InstanceID",
			getter:   func() any { return wfCtx.InstanceID() },
			expected: "test-instance",
		},
		{
			name:     "WorkerID",
			getter:   func() any { return wfCtx.WorkerID() },
			expected: "test-worker",
		},
		{
			name:     "WorkflowName",
			getter:   func() any { return wfCtx.WorkflowName() },
			expected: "test-workflow",
		},
		{
			name:     "IsReplaying",
			getter:   func() any { return wfCtx.IsReplaying() },
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.getter())
		})
	}
}

func TestWorkflowContext_GenerateActivityID(t *testing.T) {
	tests := []struct {
		name        string
		calls       []string
		wantResults []string
	}{
		{
			name:        "single function sequential calls",
			calls:       []string{"activity_a", "activity_a", "activity_a"},
			wantResults: []string{"activity_a:1", "activity_a:2", "activity_a:3"},
		},
		{
			name:        "different functions",
			calls:       []string{"func_a", "func_b", "func_c"},
			wantResults: []string{"func_a:1", "func_b:1", "func_c:1"},
		},
		{
			name:        "mixed functions",
			calls:       []string{"func_a", "func_b", "func_a", "func_b", "func_a"},
			wantResults: []string{"func_a:1", "func_b:1", "func_a:2", "func_b:2", "func_a:3"},
		},
		{
			name:        "single call",
			calls:       []string{"only_once"},
			wantResults: []string{"only_once:1"},
		},
		{
			name:        "many calls same function",
			calls:       []string{"repeat", "repeat", "repeat", "repeat", "repeat"},
			wantResults: []string{"repeat:1", "repeat:2", "repeat:3", "repeat:4", "repeat:5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

			for i, call := range tt.calls {
				result := wfCtx.GenerateActivityID(call)
				assert.Equal(t, tt.wantResults[i], result, "call %d", i)
			}
		})
	}
}

func TestWorkflowContext_GenerateActivityID_Deterministic(t *testing.T) {
	calls := []string{"a", "b", "a", "c", "b", "a"}

	ctx := context.Background()

	wfCtx1 := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)
	wfCtx2 := NewWorkflowContext(ctx, "inst-2", "worker-2", "workflow", false)

	results1 := make([]string, 0, len(calls))
	results2 := make([]string, 0, len(calls))
	for _, call := range calls {
		results1 = append(results1, wfCtx1.GenerateActivityID(call))
		results2 = append(results2, wfCtx2.GenerateActivityID(call))
	}

	assert.Equal(t, results1, results2, "same call sequence should produce same IDs")
}

func TestWorkflowContext_GenerateActivityID_Concurrency(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	const goroutines = 100
	const callsPerGoroutine = 10

	var wg sync.WaitGroup
	results := make(chan string, goroutines*callsPerGoroutine)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				id := wfCtx.GenerateActivityID("concurrent")
				results <- id
			}
		}()
	}

	wg.Wait()
	close(results)

	seen := make(map[string]bool)
	for id := range results {
		if seen[id] {
			t.Errorf("duplicate activity ID generated: %s", id)
		}
		seen[id] = true
	}

	assert.Len(t, seen, goroutines*callsPerGoroutine, "should have generated unique IDs for all calls")
}

func TestWorkflowContext_WithContext(t *testing.T) {
	parentCtx := context.Background()
	wfCtx := NewWorkflowContext(parentCtx, "inst-1", "worker-1", "workflow", true)

	wfCtx.GenerateActivityID("counter")
	wfCtx.GenerateActivityID("counter")

	newCtx := context.WithValue(context.Background(), testContextKey("key"), "new-value")
	newWfCtx := wfCtx.WithContext(newCtx)

	assert.NotEqual(t, wfCtx.Context(), newWfCtx.Context())
	assert.Equal(t, newCtx, newWfCtx.Context())

	assert.Equal(t, wfCtx.InstanceID(), newWfCtx.InstanceID())
	assert.Equal(t, wfCtx.WorkerID(), newWfCtx.WorkerID())
	assert.Equal(t, wfCtx.WorkflowName(), newWfCtx.WorkflowName())
	assert.Equal(t, wfCtx.IsReplaying(), newWfCtx.IsReplaying())

	id := newWfCtx.GenerateActivityID("counter")
	assert.Equal(t, "counter:3", id)
}

func TestWorkflowContext_Cancel(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	assert.Nil(t, wfCtx.Err())

	select {
	case <-wfCtx.Done():
		t.Fatal("context should not be done before cancel")
	default:
	}

	wfCtx.Cancel()

	<-wfCtx.Done()
	assert.Equal(t, context.Canceled, wfCtx.Err())
}

func TestWorkflowContext_Done(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	doneChan := wfCtx.Done()
	require.NotNil(t, doneChan)

	select {
	case <-doneChan:
		t.Fatal("done channel should not be closed before cancel")
	default:
	}

	wfCtx.Cancel()

	select {
	case <-doneChan:
	default:
		t.Fatal("done channel should be closed after cancel")
	}
}

func TestWorkflowContext_Err(t *testing.T) {
	tests := []struct {
		name         string
		setupContext func() (*WorkflowContext, func())
		wantErr      error
	}{
		{
			name: "no error initially",
			setupContext: func() (*WorkflowContext, func()) {
				ctx := context.Background()
				wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)
				return wfCtx, func() {}
			},
			wantErr: nil,
		},
		{
			name: "context canceled",
			setupContext: func() (*WorkflowContext, func()) {
				ctx := context.Background()
				wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)
				return wfCtx, func() { wfCtx.Cancel() }
			},
			wantErr: context.Canceled,
		},
		{
			name: "parent context canceled",
			setupContext: func() (*WorkflowContext, func()) {
				parentCtx, parentCancel := context.WithCancel(context.Background())
				wfCtx := NewWorkflowContext(parentCtx, "inst-1", "worker-1", "workflow", false)
				return wfCtx, parentCancel
			},
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wfCtx, trigger := tt.setupContext()

			if tt.wantErr == nil {
				assert.Nil(t, wfCtx.Err())
			} else {
				trigger()
				assert.Equal(t, tt.wantErr, wfCtx.Err())
			}
		})
	}
}

func TestWorkflowContext_Storage_NilWithoutExecCtx(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	assert.Nil(t, wfCtx.Storage())
}

func TestWorkflowContext_Session_NilWithoutStorage(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	assert.Nil(t, wfCtx.Session())
}

func TestWorkflowContext_IsReplaying_Values(t *testing.T) {
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
			ctx := context.Background()
			wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", tt.isReplaying)
			assert.Equal(t, tt.isReplaying, wfCtx.IsReplaying())
		})
	}
}

func TestWorkflowContext_RecordActivityID_WithoutExecCtx(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	assert.NotPanics(t, func() {
		wfCtx.RecordActivityID("test:1")
	})
}

func TestWorkflowContext_RecordActivityResult_WithoutExecCtx(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	err := wfCtx.RecordActivityResult("test:1", "result", nil)
	assert.NoError(t, err)

	_, found := wfCtx.lookupRecorded("test:1")
	assert.False(t, found)
}

func TestWorkflowContext_RecordActivityResult_WithoutExecCtx_Error(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	testErr := errors.New("activity failed")
	err := wfCtx.RecordActivityResult("test:1", nil, testErr)
	assert.NoError(t, err)
}
