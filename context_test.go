package selvedge

import (
	"context"
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
	}{
		{
			name:         "new execution without history",
			instanceID:   "inst-001",
			workerID:     "worker-1",
			workflowName: "test_workflow",
		},
		{
			name:         "new execution with empty history",
			instanceID:   "inst-002",
			workerID:     "worker-2",
			workflowName: "test_workflow",
		},
		{
			name:         "standalone context",
			instanceID:   "inst-003",
			workerID:     "worker-3",
			workflowName: "test_workflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			wfCtx := NewWorkflowContext(ctx, tt.instanceID, tt.workerID, tt.workflowName)

			require.NotNil(t, wfCtx)
			assert.Equal(t, tt.instanceID, wfCtx.InstanceID())
			assert.Equal(t, tt.workerID, wfCtx.WorkerID())
			assert.Equal(t, tt.workflowName, wfCtx.WorkflowName())
			assert.False(t, wfCtx.IsReplaying(), "no command has been issued")
			assert.NotNil(t, wfCtx.Context())
		})
	}
}

func TestWorkflowContext_Getters(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "test-instance", "test-worker", "test-workflow")

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

func TestWorkflowContext_WithContext(t *testing.T) {
	workflowContextWithRecordedHistory(t, nil, func(wfCtx *WorkflowContext) {
		func() { c, err := wfCtx.execCtx.Issue("activity:counter", ""); require.NoError(t, err); c.End() }()
		func() { c, err := wfCtx.execCtx.Issue("activity:counter", ""); require.NoError(t, err); c.End() }()

		newCtx := context.WithValue(context.Background(), testContextKey("key"), "new-value")
		newWfCtx := wfCtx.WithContext(newCtx)

		assert.NotEqual(t, wfCtx.Context(), newWfCtx.Context())
		assert.Equal(t, newCtx, newWfCtx.Context())

		assert.Equal(t, wfCtx.InstanceID(), newWfCtx.InstanceID())
		assert.Equal(t, wfCtx.WorkerID(), newWfCtx.WorkerID())
		assert.Equal(t, wfCtx.WorkflowName(), newWfCtx.WorkflowName())
		assert.Equal(t, wfCtx.IsReplaying(), newWfCtx.IsReplaying())

		cmd, err := newWfCtx.execCtx.Issue("activity:counter", "")
		require.NoError(t, err)
		defer cmd.End()
		assert.Equal(t, "#3", cmd.Key)
	})
}

func TestWorkflowContext_Cancel(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

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
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

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
				wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")
				return wfCtx, func() {}
			},
			wantErr: nil,
		},
		{
			name: "context canceled",
			setupContext: func() (*WorkflowContext, func()) {
				ctx := context.Background()
				wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")
				return wfCtx, func() { wfCtx.Cancel() }
			},
			wantErr: context.Canceled,
		},
		{
			name: "parent context canceled",
			setupContext: func() (*WorkflowContext, func()) {
				parentCtx, parentCancel := context.WithCancel(context.Background())
				wfCtx := NewWorkflowContext(parentCtx, "inst-1", "worker-1", "workflow")
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
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow")

	assert.Nil(t, wfCtx.Storage())
}
