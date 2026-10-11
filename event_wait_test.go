package selvedge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CaliLuke/selvedge/internal/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithReceiveTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{
			name:    "5 seconds",
			timeout: 5 * time.Second,
		},
		{
			name:    "1 minute",
			timeout: 1 * time.Minute,
		},
		{
			name:    "100 milliseconds",
			timeout: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &receiveOptions{}
			withReceiveEventTimeout(tt.timeout)(opts)
			require.NotNil(t, opts.timeout)
			assert.Equal(t, tt.timeout, *opts.timeout)
		})
	}
}

func TestSubscribe_ReleasedExecutionIsFenced(t *testing.T) {
	historyCache := map[string]any{
		"subscribe_test-channel:1": true,
	}
	wfCtx := workflowContextWithRecordedHistory(t, historyCache)

	err := subscribeEvent(wfCtx, "test-channel")
	require.ErrorIs(t, err, storage.ErrFenced)
}

func TestSubscribe_NoStorage(t *testing.T) {
	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	err := subscribeEvent(wfCtx, "test-channel")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage not available")
}

func TestReceive_CachedResult(t *testing.T) {
	type TestMessage struct {
		Content string
	}

	cachedMsg := &receivedEventMessage[TestMessage]{
		ID:          1,
		ChannelName: "test-channel",
		Data:        TestMessage{Content: "cached content"},
		CreatedAt:   time.Now(),
	}
	historyCache := map[string]any{
		"receive_test-channel:1": cachedMsg,
	}
	wfCtx := workflowContextWithRecordedHistory(t, historyCache)

	result, err := receiveEvent[TestMessage](wfCtx, "test-channel")

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "cached content", result.Data.Content)
}

func TestReceive_CachedTimeoutError(t *testing.T) {
	type TestMessage struct {
		Content string
	}

	cachedErr := &ChannelMessageTimeoutError{
		InstanceID:  "inst-1",
		ChannelName: "test-channel",
	}
	historyCache := map[string]any{
		"receive_test-channel:1": cachedErr,
	}
	wfCtx := workflowContextWithRecordedHistory(t, historyCache)

	result, err := receiveEvent[TestMessage](wfCtx, "test-channel")

	require.Error(t, err)
	assert.Nil(t, result)

	var timeoutErr *ChannelMessageTimeoutError
	require.True(t, errors.As(err, &timeoutErr))
	assert.Equal(t, "inst-1", timeoutErr.InstanceID)
	assert.Equal(t, "test-channel", timeoutErr.ChannelName)
}

func TestReceive_NoStorage(t *testing.T) {
	type TestMessage struct {
		Content string
	}

	ctx := context.Background()
	wfCtx := NewWorkflowContext(ctx, "inst-1", "worker-1", "workflow", false)

	result, err := receiveEvent[TestMessage](wfCtx, "test-channel")

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "storage not available")
}

func TestReceivedMessage_Structure(t *testing.T) {
	type CustomData struct {
		Field1 string
		Field2 int
	}

	msg := receivedEventMessage[CustomData]{
		ID:          123,
		ChannelName: "test-channel",
		Data:        CustomData{Field1: "test", Field2: 42},
		Metadata:    map[string]any{"key": "value"},
		CreatedAt:   time.Now(),
	}

	assert.Equal(t, int64(123), msg.ID)
	assert.Equal(t, "test-channel", msg.ChannelName)
	assert.Equal(t, "test", msg.Data.Field1)
	assert.Equal(t, 42, msg.Data.Field2)
	assert.Equal(t, "value", msg.Metadata["key"])
}

func TestReceive_WithTimeout_Option(t *testing.T) {
	timeout := 30 * time.Second
	opt := withReceiveEventTimeout(timeout)

	opts := &receiveOptions{}
	opt(opts)

	require.NotNil(t, opts.timeout)
	assert.Equal(t, timeout, *opts.timeout)
}

func TestReceive_NoTimeout_Option(t *testing.T) {
	opts := &receiveOptions{}

	assert.Nil(t, opts.timeout)
}

func TestReceive_CachedGenericError(t *testing.T) {
	type TestMessage struct {
		Content string
	}

	cachedErr := errors.New("some other error")
	historyCache := map[string]any{
		"receive_test-channel:1": cachedErr,
	}
	wfCtx := workflowContextWithRecordedHistory(t, historyCache)

	result, err := receiveEvent[TestMessage](wfCtx, "test-channel")

	require.Error(t, err)
	assert.Nil(t, result)
	assert.EqualError(t, err, cachedErr.Error())
}
