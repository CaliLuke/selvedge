package notify

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListenerRejectsConcurrentStartsAndRestartsAfterStop(t *testing.T) {
	listener := NewListener("://invalid")
	t.Cleanup(func() { require.NoError(t, listener.Stop(context.Background())) })
	require.NoError(t, listener.Start(context.Background()))
	require.Error(t, listener.Start(context.Background()))
	require.NoError(t, listener.Stop(context.Background()))
	require.False(t, listener.IsActive())
	require.NoError(t, listener.Start(context.Background()))
	require.NoError(t, listener.Stop(context.Background()))
}

func TestListenerRejectsCancelledStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	listener := NewListener("://invalid")
	require.ErrorIs(t, listener.Start(ctx), context.Canceled)
	require.NoError(t, listener.Stop(context.Background()))
}
