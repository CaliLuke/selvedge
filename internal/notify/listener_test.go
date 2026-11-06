package notify

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

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

// A TCP peer that accepts but never completes the PostgreSQL handshake makes
// cancellation during Connect deterministic without requiring a database.
func TestListenerStopDuringConnectDoesNotReportFailure(t *testing.T) {
	server, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer server.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := server.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(previous)
	listener := NewListener("postgres://test:test@" + server.Addr().String() + "/test?sslmode=disable")
	require.NoError(t, listener.Start(context.Background()))
	defer listener.Stop(context.Background())
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("listener never connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, listener.Stop(ctx))
	require.Nil(t, listener.LastError())
	require.Empty(t, logs.String())
}
