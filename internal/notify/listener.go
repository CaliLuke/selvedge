// Package notify provides PostgreSQL LISTEN/NOTIFY functionality for real-time notifications.
package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// NotifyChannel represents a PostgreSQL notification channel name.
type NotifyChannel string

const (
	// ChannelWorkflowClosed wakes clients awaiting terminal results.
	ChannelWorkflowClosed NotifyChannel = "workflow_closed"
	// ChannelActivityTask wakes workers when activity work becomes available.
	ChannelActivityTask NotifyChannel = "workflow_activity_task"
	// ChannelWorkflowResumable is notified when a workflow becomes resumable.
	ChannelWorkflowResumable NotifyChannel = "workflow_resumable"
	// ChannelTimerExpired is notified when a timer is registered.
	ChannelTimerExpired NotifyChannel = "workflow_timer_expired"
)

// AllChannels returns all notification channels.
func AllChannels() []NotifyChannel {
	return []NotifyChannel{
		ChannelWorkflowClosed,
		ChannelActivityTask,
		ChannelWorkflowResumable,
		ChannelTimerExpired,
	}
}

// WorkflowNotification is the payload for workflow resumable notifications.
type WorkflowNotification struct {
	InstanceID   string `json:"instance_id"`
	WorkflowName string `json:"workflow_name,omitempty"`
}

// TimerNotification is the payload for timer expired notifications.
type TimerNotification struct {
	InstanceID string `json:"instance_id"`
	TimerID    string `json:"timer_id"`
	ExpiresAt  string `json:"expires_at,omitempty"`
}

// NotificationHandler is a callback function that handles notifications.
type NotificationHandler func(ctx context.Context, channel NotifyChannel, payload string)

// Listener manages PostgreSQL LISTEN/NOTIFY connections.
type Listener struct {
	connString     string
	reconnectDelay time.Duration
	handlers       map[NotifyChannel][]NotificationHandler

	conn   *pgx.Conn
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.RWMutex

	// Status
	isActive   bool
	lastError  error
	errorCount int
}

// ListenerOption configures a Listener.
type ListenerOption func(*Listener)

// WithReconnectDelay sets the delay before reconnecting after a connection failure.
func WithReconnectDelay(d time.Duration) ListenerOption {
	return func(l *Listener) {
		l.reconnectDelay = d
	}
}

// NewListener creates a new PostgreSQL notification listener.
func NewListener(connString string, opts ...ListenerOption) *Listener {
	l := &Listener{
		connString:     connString,
		reconnectDelay: 60 * time.Second,
		handlers:       make(map[NotifyChannel][]NotificationHandler),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// OnNotification registers a handler for a notification channel.
// Multiple handlers can be registered for the same channel.
func (l *Listener) OnNotification(channel NotifyChannel, handler NotificationHandler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.handlers[channel] = append(l.handlers[channel], handler)
}

// Start begins listening in the background. A listener may be restarted after Stop completes.
func (l *Listener) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done != nil {
		select {
		case <-l.done:
		default:
			return fmt.Errorf("notification listener already running")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.ctx, l.cancel = context.WithCancel(ctx)
	l.done = make(chan struct{})
	go l.listenLoop(l.done)
	return nil
}

// Stop cancels the listener and waits for its connection and handlers to finish.
func (l *Listener) Stop(ctx context.Context) error {
	l.mu.RLock()
	cancel, done := l.cancel, l.done
	l.mu.RUnlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsActive returns true if the LISTEN/NOTIFY connection is active.
func (l *Listener) IsActive() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isActive
}

// LastError returns the last connection error, if any.
func (l *Listener) LastError() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastError
}

func (l *Listener) listenLoop(done chan struct{}) {
	defer close(done)
	defer l.closeConnection()

	for {
		select {
		case <-l.ctx.Done():
			l.closeConnection()
			return
		default:
		}

		// Try to connect
		if err := l.connect(); err != nil {
			l.mu.Lock()
			l.isActive = false
			l.lastError = err
			l.errorCount++
			l.mu.Unlock()

			slog.Warn("failed to connect for LISTEN/NOTIFY, retrying",
				"error", err,
				"retry_delay", l.reconnectDelay,
				"error_count", l.errorCount)

			// Wait before retrying
			select {
			case <-l.ctx.Done():
				return
			case <-time.After(l.reconnectDelay):
				continue
			}
		}

		l.mu.Lock()
		l.isActive = true
		l.lastError = nil
		l.errorCount = 0
		l.mu.Unlock()

		slog.Info("PostgreSQL LISTEN/NOTIFY connection established")

		// Listen for notifications
		if err := l.listenForNotifications(); err != nil {
			l.mu.Lock()
			l.isActive = false
			l.lastError = err
			l.mu.Unlock()

			slog.Warn("LISTEN/NOTIFY connection lost, reconnecting",
				"error", err,
				"reconnect_delay", l.reconnectDelay)

			l.closeConnection()

			// Wait before reconnecting
			select {
			case <-l.ctx.Done():
				return
			case <-time.After(l.reconnectDelay):
				continue
			}
		}
	}
}

func (l *Listener) connect() error {
	conn, err := pgx.Connect(l.ctx, l.connString)
	if err != nil {
		return err
	}

	// Subscribe to all channels
	for _, channel := range AllChannels() {
		_, err := conn.Exec(l.ctx, "LISTEN "+string(channel))
		if err != nil {
			_ = conn.Close(l.ctx)
			return err
		}
	}

	l.mu.Lock()
	l.conn = conn
	l.mu.Unlock()

	return nil
}

func (l *Listener) closeConnection() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = l.conn.Close(ctx)
		cancel()
		l.conn = nil
	}
	l.isActive = false
}

func (l *Listener) listenForNotifications() error {
	for {
		select {
		case <-l.ctx.Done():
			return nil
		default:
		}

		// Wait for notification with timeout
		l.mu.RLock()
		conn := l.conn
		l.mu.RUnlock()

		if conn == nil {
			return nil
		}

		notification, err := conn.WaitForNotification(l.ctx)
		if err != nil {
			if l.ctx.Err() != nil {
				return nil // Context cancelled, graceful shutdown
			}
			return err
		}

		// Dispatch notification to handlers
		l.dispatchNotification(NotifyChannel(notification.Channel), notification.Payload)
	}
}

func (l *Listener) dispatchNotification(channel NotifyChannel, payload string) {
	l.mu.RLock()
	handlers := l.handlers[channel]
	l.mu.RUnlock()

	for _, handler := range handlers {
		// Handlers are short wake-up callbacks. Dispatch synchronously to bound
		// resource use and ensure Stop joins every callback before returning.
		func(h NotificationHandler) {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("panic in notification handler",
						"channel", channel,
						"panic", r)
				}
			}()
			h(l.ctx, channel, payload)
		}(handler)
	}
}

// ParseWorkflowNotification parses a workflow notification payload.
func ParseWorkflowNotification(payload string) (*WorkflowNotification, error) {
	var n WorkflowNotification
	if err := json.Unmarshal([]byte(payload), &n); err != nil {
		return nil, err
	}
	return &n, nil
}

// ParseTimerNotification parses a timer notification payload.
func ParseTimerNotification(payload string) (*TimerNotification, error) {
	var n TimerNotification
	if err := json.Unmarshal([]byte(payload), &n); err != nil {
		return nil, err
	}
	return &n, nil
}
