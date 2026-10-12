package storage

import (
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
)

// LocalNotification is a hint to reread durable instance state.
type LocalNotification struct {
	InstanceID string
	Closed     bool
}

type localSubscriber struct{ receive func(LocalNotification) }

var localNotifications = struct {
	sync.Mutex
	databases map[string]map[*localSubscriber]struct{}
}{databases: make(map[string]map[*localSubscriber]struct{})}

// SubscribeLocal subscribes to one database's process-local notifications.
// Callbacks must not block. The returned unsubscribe function is idempotent.
// Memory databases are excluded because their identity is connection-specific.
func SubscribeLocal(database string, receive func(LocalNotification)) func() {
	key := localNotifyKey(database)
	if key == "" {
		return func() {}
	}
	subscriber := &localSubscriber{receive: receive}
	localNotifications.Lock()
	if localNotifications.databases[key] == nil {
		localNotifications.databases[key] = make(map[*localSubscriber]struct{})
	}
	localNotifications.databases[key][subscriber] = struct{}{}
	localNotifications.Unlock()
	return func() {
		localNotifications.Lock()
		delete(localNotifications.databases[key], subscriber)
		if len(localNotifications.databases[key]) == 0 {
			delete(localNotifications.databases, key)
		}
		localNotifications.Unlock()
	}
}

func localNotifyKey(database string) string {
	if strings.HasPrefix(database, "postgres") {
		return database
	}
	path := strings.TrimPrefix(strings.TrimPrefix(database, "sqlite://"), "file:")
	path, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)
	if path == ":memory:" || params.Get("mode") == "memory" || path == "" {
		return ""
	}
	if decoded, err := url.PathUnescape(path); err == nil {
		path = decoded
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	return absolute
}

func publishLocal(key string, n LocalNotification) {
	if key == "" {
		return
	}
	localNotifications.Lock()
	subscribers := make([]*localSubscriber, 0, len(localNotifications.databases[key]))
	for subscriber := range localNotifications.databases[key] {
		subscribers = append(subscribers, subscriber)
	}
	localNotifications.Unlock()
	for _, subscriber := range subscribers {
		subscriber.receive(n)
	}
}

func notifyClosed(ctx context.Context, s TransactionManager, id string) {
	if notifier, ok := s.(interface{ notifyClosed(context.Context, string) }); ok {
		notifier.notifyClosed(ctx, id)
	}
}

func (s *SQLiteStorage) notifyRunnable(ctx context.Context, id string) {
	s.notifyLocal(ctx, LocalNotification{InstanceID: id})
}
func (s *SQLiteStorage) notifyClosed(ctx context.Context, id string) {
	s.notifyLocal(ctx, LocalNotification{InstanceID: id, Closed: true})
}
func (s *SQLiteStorage) notifyLocal(ctx context.Context, n LocalNotification) {
	send := func() error { publishLocal(s.notifyKey, n); return nil }
	if s.InTransaction(ctx) {
		_ = s.RegisterPostCommitCallback(ctx, send)
	} else {
		_ = send()
	}
}
