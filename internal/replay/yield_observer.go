package replay

import "context"

type testYieldObserverKey struct{}

// WithTestYieldObserver observes dispatcher waits without changing their handling.
// Install it before an execution starts; the callback runs on the dispatcher.
func WithTestYieldObserver(ctx context.Context, observe func(Wait)) context.Context {
	return context.WithValue(ctx, testYieldObserverKey{}, observe)
}
