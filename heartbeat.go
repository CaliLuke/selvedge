package selvedge

import (
	"context"
	"sync/atomic"
)

type activityHeartbeat struct{ progress atomic.Bool }
type activityHeartbeatKey struct{}

// RecordHeartbeat records progress for the current activity attempt without I/O.
// It returns the context's cancellation cause after the attempt has stopped.
func RecordHeartbeat(ctx context.Context) error {
	state, ok := ctx.Value(activityHeartbeatKey{}).(*activityHeartbeat)
	if !ok {
		return ErrNotActivityContext
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	state.progress.Store(true)
	return nil
}
