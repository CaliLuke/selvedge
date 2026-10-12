package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

type shutdownNotifyStore struct {
	storage.Storage
	notified chan error
}

func (s *shutdownNotifyStore) NotifyRunnable(ctx context.Context) { s.notified <- ctx.Err() }

func TestShutdownNotifiesThroughStorageWrapper(t *testing.T) {
	h := newTypedReplayHarness(t, createTestApp)
	notifier := &shutdownNotifyStore{Storage: h.app.storage, notified: make(chan error, 1)}
	h.app.storage = faultstore.New(notifier)
	h.app.running = true
	require.NoError(t, h.app.Shutdown(context.Background()))
	select {
	case err := <-notifier.notified:
		require.NoError(t, err)
	default:
		t.Fatal("storage wrapper hid shutdown notification")
	}
}

func TestShutdownTimeoutNotifiesAfterWorkersFinish(t *testing.T) {
	for _, callerCancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "cancelled"}[callerCancelled], func(t *testing.T) {
			h := newTypedReplayHarness(t, createTestApp, WithShutdownTimeout(time.Millisecond))
			notifier := &shutdownNotifyStore{Storage: h.app.storage, notified: make(chan error, 1)}
			h.app.storage = notifier
			h.app.running = true
			h.app.wg.Add(1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if callerCancelled {
				cancel()
			}
			err := h.app.Shutdown(ctx)
			select {
			case <-notifier.notified:
				t.Error("notified before pending lease releases finished")
			default:
			}
			h.app.wg.Done()
			require.Error(t, err)
			select {
			case err := <-notifier.notified:
				require.NoError(t, err, "notify must outlive caller cancellation")
			case <-time.After(time.Second):
				t.Fatal("shutdown timeout skipped eventual notification")
			}
		})
	}
}
