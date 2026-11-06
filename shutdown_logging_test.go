package selvedge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage/faultstore"
)

func TestShutdownLogsOnlyUnexpectedBackgroundErrors(t *testing.T) {
	for _, loop := range []string{"leader", "resumption", "timers"} {
		for _, unexpected := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/unexpected=%v", loop, unexpected), func(t *testing.T) {
				h := newTypedReplayHarness(t, createTestApp, WithTimerCheckInterval(time.Millisecond))
				faults := faultstore.New(h.app.storage)
				h.app.storage = faults
				faults.Before = func(_ context.Context, _ string) error {
					h.app.cancel()
					if unexpected {
						return errors.New("database unavailable")
					}
					return fmt.Errorf("database operation: %w", context.Canceled)
				}
				var logs bytes.Buffer
				previous := slog.Default()
				slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
				t.Cleanup(func() { slog.SetDefault(previous) })
				switch loop {
				case "leader":
					h.app.tryAcquireLeadership()
				case "resumption":
					h.app.wg.Add(1)
					h.app.runWorkflowResumption()
				case "timers":
					h.app.wg.Add(1)
					h.app.runTimerCheckWithContext(h.app.ctx)
				}
				if unexpected {
					require.NotEmpty(t, logs.String())
				} else {
					require.Empty(t, logs.String())
				}
			})
		}
	}
}

func TestLiveContextCancellationErrorStillLogged(t *testing.T) {
	h := newTypedReplayHarness(t, createTestApp)
	faults := faultstore.New(h.app.storage)
	faults.Before = func(context.Context, string) error { return context.Canceled }
	h.app.storage = faults
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	h.app.tryAcquireLeadership()
	require.NotEmpty(t, logs.String(), "only cancellation owned by the loop is normal termination")
}
