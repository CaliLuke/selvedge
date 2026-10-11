package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

type leaseStore interface {
	AcquireLease(context.Context, string, string, time.Duration) (storage.Fence, bool, error)
	RenewLease(context.Context, storage.Fence, time.Duration) (bool, error)
	ReleaseLease(context.Context, storage.Fence, bool) error
	MarkRunnable(context.Context, string) error
	FindDispatchable(context.Context, int) ([]string, error)
}

func newLeaseStore(t *testing.T, factory replayAppFactory) (*typedReplayHarness, leaseStore) {
	t.Helper()
	h := newTypedReplayHarness(t, factory)
	s, ok := h.app.storage.(leaseStore)
	require.True(t, ok, "storage must implement the S4 lease protocol")
	require.NoError(t, h.app.storage.CreateInstance(h.app.ctx, &storage.WorkflowInstance{InstanceID: "lease", WorkflowName: "lease", Status: storage.StatusRunning, Framework: "other"}))
	_, err := h.app.storage.DB().Exec("UPDATE workflow_instances SET framework='other' WHERE instance_id='lease'")
	require.NoError(t, err)
	return h, s
}

func TestLeaseExpiryIsStrict(t *testing.T) { testLeaseExpiryIsStrict(t, createTestApp) }
func testLeaseExpiryIsStrict(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	setStoreClock(t, h, 1000)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker/one", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1, f.Epoch)
	setStoreClock(t, h, 2000)
	_, ok, err = s.AcquireLease(h.app.ctx, "lease", "worker/two", time.Second)
	require.NoError(t, err)
	require.False(t, ok)
	setStoreClock(t, h, 2001)
	next, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker/two", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 2, next.Epoch)
}
func TestExpiredLeaseRecoveredWithoutFlag(t *testing.T) {
	testExpiredLeaseRecoveredWithoutFlag(t, createTestApp)
}
func testExpiredLeaseRecoveredWithoutFlag(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	setStoreClock(t, h, 1000)
	_, ok, err := s.AcquireLease(h.app.ctx, "lease", "one", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	ids, err := s.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Empty(t, ids)
	setStoreClock(t, h, 2001)
	ids, err = s.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"lease"}, ids)
	_, ok, err = s.AcquireLease(h.app.ctx, "lease", "two", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	var runnable bool
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT runnable FROM workflow_instances WHERE instance_id='lease'").Scan(&runnable))
	require.False(t, runnable)
}
func TestLeaseRenewalRequiresUnexpiredLease(t *testing.T) {
	testLeaseRenewalRequiresUnexpiredLease(t, createTestApp)
}
func testLeaseRenewalRequiresUnexpiredLease(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	setStoreClock(t, h, 1000)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "one", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	setStoreClock(t, h, 2000)
	ok, err = s.RenewLease(h.app.ctx, f, time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	setStoreClock(t, h, 3001)
	ok, err = s.RenewLease(h.app.ctx, f, time.Second)
	require.NoError(t, err)
	require.False(t, ok)
}
func TestLeaseReleasePreservesInput(t *testing.T) { testLeaseReleasePreservesInput(t, createTestApp) }
func testLeaseReleasePreservesInput(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	setStoreClock(t, h, 1000)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "one", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = s.AcquireLease(h.app.ctx, "lease", "one", time.Second)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, s.MarkRunnable(h.app.ctx, "lease"))
	require.NoError(t, s.ReleaseLease(h.app.ctx, f, false))
	ids, err := s.FindDispatchable(h.app.ctx, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"lease"}, ids)
}

func setStoreClock(t *testing.T, h *typedReplayHarness, ms int64) {
	t.Helper()

	if !h.storeClock {
		_, err := h.app.storage.DB().Exec("CREATE TABLE selvedge_test_clock (now_ms BIGINT NOT NULL)")
		require.NoError(t, err)
		_, err = h.app.storage.DB().Exec("INSERT INTO selvedge_test_clock VALUES ($1)", ms)
		require.NoError(t, err)
		storage.WithTestStoreClock()(h.app.storage)
		h.storeClock = true
	} else {
		_, err := h.app.storage.DB().Exec("UPDATE selvedge_test_clock SET now_ms=$1", ms)
		require.NoError(t, err)
	}
}

func TestStalledExecutionCommitRejected(t *testing.T) {
	testStalledExecutionCommitRejected(t, createTestApp)
}
func testStalledExecutionCommitRejected(t *testing.T, factory replayAppFactory) {
	for _, owner := range []string{"worker/second", "other/second"} {
		t.Run(owner+"/engine", func(t *testing.T) { testStalledEngineCommit(t, factory, owner) })
		t.Run(owner+"/storage", func(t *testing.T) {
			h, s := newLeaseStore(t, factory)
			setStoreClock(t, h, 1000)
			f, ok, err := s.AcquireLease(h.app.ctx, "lease", "worker/first", time.Second)
			require.NoError(t, err)
			require.True(t, ok)
			setStoreClock(t, h, 2001)
			_, ok, err = s.AcquireLease(h.app.ctx, "lease", owner, time.Second)
			require.NoError(t, err)
			require.True(t, ok)
			_, err = h.app.storage.ReserveHistory(h.app.ctx, &f, "lease", "sleep", "sleep")
			require.ErrorIs(t, err, storage.ErrFenced)
			_, err = h.app.storage.RecordHistory(h.app.ctx, &f, &storage.HistoryEvent{InstanceID: "lease", ActivityID: "observation", Command: "now", EventData: []byte(`1`), DataType: "json"})
			require.ErrorIs(t, err, storage.ErrFenced)
			_, err = h.app.storage.SetHistoryValueIfNull(h.app.ctx, &f, "lease", "sleep", "sleep", storage.HistoryTimerExpired, []byte(`true`))
			require.ErrorIs(t, err, storage.ErrFenced)
			require.ErrorIs(t, s.ReleaseLease(h.app.ctx, f, true), storage.ErrFenced)
			stale := storage.ContextWithFence(h.app.ctx, f)
			for _, status := range []storage.WorkflowStatus{storage.StatusCompleted, storage.StatusFailed, storage.StatusCancelled} {
				require.ErrorIs(t, h.app.storage.CloseInstance(stale, f, status, []byte(`1`), nil), storage.ErrFenced)
			}
			require.ErrorIs(t, h.app.storage.RegisterTimerSubscription(stale, &storage.TimerSubscription{InstanceID: "lease", TimerID: "timer", ExpiresAt: time.Now()}), storage.ErrFenced)
			require.ErrorIs(t, h.app.storage.SubscribeToChannel(stale, "lease", "messages", storage.ChannelModeBroadcast), storage.ErrFenced)
			require.ErrorIs(t, h.app.storage.UpdateDeliveryCursor(stale, "lease", "messages", 1), storage.ErrFenced)
			require.ErrorIs(t, h.app.storage.ClearChannelWaitingState(stale, "lease", "messages"), storage.ErrFenced)
			require.ErrorIs(t, h.app.storage.UpdateInstanceActivity(stale, "lease", "next"), storage.ErrFenced)
			require.ErrorIs(t, h.app.storage.RemoveTimerSubscription(stale, "lease", "timer"), storage.ErrFenced)
			require.ErrorIs(t, h.app.storage.RegisterChannelReceive(stale, "lease", "messages", "next", nil), storage.ErrFenced)
			_, err = h.app.storage.ConsumeChannelMessage(stale, &f, "messages", 1, &storage.HistoryEvent{InstanceID: "lease", ActivityID: "recv", Command: "receive:messages", EventType: storage.HistoryChannelMessageReceived, EventData: []byte(`1`), DataType: "json"})
			require.ErrorIs(t, err, storage.ErrFenced)

			n, err := h.app.storage.GetHistoryCount(h.app.ctx, "lease")
			require.NoError(t, err)
			require.Zero(t, n)
		})
	}
}

func TestCommandWritesRequireFence(t *testing.T) { testCommandWritesRequireFence(t, createTestApp) }
func testCommandWritesRequireFence(t *testing.T, factory replayAppFactory) {
	h, _ := newLeaseStore(t, factory)
	_, err := h.app.storage.ReserveHistory(h.app.ctx, nil, "lease", "sleep", "sleep")
	require.ErrorIs(t, err, storage.ErrFenced)
	_, err = h.app.storage.RecordHistory(h.app.ctx, nil, &storage.HistoryEvent{InstanceID: "lease", ActivityID: "obs", Command: "now", EventData: []byte(`1`), DataType: "json"})
	require.ErrorIs(t, err, storage.ErrFenced)
	_, err = h.app.storage.SetHistoryValueIfNull(h.app.ctx, nil, "lease", "recv", "receive:c", storage.HistoryChannelMessageReceived, []byte(`1`))
	require.ErrorIs(t, err, storage.ErrFenced)
}

func TestTimerFireRollsBackAsOneWrite(t *testing.T) {
	testTimerFireRollsBackAsOneWrite(t, createTestApp)
}
func testTimerFireRollsBackAsOneWrite(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	setStoreClock(t, h, 1000)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "owner", time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = h.app.storage.ReserveHistory(h.app.ctx, &f, "lease", "timer", "another-command")
	require.NoError(t, err)
	timer := &storage.TimerSubscription{InstanceID: "lease", TimerID: "timer", ExpiresAt: time.Now()}
	require.NoError(t, h.app.storage.RegisterTimerSubscription(storage.ContextWithFence(h.app.ctx, f), timer))
	require.NoError(t, s.ReleaseLease(h.app.ctx, f, false))
	require.ErrorIs(t, h.app.storage.FireTimer(h.app.ctx, timer), storage.ErrHistoryKeyConflict)
	var subscriptions int
	require.NoError(t, h.app.storage.DB().QueryRow("SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id='lease'").Scan(&subscriptions))
	require.Equal(t, 1, subscriptions)
	inst, err := h.app.storage.GetInstance(h.app.ctx, "lease")
	require.NoError(t, err)
	require.False(t, inst.Runnable)
	row, err := h.app.storage.GetHistoryEvent(h.app.ctx, "lease", "timer")
	require.NoError(t, err)
	require.Nil(t, row.EventData)
}

func TestLeaseTTLDoesNotRoundDown(t *testing.T) { testLeaseTTLDoesNotRoundDown(t, createTestApp) }
func testLeaseTTLDoesNotRoundDown(t *testing.T, factory replayAppFactory) {
	h, s := newLeaseStore(t, factory)
	setStoreClock(t, h, 1000)
	f, ok, err := s.AcquireLease(h.app.ctx, "lease", "owner", 1500*time.Microsecond)
	require.NoError(t, err)
	require.True(t, ok)
	inst, err := h.app.storage.GetInstance(h.app.ctx, "lease")
	require.NoError(t, err)
	require.EqualValues(t, 1002, *inst.LeaseExpiresMS)
	setStoreClock(t, h, 1001)
	ok, err = s.RenewLease(h.app.ctx, f, 1500*time.Microsecond)
	require.NoError(t, err)
	require.True(t, ok)
	inst, err = h.app.storage.GetInstance(h.app.ctx, "lease")
	require.NoError(t, err)
	require.EqualValues(t, 1003, *inst.LeaseExpiresMS)
}
