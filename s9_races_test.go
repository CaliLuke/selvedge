package selvedge

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestPendingAndReceiveAsyncAgree(t *testing.T) { testPendingAndReceiveAsyncAgree(t, createTestApp) }
func testPendingAndReceiveAsyncAgree(t *testing.T, factory replayAppFactory) {
	for _, race := range []string{"candidate", "commit", "empty"} {
		t.Run(race, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			h.app.storage = fs
			var counts, values []int
			var found []bool
			injected := false
			var id string
			inject := func() error {
				if injected {
					return nil
				}
				injected = true
				_, err := fs.Storage.AppendSignal(h.app.ctx, id, "c", []byte(`2`))
				return err
			}
			w := DefineWorkflow("race", func(ctx *WorkflowContext, _ string) (string, error) {
				if race != "empty" {
					if err := Await(ctx, func() bool { return Pending(ctx, "c") > 0 }); err != nil {
						return "", err
					}
				}
				counts = append(counts, Pending(ctx, "c"))
				if race == "empty" {
					if err := inject(); err != nil {
						return "", err
					}
				}
				v, ok, err := ReceiveAsync[int](ctx, "c")
				values = append(values, v)
				found = append(found, ok)
				if err != nil {
					return "", err
				}
				counts = append(counts, Pending(ctx, "c"))
				return "", Sleep(ctx, time.Hour)
			})
			id = seedErrorWorkflow(t, h, w)
			if race != "empty" {
				require.NoError(t, h.app.Signal(h.app.ctx, id, "c", 1))
			}
			fs.After = func(_ context.Context, method string) error {
				if (race == "candidate" && method == "ArrivedUndelivered") || (race == "commit" && method == "CommitObservation") {
					return inject()
				}
				return nil
			}
			require.NoError(t, h.RunOnce(id))
			require.NoError(t, h.RunOnce(id))
			if race == "empty" {
				require.Equal(t, []int{0, 0, 0, 0}, counts)
				require.Equal(t, []bool{false, false}, found)
				row, err := fs.GetHistoryEvent(h.app.ctx, id, "#1")
				require.NoError(t, err)
				require.Equal(t, storage.HistorySignalNone, row.EventType)
				require.Nil(t, row.EventData)
			} else {
				require.Equal(t, []int{1, 0, 1, 0}, counts)
				require.Equal(t, []bool{true, true}, found)
				require.Equal(t, []int{1, 1}, values)
			}
			b, err := fs.Storage.ArrivedUndelivered(h.app.ctx, id)
			require.NoError(t, err)
			require.Len(t, b.Signals, 1)
		})
	}
}
func TestReplayCommandMismatchFailsFastAwait(t *testing.T) {
	testReplayCommandMismatchFailsFastAwait(t, createTestApp)
}
func testReplayCommandMismatchFailsFastAwait(t *testing.T, factory replayAppFactory) {
	for _, kind := range []string{"activity-await", "none-removed", "async-receive"} {
		t.Run(kind, func(t *testing.T) {
			h, _, fs := leaseHarness(t, factory)
			changed := false
			w := DefineWorkflow("mismatch", func(ctx *WorkflowContext, _ string) (string, error) {
				switch kind {
				case "activity-await":
					return "", Await(ctx, func() bool { return false })
				case "none-removed":
					if !changed {
						_, _, err := ReceiveAsync[int](ctx, "c")
						if err != nil {
							return "", err
						}
					}
					return "", nil
				default:
					if changed {
						_, err := Receive[int](ctx, "c")
						return "", err
					}
					_, _, err := ReceiveAsync[int](ctx, "c")
					return "", err
				}
			})
			id := seedErrorWorkflow(t, h, w)
			if kind == "activity-await" {
				seedTask(t, h, id)
			} else {
				require.NoError(t, h.RunOnce(id))
				changed = true
				_, err := fs.DB().Exec(`UPDATE workflow_instances SET status='running',runnable=TRUE WHERE instance_id=$1`, id)
				require.NoError(t, err)
			}
			var nondet *NondeterminismError
			require.ErrorAs(t, h.RunOnce(id), &nondet)
			require.EqualValues(t, 1, nondet.Seq)
			if kind == "activity-await" {
				require.Equal(t, "await", nondet.Issued)
				require.Equal(t, "activity:A", nondet.Recorded)
			}
		})
	}
}

// databaseFenceOnlyStore keeps the lease's in-memory check from rejecting an
// observation before the database can check the holder's stale epoch.
type databaseFenceOnlyStore struct {
	storage.Storage
	commitChecks atomic.Int32
}

func (s *databaseFenceOnlyStore) CommitObservation(ctx context.Context, f *storage.Fence, c *storage.ObservationCommit) (*storage.ObservationResult, error) {
	ctx = storage.ContextWithCommitCheck(ctx, func() error {
		s.commitChecks.Add(1)
		return nil
	})
	return s.Storage.CommitObservation(ctx, f, c)
}

func TestStalledExecutionCommitRejectedAwait(t *testing.T) {
	testStalledExecutionCommitRejectedAwait(t, createTestApp)
}
func testStalledExecutionCommitRejectedAwait(t *testing.T, factory replayAppFactory) {
	h, _, fs := leaseHarness(t, factory)
	databaseFence := &databaseFenceOnlyStore{Storage: fs.Storage}
	fs.Storage = databaseFence
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	var entered atomic.Int32
	ready := make(chan bool, 2)
	w := DefineWorkflow("stalled", func(ctx *WorkflowContext, in string) (string, error) {
		f, err := a.ExecuteAsync(ctx, in)
		if err != nil {
			return "", err
		}
		ready <- f.IsReady()
		if entered.Add(1) == 2 {
			// Leave #2 empty so a missing fence cannot be masked by a key conflict.
			return "", Await(ctx, func() bool { return false })
		}
		return "", Await(ctx, f.IsReady)
	})
	id := seedErrorWorkflow(t, h, w)
	seedTask(t, h, id)
	claim, err := h.ClaimTask(id, "#1")
	require.NoError(t, err)
	require.NoError(t, claim.Complete(`"A"`))
	stalled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	fs.Before = func(_ context.Context, method string) error {
		if method == "CommitObservation" {
			close(stalled)
			<-release
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- h.RunOnce(id) }()
	select {
	case <-stalled:
	case err := <-done:
		t.Fatalf("did not reach commit: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("did not reach commit")
	}
	require.False(t, <-ready)
	setStoreClock(t, h, 3000)
	second := replay.NewEngine(fs.Storage, h.app.hooks, "takeover", errorCodec{})
	// Use the normal workflow runner via a separate engine; no delivery write is made by this holder.
	old := h.app.engine
	h.app.engine = second
	require.NoError(t, h.RunOnce(id))
	h.app.engine = old
	require.False(t, <-ready)
	before, _, err := fs.Storage.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	// Unblock without closing release twice in deferred cleanup.
	release <- struct{}{}
	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("zombie did not return")
	}
	after, _, err := fs.Storage.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Zero(t, after[0].DeliveredSeq)
	require.EqualValues(t, 1, databaseFence.commitChecks.Load())
	require.ErrorIs(t, runErr, storage.ErrFenced)
}

func TestReplayDeterministicAfterCrashDelivery(t *testing.T) {
	testReplayDeterministicAfterCrashDelivery(t, createTestApp)
}
func testReplayDeterministicAfterCrashDelivery(t *testing.T, factory replayAppFactory) {
	for _, scenario := range []string{"late-B", "late-signal"} {
		t.Run(scenario, func(t *testing.T) {
			baseline, writes := runDeliveryCrash(t, factory, scenario, 0, false)
			for cut := 1; cut <= writes; cut++ {
				for _, after := range []bool{false, true} {
					t.Run(fmt.Sprintf("write_%d/after_%t", cut, after), func(t *testing.T) {
						got, _ := runDeliveryCrash(t, factory, scenario, cut, after)
						require.Equal(t, baseline, got)
					})
				}
			}
		})
	}
}

type deliverySnapshot struct {
	History []deliveryHistoryValue
	Signals []storage.SignalPosition
	Output  string
}
type deliveryHistoryValue struct {
	Seq, Delivered int64
	Key, Command   string
	Type           storage.HistoryEventType
	Data           string
}

func (h *typedReplayHarness) Deliveries(id string) ([]storage.SignalPosition, error) {
	rows, err := h.app.storage.DB().QueryContext(h.app.ctx, `SELECT id,channel,COALESCE(delivered_seq,0),COALESCE(consumed_seq,0) FROM workflow_signals WHERE instance_id=$1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var positions []storage.SignalPosition
	for rows.Next() {
		var p storage.SignalPosition
		if err := rows.Scan(&p.ID, &p.Channel, &p.DeliveredSeq, &p.ConsumedSeq); err != nil {
			return nil, err
		}
		positions = append(positions, p)
	}
	return positions, rows.Err()
}
func runDeliveryCrash(t *testing.T, factory replayAppFactory, scenario string, cut int, after bool) (deliverySnapshot, int) {
	t.Helper()
	h, _, fs := leaseHarness(t, factory)
	h.app.storage = fs
	a := DefineActivity("A", func(context.Context, string) (string, error) { return "A", nil })
	b := DefineActivity("B", func(context.Context, string) (string, error) { return "B", nil })
	var observations []bool
	var waits []replay.Wait
	h.app.ctx = replay.WithTestYieldObserver(h.app.ctx, func(w replay.Wait) { waits = append(waits, w) })
	w := DefineWorkflow("delivery-crash", func(ctx *WorkflowContext, in string) (string, error) {
		if scenario == "late-B" {
			fa, err := a.ExecuteAsync(ctx, in)
			if err != nil {
				return "", err
			}
			fb, err := b.ExecuteAsync(ctx, in)
			if err != nil {
				return "", err
			}
			if _, err := fs.ClaimActivityTasks(ctx.Context(), "controlled-pool", time.Second, 10, "B"); err != nil {
				ctx.execCtx.Fail(err)
				return "", err
			}
			if err := Await(ctx, func() bool { return fa.IsReady() || fb.IsReady() }); err != nil {
				return "", err
			}
			observations = append(observations, fb.IsReady())
			va, err := fa.Get(ctx)
			if err != nil {
				return "", err
			}
			vb, err := fb.Get(ctx)
			return va + vb, err
		}
		if _, err := Receive[int](ctx, "c1"); err != nil {
			return "", err
		}
		observations = append(observations, Pending(ctx, "c2") > 0)
		_, ok, err := ReceiveAsync[int](ctx, "c2")
		if err != nil {
			return "", err
		}
		if ok {
			return "", errors.New("late signal became visible")
		}
		if err := Sleep(ctx, time.Hour, WithSleepID("pause")); err != nil {
			return "", err
		}
		if Pending(ctx, "c2") != 0 {
			return "", errors.New("fire delivered signal")
		}
		return "done", nil
	})
	id := seedErrorWorkflow(t, h, w)
	if scenario == "late-signal" {
		require.NoError(t, h.app.Signal(h.app.ctx, id, "c1", 1))
	}
	writes := 0
	crashed := false
	injected := false
	inject := func(_ context.Context, method string) error {
		switch method {
		case "AcquireLease", "ReserveActivity", "ClaimActivityTasks", "ClaimActivityTask", "CompleteActivityTask", "ReleaseActivityTask", "ReleaseLease", "ReserveTimer", "FireTimer", "CommitObservation", "CloseInstance":
			writes++
		default:
			return nil
		}
		if cut > 0 && !crashed && writes == cut {
			crashed = true
			return storage.ErrFenced
		}
		return nil
	}
	if after {
		fs.After = inject
	} else {
		fs.Before = inject
	}
	oldAfter := fs.After
	fs.After = func(ctx context.Context, method string) error {
		if method == "CommitObservation" && !injected && scenario == "late-signal" {
			injected = true
			_, err := fs.Storage.AppendSignal(h.app.ctx, id, "c2", []byte(`2`))
			if err != nil {
				return err
			}
		}
		if oldAfter != nil {
			return oldAfter(ctx, method)
		}
		return nil
	}

	for attempt := 0; attempt < 20; attempt++ {
		err := h.RunOnce(id)
		require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "run: %v", err)
		inst, err := fs.GetInstance(h.app.ctx, id)
		require.NoError(t, err)
		if inst.Status == storage.StatusCompleted {
			break
		}
		setStoreClock(t, h, int64(4000+attempt*4000000))
		if scenario == "late-B" {
			row, err := fs.GetHistoryEvent(h.app.ctx, id, "#3")
			require.NoError(t, err)
			name := "A"
			if row != nil {
				name = "B"
			}
			err = h.RunTasks(id, name)
			require.True(t, err == nil || errors.Is(err, storage.ErrFenced), "tasks: %v", err)
		} else {
			var n int
			require.NoError(t, fs.DB().QueryRow(`SELECT COUNT(*) FROM workflow_timer_subscriptions WHERE instance_id=$1`, id).Scan(&n))
			if n > 0 {
				err = fs.FireTimer(h.app.ctx, &storage.TimerSubscription{InstanceID: id, TimerID: "pause"})
				require.True(t, err == nil || errors.Is(err, storage.ErrFenced))
			}
		}
	}
	inst, err := fs.GetInstance(h.app.ctx, id)
	require.NoError(t, err)
	require.Equal(t, storage.StatusCompleted, inst.Status)
	if cut == 0 && scenario == "late-B" {
		require.Equal(t, []replay.Wait{{Seq: 3, Kind: replay.WaitAwait, Key: "#3"}, {Seq: 3, Kind: replay.WaitActivity, Key: "#2"}}, waits)
	}
	require.NotEmpty(t, observations)
	for _, ready := range observations {
		require.False(t, ready)
	}
	if cut > 0 {
		require.True(t, crashed)
	}
	rows, _, err := fs.GetHistoryPaginated(h.app.ctx, id, 0, 100)
	require.NoError(t, err)
	result := deliverySnapshot{Output: string(inst.OutputData)}
	for _, r := range rows {
		result.History = append(result.History, deliveryHistoryValue{r.Seq, r.DeliveredSeq, r.ActivityID, r.Command, r.EventType, string(r.EventData)})
	}
	result.Signals, err = h.Deliveries(id)
	require.NoError(t, err)
	return result, writes
}
