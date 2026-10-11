package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type signalTestStorage interface {
	Storage
	SignalManager
}

func signalInstance(t *testing.T, s Storage, suffix string, status WorkflowStatus) string {
	t.Helper()
	id := t.Name() + suffix
	now := time.Now().UTC()
	require.NoError(t, s.CreateInstance(context.Background(), &WorkflowInstance{
		InstanceID: id, WorkflowName: "signals", Status: status,
		InputData: []byte(`{}`), StartedAt: now, UpdatedAt: now,
	}))
	return id
}

func signalObservation(instance, key, channel, payload string) *HistoryEvent {
	return &HistoryEvent{
		InstanceID: instance, ActivityID: key, Command: "receive:" + channel,
		EventType: HistorySignalReceived, EventData: []byte(payload), DataType: "json",
	}
}

func testSignalInbox(t *testing.T, s signalTestStorage) {
	ctx := context.Background()
	t.Run("BeforeFirstReceiveAndSeparateCursors", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		other := signalInstance(t, s, "-other", StatusRunning)
		first, err := s.AppendSignal(ctx, instance, "c1", []byte(`{"n":9007199254740993}`))
		require.NoError(t, err)
		second, err := s.AppendSignal(ctx, instance, "c2", []byte(`"second"`))
		require.NoError(t, err)
		third, err := s.AppendSignal(ctx, instance, "c1", []byte(`"third"`))
		require.NoError(t, err)
		require.Less(t, first, second)
		require.Less(t, second, third)
		none, err := s.NextSignal(ctx, other, "c1")
		require.NoError(t, err)
		require.Nil(t, none)
		none, err = s.NextSignal(ctx, instance, "missing")
		require.NoError(t, err)
		require.Nil(t, none)

		for _, want := range []struct {
			id                    int64
			channel, payload, key string
		}{{second, "c2", `"second"`, "r2"}, {first, "c1", `{"n":9007199254740993}`, "r1"}, {third, "c1", `"third"`, "r3"}} {
			next, err := s.NextSignal(ctx, instance, want.channel)
			require.NoError(t, err)
			require.NotNil(t, next)
			require.Equal(t, want.id, next.ID)
			require.Equal(t, instance, next.InstanceID)
			require.Equal(t, want.channel, next.Channel)
			require.Equal(t, want.payload, string(next.Payload))
			require.False(t, next.SentAt.IsZero())
			require.Empty(t, next.ConsumedKey)
			obs := signalObservation(instance, want.key, want.channel, want.payload)
			stored, err := s.ConsumeSignal(ctx, signalTestFence(t, s, instance), next.ID, obs)
			require.NoError(t, err)
			require.Equal(t, obs.EventData, stored.EventData)
			require.Equal(t, obs.Command, stored.Command)
			require.Equal(t, obs.EventType, stored.EventType)
			require.Positive(t, stored.ID)
			require.False(t, stored.CreatedAt.IsZero())
			history, err := s.GetHistoryEvent(ctx, instance, want.key)
			require.NoError(t, err)
			require.Equal(t, stored, history)
			var consumed string
			require.NoError(t, s.DB().QueryRowContext(ctx, "SELECT consumed_key FROM workflow_signals WHERE id = $1", next.ID).Scan(&consumed))
			require.Equal(t, want.key, consumed)
		}
		none, err = s.NextSignal(ctx, instance, "c1")
		require.NoError(t, err)
		require.Nil(t, none)
		none, err = s.NextSignal(ctx, instance, "c2")
		require.NoError(t, err)
		require.Nil(t, none)
	})

	t.Run("ClosedOrUnknownTyped", func(t *testing.T) {
		id, err := s.AppendSignal(ctx, "unknown", "c", []byte(`null`))
		require.ErrorIs(t, err, ErrInstanceNotFound)
		require.Zero(t, id)
		for _, status := range []WorkflowStatus{StatusCompleted, StatusFailed, StatusCancelled} {
			t.Run(string(status), func(t *testing.T) {
				instance := signalInstance(t, s, "", status)
				id, err := s.AppendSignal(ctx, instance, "c", []byte(`null`))
				var closed *InstanceClosedError
				require.ErrorAs(t, err, &closed)
				require.Equal(t, status, closed.Status)
				require.Equal(t, instance, closed.InstanceID)
				require.Zero(t, id)
				next, err := s.NextSignal(ctx, instance, "c")
				require.NoError(t, err)
				require.Nil(t, next)
			})
		}
	})

	t.Run("ChannelNameBytesAndPassThrough", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		for _, channel := range []string{"", strings.Repeat("x", 256), strings.Repeat("é", 128)} {
			_, err := s.AppendSignal(ctx, instance, channel, []byte(`null`))
			require.ErrorIs(t, err, ErrInvalidSignalChannel)
		}
		for _, channel := range []string{strings.Repeat("x", 255), " Mixed CASE / é "} {
			_, err := s.AppendSignal(ctx, instance, channel, []byte(`null`))
			require.NoError(t, err)
			next, err := s.NextSignal(ctx, instance, channel)
			require.NoError(t, err)
			require.Equal(t, channel, next.Channel)
		}
	})

	t.Run("GuardRejectsStaleAndMissingFences", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		f := signalTestFence(t, s, instance)
		stale := *f
		stale.Epoch++
		_, err = s.ConsumeSignal(ctx, &stale, id, signalObservation(instance, "r", "c", `1`))
		require.ErrorIs(t, err, ErrFenced)
		_, err = s.ConsumeSignal(ctx, nil, id, signalObservation(instance, "r", "c", `1`))
		require.ErrorIs(t, err, ErrFenced)
		assertSignalPendingWithoutHistory(t, s, instance, id, "r")
	})

	t.Run("RecordConflictRollsBackConsumption", func(t *testing.T) {
		for _, command := range []string{"sleep", "receive:c"} {
			t.Run(command, func(t *testing.T) {
				instance := signalInstance(t, s, "", StatusRunning)
				id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
				require.NoError(t, err)
				original, err := s.RecordHistory(ctx, signalTestFence(t, s, instance), &HistoryEvent{InstanceID: instance, ActivityID: "r", Command: command, EventType: HistoryTimerExpired, EventData: []byte(`"original"`), DataType: "json"})
				require.NoError(t, err)
				_, err = s.ConsumeSignal(ctx, signalTestFence(t, s, instance), id, signalObservation(instance, "r", "c", `1`))
				require.ErrorIs(t, err, ErrHistoryKeyConflict)
				next, err := s.NextSignal(ctx, instance, "c")
				require.NoError(t, err)
				require.Equal(t, id, next.ID)
				stored, err := s.GetHistoryEvent(ctx, instance, "r")
				require.NoError(t, err)
				require.Equal(t, original, stored)
			})
		}
	})

	t.Run("ConsumptionMustMatchInstanceChannelAndPayload", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		other := signalInstance(t, s, "-other", StatusRunning)
		id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		for _, obs := range []*HistoryEvent{nil, signalObservation(other, "r", "c", `1`), signalObservation(instance, "r", "other", `1`), signalObservation(instance, "r", "c", `2`), signalObservation(instance, "", "c", `1`)} {
			stored, err := s.ConsumeSignal(ctx, signalTestFence(t, s, instance), id, obs)
			require.Error(t, err)
			require.Nil(t, stored)
		}
		assertSignalPendingWithoutHistory(t, s, instance, id, "r")
		stored, err := s.ConsumeSignal(ctx, signalTestFence(t, s, instance), -1, signalObservation(instance, "r", "c", `1`))
		require.ErrorIs(t, err, ErrSignalNotAvailable)
		require.Nil(t, stored)
	})

	t.Run("ConsumeExactlyOnce", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		_, err = s.ConsumeSignal(ctx, signalTestFence(t, s, instance), id, signalObservation(instance, "r", "c", `1`))
		require.NoError(t, err)
		_, err = s.ConsumeSignal(ctx, signalTestFence(t, s, instance), id, signalObservation(instance, "r", "c", `1`))
		require.ErrorIs(t, err, ErrSignalNotAvailable)
		_, err = s.ConsumeSignal(ctx, signalTestFence(t, s, instance), id, signalObservation(instance, "r2", "c", `1`))
		require.ErrorIs(t, err, ErrSignalNotAvailable)
		history, err := s.GetHistoryEvent(ctx, instance, "r2")
		require.NoError(t, err)
		require.Nil(t, history)
	})

	t.Run("CallerRollbackUndoesBothWrites", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		txCtx, err := s.BeginTransaction(ContextWithFence(ctx, *signalTestFence(t, s, instance)))
		require.NoError(t, err)
		defer s.RollbackTransaction(txCtx)
		_, err = s.ConsumeSignal(txCtx, contextFence(txCtx, instance), id, signalObservation(instance, "r", "c", `1`))
		require.NoError(t, err)
		require.NoError(t, s.RollbackTransaction(txCtx))
		assertSignalPendingWithoutHistory(t, s, instance, id, "r")
	})

	t.Run("AppendParticipatesInCallerTransaction", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		txCtx, err := s.BeginTransaction(ContextWithFence(ctx, *signalTestFence(t, s, instance)))
		require.NoError(t, err)
		defer s.RollbackTransaction(txCtx)
		_, err = s.AppendSignal(txCtx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		next, err := s.NextSignal(txCtx, instance, "c")
		require.NoError(t, err)
		require.NotNil(t, next)
		require.NoError(t, s.RollbackTransaction(txCtx))
		next, err = s.NextSignal(ctx, instance, "c")
		require.NoError(t, err)
		require.Nil(t, next)
		inst, err := s.GetInstance(ctx, instance)
		require.NoError(t, err)
		require.False(t, inst.Runnable)
	})

	t.Run("OneObservationCannotConsumeTwoSignals", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		first, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		second, err := s.AppendSignal(ctx, instance, "c", []byte(`2`))
		require.NoError(t, err)
		stored, err := s.ConsumeSignal(ctx, signalTestFence(t, s, instance), first, signalObservation(instance, "r", "c", `1`))
		require.NoError(t, err)
		_, err = s.ConsumeSignal(ctx, signalTestFence(t, s, instance), second, signalObservation(instance, "r", "c", `2`))
		require.Error(t, err)
		next, err := s.NextSignal(ctx, instance, "c")
		require.NoError(t, err)
		require.Equal(t, second, next.ID)
		history, err := s.GetHistoryEvent(ctx, instance, "r")
		require.NoError(t, err)
		require.Equal(t, stored, history)
	})

	t.Run("FailedConsumeCannotLeakIntoCallerCommit", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		_, err = s.ReserveHistory(ctx, signalTestFence(t, s, instance), instance, "r", "sleep")
		require.NoError(t, err)
		txCtx, err := s.BeginTransaction(ContextWithFence(ctx, *signalTestFence(t, s, instance)))
		require.NoError(t, err)
		defer s.RollbackTransaction(txCtx)
		require.NoError(t, s.UpdateInstanceActivity(txCtx, instance, "outer"))
		_, err = s.ConsumeSignal(txCtx, contextFence(txCtx, instance), id, signalObservation(instance, "r", "c", `1`))
		require.ErrorIs(t, err, ErrHistoryKeyConflict)
		require.NoError(t, s.CommitTransaction(txCtx))
		next, err := s.NextSignal(ctx, instance, "c")
		require.NoError(t, err)
		require.Equal(t, id, next.ID)
		inst, err := s.GetInstance(ctx, instance)
		require.NoError(t, err)
		require.Equal(t, "outer", inst.CurrentActivityID)
	})

	t.Run("ConcurrentConsumersHaveOneWinner", func(t *testing.T) {
		instance := signalInstance(t, s, "", StatusRunning)
		id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
		require.NoError(t, err)
		f := signalTestFence(t, s, instance)
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, errs[i] = s.ConsumeSignal(ctx, f, id, signalObservation(instance, fmt.Sprintf("r%d", i), "c", `1`))
			}()
		}
		close(start)
		wg.Wait()
		wins := 0
		for _, err := range errs {
			if err == nil {
				wins++
			} else {
				require.ErrorIs(t, err, ErrSignalNotAvailable)
			}
		}
		require.Equal(t, 1, wins)
		count, err := s.GetHistoryCount(ctx, instance)
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	})
}

// A deferred foreign key fails at COMMIT, after both inbox and history writes.
// This exercises a real database commit failure without timing or driver mocks.
func testSignalCommitFailure(t *testing.T, s signalTestStorage) {
	ctx := context.Background()
	_, err := s.DB().ExecContext(ctx, `CREATE TABLE signal_commit_fault (
		instance_id TEXT REFERENCES workflow_instances(instance_id) DEFERRABLE INITIALLY DEFERRED
	)`)
	require.NoError(t, err)
	instance := signalInstance(t, s, "", StatusRunning)
	id, err := s.AppendSignal(ctx, instance, "c", []byte(`1`))
	require.NoError(t, err)
	f := signalTestFence(t, s, instance)
	var trigger string
	switch s.(type) {
	case *SQLiteStorage:
		trigger = `CREATE TRIGGER fail_signal_commit AFTER INSERT ON workflow_history BEGIN INSERT INTO signal_commit_fault VALUES ('missing-instance'); END`
	default:
		trigger = `CREATE FUNCTION fail_signal_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO signal_commit_fault VALUES ('missing-instance'); RETURN NEW; END $$;
 CREATE TRIGGER fail_signal_commit AFTER INSERT ON workflow_history FOR EACH ROW EXECUTE FUNCTION fail_signal_commit()`
	}
	_, err = s.DB().ExecContext(ctx, trigger)
	require.NoError(t, err)
	stored, err := s.ConsumeSignal(ctx, f, id, signalObservation(instance, "r", "c", `1`))
	require.Error(t, err)
	require.Nil(t, stored)
	assertSignalPendingWithoutHistory(t, s, instance, id, "r")
}

func assertSignalPendingWithoutHistory(t *testing.T, s signalTestStorage, instance string, id int64, key string) {
	t.Helper()
	ctx := context.Background()
	var consumed sql.NullString
	require.NoError(t, s.DB().QueryRowContext(ctx, "SELECT consumed_key FROM workflow_signals WHERE id = $1", id).Scan(&consumed))
	require.False(t, consumed.Valid)
	history, err := s.GetHistoryEvent(ctx, instance, key)
	require.NoError(t, err)
	require.Nil(t, history)
}

func signalTestFence(t *testing.T, s Storage, id string) *Fence {
	t.Helper()
	inst, err := s.GetInstance(context.Background(), id)
	require.NoError(t, err)
	if inst.LeaseOwner != "" {
		return &Fence{InstanceID: id, Owner: inst.LeaseOwner, Epoch: inst.LeaseEpoch}
	}
	f, ok, err := s.AcquireLease(context.Background(), id, "signal-test", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	return &f
}
