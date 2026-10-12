package selvedge

import (
	"encoding/json"
	"errors"

	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// Await waits until cond is true in the delivered view. Predicates must be
// deterministic and side-effect free; only IsReady, Pending and local reads are
// allowed. A never-true predicate parks at one replay per input. Replay runs the
// pre-check but never reevaluates a recorded command's candidate batch.
func Await(ctx *WorkflowContext, cond func() bool) error {
	ec := ctx.execCtx
	end, err := ec.Enter()
	if err != nil {
		return err
	}
	if cond == nil {
		end()
		err := errors.New("Await requires a predicate")
		ec.Fail(err)
		return err
	}
	ready := func() bool { defer end(); return ec.WithDeliveryCandidate(nil, cond) }()
	if err := ec.Check(); err != nil {
		return err
	}
	if ready {
		return nil
	}
	cmd, err := ec.Issue("await", "")
	if err != nil {
		return err
	}
	defer cmd.End()
	if cmd.Recorded != nil {
		return awaitOutcome(ctx, cmd.Recorded)
	}
	batch, err := ec.Storage().ArrivedUndelivered(ctx.Context(), ctx.InstanceID())
	if err != nil {
		ec.Fail(err)
		return err
	}
	ready = ec.WithDeliveryCandidate(batch, cond)
	if err := ec.Check(); err != nil {
		return err
	}
	if ready {
		data, _ := json.Marshal(struct {
			History int `json:"history"`
			Signals int `json:"signals"`
		}{len(batch.HistoryKeys), len(batch.Signals)})
		row, err := commitObservation(ctx, &storage.ObservationCommit{Seq: cmd.Seq, Batch: batch, Record: &storage.HistoryEvent{InstanceID: ctx.InstanceID(), Seq: cmd.Seq, ActivityID: cmd.Key, Command: "await", EventType: storage.HistoryAwaitCompleted, EventData: data, DataType: "json"}})
		if err != nil {
			return err
		}
		return awaitOutcome(ctx, row)
	}
	cancellation, err := scopeCancellation(ctx, ctx.scope)
	if err != nil {
		ec.Fail(err)
		return err
	}
	if cancellation != nil {
		row, err := commitObservation(ctx, &storage.ObservationCommit{Seq: cmd.Seq, DeliverAll: true, Record: &storage.HistoryEvent{InstanceID: ctx.InstanceID(), Seq: cmd.Seq, ActivityID: cmd.Key, Command: "await", EventType: storage.HistoryAwaitCancelled, EventData: encodeError(cancellation), DataType: "json"}})
		if err != nil {
			return err
		}
		return awaitOutcome(ctx, row)
	}
	return ec.Yield(replay.Wait{Seq: cmd.Seq, Key: cmd.Key, Kind: replay.WaitAwait})
}
func awaitOutcome(ctx *WorkflowContext, row *storage.HistoryEvent) error {
	if row.EventType == storage.HistoryAwaitCancelled {
		return replayRecordedError(ctx, row)
	}
	if row.EventType != storage.HistoryAwaitCompleted {
		err := errors.New("invalid recorded Await observation")
		ctx.execCtx.Fail(err)
		return err
	}
	_, err := decodeRecorded[struct {
		History int `json:"history"`
		Signals int `json:"signals"`
	}](row)
	if err != nil {
		ctx.execCtx.Fail(err)
	}
	return err
}

// commitObservation updates the local view only after the fenced transaction is
// acknowledged. A lost acknowledgement ends the run; replay reloads durable tags.
func commitObservation(ctx *WorkflowContext, c *storage.ObservationCommit) (*storage.HistoryEvent, error) {
	r, err := ctx.execCtx.Storage().CommitObservation(ctx.Context(), ctx.execCtx.Fence(), c)
	if err != nil {
		ctx.execCtx.Fail(err)
		return nil, err
	}
	ctx.execCtx.ApplyDelivery(c.Seq, r.Delivered)
	if c.Consume != nil {
		ctx.execCtx.ConsumeDeliveredSignal(c.Consume.ID, c.Seq)
	}
	return r.Stored, nil
}
