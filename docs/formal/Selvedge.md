# Selvedge formal model

`Selvedge.tla` specifies the durable workflow engine Selvedge will become.
That engine implements loom-mcp's `engine.Engine` and
`engine.WorkflowContext` (`loom-mcp/runtime/agent/engine/engine.go`). The
model describes the engine we intend to build. It does not describe Romancy
v0.6.2 as it is today. Each known Romancy defect, and each design rule the
engine depends on, has a fault configuration. TLC must find the
counterexample for each one.

Run every configuration with:

```bash
scripts/model_test.sh [--long] /absolute/path/to/tla2tools.jar
```

The runner copies the module to a temporary directory and runs one TLC
process at a time, with `-workers 4 -Xmx4g` and deadlock checking on. It
requires the following:

- Every pass configuration passes and explores at least its floor of
  distinct states.
- Every `Unsafe*` configuration fails with its named invariant.
- Every `Witness*` configuration is violated. A violation proves that the
  scenario is reachable.
- Every `LivenessUnsafe*` configuration violates its temporal property.

`--long` adds `SelvedgeTwoWorkersLong`: about 40M states and 20 minutes.

## What is modelled

### The workflow script

One parent instance runs a fixed, deterministic script. Each command that
holds a key records its command name and its observation in the durable
history `hist`. Each field is written once.

| pc | Engine operation | Key | Observation |
|----|------------------|-----|-------------|
| `sub` | the `Receiver`s on channels c1 and c2 exist | `sub` | `ok` |
| `startA`, `startB` | `ExecuteToolActivityAsync` returns a `Future` | `act1`, `act2` (issue order) | reservation; the activity worker records the result |
| `timer` | `NewTimer` | `T` | reservation; the timer poller records `fired` |
| `child` | `StartChildWorkflow` | `C` | `started` |
| `select` | `Await` over A, B, T, c1 and cancellation | `sel` | `A`, `B`, `T`, `S` or `X` |
| `getW` | `Future.Get` on the winning future | none | the recorded result value |
| `readyB` | `Future.IsReady(B)` after A won | none | from the delivered view |
| `getB` | `Future.Get(B)` when it was ready | none | the recorded result value |
| `now` | `Now()` compared with a deadline | `now` | `early` or `late` |
| `dupA` | `ExecuteToolActivity` with explicit ID `act1` | `act1` | rejected; the run fails |
| `recv` | `Receiver.Receive` on c1 | `recv` | `sig` or `X` |
| `pend` | `Receiver.Pending()` on c2 | none | `zero` or `some`, from the delivered view |
| `async` | `Receiver.ReceiveAsync` on c2 | `async` | `sig` or `nosig`, from the delivered view |
| `sleep`, `wake` | `NewTimer` + `Future.Get` | `sleep` | `fired` or `X` |
| `rb2` | `Future.IsReady(B)` after the sleep | none | from the delivered view; can follow a wake completed in the acquire replay |
| `await` | `ChildWorkflowHandle.Get` | `cdone` | the child outcome or `X` |
| `ccancel` | `WithCancel` scope cancellation reaches the child | `ccancel` | `ok` |
| `cgetB` | `Future.Get(B)` in the cancelled scope | `cgetB` | B's value if delivered, otherwise `canceled` (B's task is cancelled) |
| `clean`, `cleanWait` | `Detached().ExecuteXActivity` for cleanup | `clean` | the activity result |

After `now` the script branches on recorded observations:

- If the timer won, the workflow runs `dupA`.
- If A or B won and the deadline has passed, it completes at once and
  leaves the child running.
- Otherwise it goes on to `recv`.

The workflow can observe a cancel at each blocking yield. It then cancels
the child, reads B in the cancelled scope, runs the detached cleanup, and
closes as cancelled.

### Replay, delivery and the Await contract

An execution takes the instance with `Acquire`. It then re-runs the code
from the top and replays every command whose observation is recorded. The
replay is one atomic step, because replay only reads history.

For each keyed command, replay checks that the key holds the command the
code issues. On a mismatch the run fails at once with a nondeterminism
error (`nondet`). A command without a recorded observation reads live
state and records the observation in the same write that acts on it.

**Delivery.** An event (an activity result, a timer fire, a signal, a
child result, a cancel request) arrives when it is written durably. The
workflow sees it only once it has been delivered. Delivery happens when a
blocking yield completes: that yield's commit writes the **delivery batch**,
which is every event that has arrived and is not yet delivered, tagged with
the yield's position (`dpos`). The model tags B's result and every signal.
Signal consumption is tagged too (`tpos`).

**Await (S9).** Specify it as follows. The engine records the delivery
batch at each completed yield. The `Await` predicate, and every later
`IsReady`, `Pending()` and `ReceiveAsync` read, are pure functions of the
delivered view at that position. Do not specify it as "record the winner":
loom-mcp's `Await` takes an opaque predicate that reads `IsReady` on every
pending future, the finalize timer and child handles.

The model also records the select's winner, chosen from everything ready in
the view. This is a sound over-approximation: a real engine's predicate is
a deterministic function of the same view, so its choice is one of the
model's behaviours, and replay reads it from history.

The concrete reads:

- `IsReady(B)` at position p is true only if B was delivered at a yield at
  or before p.
- `Pending()` on a channel at p counts signals delivered by p and not
  consumed before p.
- `ReceiveAsync` takes the next such signal and records what it found,
  `nosig` included.

Pending and ReceiveAsync are safe inside an `Await` predicate. A replay sees
the same answers at the same positions.

**A live yield completes only through a fenced commit that writes its
delivery batch.** This holds even when the timer poller or the activity
worker wrote the value the yield observes (`StepReplay` inside `Commit`).
`SelvedgeUnsafeUnfencedDelivery` gates it. Without fencing, a stalled
zombie's commit at `select` delivers B after the new holder has already read
`IsReady(B)` as false, and the next replay diverges (`ReplayDeterministic`).

**The acquire replay is the one exception.** When an execution takes the
instance, its replay (`FastForward` in `Acquire`) passes every yield whose
value is in history. That includes `wake` and `cleanWait`, whose values the
timer poller and the activity worker write, even when no workflow commit has
completed them yet. The replay writes nothing, so those yields complete
without a delivery batch, and the arrived events are delivered at the next
yield that the workflow completes in a commit.

This is safe for two reasons:

- **`dpos` and `tpos` are durable.** A batch is written at most once per
  event, at the position of the yield that delivered it. Every unrecorded
  read (`IsReady`, `Pending()`, `ReceiveAsync`) at position q sees exactly
  the events delivered at positions up to q. Every later execution derives
  the same q from the same history and sees the same view.
- **No batch is ever written at or below a position where a read already
  happened.** Batches are written only by the holder's fenced commit at its
  current yield, which is past every position it has read. A stalled
  execution that could write a batch at an earlier position is fenced out.
  `SelvedgeUnsafeUnfencedDelivery` shows what happens without fencing: a
  zombie writes a batch below a position the new holder already read, and
  replay diverges.

**The gate.** The script reads `IsReady(B)` again right after the sleep
(`rb2`, between `wake` and `await`). The pass configurations check
`ReplayDeterministic` across it. `SelvedgeWitnessReadyAfterWake` proves the
path is reachable. Its action constraint `NoBAfterFire` stops B's result
from arriving after the fire. So when `rb2` reads "not" and B is delivered
later, the wake completed in the acquire replay. The witness trace shows the
whole sequence:

1. The execution suspends at `wake`.
2. B's result arrives.
3. The timer fires.
4. The next `Acquire` replays past `wake` and `rb2` (which reads "not").
5. The `await` commit delivers B.
6. The run completes.

### Activity keys

Async activities issued in a loop get keys in issue order: A is `act1`
and B is `act2`, whichever finishes first. `ExecuteToolActivity` with an
explicit ID that is already reserved for another command is rejected
loudly, and the run fails.

### Leases, executions and fencing

An execution, not a process, holds the lease. Each execution has its own
token, so two handlers in one process (Romancy's timer, cancel,
channel-timeout and recur handlers share one workerID) are two holders.
Same-process re-entry is not allowed.

**The store computes the expiry and compares it with its own clock.** Use
`now()` in SQL, never the client's `time.Now()`. A lease is expired exactly
when the store's clock is strictly past the expiry (`left < 0`).

`mine[e]` is the time that execution `e` believes remains. It comes from a
monotonic clock started before the acquire or renew request is sent. It may
lag store time by up to `MaxSkew` ticks per grant, which covers drift and
request latency.

Before each step an execution checks that `mine[e] >= Margin`. Because
expiry needs `left < 0` and `mine <= left + MaxSkew`, a passed check implies
`left >= Margin - MaxSkew >= 0`. So `Margin = MaxSkew` is safe and
`Margin = MaxSkew - 1` is not (`UnsafeMarginBelowSkew`).

An execution may stall for any time after its check (`Stall`).

**Fencing is required.** Every history write, status write, activity
dispatch, timer write and child write is conditional on the execution's own
token (`WHERE owner = me AND epoch = e`). A rejected commit stops the
execution. The clock check only keeps executions from starting work after a
takeover; only fencing protects a commit that stalls past expiry.

An execution that sees its own lease lapse abandons the run. It releases
the lease and sets the runnable flag **in the same write**, so the run is
dispatched again. A crashed execution never re-acquires. A new execution
must wait for the lease to expire, and expiry recovery dispatches the
instance whatever the flag says.

### Activities

Activity attempts run outside the workflow. Execution is at least once:

- an attempt can crash and be retried;
- a second attempt of A may run concurrently (a timeout retry);
- each attempt of A returns its own value.

The first result is recorded. A later result finds the value written and
is discarded. B and the cleanup start a new attempt only after the
previous one crashed, so the crash budget bounds them. The workflow
observes values only through `Get`, which reads history. Attempts start
only from a durable queued task row.

### Signals and channels

Each channel has its own cursor. Signals buffer durably from instance
creation and can be sent at any time while the instance runs. A receive
consumes the next signal of its own channel. The consumption advances that
channel's cursor in the same write that records the observation.

### Cancellation and children

A cancel request is recorded durably. The workflow observes it at its next
blocking yield (`select`, `recv`, `wake`, `await`). The timer poller records
a fire only for a running instance and never writes the status. A parent
that closes (completed, failed or cancelled) with a running child records a
cancel request for the child in the same write. This is the parent-close
policy, like Temporal's `REQUEST_CANCEL`.

**Assumption: child creation is atomic.** `StartChildWorkflow` records `C`
and creates the child in one transaction, or it creates the child
idempotently by a deterministic child ID derived from the parent ID and
the key `C`.

### Wake-up

Selvedge dispatches by in-process notification and by polling. The model
relies only on a durable `runnable` flag. Every input sets it: a signal, a
timer fire, a cancel request, an activity result, or a child result. It is
set even while an execution holds the lease. An abandoning execution also
sets it.

An execution starts only for a runnable instance, or to recover an expired
lease, and taking the instance clears the flag. A blocked execution
suspends in two steps: it decides to suspend, then releases the lease. The
release does not set the flag. Input that arrives between the two has
already set it.

`ParkedOnlyWhenWaiting` checks that an instance with no execution, a free
lease and no runnable flag always replays to a blocked yield. This keeps
the explicit stuttering end state from hiding a stranded run.

## Requirements for the Go implementation

Each rule names the fault configuration or invariant that shows it is
load-bearing. A rule that no configuration gates names a Go test instead.
Those tests are requirements for the implementation tickets: none of them
exists yet, and each must be written before its rule's code lands.

### History writes

There are three write classes:

1. **Command reservation.** A fenced insert-if-absent of `[cmd]` at the
   command's key. A conflict with another command fails the run; it is never
   `ON CONFLICT DO NOTHING`. Gated by `UnsafeSilentDuplicateKey`
   (`ResultRecordedOnce`); the model's case is `dupA`.
2. **Workflow observation.** Most observations (`sel`, `now`, `recv`,
   `async`, `cdone`, `cgetB`) are a single fenced `Record` insert of command
   and value together. Only `sleep` is reserved first (by `StepSleep`). Its
   value is then a fenced set-if-null: the cancel written by `StepWake`/
   `SetVal`, unless the poller's fire is already there. On that conflict the
   execution re-reads the key and uses the stored value (`StepReplay`). The
   fencing of these writes is gated by `UnsafeUnfencedWrite`
   (`WritesFenced`). The re-read-on-conflict semantics is not gated by any
   fault; it must be checked by Go test `TestWakeCancelRacesTimerFire`.
3. **Worker or poller result.** An unfenced set-if-null of the value written
   by an activity attempt or the timer poller.
   - Activity results: the first writer wins and later writes are dropped
     (`ActFinish`). Gated by `UnsafeLastWriteWins` (`ResultRecordedOnce`).
   - Timer fires: the first fire wins. This half is not gated, because each
     timer is armed once and fires once. It must be checked by Go test
     `TestTimerFireRecordedOnce`. The poller writes a fire only for a running
     instance; that is rule 24.

### Lease and dispatch

4. Lease expiry is computed and compared in the store's clock, strictly
   (`now() > expires_at`, `left < 0`). The store-clock half is gated by
   `UnsafeAcquirerClockExpiry`. The strictness (`<` rather than `<=`) is not
   gated; it must be checked by Go test `TestLeaseExpiryIsStrict`.
5. An execution's lease belief comes from a monotonic clock started before
   the request. It begins a step only while that clock says at least
   `Margin >= MaxSkew` remains. Gated by `UnsafeNoLeaseCheck` and
   `UnsafeMarginBelowSkew` (`NoConcurrentOwners`).
6. There is one lease token per execution, never per process, and a holder's
   process cannot re-acquire while the lease is held. Gated by
   `UnsafeProcessReentrantAcquire` (`NoConcurrentOwners`) and
   `UnsafeProcessFence` (`WritesFenced`).
7. Every commit is fenced on that token (`owner = me AND epoch = e`), and a
   rejected commit stops the execution. Gated by `UnsafeUnfencedWrite`,
   `UnsafeUnfencedDelivery` and `WitnessZombieFenced`.
8. Acquire clears the runnable flag in the grant write. An expired lease is
   recovered whatever the flag says (`AcquireGuard`). Not gated by a fault
   config; `SelvedgeLivenessCrash` (`Termination`) exercises the recovery,
   and Go test `TestExpiredLeaseRecoveredWithoutFlag` must check it.
9. Every input sets the runnable flag, even while the lease is held. Gated by
   `LivenessUnsafeLostWakeup`.
10. Suspension is two steps: decide, then release. The release does not set
    the flag. Not gated by a fault config. The pass configurations
    (`ParkedOnlyWhenWaiting`, `Termination`) exercise it, and Go test
    `TestInputDuringSuspendIsNotLost` must check it.
11. An execution that abandons a running instance (its own lease lapsed)
    releases the lease and sets the flag in the same write. This is distinct
    from suspension. Gated by `LivenessUnsafeAbandonWithoutWake`
    (`Termination`) and `UnsafeAbandonWithoutWake` (`ParkedOnlyWhenWaiting`).

### Replay

12. Activity keys follow issue order, not completion order. Gated by
    `UnsafeCompletionOrderKeys` (`ReplayDeterministic`).
13. A replayed key that holds another command fails the run at once. It has
    no fault of its own: `UnsafeCompletionOrderKeys` fails through this check.
    Go test `TestReplayCommandMismatchFailsFast` must check it directly.
14. `Now()` is recorded. Gated by `UnsafeUnrecordedNow`.
15. The Await decision is recorded with its delivery batch. Gated by
    `UnsafeUnrecordedWakeOrder`.
16. `Future.Get` reads history, never the in-memory attempt result. Gated by
    `UnsafeLiveFutureValue`.
17. Replay never re-queues an activity whose key is reserved. Gated by
    `UnsafeScheduleWithoutLookup` (`CompletedStepNotReplayedAsNew`).
18. Activities start only from the durable task row, never from in-memory
    scheduling. Not gated: the model starts attempts only from `task`, and no
    fault breaks that (`CompletedStepNotReplayedAsNew` measures re-issue, not
    this). It must be checked by Go test `TestActivityStartsOnlyFromTaskRow`.
19. A live yield completes only through a fenced commit that writes its
    delivery batch; this sentence is gated by `UnsafeUnfencedDelivery`
    (`ReplayDeterministic`). `IsReady`, `Pending()` and `ReceiveAsync` read
    only the delivered view; this is gated by `UnsafeLiveIsReady` and
    `UnsafeLivePending` (`ReplayDeterministic`) and by
    `UnsafeLiveReceiveAsync` (`PendingMatchesReceive`). The acquire-replay exception is described and
    witnessed above (`SelvedgeWitnessReadyAfterWake`).

### Signals

20. Each channel has its own cursor. Gated by `UnsafeSharedCursor`
    (`SignalsNotLost`).
21. Every cursor starts before the first signal ever sent. Gated by
    `UnsafeCursorFromMax`.
22. `Pending()` at position p excludes only the signals consumed before p,
    using the durable consumption positions, not the live cursor. Gated by
    `UnsafePendingLiveCursor` (`ReplayDeterministic`).
23. `ReceiveAsync` records `nosig` too. The reason is not determinism: an
    unrecorded "none" re-evaluates to the same answer on the delivered view.
    It is the fail-fast check: a recorded `async` command lets a replay
    detect a code change that removed or moved the receive (rule 13). Not
    gated; a mutation that drops the record passed `SelvedgeChannels` and a
    reduced channels configuration on the revision-3 model.

### Cancellation and children

24. The timer poller never writes the status. Gated by
    `UnsafeTimerRevivesCancelled` (`CancelledStaysCancelled`).
25. The parent-close cancel request for a running child is written in the
    same write as the close. Gated by `UnsafeNoParentClose`
    (`ChildrenFollowParent`).
26. Cleanup runs on `Detached()`. Gated by `UnsafeCleanupInCancelledScope`
    (`CleanupRunsAfterCancel`).
27. Every blocking yield observes cancellation. Gated by
    `LivenessUnsafeCancelIgnoredWhileReceiving`, `...WhileSleeping` and
    `...WhileAwaitingChild` (`CancelEnds`).
28. Child creation is atomic: record `C` and create the child in one
    transaction, or create it idempotently by a deterministic child ID. This
    is an assumption, not modelled (Go test `TestChildStartAtomic`).

### Queries
29. A query runs the workflow's handler against the delivered view at a position q that an acquire replay reaches. The engine takes one consistent snapshot of `hist`, `dpos` and `tpos`. It re-runs the workflow code as `FastForward` in `Acquire` does, and stops at the first unrecorded command, the first blocking yield without a recorded value, or the first other write. q is the position there. The handler sees the handlers registered up to q and the view at q: `IsReady`, `Pending()` and child readiness exactly as rule 19 and rule 22 define them. Events that have arrived but are not delivered are not visible, even when the replay passed a yield whose value a worker wrote. A query issues no commands, takes no lease, sets no runnable flag, fires no hooks and writes nothing; a command in a handler fails the query, not the run. The answer is not recorded, and a later query can see a later q. Not gated: queries are stuttering steps. Go tests `TestQueryReadsDeliveredView` and `TestQueryWritesNothing` must check it.

## How it maps to loom-mcp's engine interface

| Interface | In the model |
|-----------|--------------|
| `ExecutePlannerActivity`, `ExecuteToolActivity`, `ExecuteSealRecordActivity`, `ExecuteFinishRunActivity`, `ExecuteRecordContentActivity`, `ExecuteChildDispatchActivity`, `PublishHook` | A synchronous activity: reserve the key in issue order, queue the task, and block until the result is recorded and delivered (`clean`/`cleanWait`). An explicit ID that collides with another command's key fails the run (`dupA`). |
| `ExecuteToolActivityAsync`, `Future.Get`, `Future.IsReady` | `startA`/`startB` reserve the key and queue the task. `Get` returns the recorded value. `IsReady` is true only if the result was delivered at or before the current position, not merely recorded. In a cancelled scope, `Get` on an undelivered future returns `Canceled`, which is recorded (`cgetB`). |
| `Await(ctx, cond)` | `select`. The completing commit writes the delivery batch. The predicate is a pure function of the delivered view (see above). |
| `PauseRequests`, `ResumeRequests`, `ClarificationAnswers`, `ExternalToolResults`, `ConfirmationDecisions`, `TypedInputAnswers` | Named channels with separate cursors (c1, c2). |
| `Receive` | `recv`. |
| `Pending()` | `pend`. It counts delivered, unconsumed signals, and is safe inside an `Await` predicate. This is the enginetest scenario: await `Pending() > 0`, check it, then `ReceiveAsync`. |
| `ReceiveAsync` | `async`. It reads the delivered view and records the result. `PollPause` calls it. |
| `ReceiveWithTimeout` | A `select` over a signal and a timer; no separate command is modelled. |
| `NewTimer` | `timer`/`sleep`. |
| `Now` | `now`, recorded on first execution. |
| `StartChildWorkflow`, `ChildWorkflowHandle.Get`/`Cancel` | `child` (atomic), `await`, `ccancel`, and the parent-close policy in `Close`. |
| `Detached` | `clean`: the cleanup activity is scheduled outside the cancelled scope. |
| `WithCancel` | The cancelled scope. It is observed at yields, propagated to the child, and it cancels B's future. |
| `WorkflowHandle.Signal`, `Signaler.SignalByID` | `SendSignal(ch)`. |
| `WorkflowHandle.Cancel`, `Canceler.CancelByID` | `RequestCancel`. |
| `SetQueryHandler`, `Engine.QueryRootRunID`, `ToolAwaitQuerier` | No action; rule 29 (a read-only replay to q, the handler on the view at q) |
| `QueryRunStatus`, `QueryRunCompletion` | Read durable `status` and `hist` (`Describe`, `WaitWorkflow`) |
| `StartWorkflow` | `Init`. |

## What is deliberately not modelled

- **Data values.** Payloads, typed results and error decoding are not
  modelled (Romancy's decode bug, and its replay of errors as strings).
- **Queries.** The model has no query action. A query writes nothing, so it is a stuttering step, and every invariant and liveness property holds with queries added. Rule 29 states what the engine must do.
- **Versioning markers (S16).** These are not modelled, and neither is
  their interaction with the fail-fast command-mismatch check. A version
  marker must be a recorded command, so that replay of old histories takes
  the old branch rather than failing.
- **`ReceiveWithTimeout` edge cases.** A non-positive timeout must be a
  recorded immediate timeout. When the signal wins the race, the losing
  timer's later fire must not wake the instance or change its observation.
- **Late activity results after Close or cancel.** They still write `hist`
  in the model (B's result after `cgetB` recorded `canceled`, the cleanup
  after a close). This is harmless: no execution reads them, and the
  observation already recorded wins.
- **More than one parent instance**, including `StartWorkflow`
  duplicate-ID rejection. The Go test `TestStartWorkflowDuplicateIDRejected`
  must cover it.
- **The child's internals**, clock jumps and unbounded drift, real-time
  timer accuracy, activity heartbeats, retry exhaustion and failure values,
  continue-as-new, run timeouts, compensation and the outbox.
- **Delivery of events other than B and signals.** Every other readiness
  decision in the script is a recorded observation.

## Invariants and properties

Each invariant names the Go test that an implementation ticket must write
to prove it against the real engine. Tests are table-driven and
deterministic, and they use an injectable store clock, a fake monotonic
clock per execution, and a store with crash and stall injection.

| Invariant | Meaning in the model | Required Go test |
|-----------|----------------------|------------------|
| `ReplayDeterministic` | Every execution derives the same command and observation at each position as the first execution there. Replay never finds another command at a key. | `TestReplayDeterministicAfterCrash`: crash after every write and resume. Sub-cases: A and B finish in either order; B finishes after `Await` returned A; a signal arrives after `Pending()` returned 0; the deadline passes between runs; two attempts of A return different values. `TestReplayCommandMismatchFailsFast`. |
| `ResultRecordedOnce` | Every acknowledged write is what the store holds. | `TestHistoryKeyWrittenOnce`: duplicates keep the first value; an explicit-ID collision fails with `ErrHistoryKeyConflict`. |
| `NoConcurrentOwners` | No execution begins a step after another execution was granted the lease. | `TestLeaseCheckStopsBeforeTakeover`; `TestTwoHandlersOneProcessDoNotShareLease`. |
| `WritesFenced` | Every commit lands while its own execution holds the lease (`holder = e`). | `TestStalledExecutionCommitRejected`: stall after the lease check, take over, release the stall, and assert every write is rejected. Include the case where both executions run in one process. |
| `CompletedStepNotReplayedAsNew` | No step whose result is recorded is issued again. | `TestRecordedActivityNotRescheduledOnReplay`. |
| `SignalsNotLost` | Every sent signal was consumed or is still pending on its own channel. | `TestSignalBeforeFirstReceiveIsDelivered`; `TestChannelsHaveSeparateCursors`. |
| `PendingMatchesReceive` | `Pending() == 0` is never followed by a `ReceiveAsync` that returns a signal. | `TestPendingAndReceiveAsyncAgree`: the enginetest lifecycle scenario, with a signal racing the read. |
| `CancelledStaysCancelled` | A cancelled instance is never running again. | `TestCancelWhileSleepingStaysCancelled`, repeated while waiting on `Receive` and on a child. |
| `ChildrenFollowParent` | A closed parent leaves no running child without a recorded cancel request. | `TestParentCloseCancelsRunningChild`; `TestChildStartAtomic`. |
| `CleanupRunsAfterCancel` | A run that ends cancelled has recorded its detached cleanup. | `TestDetachedCleanupRunsAfterCancel`. |
| `ParkedOnlyWhenWaiting` | An instance with no execution, a free lease and no runnable flag replays to a blocked yield. | `TestAbandonedRunIsRedispatched`: let an execution's own lease lapse mid-script with a pending cancel and signal, and assert the instance is dispatched again and the cancel is served. |
| `Termination` (liveness) | Every run ends, with fair, timely executions and a fair environment. Also with one crash or one lapse of a holder's lease (`SelvedgeLivenessCrash`): the lapsed holder abandons, or the orphan lease expires, and another execution finishes. | `TestRunsTerminateWithoutFaults`; `TestTakeoverAfterCrashCompletes`. |
| `CancelEnds` (liveness) | A cancel request ends the run with no timer, child or signal help. | `TestCancelEndsBlockedWaits`. |
| `ChildStops` (liveness) | A child with a cancel request stops. | Covered by `TestParentCloseCancelsRunningChild`. |

The pass liveness configurations also check every safety invariant.

## Configurations

`MaxC1`/`MaxC2` are the signal budgets for channels c1 and c2. All pass
configurations check every safety invariant plus `ParkedOnlyWhenWaiting`.

| Configuration | Constants | Distinct states |
|---------------|-----------|-----------------|
| `Selvedge` | 1 slot, 1 crash, c1 1, cancel, deadline, duplicate attempts | 5,076,584 |
| `SelvedgeChannels` | 1 slot, no crash, c1 1, c2 1, cancel, deadline, duplicate attempts | 6,828,420 |
| `SelvedgeTwoWorkers` | 2 slots, lag 1, cancel | 4,581,494 |
| `SelvedgeTwoWorkersSignals` | 2 slots, lag 1, c1 1, c2 1 | 15,835,379 |
| `SelvedgeTwoWorkersCrash` | 2 slots, lag 1, 1 crash | 1,520,935 |
| `SelvedgeLiveness` | 2 slots, timely, fair environment: `Termination`, `ChildStops` | 648,406 |
| `SelvedgeLivenessCancel` | as above, with timers, child completion and signals not fair: `CancelEnds`, `ChildStops` | 648,406 |
| `SelvedgeLivenessCrash` | as above, with one crash or one lapse of a holder's lease: `Termination` | 3,641,178 |
| `SelvedgeTwoWorkersLong` (`--long`) | 2 slots, lag 1, cancel, c1 1 | 39,544,878 (19 min) |

`SelvedgeTwoWorkers` and `SelvedgeTwoWorkersCrash` have `MaxC1 = 0`: their
runs reach `recv` only to take the cancel, or not at all. So only the signal
configurations cover the `recv`/`pend`/`async`/`wake`/`rb2` half of the script:
`Selvedge`, `SelvedgeChannels`, `SelvedgeTwoWorkersSignals`, the liveness
configurations and `--long`.

The two-slot configurations turn off the deadline and duplicate attempts,
which the one-slot configurations cover. Combining signals, cancel and
crashes on two slots exceeds the 10-minute budget; the combination of
cancel and signals is the `--long` gate.

Fault and witness configurations:

| Configuration | Expected |
|---------------|----------|
| `SelvedgeUnsafeNoLeaseCheck` | `NoConcurrentOwners` |
| `SelvedgeUnsafeMarginBelowSkew` | `NoConcurrentOwners` |
| `SelvedgeUnsafeAcquirerClockExpiry` | `NoConcurrentOwners` |
| `SelvedgeUnsafeProcessReentrantAcquire` | `NoConcurrentOwners` |
| `SelvedgeUnsafeProcessFence` | `WritesFenced` |
| `SelvedgeUnsafeUnfencedWrite` | `WritesFenced` |
| `SelvedgeUnsafeUnrecordedWakeOrder` | `ReplayDeterministic` |
| `SelvedgeUnsafeUnrecordedNow` | `ReplayDeterministic` |
| `SelvedgeUnsafeLiveIsReady` | `ReplayDeterministic` |
| `SelvedgeUnsafeLivePending` | `ReplayDeterministic` |
| `SelvedgeUnsafeLiveReceiveAsync` | `PendingMatchesReceive` |
| `SelvedgeUnsafePendingLiveCursor` | `ReplayDeterministic` |
| `SelvedgeUnsafeAbandonWithoutWake` | `ParkedOnlyWhenWaiting` |
| `SelvedgeUnsafeUnfencedDelivery` | `ReplayDeterministic` |
| `SelvedgeUnsafeCompletionOrderKeys` | `ReplayDeterministic` |
| `SelvedgeUnsafeLiveFutureValue` | `ReplayDeterministic` |
| `SelvedgeUnsafeScheduleWithoutLookup` | `CompletedStepNotReplayedAsNew` |
| `SelvedgeUnsafeSilentDuplicateKey` | `ResultRecordedOnce` |
| `SelvedgeUnsafeLastWriteWins` | `ResultRecordedOnce` |
| `SelvedgeUnsafeCursorFromMax` | `SignalsNotLost` |
| `SelvedgeUnsafeSharedCursor` | `SignalsNotLost` |
| `SelvedgeUnsafeTimerRevivesCancelled` | `CancelledStaysCancelled` |
| `SelvedgeUnsafeNoParentClose` | `ChildrenFollowParent` |
| `SelvedgeUnsafeCleanupInCancelledScope` | `CleanupRunsAfterCancel` |
| `SelvedgeLivenessUnsafeCancelIgnoredWhileReceiving`, `...WhileSleeping`, `...WhileAwaitingChild` | `CancelEnds` violated |
| `SelvedgeLivenessUnsafeLostWakeup` | `Termination` violated |
| `SelvedgeLivenessUnsafeAbandonWithoutWake` | `Termination` violated |
| `SelvedgeWitness*` | reachable: a takeover then completion; a zombie's commit fenced after a takeover; A run twice and recorded once; a cancel while sleeping; parent close cancelling a child; `IsReady(B)` false and B delivered later; `Pending()` seeing a signal; `IsReady(B)` false after a wake completed in the acquire replay (`WitnessReadyAfterWake`, with action constraint `NoBAfterFire`) |

### What each fault changes

- **`no_lease_check`**: the execution never checks its lease before a step,
  as with Romancy's fixed lock.
- **`MarginBelowSkew`**: the `SelvedgeTwoWorkers` constants with
  `Margin = 0`.
- **`acquirer_clock_expiry`**: the expiry is written from the holder's wall
  clock and compared with the acquirer's. This is Romancy's Postgres and
  MySQL `TryAcquireLock`, which use `time.Now()` in Go.
- **`process_reentrant_acquire`**: a second execution in the holder's process
  takes the lease again (Romancy's re-entrant lock on workerID). The first
  execution then begins a step after the second was granted the lease
  (`NoConcurrentOwners`). Its commit is still rejected by per-execution
  fencing. Duplicate writes need this fault combined with `process_fence`,
  which is what Romancy does.
- **`process_fence`**: commits are fenced on the process, not the execution.
- **`unfenced_write`**: commits carry no `owner/epoch` condition.
- **`abandon_without_wake`**: abandoning releases the lease without setting
  the runnable flag, which is the stranding the second review found.
- **`unrecorded_wake_order`**, **`unrecorded_now`**: no recorded decision,
  and no recorded `Now()`.
- **`live_is_ready`**, **`live_pending`**, **`live_receive_async`**: the read
  uses the live inbox or results, not the delivered view.
- **`pending_live_cursor`**: `Pending()` excludes consumed signals by the live
  cursor instead of the durable consumption positions.
- **`SelvedgeUnsafeUnfencedDelivery`**: the `unfenced_write` fault, checked
  without the fencing ghosts, to show that it breaks replay.
- **`completion_order`**: activity keys are assigned when results arrive.
- **`live_future`**: `Get` returns the in-memory attempt value.
- **`schedule_without_lookup`**: activities are queued again on replay.
- **`silent_duplicate_key`**, **`last_write_wins`**: a key collision is
  dropped silently; a later result overwrites the first.
- **`cursor_from_max`**, **`shared_cursor`**: the cursor starts at
  `MAX(id)`; one cursor serves all channels.
- **`timer_revives_cancelled`**, **`no_parent_close`**,
  **`cleanup_in_cancelled_scope`**: Romancy's timer resume; no parent-close
  policy; cleanup scheduled in the cancelled scope.
- **`cancel_ignored_while_*`**, **`lost_wakeup`**: a yield that ignores
  cancel; input during a held lease that does not set the flag.

`unrecorded_async_none` from the previous revision is gone. With
`ReceiveAsync` reading the delivered view, an unrecorded "none" re-evaluates
to the same answer, so it is no longer a fault. The live-read variant is
`live_receive_async`.
