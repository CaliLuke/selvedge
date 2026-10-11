------------------------------ MODULE Selvedge ------------------------------
(***************************************************************************)
(* The durable-execution engine Selvedge intends to build: the engine      *)
(* behind loom-mcp's engine.WorkflowContext, written before any code       *)
(* changes. See Selvedge.md for the mapping to the engine interface.       *)
(*                                                                         *)
(* One parent workflow instance runs a fixed, deterministic script. An     *)
(* execution re-runs the code from the top whenever it takes the instance. *)
(* Each command has a key; the durable history maps a key to the command   *)
(* that holds it and the observation recorded for it, each written once.  *)
(* Replay reads every observation from history and checks that the stored  *)
(* command is the one the code issues; a mismatch fails the run at once.   *)
(* A command with no recorded observation reads live state and records     *)
(* what it read in the same write that acts on it. A non-blocking read     *)
(* that is not recorded (IsReady) sees an event only once it has been      *)
(* delivered: events are delivered when a yield completes, and tagged with *)
(* that yield's position, so a replay sees the same events at the same     *)
(* position (dpos).                                                       *)
(*                                                                         *)
(* The script:                                                             *)
(*   sub      receivers on channels c1 and c2 exist                        *)
(*   startA   ExecuteToolActivityAsync A               key act1 (issue 1)  *)
(*   startB   ExecuteToolActivityAsync B               key act2 (issue 2)  *)
(*   timer    NewTimer T                               key T               *)
(*   child    StartChildWorkflow C                     key C               *)
(*   select   Await the first of A, B, T, c1, cancel   key sel             *)
(*   getW     Get on the winning future (A or B)                           *)
(*   readyB   IsReady(B) after A won (not recorded: delivery view)         *)
(*   getB     Get(B) when it was ready                                     *)
(*   now      Now(), compared with a deadline          key now             *)
(*   branch   T won            -> dupA: activity with explicit ID act1     *)
(*            A or B won, late -> complete (child still running)           *)
(*            otherwise        -> recv                                     *)
(*   recv     Receive on c1 (or cancel)                key recv            *)
(*   async    ReceiveAsync on c2: a signal or "none"   key async           *)
(*   sleep    NewTimer + Get (or cancel)               key sleep           *)
(*   await    ChildWorkflowHandle.Get (or cancel)      key cdone           *)
(*   complete                                                              *)
(* After a cancel observed at a yield: cancel the child (ccancel), Get(B)  *)
(* in the cancelled scope (cgetB: its value or "canceled"), run a cleanup  *)
(* activity on Detached() (clean), close cancelled.                        *)
(*                                                                         *)
(* Executions, not processes, hold the lease. The store computes and       *)
(* compares the expiry with its own clock; lease.left is store time left.  *)
(* mine[e] is what execution e believes remains, from a monotonic clock    *)
(* started before the request; it may lag store time by up to MaxSkew      *)
(* ticks (lag). An execution checks its lease before each step; it may    *)
(* stall after the check (Stall) for any time before the step commits     *)
(* (Commit). Every commit is fenced by the store: it succeeds only while   *)
(* e holds the lease (WHERE holder = e, the execution's own token).        *)
(*                                                                         *)
(* Inputs (signals, timer fires, cancel, activity results, child results)  *)
(* set a durable runnable flag; an execution starts only for a runnable    *)
(* instance or to recover an expired lease.                                *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Slots,           \* execution slots; an execution holds the lease, not a process
    OneProcess,      \* TRUE: all slots run in one process (Romancy's shared workerID)
    MaxC1, MaxC2,    \* signals the environment may send on channels c1 and c2
    TTL,             \* lease time to live, in store ticks
    MaxSkew,         \* bound on clock error: monotonic lag, and wall-clock offset
    Margin,          \* an execution begins a step only while it believes >= Margin remain
    MaxCrashes,      \* crashes (execution or activity attempt) per behaviour
    Cancels,         \* TRUE: the environment may request cancellation
    Deadline,        \* TRUE: the wall-clock deadline the workflow reads may pass
    DupAttempts,     \* TRUE: a second attempt of A may run concurrently (timeout retry)
    Timely,          \* TRUE: time never passes a live holder's renewal window (liveness)
    FairEnvironment, \* TRUE: signals on c1, timers and child completion are fair (liveness)
    Fault            \* "none", or one rule deliberately broken

Faults == {"none",
           \* lease
           "no_lease_check", "acquirer_clock_expiry", "process_reentrant_acquire",
           "process_fence", "unfenced_write", "abandon_without_wake",
           \* replay
           "unrecorded_wake_order", "unrecorded_now", "live_is_ready",
           "live_pending", "live_receive_async", "pending_live_cursor",
           "completion_order", "live_future",
           "schedule_without_lookup",
           \* history
           "silent_duplicate_key", "last_write_wins",
           \* signals
           "cursor_from_max", "shared_cursor",
           \* cancellation and children
           "timer_revives_cancelled", "no_parent_close", "cleanup_in_cancelled_scope",
           \* liveness
           "cancel_ignored_while_receiving", "cancel_ignored_while_sleeping",
           "cancel_ignored_while_awaiting_child", "lost_wakeup"}

ASSUME Fault \in Faults
ASSUME MaxC1 \in Nat /\ MaxC2 \in Nat /\ MaxCrashes \in Nat /\ MaxSkew \in Nat
ASSUME Margin \in Nat /\ TTL \in Nat /\ TTL > Margin
ASSUME OneProcess \in BOOLEAN /\ Cancels \in BOOLEAN /\ Deadline \in BOOLEAN
ASSUME DupAttempts \in BOOLEAN
ASSUME Timely \in BOOLEAN /\ FairEnvironment \in BOOLEAN

None == "none"
Terminal == {"completed", "failed", "cancelled"}
Chans == {"c1", "c2"}
Keys == {"sub", "act1", "act2", "T", "C", "sel", "selS", "now", "recv", "async", "sleep",
         "cdone", "ccancel", "cgetB", "clean"}
Acts == {"A", "B", "clean"}
TimerKeys == {"T", "sleep"}
\* Attempts of A: one beyond the crash budget always finishes, and one more
\* may run concurrently after a timeout. B and the cleanup start a new
\* attempt only after the previous one crashed, so the crash budget bounds
\* them (tries stays 0 for them).
MaxTries == MaxCrashes + 2
Vals == <<"v1", "v2", "v3", "v4", "v5", "v6">>
MaxPos == 22
MaxSig == MaxC1 + MaxC2
NoEntry == [cmd |-> None, val |-> None]
NoExec == [pc |-> None, pos |-> 0, winner |-> None, phase |-> None]
Released == [holder |-> None, left |-> -1, wskew |-> 0]

Max(a, b) == IF a >= b THEN a ELSE b
Proc(e) == IF OneProcess THEN "p1" ELSE e
MaxOn(ch) == IF ch = "c1" THEN MaxC1 ELSE MaxC2

VARIABLES
    hist,           \* store: key -> [cmd, val]; each field written once
    status,         \* store: "running", "completed", "failed", "cancelled"
    cancelReq,      \* store: a cancel request is recorded
    inbox,          \* store: the channel of each signal, in send order
    cur,            \* store: channel -> index of the last signal it consumed
    task,           \* store: activity -> "none" or "queued"
    att,            \* activity attempts in flight
    tries,          \* attempts of A started
    doneAtt,        \* attempts of A whose result reached a worker (live_future only)
    armed,          \* store: timers armed and not yet fired
    child,          \* child instance: "none", "running", "completed", "cancelled"
    childCancelReq, \* store: a cancel request is recorded for the child
    lease,          \* store: [holder, left, wskew]; left < 0 means expired
    runnable,       \* store: new input since the last execution took the instance
    late,           \* the wall-clock deadline the workflow reads with Now() has passed
    dpos,           \* store: [b, s]: position of the yield that delivered B's result and each signal, 0 if not yet
    exec,           \* slot -> [pc, pos, winner, phase], or NoExec
    mine,           \* slot -> lease time the execution believes remains (-1 without one)
    lag,            \* slot -> how far the execution's clock lags store time since its last grant
    skew,           \* slot -> wall-clock offset (only acquirer_clock_expiry reads it)
    crashes,
    seen,           \* ghost: position -> <<pc, observation>> first acted on
    acked,          \* ghost: <<key, cmd, val>> writes the store acknowledged
    tpos,           \* store: signal -> position of the step that consumed it, 0 if not yet
    diverged,       \* ghost: an execution derived something other than seen, or replay found another command
    overlap,        \* ghost: an execution began a step after another was granted the lease
    unfenced,       \* ghost: a commit landed while its execution did not hold the lease
    rejected,       \* ghost: the store rejected a commit from an execution that lost the lease
    grantee,        \* ghost: the execution most recently granted the lease
    replayedAsNew,  \* ghost: a step with a recorded result was issued again
    everCancelled,  \* ghost: the instance has closed as cancelled
    takeover        \* ghost: an execution took the lease from another

vars == <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed, child,
          childCancelReq, lease, runnable, late, dpos, exec, mine, lag, skew, crashes,
          seen, acked, tpos, diverged, overlap, unfenced, rejected, grantee,
          replayedAsNew, everCancelled, takeover>>

Init ==
    /\ hist = [k \in Keys |-> NoEntry]
    /\ status = "running"
    /\ cancelReq = FALSE
    /\ inbox = <<>>
    /\ cur = [c \in Chans |-> 0]
    /\ task = [a \in Acts |-> "none"]
    /\ att = [a \in Acts |-> {}]
    /\ tries = 0
    /\ doneAtt = {}
    /\ armed = {}
    /\ child = "none"
    /\ childCancelReq = FALSE
    /\ lease = Released
    /\ runnable = TRUE
    /\ late = FALSE
    /\ dpos = [b |-> 0, s |-> [i \in 1..MaxSig |-> 0]]
    /\ exec = [e \in Slots |-> NoExec]
    /\ mine = [e \in Slots |-> -1]
    /\ lag = [e \in Slots |-> 0]
    /\ skew \in IF Fault = "acquirer_clock_expiry"
                   THEN {f \in [Slots -> -MaxSkew..MaxSkew] :
                            \A x, y \in Slots : Proc(x) = Proc(y) => f[x] = f[y]}
                   ELSE {[e \in Slots |-> 0]}
    /\ crashes = 0
    /\ seen = [p \in 1..MaxPos |-> <<>>]
    /\ acked = {}
    /\ tpos = [i \in 1..MaxSig |-> 0]
    /\ diverged = FALSE
    /\ overlap = FALSE
    /\ unfenced = FALSE
    /\ rejected = FALSE
    /\ grantee = None
    /\ replayedAsNew = FALSE
    /\ everCancelled = FALSE
    /\ takeover = FALSE

-----------------------------------------------------------------------------
(* Store reads and writes.                                                  *)

Reserve(k, c) ==
    /\ hist' = [hist EXCEPT ![k] = [cmd |-> c, val |-> None]]
    /\ acked' = acked \cup {<<k, c, None>>}

Record(k, c, v) ==
    /\ hist' = [hist EXCEPT ![k] = [cmd |-> c, val |-> v]]
    /\ acked' = acked \cup {<<k, c, v>>}

SetVal(k, v) ==
    /\ hist' = [hist EXCEPT ![k].val = v]
    /\ acked' = acked \cup {<<k, hist[k].cmd, v>>}

\* The history key holding activity a's result: issue order (A is act1, B is
\* act2). The broken configuration assigns keys in completion order.
ActKey(a) ==
    IF a = "clean" THEN "clean"
    ELSE IF Fault # "completion_order" THEN (IF a = "A" THEN "act1" ELSE "act2")
    ELSE IF hist["act1"].cmd \in {None, a} THEN "act1" ELSE "act2"

\* The recorded result of activity a, wherever it is keyed.
ResultOf(a) ==
    IF a = "clean" THEN hist["clean"].val
    ELSE IF hist["act1"].cmd = a THEN hist["act1"].val
    ELSE IF hist["act2"].cmd = a THEN hist["act2"].val
    ELSE None

\* The next signal on channel ch after its cursor, 0 if none.
NextSig(ch) ==
    LET is == {i \in 1..Len(inbox) : i > cur[ch] /\ inbox[i] = ch}
    IN IF is = {} THEN 0 ELSE CHOOSE i \in is : \A j \in is : i <= j

\* The next signal on channel ch after its cursor that was delivered at or
\* before position p, 0 if none.
NextDeliv(ch, p) ==
    LET is == {i \in 1..Len(inbox) : i > cur[ch] /\ inbox[i] = ch /\ dpos.s[i] \in 1..p}
    IN IF is = {} THEN 0 ELSE CHOOSE i \in is : \A j \in is : i <= j

\* Receiver.Pending() at position p: signals on ch delivered at or before p
\* and not consumed before p, from the durable positions. A pure function of
\* history, so a replay at p gets the same count. The broken configuration
\* excludes consumed signals by the live cursor, which a later receive has
\* moved.
PendingView(ch, p) ==
    Cardinality({i \in 1..Len(inbox) :
                    /\ inbox[i] = ch
                    /\ dpos.s[i] \in 1..p
                    /\ IF Fault = "pending_live_cursor" THEN i > cur[ch]
                       ELSE tpos[i] \notin 1..(p - 1)})

\* Consuming signal i at position p advances its channel's cursor in the
\* same write. The broken configuration keeps one cursor for all channels.
Consume(ch, i, p) ==
    /\ cur' = IF Fault = "shared_cursor" THEN [c \in Chans |-> i] ELSE [cur EXCEPT ![ch] = i]
    /\ tpos' = [tpos EXCEPT ![i] = p]

\* B is ready at position p if its result was delivered at or before p.
ReadyB(p) == dpos.b \in 1..p

\* A yield that completes at position p, in its own fenced commit, delivers
\* every event that has arrived and was not delivered before: B's result
\* and every buffered signal. This is the recorded delivery batch.
Deliver(p) ==
    dpos' = [b |-> IF dpos.b = 0 /\ ResultOf("B") # None THEN p ELSE dpos.b,
             s |-> [i \in 1..MaxSig |-> IF dpos.s[i] = 0 /\ i <= Len(inbox) THEN p ELSE dpos.s[i]]]

\* New input sets the runnable flag. The broken configuration skips it
\* while an execution holds the lease, trusting the holder to notice.
Wake == runnable' = IF Fault = "lost_wakeup" /\ lease.holder # None THEN runnable ELSE TRUE

-----------------------------------------------------------------------------
(* The script, as a function of the execution and recorded history.         *)

AfterSelect(x) ==
    CASE x = "X" -> "ccancel" [] x \in {"A", "B"} -> "getW" [] OTHER -> "now"

AfterGetW(win) == IF win = "A" THEN "readyB" ELSE "now"

AfterNow(win, obs) ==
    IF win = "T" THEN "dupA"
    ELSE IF win \in {"A", "B"} /\ obs = "late" THEN "complete"
    ELSE "recv"

AfterRecv(obs) == IF obs = "X" THEN "ccancel" ELSE "pend"

PendObs(n) == IF n > 0 THEN "some" ELSE "zero"

AfterWake(obs) == IF obs = "X" THEN "ccancel" ELSE "rb2"

AfterAwait(obs) == IF obs = "X" THEN "ccancel" ELSE "complete"

LateObs == IF late THEN "late" ELSE "early"

\* <<key, command>> for a command that holds a key.
KeyCmd(pc) ==
    CASE pc = "sub" -> <<"sub", "sub">>
      [] pc = "startA" -> <<"act1", "A">>
      [] pc = "startB" -> <<"act2", "B">>
      [] pc = "timer" -> <<"T", "T">>
      [] pc = "child" -> <<"C", "C">>
      [] pc = "select" -> <<"sel", "sel">>
      [] pc = "now" -> <<"now", "now">>
      [] pc = "recv" -> <<"recv", "recv">>
      [] pc = "async" -> <<"async", "async">>
      [] pc \in {"sleep", "wake"} -> <<"sleep", "sleep">>
      [] pc = "await" -> <<"cdone", "cdone">>
      [] pc = "ccancel" -> <<"ccancel", "ccancel">>
      [] pc = "cgetB" -> <<"cgetB", "cgetB">>
      [] pc \in {"clean", "cleanWait"} -> <<"clean", "clean">>
      [] OTHER -> <<>>

\* <<observation, next pc, winner>> for the command at e.pc when its
\* observation is recorded (or, for getW, getB and readyB, determined by
\* recorded data); <<"MISMATCH">> when the key holds another command; <<>>
\* when it must run live. Commands a broken configuration leaves unrecorded
\* or always issues are never replayed.
Recorded(e) ==
    LET kc == KeyCmd(e.pc)
        p == e.pos + 1
    IN IF kc # <<>> /\ hist[kc[1]].cmd \notin {None, kc[2]} THEN <<"MISMATCH">>
       ELSE CASE e.pc = "sub" /\ hist["sub"].cmd = "sub" -> <<"ok", "startA", None>>
         [] e.pc = "startA" /\ hist["act1"].cmd = "A" /\ Fault # "schedule_without_lookup"
              -> <<"ok", "startB", None>>
         [] e.pc = "startB" /\ hist["act2"].cmd = "B" /\ Fault # "schedule_without_lookup"
              -> <<"ok", "timer", None>>
         [] e.pc = "timer" /\ hist["T"].cmd = "T" -> <<"ok", "child", None>>
         [] e.pc = "child" /\ hist["C"].cmd = "C" -> <<"ok", "select", None>>
         [] e.pc = "select" /\ hist["sel"].val # None /\ Fault # "unrecorded_wake_order"
              -> <<hist["sel"].val, AfterSelect(hist["sel"].val), hist["sel"].val>>
         [] e.pc = "getW" /\ Fault # "live_future"
              -> <<ResultOf(e.winner), AfterGetW(e.winner), e.winner>>
         [] e.pc = "readyB" /\ Fault # "live_is_ready"
              -> IF ReadyB(p) THEN <<"ready", "getB", e.winner>> ELSE <<"not", "now", e.winner>>
         [] e.pc = "getB" /\ Fault # "live_future" -> <<ResultOf("B"), "now", e.winner>>
         [] e.pc = "now" /\ hist["now"].val # None /\ Fault # "unrecorded_now"
              -> <<hist["now"].val, AfterNow(e.winner, hist["now"].val), None>>
         [] e.pc = "recv" /\ hist["recv"].val # None
              -> <<hist["recv"].val, AfterRecv(hist["recv"].val), None>>
         [] e.pc = "pend" /\ Fault # "live_pending"
              -> <<PendObs(PendingView("c2", p)), "async", None>>
         [] e.pc = "async" /\ hist["async"].val # None -> <<hist["async"].val, "sleep", None>>
         [] e.pc = "sleep" /\ hist["sleep"].cmd = "sleep" -> <<"ok", "wake", None>>
         [] e.pc = "wake" /\ hist["sleep"].val # None
              -> <<hist["sleep"].val, AfterWake(hist["sleep"].val), None>>
         [] e.pc = "rb2" /\ Fault # "live_is_ready"
              -> IF ReadyB(p) THEN <<"ready", "await", None>> ELSE <<"not", "await", None>>
         [] e.pc = "await" /\ hist["cdone"].val # None
              -> <<hist["cdone"].val, AfterAwait(hist["cdone"].val), None>>
         [] e.pc = "ccancel" /\ hist["ccancel"].cmd = "ccancel" -> <<"ok", "cgetB", None>>
         [] e.pc = "cgetB" /\ hist["cgetB"].val # None -> <<hist["cgetB"].val, "clean", None>>
         [] e.pc = "clean" /\ hist["clean"].cmd = "clean" -> <<"ok", "cleanWait", None>>
         [] e.pc = "cleanWait" /\ hist["clean"].val # None /\ Fault # "cleanup_in_cancelled_scope"
              -> <<"ok", "closeX", None>>
         [] OTHER -> <<>>

\* Replays from execution e through every recorded command, checking each
\* position against seen (s) as Observe does. A key that holds another
\* command stops the replay at "nondet", which fails the run.
RECURSIVE FastForward(_, _, _)
FastForward(e, s, d) ==
    LET r == Recorded(e)
    IN IF r = <<>> THEN [exec |-> e, seen |-> s, diverged |-> d]
       ELSE IF r = <<"MISMATCH">>
          THEN [exec |-> [e EXCEPT !.pc = "nondet"], seen |-> s, diverged |-> TRUE]
       ELSE LET p == e.pos + 1
                entry == <<e.pc, r[1]>>
            IN FastForward([pc |-> r[2], pos |-> p, winner |-> r[3], phase |-> e.phase],
                           IF s[p] = <<>> THEN [s EXCEPT ![p] = entry] ELSE s,
                           d \/ (s[p] # <<>> /\ s[p] # entry))

-----------------------------------------------------------------------------
(* The lease.                                                               *)

\* The store's own clock says the lease has expired. The broken
\* configuration is Romancy's Postgres and MySQL lock: the expiry is
\* written from the holder's wall clock and compared with the acquirer's.
Expired(e) ==
    IF Fault = "acquirer_clock_expiry" THEN lease.left + lease.wskew - skew[e] < 0
    ELSE lease.left < 0

\* The store's fencing condition on every commit: WHERE holder = e (the
\* execution's own token). The broken configuration fences on the
\* process, Romancy's workerID.
FenceOK(e) ==
    IF Fault = "process_fence" THEN lease.holder # None /\ Proc(lease.holder) = Proc(e)
    ELSE lease.holder = e

\* A slot starts an execution for a runnable instance, or to recover an
\* expired lease. The lease must be free or expired; the broken
\* configuration also lets the holder's process take it again (Romancy's
\* re-entrant lock on workerID).
AcquireGuard(e) ==
    /\ exec[e] = NoExec
    /\ status = "running"
    /\ runnable \/ (lease.holder # None /\ Expired(e))
    /\ \/ lease.holder = None
       \/ Expired(e)
       \/ Fault = "process_reentrant_acquire" /\ Proc(lease.holder) = Proc(e)

\* The store grants the lease (expiry from its own clock), and the new
\* execution replays the recorded prefix. Taking the instance consumes the
\* runnable flag.
Acquire(e) ==
    /\ AcquireGuard(e)
    /\ takeover' = (takeover \/ lease.holder \notin {None, e})
    /\ grantee' = e
    /\ lease' = [holder |-> e, left |-> TTL, wskew |-> skew[e]]
    /\ mine' = [mine EXCEPT ![e] = TTL]
    /\ lag' = [lag EXCEPT ![e] = 0]
    /\ runnable' = FALSE
    /\ LET ff == FastForward([pc |-> "sub", pos |-> 0, winner |-> None, phase |-> "idle"],
                             seen, diverged)
       IN /\ exec' = [exec EXCEPT ![e] = ff.exec]
          /\ seen' = ff.seen
          /\ diverged' = ff.diverged
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, late, dpos, skew, crashes, acked, tpos,
                   overlap, unfenced, rejected, replayedAsNew, everCancelled>>

\* Renewal is a fenced write; the store extends from its own clock.
Renew(e) ==
    /\ exec[e] # NoExec
    /\ status = "running"
    /\ Fault = "no_lease_check" \/ mine[e] >= Margin
    /\ mine[e] < TTL
    /\ FenceOK(e)
    /\ lease' = [holder |-> lease.holder, left |-> TTL, wskew |-> skew[e]]
    /\ mine' = [mine EXCEPT ![e] = TTL]
    /\ lag' = [lag EXCEPT ![e] = 0]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, runnable, late, dpos, exec, skew, crashes, seen,
                   acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

\* With Timely, time never passes the renewal window of a live holder in S.
TimelyFor(S) ==
    Timely => \A x \in S : exec[x] # NoExec /\ lease.holder = x => mine[x] > Margin

\* One store tick; every live execution's clock ticks too. With Timely, a
\* tick past a live holder's renewal window (a lapse: the holder stalled)
\* uses the crash budget.
Tick ==
    /\ TimelyFor(Slots) \/ crashes < MaxCrashes
    /\ crashes' = IF TimelyFor(Slots) THEN crashes ELSE crashes + 1
    /\ lease.left >= 0 \/ \E x \in Slots : mine[x] >= 0
    /\ lease' = [lease EXCEPT !.left = Max(@ - 1, -1)]
    /\ mine' = [x \in Slots |-> Max(mine[x] - 1, -1)]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, runnable, late, dpos, exec, lag, skew,
                   seen, acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

\* One store tick that e's monotonic clock misses, at most MaxSkew times
\* per grant: e believes more time remains than the store does.
SlowTick(e) ==
    /\ exec[e] # NoExec
    /\ lag[e] < MaxSkew
    /\ TimelyFor(Slots \ {e})
    /\ lease.left >= 0
    /\ lease' = [lease EXCEPT !.left = @ - 1]
    /\ mine' = [x \in Slots |-> IF x = e THEN mine[x] ELSE Max(mine[x] - 1, -1)]
    /\ lag' = [lag EXCEPT ![e] = @ + 1]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, runnable, late, dpos, exec, skew, crashes,
                   seen, acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

\* The lease has a holder whose execution is gone; time must pass for it
\* to expire (fair in the liveness configurations).
TickOrphan == Tick /\ lease.holder # None /\ exec[lease.holder] = NoExec

\* The execution drops its state; it releases the lease if it holds it.
Drop(e) ==
    /\ lease' = IF lease.holder = e THEN Released ELSE lease
    /\ mine' = [mine EXCEPT ![e] = -1]
    /\ lag' = [lag EXCEPT ![e] = 0]
    /\ exec' = [exec EXCEPT ![e] = NoExec]

Crash(e) ==
    /\ exec[e] # NoExec
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ mine' = [mine EXCEPT ![e] = -1]
    /\ lag' = [lag EXCEPT ![e] = 0]
    /\ exec' = [exec EXCEPT ![e] = NoExec]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, lease, runnable, late, dpos, skew, seen, acked,
                   tpos, diverged, overlap, unfenced, rejected, grantee, replayedAsNew,
                   everCancelled, takeover>>

\* The execution sees that its lease has lapsed, or the instance closed.
\* Releasing a held lease of a running instance sets the runnable flag in
\* the same write, so the unfinished run is dispatched again. The broken
\* configurations never check the lease, or release without the flag.
Abandon(e) ==
    /\ exec[e] # NoExec
    /\ exec[e].phase # "suspending"
    /\ status # "running" \/ (Fault # "no_lease_check" /\ mine[e] < Margin)
    /\ Drop(e)
    /\ runnable' = IF Fault # "abandon_without_wake" /\ lease.holder = e /\ status = "running"
                     THEN TRUE ELSE runnable
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, late, dpos, skew, crashes, seen,
                   acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

\* The lease check an execution makes before each step. The broken
\* configuration never checks.
CheckOK(e) == Fault = "no_lease_check" \/ mine[e] >= Margin

\* A step's lease check passes or was passed before a stall.
Checked(e) == exec[e].phase = "stalled" \/ (exec[e].phase = "idle" /\ CheckOK(e))

\* The ghost for NoConcurrentOwners at the moment of the check.
CheckGhost(e) == overlap' = (overlap \/ (exec[e].phase = "idle" /\ grantee # e))

-----------------------------------------------------------------------------
(* Workflow steps. Each runs inside a fenced commit (Commit).               *)

\* The execution acts on observation obs at its next position. The ghost
\* compares it with what the first execution at that position acted on.
Observe(e, obs, next, win) ==
    LET p == exec[e].pos + 1
        entry == <<exec[e].pc, obs>>
    IN /\ exec' = [exec EXCEPT ![e] = [pc |-> next, pos |-> p, winner |-> win, phase |-> "idle"]]
       /\ seen' = IF seen[p] = <<>> THEN [seen EXCEPT ![p] = entry] ELSE seen
       /\ diverged' = (diverged \/ (seen[p] # <<>> /\ seen[p] # entry))

Yields == {"select", "recv", "wake", "await", "cleanWait"}

\* A recorded command replays inside a commit (after the replay at Acquire
\* stopped). A yield that completes now delivers B.
StepReplay(e) ==
    LET r == Recorded(exec[e])
    IN /\ r # <<>>
       /\ r # <<"MISMATCH">>
       /\ Observe(e, r[1], r[2], r[3])
       /\ IF exec[e].pc \in Yields THEN Deliver(exec[e].pos + 1) ELSE UNCHANGED dpos
       /\ UNCHANGED <<hist, acked, cur, tpos, task, armed, child, childCancelReq,
                      replayedAsNew>>

\* Live steps run only when the command's observation is not recorded.
Live(e, pc) == exec[e].pc = pc /\ Recorded(exec[e]) = <<>>

\* Receivers exist from the start: each cursor starts before the first
\* signal. The broken configuration is Romancy's cursor at MAX(id).
StepSub(e) ==
    /\ Live(e, "sub")
    /\ Record("sub", "sub", "ok")
    /\ cur' = IF Fault = "cursor_from_max" THEN [c \in Chans |-> Len(inbox)] ELSE cur
    /\ Observe(e, "ok", "startA", None)
    /\ UNCHANGED <<tpos, task, armed, child, childCancelReq, replayedAsNew, dpos>>

\* An async activity reserves its key in issue order and queues its task.
\* The broken configurations reserve nothing (keys come at completion), or
\* queue the task again on replay.
StepStart(e, pc, a, k, next) ==
    /\ Live(e, pc)
    /\ IF hist[k].cmd = None /\ Fault # "completion_order"
          THEN Reserve(k, a) ELSE UNCHANGED <<hist, acked>>
    /\ task' = [task EXCEPT ![a] = "queued"]
    /\ replayedAsNew' = (replayedAsNew \/ ResultOf(a) # None)
    /\ Observe(e, "ok", next, None)
    /\ UNCHANGED <<cur, tpos, armed, child, childCancelReq, dpos>>

StepTimer(e) ==
    /\ Live(e, "timer")
    /\ Reserve("T", "T")
    /\ armed' = armed \cup {"T"}
    /\ Observe(e, "ok", "child", None)
    /\ UNCHANGED <<cur, tpos, task, child, childCancelReq, replayedAsNew, dpos>>

\* Recording C and creating the child are one transaction.
StepChild(e) ==
    /\ Live(e, "child")
    /\ Record("C", "C", "started")
    /\ child' = "running"
    /\ Observe(e, "ok", "select", None)
    /\ UNCHANGED <<cur, tpos, task, armed, childCancelReq, replayedAsNew, dpos>>

SelReady ==
    {x \in {"A", "B"} : ResultOf(x) # None}
    \cup (IF hist["T"].val # None THEN {"T"} ELSE {})
    \cup (IF NextSig("c1") # 0 THEN {"S"} ELSE {})
    \cup (IF cancelReq THEN {"X"} ELSE {})

FirstReady(R) ==
    IF "A" \in R THEN "A" ELSE IF "B" \in R THEN "B" ELSE IF "T" \in R THEN "T"
    ELSE IF "S" \in R THEN "S" ELSE "X"

\* The decision is recorded with the delivery it makes. The broken
\* configuration records no decision and takes the first ready
\* alternative; its signal consumption is still recorded (selS).
StepSelect(e) ==
    /\ Live(e, "select")
    /\ Deliver(exec[e].pos + 1)
    /\ \/ /\ Fault # "unrecorded_wake_order"
          /\ \E x \in SelReady :
                /\ Record("sel", "sel", x)
                /\ IF x = "S" THEN Consume("c1", NextSig("c1"), exec[e].pos + 1)
                   ELSE UNCHANGED <<cur, tpos>>
                /\ Observe(e, x, AfterSelect(x), x)
       \/ /\ Fault = "unrecorded_wake_order"
          /\ SelReady # {}
          /\ LET x == FirstReady(SelReady)
             IN /\ IF x = "S" /\ hist["selS"].val = None
                      THEN /\ Record("selS", "selS", "taken")
                           /\ Consume("c1", NextSig("c1"), exec[e].pos + 1)
                      ELSE UNCHANGED <<hist, acked, cur, tpos>>
                /\ Observe(e, x, AfterSelect(x), x)
    /\ UNCHANGED <<task, armed, child, childCancelReq, replayedAsNew>>

\* Live only in live_future: Get returns the value of whichever attempt's
\* response reached this worker, not the recorded one.
StepGetW(e) ==
    /\ Live(e, "getW")
    /\ IF exec[e].winner = "A"
          THEN \E i \in doneAtt : Observe(e, Vals[i], "readyB", "A")
          ELSE Observe(e, ResultOf("B"), "now", "B")
    /\ UNCHANGED <<hist, acked, cur, tpos, task, armed, child, childCancelReq,
                   replayedAsNew, dpos>>

\* Live only in live_is_ready: IsReady(B) reads whether B's result exists
\* now, not whether it was delivered by this position.
StepReadyB(e) ==
    /\ Live(e, "readyB")
    /\ IF ResultOf("B") # None THEN Observe(e, "ready", "getB", exec[e].winner)
       ELSE Observe(e, "not", "now", exec[e].winner)
    /\ UNCHANGED <<hist, acked, cur, tpos, task, armed, child, childCancelReq,
                   replayedAsNew, dpos>>

\* IsReady(B) again right after the sleep. When the poller wrote the fire,
\* this read can follow a wake that completed in Acquire's replay without
\* a delivery batch. Live only in live_is_ready.
StepRb2(e) ==
    /\ Live(e, "rb2")
    /\ Observe(e, IF ResultOf("B") # None THEN "ready" ELSE "not", "await", None)
    /\ UNCHANGED <<hist, acked, cur, tpos, task, armed, child, childCancelReq,
                   replayedAsNew, dpos>>

StepGetB(e) ==
    /\ Live(e, "getB")
    /\ Observe(e, ResultOf("B"), "now", exec[e].winner)
    /\ UNCHANGED <<hist, acked, cur, tpos, task, armed, child, childCancelReq,
                   replayedAsNew, dpos>>

\* Now() is recorded the first time. The broken configuration reads the
\* clock on every execution.
StepNow(e) ==
    /\ Live(e, "now")
    /\ IF Fault # "unrecorded_now" THEN Record("now", "now", LateObs)
       ELSE UNCHANGED <<hist, acked>>
    /\ Observe(e, LateObs, AfterNow(exec[e].winner, LateObs), None)
    /\ UNCHANGED <<cur, tpos, task, armed, child, childCancelReq, replayedAsNew, dpos>>

\* An activity with explicit ID act1 while act1 holds A: the store rejects
\* the reservation and the run fails (StepCloseFail). The broken
\* configuration drops the conflicting write silently and acknowledges it.
StepDupA(e) ==
    /\ Live(e, "dupA")
    /\ Fault = "silent_duplicate_key"
    /\ acked' = acked \cup {<<"act1", "A2", None>>}
    /\ Observe(e, "ok", "recv", None)
    /\ UNCHANGED <<hist, cur, tpos, task, armed, child, childCancelReq, replayedAsNew, dpos>>

StepRecv(e) ==
    /\ Live(e, "recv")
    /\ Deliver(exec[e].pos + 1)
    /\ \/ /\ NextSig("c1") # 0
          /\ Record("recv", "recv", "sig")
          /\ Consume("c1", NextSig("c1"), exec[e].pos + 1)
          /\ Observe(e, "sig", "pend", None)
       \/ /\ cancelReq
          /\ Fault # "cancel_ignored_while_receiving"
          /\ Record("recv", "recv", "X")
          /\ Observe(e, "X", "ccancel", None)
          /\ UNCHANGED <<cur, tpos>>
    /\ UNCHANGED <<task, armed, child, childCancelReq, replayedAsNew>>

\* Live only in live_pending: Pending() counts the signals in the inbox now,
\* delivered or not.
StepPend(e) ==
    /\ Live(e, "pend")
    /\ Observe(e, PendObs(Cardinality({i \in 1..Len(inbox) : inbox[i] = "c2" /\ tpos[i] = 0})),
               "async", None)
    /\ UNCHANGED <<hist, acked, cur, tpos, task, armed, child, childCancelReq,
                   replayedAsNew, dpos>>

\* ReceiveAsync takes the next delivered signal, as Pending() counts them,
\* and records what it found, "nosig" included. The broken configuration
\* reads the inbox now, so it can take a signal that Pending() did not see.
StepAsync(e) ==
    /\ Live(e, "async")
    /\ LET p == exec[e].pos + 1
           i == IF Fault = "live_receive_async" THEN NextSig("c2") ELSE NextDeliv("c2", p)
       IN IF i # 0
             THEN /\ Record("async", "async", "sig")
                  /\ Consume("c2", i, p)
                  /\ Observe(e, "sig", "sleep", None)
             ELSE /\ Record("async", "async", "nosig")
                  /\ Observe(e, "nosig", "sleep", None)
                  /\ UNCHANGED <<cur, tpos>>
    /\ UNCHANGED <<task, armed, child, childCancelReq, replayedAsNew, dpos>>

StepSleep(e) ==
    /\ Live(e, "sleep")
    /\ Reserve("sleep", "sleep")
    /\ armed' = armed \cup {"sleep"}
    /\ Observe(e, "ok", "wake", None)
    /\ UNCHANGED <<cur, tpos, task, child, childCancelReq, replayedAsNew, dpos>>

\* The sleep ends with the fire (StepReplay) or the cancel, whichever is
\* written to key sleep first.
StepWake(e) ==
    /\ Live(e, "wake")
    /\ cancelReq
    /\ Fault # "cancel_ignored_while_sleeping"
    /\ SetVal("sleep", "X")
    /\ Deliver(exec[e].pos + 1)
    /\ Observe(e, "X", "ccancel", None)
    /\ UNCHANGED <<cur, tpos, task, armed, child, childCancelReq, replayedAsNew>>

StepAwait(e) ==
    /\ Live(e, "await")
    /\ Deliver(exec[e].pos + 1)
    /\ \/ /\ child \in {"completed", "cancelled"}
          /\ Record("cdone", "cdone", child)
          /\ Observe(e, child, "complete", None)
       \/ /\ cancelReq
          /\ Fault # "cancel_ignored_while_awaiting_child"
          /\ Record("cdone", "cdone", "X")
          /\ Observe(e, "X", "ccancel", None)
    /\ UNCHANGED <<cur, tpos, task, armed, child, childCancelReq, replayedAsNew>>

\* The cancelled scope propagates the cancel to the running child.
StepCancelChild(e) ==
    /\ Live(e, "ccancel")
    /\ Record("ccancel", "ccancel", "ok")
    /\ childCancelReq' = (childCancelReq \/ child = "running")
    /\ Observe(e, "ok", "cgetB", None)
    /\ UNCHANGED <<cur, tpos, task, armed, child, replayedAsNew, dpos>>

\* Get(B) in the cancelled scope: its value if delivered by now, otherwise
\* Canceled, which is recorded and cancels B's task.
StepCancelGetB(e) ==
    /\ Live(e, "cgetB")
    /\ LET obs == IF ReadyB(exec[e].pos + 1) THEN ResultOf("B") ELSE "canceled"
       IN /\ Record("cgetB", "cgetB", obs)
          /\ task' = IF obs = "canceled" THEN [task EXCEPT !["B"] = "none"] ELSE task
          /\ Observe(e, obs, "clean", None)
    /\ UNCHANGED <<cur, tpos, armed, child, childCancelReq, replayedAsNew, dpos>>

\* Cleanup runs on Detached(): the cancel does not reach it.
StepClean(e) ==
    /\ Live(e, "clean")
    /\ Reserve("clean", "clean")
    /\ task' = [task EXCEPT !["clean"] = "queued"]
    /\ Observe(e, "ok", "cleanWait", None)
    /\ UNCHANGED <<cur, tpos, armed, child, childCancelReq, replayedAsNew, dpos>>

\* Live only in cleanup_in_cancelled_scope: the cleanup was scheduled in the
\* cancelled scope, so its Get returns Canceled at once and its task is
\* cancelled.
StepCleanWait(e) ==
    /\ Live(e, "cleanWait")
    /\ Fault = "cleanup_in_cancelled_scope"
    /\ task' = [task EXCEPT !["clean"] = "none"]
    /\ Observe(e, "canceled", "closeX", None)
    /\ UNCHANGED <<hist, acked, cur, tpos, armed, child, childCancelReq, replayedAsNew, dpos>>

StepBody(e) ==
    \/ StepReplay(e)
    \/ StepSub(e)
    \/ StepStart(e, "startA", "A", "act1", "startB")
    \/ StepStart(e, "startB", "B", "act2", "timer")
    \/ StepTimer(e)
    \/ StepChild(e)
    \/ StepSelect(e)
    \/ StepGetW(e)
    \/ StepReadyB(e)
    \/ StepGetB(e)
    \/ StepRb2(e)
    \/ StepNow(e)
    \/ StepDupA(e)
    \/ StepRecv(e)
    \/ StepPend(e)
    \/ StepAsync(e)
    \/ StepSleep(e)
    \/ StepWake(e)
    \/ StepAwait(e)
    \/ StepCancelChild(e)
    \/ StepCancelGetB(e)
    \/ StepClean(e)
    \/ StepCleanWait(e)

\* Closing writes the terminal status, drops queued activity tasks and, by
\* the parent-close policy, records a cancel request for a running child.
\* The broken configuration has no parent-close policy.
Close(e, s, d) ==
    /\ status' = s
    /\ everCancelled' = (everCancelled \/ s = "cancelled")
    /\ diverged' = d
    /\ childCancelReq' = IF child = "running" /\ Fault # "no_parent_close"
                            THEN TRUE ELSE childCancelReq
    /\ task' = [a \in Acts |-> "none"]
    /\ Drop(e)
    /\ UNCHANGED <<hist, cancelReq, inbox, cur, att, tries, doneAtt, armed, child,
                   runnable, late, dpos, skew, crashes, seen, acked, tpos,
                   rejected, grantee, replayedAsNew, takeover>>

CloseBody(e) ==
    \/ exec[e].pc = "complete" /\ Close(e, "completed", diverged)
    \/ exec[e].pc = "closeX" /\ Close(e, "cancelled", diverged)
    \/ exec[e].pc = "nondet" /\ Close(e, "failed", TRUE)
    \/ Recorded(exec[e]) = <<"MISMATCH">> /\ exec[e].pc # "dupA" /\ Close(e, "failed", TRUE)
    \/ /\ exec[e].pc = "dupA"
       /\ Fault # "silent_duplicate_key"
       /\ hist["act1"].cmd \notin {None, "A2"}
       /\ Close(e, "failed", diverged)

StepFrame ==
    UNCHANGED <<status, cancelReq, inbox, att, tries, doneAtt, lease, runnable, late, mine,
                lag, skew, crashes, rejected, grantee, everCancelled, takeover>>

\* A step checks the lease (or was checked before a stall) and commits,
\* fenced by the store. The broken configuration has no fencing condition,
\* so a stalled execution's commit lands after another took the lease.
Commit(e) ==
    /\ exec[e] # NoExec
    /\ status = "running"
    /\ Checked(e)
    /\ CheckGhost(e)
    /\ FenceOK(e) \/ Fault = "unfenced_write"
    /\ unfenced' = (unfenced \/ lease.holder # e)
    /\ \/ StepBody(e) /\ StepFrame
       \/ CloseBody(e)

\* Execution x waits at a yield with nothing to observe.
BlockedAt(x) ==
    LET pc == x.pc
    IN /\ Recorded(x) = <<>>
       /\ \/ pc = "select" /\ SelReady = {}
          \/ /\ pc = "recv"
             /\ NextSig("c1") = 0
             /\ ~(cancelReq /\ Fault # "cancel_ignored_while_receiving")
          \/ pc = "wake" /\ ~(cancelReq /\ Fault # "cancel_ignored_while_sleeping")
          \/ /\ pc = "await"
             /\ child = "running"
             /\ ~(cancelReq /\ Fault # "cancel_ignored_while_awaiting_child")
          \/ pc = "cleanWait" /\ Fault # "cleanup_in_cancelled_scope"

Blocked(e) == BlockedAt(exec[e])

\* A blocked execution decides to suspend, then releases the lease. Input
\* that arrives in between has set the runnable flag.
DecideSuspend(e) ==
    /\ exec[e] # NoExec
    /\ exec[e].phase = "idle"
    /\ status = "running"
    /\ Blocked(e)
    /\ exec' = [exec EXCEPT ![e].phase = "suspending"]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, lease, runnable, late, dpos, mine, lag, skew,
                   crashes, seen, acked, tpos, diverged, overlap, unfenced, rejected,
                   grantee, replayedAsNew, everCancelled, takeover>>

ReleaseSuspend(e) ==
    /\ exec[e].phase = "suspending"
    /\ Drop(e)
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, runnable, late, dpos, skew, crashes, seen,
                   acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

\* The execution passes its lease check and then stalls (a GC pause, a slow
\* network) for any time before the step commits.
Stall(e) ==
    /\ exec[e] # NoExec
    /\ exec[e].phase = "idle"
    /\ status = "running"
    /\ CheckOK(e)
    /\ ~Blocked(e)
    /\ CheckGhost(e)
    /\ exec' = [exec EXCEPT ![e].phase = "stalled"]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, lease, runnable, late, dpos, mine, lag, skew,
                   crashes, seen, acked, tpos, diverged, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

\* The store rejects a commit from an execution that no longer holds the
\* lease, and the execution stops.
FenceReject(e) ==
    /\ exec[e] # NoExec
    /\ status = "running"
    /\ Checked(e)
    /\ ~Blocked(e)
    /\ ~FenceOK(e)
    /\ Fault # "unfenced_write"
    /\ CheckGhost(e)
    /\ rejected' = TRUE
    /\ mine' = [mine EXCEPT ![e] = -1]
    /\ lag' = [lag EXCEPT ![e] = 0]
    /\ exec' = [exec EXCEPT ![e] = NoExec]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, lease, runnable, late, dpos, skew, crashes,
                   seen, acked, tpos, diverged, unfenced, grantee,
                   replayedAsNew, everCancelled, takeover>>

-----------------------------------------------------------------------------
(* Activities, timers, the child and the environment.                       *)

\* An attempt starts. For A a second attempt may start while the first
\* still runs (a timeout retry), and each attempt returns its own value.
ActStart(a) ==
    /\ task[a] = "queued"
    /\ IF a = "A" THEN tries < MaxTries /\ Cardinality(att[a]) < (IF DupAttempts THEN 2 ELSE 1)
       ELSE att[a] = {}
    /\ tries' = IF a = "A" THEN tries + 1 ELSE tries
    /\ att' = [att EXCEPT ![a] = @ \cup {IF a = "A" THEN tries + 1 ELSE 1}]
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, doneAtt, armed, child,
                   childCancelReq, lease, runnable, late, dpos, exec, mine, lag, skew,
                   crashes, seen, acked, tpos, diverged, overlap, unfenced, rejected,
                   grantee, replayedAsNew, everCancelled, takeover>>

\* The first result is recorded; a later one finds the value written and is
\* discarded. The broken configuration lets the last write win.
ActFinish(a, i) ==
    LET k == ActKey(a)
        v == Vals[i]
    IN /\ i \in att[a]
       /\ att' = [att EXCEPT ![a] = @ \ {i}]
       /\ task' = [task EXCEPT ![a] = "none"]
       /\ doneAtt' = IF Fault = "live_future" /\ a = "A" THEN doneAtt \cup {i} ELSE doneAtt
       /\ IF hist[k].val = None \/ Fault = "last_write_wins"
             THEN /\ hist' = [hist EXCEPT ![k] = [cmd |-> a, val |-> v]]
                  /\ acked' = acked \cup {<<k, a, v>>}
                  /\ Wake
             ELSE UNCHANGED <<hist, acked, runnable>>
       /\ UNCHANGED <<status, cancelReq, inbox, cur, tries, armed, child, childCancelReq,
                      lease, late, dpos, exec, mine, lag, skew, crashes, seen, tpos,
                      diverged, overlap, unfenced, rejected, grantee, replayedAsNew,
                      everCancelled, takeover>>

ActCrash(a, i) ==
    /\ i \in att[a]
    /\ crashes < MaxCrashes
    /\ att' = [att EXCEPT ![a] = @ \ {i}]
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, tries, doneAtt, armed, child,
                   childCancelReq, lease, runnable, late, dpos, exec, mine, lag, skew,
                   seen, acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

\* The timer poller records a fire only for a running instance and never
\* writes the status. The broken configuration is Romancy's: it records the
\* fire and resumes the instance, setting it running unconditionally.
FireTimer(k) ==
    /\ k \in armed
    /\ armed' = armed \ {k}
    /\ IF Fault = "timer_revives_cancelled"
          THEN /\ IF hist[k].val = None THEN SetVal(k, "fired") ELSE UNCHANGED <<hist, acked>>
               /\ status' = "running"
               /\ runnable' = TRUE
          ELSE /\ IF status = "running" /\ hist[k].val = None
                     THEN SetVal(k, "fired") /\ Wake
                     ELSE UNCHANGED <<hist, acked, runnable>>
               /\ UNCHANGED status
    /\ UNCHANGED <<cancelReq, inbox, cur, task, att, tries, doneAtt, child, childCancelReq,
                   lease, late, dpos, exec, mine, lag, skew, crashes, seen, tpos,
                   diverged, overlap, unfenced, rejected, grantee, replayedAsNew,
                   everCancelled, takeover>>

\* The child is itself a Selvedge workflow; only its lifecycle is modelled.
ChildEnd(s) ==
    /\ child = "running"
    /\ child' = s
    /\ Wake
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   childCancelReq, lease, late, dpos, exec, mine, lag, skew, crashes, seen,
                   acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

ChildComplete == ChildEnd("completed")

ChildCancel == childCancelReq /\ ChildEnd("cancelled")

\* Signals to a running instance are buffered durably, before or after the
\* workflow first receives.
SendSignal(ch) ==
    /\ status = "running"
    /\ Cardinality({i \in 1..Len(inbox) : inbox[i] = ch}) < MaxOn(ch)
    /\ inbox' = Append(inbox, ch)
    /\ Wake
    /\ UNCHANGED <<hist, status, cancelReq, cur, task, att, tries, doneAtt, armed, child,
                   childCancelReq, lease, late, dpos, exec, mine, lag, skew, crashes, seen,
                   acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

RequestCancel ==
    /\ Cancels
    /\ status = "running"
    /\ ~cancelReq
    /\ cancelReq' = TRUE
    /\ Wake
    /\ UNCHANGED <<hist, status, inbox, cur, task, att, tries, doneAtt, armed, child,
                   childCancelReq, lease, late, dpos, exec, mine, lag, skew, crashes, seen,
                   acked, tpos, diverged, overlap, unfenced, rejected, grantee,
                   replayedAsNew, everCancelled, takeover>>

PassDeadline ==
    /\ Deadline
    /\ status = "running"
    /\ ~late
    /\ late' = TRUE
    /\ UNCHANGED <<hist, status, cancelReq, inbox, cur, task, att, tries, doneAtt, armed,
                   child, childCancelReq, lease, runnable, dpos, exec, mine, lag, skew,
                   crashes, seen, acked, tpos, diverged, overlap, unfenced, rejected,
                   grantee, replayedAsNew, everCancelled, takeover>>

-----------------------------------------------------------------------------
(* End states, the specification and fairness.                              *)

\* The instance is closed and nothing can still act on it.
Quiet ==
    /\ status \in Terminal
    /\ \A e \in Slots : exec[e] = NoExec
    /\ armed = {}
    /\ \A a \in Acts : task[a] = "none" /\ att[a] = {}
    /\ child # "running"

\* The instance waits for input with no execution and a free lease.
Parked ==
    /\ status = "running"
    /\ \A e \in Slots : exec[e] = NoExec
    /\ lease.holder = None
    /\ ~runnable

Done == (Quiet \/ Parked) /\ UNCHANGED vars

Rest(e) == Commit(e) \/ ReleaseSuspend(e) \/ FenceReject(e) \/ Abandon(e)

Next ==
    \/ \E e \in Slots :
          \/ Acquire(e) \/ Rest(e) \/ Stall(e) \/ Renew(e) \/ DecideSuspend(e)
          \/ Abandon(e) \/ Crash(e) \/ SlowTick(e)
    \/ Tick
    \/ \E a \in Acts :
          \/ ActStart(a)
          \/ \E i \in att[a] : ActFinish(a, i) \/ ActCrash(a, i)
    \/ \E k \in TimerKeys : FireTimer(k)
    \/ ChildComplete \/ ChildCancel
    \/ \E ch \in Chans : SendSignal(ch)
    \/ RequestCancel \/ PassDeadline
    \/ Done

Spec == Init /\ [][Next]_vars

\* Liveness assumes fair executions (WorkFair), and that activities
\* progress, a requested child cancel is delivered, and an orphaned lease
\* expires (EnvFair). With FairEnvironment, timers fire, the child
\* completes and c1 signals are sent. Time, the deadline and the cancel
\* request are never forced.
\*
\* Acquire(e) and Rest(e) are never enabled together. WorkFair(e) is
\* WF_vars(Acquire(e) \/ Rest(e)) written out: Acquire's effect always
\* exists and changes exec, so AcquireGuard(e) is ENABLED Acquire(e).
\* Spelling it out keeps TLC from evaluating the recursive replay inside
\* ENABLED, which is very slow. Every action in EnvProgress can happen
\* only boundedly often and stays enabled until it happens or the run
\* closes, so weak fairness on the disjunction forces each of them.
Work(e) == Acquire(e) \/ Rest(e)

WorkFair(e) ==
    \/ []<>~(AcquireGuard(e) \/ ENABLED <<Rest(e)>>_vars)
    \/ []<><<Work(e)>>_vars

EnvProgress ==
    \/ \E a \in Acts : ActStart(a) \/ \E i \in att[a] : ActFinish(a, i)
    \/ ChildCancel
    \/ TickOrphan
    \/ FairEnvironment /\ (\E k \in TimerKeys : FireTimer(k))
    \/ FairEnvironment /\ ChildComplete
    \/ FairEnvironment /\ SendSignal("c1")

Fairness == (\A e \in Slots : WorkFair(e)) /\ WF_vars(EnvProgress)

LiveSpec == Spec /\ Fairness

\* Execution slots are interchangeable (safety configurations only).
SlotSymmetry == Permutations(Slots)

-----------------------------------------------------------------------------
(* Invariants and properties.                                               *)

\* Every execution derives the same command and observation at each
\* position as the first execution that acted there, and replay never
\* finds another command at a key.
ReplayDeterministic == ~diverged

\* Every write the store acknowledged is what the store holds: no key was
\* recorded twice with different commands or values.
ResultRecordedOnce ==
    \A t \in acked : hist[t[1]].cmd = t[2] /\ (t[3] = None \/ hist[t[1]].val = t[3])

\* No execution begins a step after another execution was granted the
\* lease: two executions never run workflow code at once.
NoConcurrentOwners == ~overlap

\* Every commit lands while its execution holds the lease.
WritesFenced == ~unfenced

\* No step whose result is recorded is issued again.
CompletedStepNotReplayedAsNew == ~replayedAsNew

\* Every sent signal was consumed by the workflow or is still pending.
SignalsNotLost == \A i \in 1..Len(inbox) : tpos[i] # 0 \/ i > cur[inbox[i]]

\* Pending() and the ReceiveAsync right after it read the same delivered
\* view: a zero count is never followed by a received signal.
PendingMatchesReceive ==
    \A p \in 1..(MaxPos - 1) : seen[p] = <<"pend", "zero">> => seen[p + 1] # <<"async", "sig">>

\* An instance waits with no execution only where its replay stops at a
\* blocked yield, so the Parked stutter never hides a stranded run.
ParkedOnlyWhenWaiting ==
    Parked => BlockedAt(FastForward([pc |-> "sub", pos |-> 0, winner |-> None, phase |-> "idle"],
                                    seen, diverged).exec)

CancelledStaysCancelled == everCancelled => status = "cancelled"

\* A closed parent leaves no child running without a recorded cancel
\* request; the request reaches the child asynchronously (ChildStops).
ChildrenFollowParent == status \in Terminal /\ child = "running" => childCancelReq

\* A run that ends cancelled has recorded its detached cleanup.
CleanupRunsAfterCancel == status = "cancelled" => hist["clean"].val # None

\* Every run terminates (with FairEnvironment).
Termination == <>(status \in Terminal)

\* A cancel request ends the run, even when no timer, child or signal helps.
CancelEnds == cancelReq ~> (status \in Terminal)

\* A child with a cancel request stops.
ChildStops == childCancelReq ~> (child # "running")

\* Witnesses: scenarios that must be reachable, stated as invariants that
\* must be violated.
WitnessTakeoverCompletes == ~(takeover /\ status = "completed")

WitnessZombieFenced == ~(takeover /\ rejected)

WitnessActivityRetried ==
    ~(tries = 2 /\ ResultOf("A") # None /\ status = "completed")

WitnessCancelWhileSleeping == ~(status = "cancelled" /\ hist["sleep"].val = "X")

WitnessParentCloseCancelsChild == ~(status \in {"completed", "failed"} /\ child = "cancelled")

WitnessReadyBLater == ~(seen[8] = <<"readyB", "not">> /\ dpos.b > 8)

\* Action constraint for SelvedgeWitnessReadyAfterWake: B's result never
\* arrives after the sleep fired. Then B arrived before the wake completed,
\* so a wake completed in a commit would have delivered it; a "not" at rb2
\* means the wake completed in Acquire's replay, without a batch.
NoBAfterFire == hist["sleep"].val = None \/ hist'["act2"].val = hist["act2"].val

\* IsReady(B) right after the sleep reads "not", B is delivered by a later
\* yield, and the run completes.
WitnessReadyAfterWake ==
    ~(\E p \in 1..MaxPos :
         /\ seen[p] = <<"rb2", "not">>
         /\ dpos.b > p
         /\ ResultOf("B") # None
         /\ status = "completed")

WitnessPendingSeen == ~(\E p \in 1..MaxPos : seen[p] = <<"pend", "some">>)
=============================================================================
