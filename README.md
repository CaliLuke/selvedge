# Selvedge

Selvedge is an embedded durable-execution engine for Go. It runs inside your application and stores workflow state in a database.

Workflows coordinate activities. Activities perform I/O and record their results for replay. Signal and timer waits suspend execution until work can resume.

## Derived from Romancy

Selvedge is derived from Yasushi Itoh's [Romancy](https://github.com/i2y/romancy) v0.6.2, licensed under the MIT License. Selvedge is a separate project that will diverge from Romancy.

Removed from Romancy: LLM and MCP integrations, public channels and groups, Saga compensation, transactional outbox, CloudEvents and HTTP handlers, recur, MySQL, CLI tools, examples, and website.

## Engine core

- `App` manages workflow execution, storage, and background tasks.
- `DefineWorkflow` and `RegisterWorkflow` define and register typed workflows.
- `DefineActivity` creates activities with retry and timeout options. `RegisterActivity` makes them available to recovery workers.
- `WorkflowContext` provides execution state and activity identifiers.
- The replay engine reads stored history and returns recorded results.
- `Receive`, `Sleep`, and `SleepUntil` provide signal and timer waits.
- `CancelWorkflow` requests cancellation of an instance. `GetWorkflowResult` reads its status and output.
- Workflow hooks and `hooks/otel` provide observability.

## Signals and cancellation

`App.Signal(ctx, instanceID, channel, payload)` stores a JSON payload for one workflow instance. The inbox accepts signals immediately after instance creation.

`Receive[T](ctx, channel)` consumes the oldest pending signal from that channel. Each channel has its own sequence. Replay reads the recorded payload from history.

```go
// Outside the workflow:
err := app.Signal(ctx, instanceID, "confirmation", true)

// Inside the workflow:
confirmed, err := selvedge.Receive[bool](workflowCtx, "confirmation")
if err != nil {
    return false, err
}
return confirmed, nil
```

Channel names pass through unchanged and contain 1–255 bytes. Every signal wakes its instance, including during an active execution. Unknown instances return `ErrInstanceNotFound`. Closed instances return `*InstanceClosedError`, which also matches `ErrInstanceClosed` through `errors.Is`.

### Cancellation

`CancelWorkflow` stores a durable request and wakes the instance. Attached scopes
observe this request at blocking waits: Sleep, Receive, Await, Timer.Get,
Future.Get, and ReceiveWithTimeout. A stored outcome takes precedence over
cancellation. At a new Receive, cancellation takes precedence over a pending signal.

`WithCancel(ctx)` returns a child context and a cancel function. `Detached(ctx)`
returns a parentless context that ignores the durable request and ancestor
cancellation. These functions issue no commands and write no history.
`WithContext` retains the same scope.

The first cancel call inserts one `cancel_scope` history row, even for an empty
scope. One fenced transaction cancels unfinished activities and timers in that
scope and its attached descendants. It also cancels their queued or running tasks
and deletes their timer subscriptions. Repeated calls, including after ancestor
cancellation, write nothing and consume no command position. Replay repeats the
same cancel calls without new writes.

A scope cancellation matches `context.Canceled` and is not a
`WorkflowCancelledError`. Commands issued in a locally cancelled scope record
cancellation without creating tasks or timer subscriptions. A nonpositive
ReceiveWithTimeout still records an immediate timeout. Await evaluates its
predicate before cancellation. IsReady, Pending, ReceiveAsync, and Now ignore
cancellation.

Future.Get and Timer.Get use their issuing scope, regardless of the context
passed to Get. Newly cancelled outcomes become ready at the cancel command's
position. A stored outcome remains unchanged, including its delivery position.

Detached cleanup can run after cancellation:

```go
_, cancelErr := selvedge.Receive[string](ctx, "work")
if errors.Is(cancelErr, context.Canceled) {
    if _, err := cleanup.Execute(selvedge.Detached(ctx), input); err != nil {
        return output, err
    }
}
return output, cancelErr
```

`WithCancel(Detached(ctx))` supports its own local cancellation. Detached scopes
retain lease and shutdown cancellation. WorkflowContext.Done and Err report
those Go context signals, never scope cancellation. WorkflowContext.Cancel was
removed. The cancel function must run on the workflow coroutine, outside Await
predicates. Calls during coroutine closure write nothing.

A durable request alone does not stop activity tasks. Future.Get cancels its
unfinished task when it observes the request. Scope cancellation stops its
unfinished tasks immediately. Running attempts lose renewal, receive context
cancellation, and cannot overwrite the recorded cancellation.

An uncaught cancellation error closes the workflow as cancelled only if a durable
request exists. Without that request, a scope cancellation error closes it as
failed. A workflow that catches cancellation and returns successfully completes
normally. Closure cancels every unfinished task, including tasks from detached
scopes.

Signals have no idempotency key.

`ReceiveWithTimeout[T](ctx, channel, timeout)` returns a signal or an error that
matches `context.DeadlineExceeded`. A nonpositive timeout records an immediate
timeout without consuming a pending signal. Positive waits prefer cancellation,
then a pending signal, then the timer fire. A signal or cancellation retires the
timer in the same transaction as its observation. A later fire cannot wake the
instance or change that observation.

`NewTimer(ctx, duration, opts...)` returns a `*Timer` without suspending the
workflow. `Timer.Key()` returns its history key. `Timer.IsReady()` reads the
delivered view, like activity futures. `Timer.Get(ctx)` returns the recorded
expiry time or cancellation and caches that outcome for this execution.
A nonpositive duration expires immediately. `Sleep` and `NewTimer` followed by
`Get` share the same command and support replay with either form. Timer Get
consumes no command position and delivers no batch.

`Await(ctx, cond)` waits until a predicate is true in the delivered view.
`future.IsReady()` reports whether the future has been delivered or a Get has
already returned its outcome in this execution. `Pending(ctx, channel)` counts
that channel's delivered, unconsumed signals. `ReceiveAsync[T](ctx, channel)`
records either the next such signal or that none was available, without waiting
or observing cancellation.

Arrival and delivery are separate. Activity workers, timer fires, and signal
senders record arrivals. A completing Await or Receive, or Sleep's cancellation
path, commits a delivery batch under the workflow lease. Await delivers exactly
the arrivals its predicate evaluated. Other batching waits deliver all arrivals
at commit time. Get never delivers a batch; passing a recorded timer fire also
leaves arrivals undelivered until a later batching wait. Replay reconstructs the
view from durable delivery and consumption positions, so later arrivals cannot
change earlier readiness or pending counts.

Await predicates must be deterministic and side-effect free. They may read
IsReady, Pending, and local values. Workflow commands inside a predicate fail
the run, and a predicate panic fails the run. An immediately true pre-check
returns without issuing a command or writing history. Replay runs that same
pre-check, then uses the recorded observation instead of reevaluating a recorded
Await's candidate batch. A never-true predicate parks the instance and costs one
replay per input. Nil predicates and invalid Pending channels fail the run.

Use IsReady and Pending only on the workflow coroutine, including inside Await
predicates. Off-coroutine calls to these pure reads are unsupported and are not
detected.

## Starting, signalling and cancelling from other processes

A client App can start with the same database configuration as a worker and
register no workflows. Use `StartWorkflowByName(ctx, client, name, input)` to
start by name, `client.Signal(ctx, id, channel, payload)` to send JSON payloads,
and `CancelWorkflow(ctx, client, id, reason)` to request cancellation. These
operations also accept child instance IDs.

`WaitWorkflow[Output](ctx, client, id)` waits for a terminal result. Workflow
failures appear in `result.Error`; cancelling the wait context stops the wait.
`GetWorkflowResult` reads the current result without waiting. Application error
messages and typed cancellation errors survive storage and replay.

Call `App.Start` before using the client APIs. A start rejected before the insert
matches `ErrWorkflowStartRejected`. An unstarted App also matches
`ErrAppNotStarted`. Duplicate IDs, including closed and child IDs, match
`ErrWorkflowAlreadyStarted` and cannot be reused. Storage errors can leave the
start outcome uncertain: retrying an ID whose insert committed returns
`ErrWorkflowAlreadyStarted`.

Unknown IDs return `*WorkflowNotFoundError`, which matches `ErrInstanceNotFound`.
Signals to closed instances and cancellation of completed or failed instances
return `*InstanceClosedError` with the terminal status. Cancelling an already
cancelled instance succeeds. Signal encoding failures match
`ErrInvalidSignalPayload` and do not write a signal.

Apps in one process share notifications for the same database. PostgreSQL also
notifies other processes of runnable and closed instances. SQLite processes use
polling. `WithMaxPollInterval(d)` caps workflow and activity polling delays
(default one minute). `WithWaitPollInterval(d)` sets completion waits' fallback
polling interval (default one second). Nonpositive values leave the current
setting unchanged. SQLite memory databases do not share the local bus.

There are no separate run IDs: the instance ID identifies the run. RunTimeout,
Memo, SearchAttributes, and start RetryPolicy are not supported.

## Install

Use Go 1.24.3 or later.

```sh
go get github.com/CaliLuke/selvedge
```

## Run a workflow

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/CaliLuke/selvedge"
)

func run() error {
    ctx := context.Background()
    app := selvedge.NewApp(selvedge.WithDatabase("file:workflow.db"))

    greet := selvedge.DefineActivity("greet",
        func(ctx context.Context, name string) (string, error) {
            return "Hello, " + name, nil
        },
    )
    workflow := selvedge.DefineWorkflow("greeting",
        func(ctx *selvedge.WorkflowContext, name string) (string, error) {
            return greet.Execute(ctx, name)
        },
    )
    if err := selvedge.RegisterActivity(app, greet); err != nil {
        return err
    }
    selvedge.RegisterWorkflow(app, workflow)

    if err := app.Start(ctx); err != nil {
        return err
    }
    defer func() { _ = app.Shutdown(ctx) }()

    id, err := selvedge.StartWorkflow(ctx, app, workflow, "Ada")
    if err != nil {
        return err
    }
    for {
        result, err := selvedge.GetWorkflowResult[string](ctx, app, id)
        if err != nil {
            return err
        }
        switch result.Status {
        case "completed":
            fmt.Println(result.Output)
            return nil
        case "failed":
            return result.Error
        }
        time.Sleep(20 * time.Millisecond)
    }
}

func main() {
    if err := run(); err != nil {
        panic(err)
    }
}
```

Activities start from durable task rows and run without a database transaction around user code. Each attempt claims its task before it runs. Sync `Execute` tries to claim and run its own task inline. If another worker holds the task, the workflow yields until the stored result is available. Execute reads results and failures from history, including after an inline attempt.

`ExecuteAsync(ctx, input)` returns a `*Future[O]` after reserving the activity. Issue several futures serially to let activity workers run them concurrently, then call `future.Get(ctx)` for each result. `future.Key()` is fixed at issue time. Get consumes no command position and repeated calls return the same value or error. It can run a claimable task inline; otherwise it yields for redispatch. Execute and ExecuteAsync use the same history command, so switching between them preserves replay compatibility. Futures belong to their issuing execution and must be used serially from workflow code. Completing without awaiting a future cancels its unfinished task.

`GetActivityInfo(ctx)` provides `InstanceID`, `WorkflowName`, `ActivityID`, `ActivityName`, and `Attempt` inside an activity. `Attempt` counts task claims, not retry iterations. Activities no longer receive a `WorkflowContext`.

Every App runs activity workers. Register each activity with `RegisterActivity(app, activity)` before `App.Start` so workers can recover pending tasks after a restart. Execute also registers its definition with the issuing App. A different definition with the same name returns `ErrActivityNameConflict`. Reusing the same definition pointer is allowed. Tasks remain queued if no worker registers their activity name.

`WithMaxConcurrentActivities` limits background attempts (default 10). `WithActivityPollInterval` sets the base polling interval (default one second), with adaptive backoff during idle periods. New tasks wake local workers and PostgreSQL listeners. Activity claims use the workflow lease configuration for expiry and renewal. A lost claim cancels the attempt context. Shutdown releases interrupted claims for another worker. Activities must honor context cancellation for prompt shutdown.

## Activity retries, timeouts and heartbeats

Each task records its resolved `ActivityOptions` when it is reserved. Use
`WithActivityDefaults` on the definition and `WithActivityOptions` on an
invocation. Non-zero invocation fields override the defaults field by field.
`WithRetryPolicy` and `WithTimeout` remain shorthands. Existing tasks keep their
recorded options after code changes, including replay and another worker's
retries. Definition-level `NonRetryableErrors` are matched by the worker running
the attempt and are not recorded; per-call `NonRetryableErrors` are rejected.

Claim acquisition commits only after its full result has been read. Interrupted
acquisition rolls back ownership and does not consume an attempt or retry budget.
Once acquisition succeeds, shutdown releases the claim through its activity runner.

A claim runs one attempt. A retryable failure returns the task to the durable
queue with a failure count and a not-before time. Backoff survives a restart and
does not hold a workflow coroutine or worker slot. A lapsed claim counts as a
failure and can be reclaimed immediately, without backoff. Success, non-retryable
failure, or retry exhaustion records the single final history value. Cancellation
and workflow closure stop tasks even during backoff.

`GetActivityInfo(ctx)` includes a copy of the recorded options. Its `Attempt`
counts claims, starting at one. Graceful release consumes an attempt number but
not the retry budget, so the number can exceed `MaxAttempts`. Retry and failure
hooks report that same claim number and fire only after their durable write.
`ListActivityTasks(ctx, app, instanceID)` returns tasks in issue order, including
recorded options, claim and failure counts, and the last retry error.

`WithActivityQueues(queues...)` restricts both background workers and inline Get
attempts to those queues. Omitting it serves every queue; an empty list serves
none. An empty task queue name denotes the default queue. Workflow and child task
queues are not supported.

`StartToCloseTimeout` sets the attempt's context deadline and caps its durable
claim, including every renewal. `HeartbeatTimeout` requires the activity to call
`RecordHeartbeat(ctx)` to retain its claim. The call marks progress in memory
without database I/O; a renewal tick extends the claim only after new progress.
With no heartbeat timeout, claims renew automatically. Heartbeat calls return the
context cause after cancellation or claim loss, and `ErrNotActivityContext`
outside an attempt.

`ScheduleToStartTimeout` limits each wait for a worker, measured from reservation,
the retry's due time, graceful release, or claim lapse. Deadlines expire strictly
after the stored deadline. Schedule timeout is final; start-to-close and heartbeat
lapses consume retry budget. The leader records exhausted lapsed claims and
schedule timeouts. `ActivityTimeoutError` identifies the timeout kind and matches
`context.DeadlineExceeded`, including when nested in `RetryExhaustedError`.

Activities should honor their context. An activity that ignores cancellation can
continue running after its claim expires while another attempt starts. Its late
result cannot replace the durable outcome. Inline attempts use the same deadline
and heartbeat rules; losing a claim yields the workflow and releases its worker.
Pre-S15 databases must be recreated because the activity task schema changed.

Workflow code runs on one coroutine per execution. A pending `Sleep` or `Receive` yields to the dispatcher without returning. The dispatcher closes that coroutine, runs its defers, and releases the lease. A later dispatch replays from the beginning.

Workflow defers must not block: the dispatcher waits for them without a time limit. During coroutine closure, the runtime rejects commands from defers without hooks or writes. Workflow panics and unexpected `runtime.Goexit` calls fail the instance with `*WorkflowPanicError`, including the value and stack. An activity function panic also fails the instance with `*WorkflowPanicError` and is not retried.

Workflow code must issue workflow commands serially. Commands inside activity functions and overlapping commands from user goroutines fail the run with nondeterminism. The runtime also rejects commands while the workflow coroutine is not running. If a command reaches a blocking yield on a foreign goroutine, Yield returns a sticky nondeterminism error without a dispatcher handoff.

The runtime cannot detect non-overlapping foreign commands that finish without a blocking yield while the workflow coroutine runs pure user code. Such use remains unsupported. Activities can use `GetActivityInfo` to read metadata.

S7 removes the public `SuspendSignal` family and the unused `OnEvent*` hooks.

## Child workflows

`StartChild[O](ctx, workflowName, input, opts...)` starts a child by name and
returns a `*Child[O]`. Only the worker that runs the child needs to register
its workflow. The parent command and child instance commit in one transaction.

`child.ID()` returns the instance ID. The default is `<parentID>/<historyKey>`.
`WithChildID(id)` supplies an explicit ID, and `child.Key()` returns the history key.
A duplicate ID returns `ErrWorkflowAlreadyStarted`. The recorded error also
returns on replay. The parent can catch this error and continue.

`child.Get(ctx)` returns the stored result or yields until an outcome exists.
It caches its outcome for the execution and consumes no command position or
delivery batch. Child failures preserve their error envelopes. The parent can
catch child panic and nondeterminism errors. Invalid result data fails the parent.

`child.IsReady()` reads the delivered view, like activity futures. It becomes
true after a delivery batch or after Get returns. It is safe inside Await predicates.
The child methods belong to their issuing execution and require serial use on
the workflow coroutine.

`child.Cancel(ctx)` records one command and requests cancellation of a running
child. It resolves an unfinished handle immediately, without waiting for the
child to close. Further calls on that handle consume no command position.
A stored result takes precedence over cancellation.

When the parent completes, fails, or cancels, its close transaction requests
cancellation of every running child. Each child observes the request at its next
blocking yield. Closing that child applies the same policy to its own children.
A parent cancellation observed by a blocked Get records the outcome and requests
child cancellation together.

Child RunTimeout, TaskQueue, and RetryPolicy are not supported. Scope integration
and detached child handles belong to S12b.

## Workflow time

Use `Now(ctx)` for time-based decisions in workflow code. It returns a
`(time.Time, error)`. Each live call writes one fenced history observation;
values are not cached or batched. Replay returns the recorded value, even if
the deadline has passed since the previous execution.

Values use UTC and contain no monotonic clock reading. Live and replay values
compare identically with both `==` and `Equal`. Each execution tracks the largest
time returned so far, including replayed values and timer expiries observed by
`Sleep` or `Timer.Get`. Contexts made with
`WithContext` share this floor, so a backward clock step cannot lower a later
live value. On error, `Now` returns the floor and a sticky error that fails the
execution even if workflow code ignores it.

`WithClock(func() time.Time)` configures the wall clock used by `Now` and timer
expiry; the default (also for a nil clock) is `time.Now`. `Sleep` and `NewTimer`
set expiry to the larger of that clock and the workflow time floor, plus the
duration. Nonpositive durations still expire immediately. `SleepUntil` computes
its duration against that same base. Replay keeps the stored expiry.
Observing a fire advances the floor, so a later live `Now` cannot precede it.
Issuing a timer or checking `IsReady` does not advance the floor.
Lease clocks and the timer poller's store clock are unchanged.

Like other workflow commands, `Now` must be called serially from workflow
code. Calls while the coroutine is closing return an error and the floor,
without writing history.

## Queries and status

`SetQueryHandler(ctx, name, handler)` registers a zero-argument handler for the
current execution. A registration replaces the previous handler with that name.
A nil handler removes it. Names contain 1–255 bytes. Registration consumes no
command position and writes no history. Live executions store handlers without
calling them. Register handlers on the workflow coroutine, outside Await predicates.

```go
// Inside the workflow:
err := selvedge.SetQueryHandler(ctx, "pending", func() (int, error) {
    return selvedge.Pending(ctx, "results"), nil
})

// Outside the workflow:
count, err := selvedge.Query[int](ctx, app, instanceID, "pending")
```

Each Query takes one consistent database snapshot and replays the workflow from
its beginning. It stops before the first unrecorded command, blocking wait, or
other storage action that requires live state. The selected handler runs before
that replay closes. Each query therefore costs one full replay. Answers are not
recorded. A later query can reach a later position and return a different answer.

In-memory SQLite databases (`":memory:"` with shared cache) do not use WAL.
A snapshot on these databases blocks writers for the duration of its read transaction.

Handlers can read local values, Pending, and activity, timer, or child IsReady.
These reads use the delivered view at the replay position. Arrived events stay
invisible until a recorded delivery makes them visible. A recorded Get can return
without delivering other events.

Queries take no lease, fire no hooks, and write no workflow state. Commands and
guarded calls in handlers return ErrQueryCommand and fail only the query.
Handler errors and panics return QueryFailedError, which preserves wrapped errors.
Results always pass through JSON encoding and decoding into the requested type.
Encoding and decoding errors also return QueryFailedError.

Queries work on running and closed instances. The querying App must register the
workflow code. Otherwise, Query returns WorkflowNotRegisteredError. A handler
absent at the replay position returns QueryNotFoundError. An unknown instance
returns WorkflowNotFoundError. Replay mismatches return the original replay error.
Caller cancellation is checked at command boundaries. Workflow code and handlers
must not perform external I/O or block outside workflow commands.

`Describe(ctx, app, instanceID)` returns WorkflowDescription with the workflow
name, status, cancellation request and reason, parent links, and timestamps.
WorkflowStatus has four values: StatusRunning, StatusCompleted, StatusFailed, and
StatusCancelled. A newly created instance reports StatusRunning before execution.
Describe returns WorkflowNotFoundError for an unknown instance.

## Replay checks

Use `ExportHistory(ctx, app, instanceID)` to capture one consistent snapshot of an
instance. The App must have initialized storage. Marshal the returned
`RecordedHistory` with `json.Marshal`, and load it with `ParseHistory`. The
`selvedge.history/v1` format contains command history, workflow metadata, and
signal delivery positions. It excludes ownership, leases, and signal payloads.
Missing event values remain distinct from recorded JSON `null` results.
`ParseHistory` rejects unknown fields and unsupported formats or versions.

Register the workflow on an App, then call `ReplayHistory(ctx, app, history)` to
check its code against the snapshot. This App need not be started. Replay takes
no lease, writes no state, fires no hooks, and neither registers nor executes
activities. Workflow code must still avoid external I/O and blocking outside
workflow commands. Query and replay checks use the same read-only execution path.

A running history can stop at its recorded frontier. A closed history must reach
the recorded close: completed outputs must contain the same JSON value, failed
outcomes must preserve the error kind and message, and cancelled outcomes must
preserve cancellation. JSON object order and equivalent number spellings do not
change an output. Command changes and leftover commands produce
`NondeterminismError`; changed outcomes produce `ReplayOutcomeError`.
`ReplayError` wraps the divergence and the workflow's original Go error, so
`errors.Is` and `errors.As` can inspect either cause.

`ReplayInstance` combines export and verification. For example, check one page of
running instances using registered workflow code:

```go
func checkRunning(ctx context.Context, app *selvedge.App) error {
    instances, err := app.FindInstancesWithOptions(ctx, selvedge.FindInstancesOptions{
        StatusFilter: selvedge.StatusRunning,
        Limit: 100,
    })
    if err != nil {
        return err
    }
    for _, instance := range instances {
        if err := selvedge.ReplayInstance(ctx, app, instance.InstanceID); err != nil {
            return err
        }
    }
    return nil
}
```

Check exported histories before deployment, and drain running instances before
changing their workflow code. A successful check covers only the recorded
history; it does not predict commands on paths the instance has not reached.

## Storage and migrations

SQLite and PostgreSQL are the supported storage backends. PostgreSQL also supports LISTEN/NOTIFY for workflow wakeups.

`WithDatabase` accepts a SQLite path, a `file:` URL, a `sqlite://` URL, or a PostgreSQL connection URL.

`App.Start` applies embedded migrations by default. `WithAutoMigrate(false)` disables this behavior. `WithMigrationsFS` supplies an alternative migration filesystem.

`internal/migrations/sql/` is the schema source of truth. The module embeds one initial schema file for each dialect, and tests apply those same files. Before the first release, edit both schema files in place. Numbered migrations start at S20.

Error envelopes use snake_case JSON fields, including panic and nondeterminism details. Pre-1.0 databases must be recreated after envelope-format changes; no compatibility shim is provided.

Pre-S12 databases must be recreated: child instances now have parent links and constraints.

Pre-S9 databases must be recreated: delivery and consumption positions were added
to history and signals. The initial schema also includes the S8 durable activity
tasks and command positions. Histories without command positions fail on first
history access.

Schema application is transactional: SQLite uses `BEGIN IMMEDIATE`; PostgreSQL takes a transaction-scoped advisory lock before checking `schema_migrations`. SQL errors abort and roll back the schema application.

Migration lock waits use the context passed to `App.Start`. Without a deadline, startup can wait indefinitely unless that context is canceled. This applies to SQLite writer locks and PostgreSQL advisory locks.

## Development

```sh
go build ./...
go vet ./...
go test ./...
make lint
```

For PostgreSQL storage tests, start the development database and run:

```sh
docker compose up -d postgres
SELVEDGE_TEST_POSTGRES_URL='postgres://selvedge:selvedge@localhost:5433/selvedge_test?sslmode=disable' \
    go test -tags=postgres,pglocal ./internal/storage
```

Tests with the `integration` build tag use PostgreSQL testcontainers and require Docker.

Dispatch filters use registered workflow names. The storage `FindDispatchable` variadic names filter treats omitted names or a nil slice as all workflows, and a non-nil empty slice as none.

Workers sharing a database must use distinct workflow names for different implementations. Dispatch does not scope names by `owner_service`; workers that register the same name can execute each other's instances. Use separate databases when services need independent namespaces.

`WorkflowContext.Storage()` exposes read-only history lookup for diagnostics. It does not expose database connections or storage mutations.

The development Compose file also includes Jaeger for OpenTelemetry traces.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
