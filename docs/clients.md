# Clients and queries

[Documentation](README.md) · [Project overview](../README.md)

A producer submits and inspects work without running workers. An App can also send signals, request cancellation, wait for results, and query workflow state.

## Producer-only processes

Use `OpenProducer` in API servers or command-line clients that submit and inspect
work without executing it. A `Producer` has no workflow or activity registration
API. It starts no workers, leader election, maintenance, or notification listener.
Dedicated `App` workers use the same database and existing registration APIs.

```go
initCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
producer, err := selvedge.OpenProducer(initCtx,
    selvedge.WithDatabase(databaseURL),
    selvedge.WithAutoMigrate(false), // migrations are managed by this deployment
)
cancel() // initialization is finished; the producer remains usable
if err != nil {
    return err
}
// Arrange producer.Shutdown with a bounded context at process shutdown.

id, err := producer.StartWorkflowByName(ctx, "send_email", input,
    selvedge.WithInstanceID("email-order-123"))
if err != nil {
    return err
}
status, err := producer.Describe(ctx, id)
if err != nil {
    return err
}
// StatusRunning includes admitted work waiting for a worker.
result, err := selvedge.GetProducerWorkflowResult[EmailResult](ctx, producer, id)
```

`Describe` returns the stored workflow name, JSON input in `InputData`, status,
and parent identity. Empty `ParentInstanceID` and `ParentKey` identify a root.
`GetProducerWorkflowResult[T]` is nonblocking and follows `GetWorkflowResult`:
workflow failures are in `result.Error`; lookup and output decode failures are
returned as Go errors. Poll with caller-owned deadlines if completion is needed.

Submission preserves the existing admission classifications:

- `ErrWorkflowStartRejected`: no instance insert was attempted. A producer that
  has begun shutdown also returns `ErrProducerClosed` through `errors.Is`.
- `ErrWorkflowAlreadyStarted`: that ID already exists, including a completed
  workflow or child. Read `Describe` and compare workflow name, parent identity,
  and input before treating it as the intended submission.
- Other storage errors can leave admission uncertain. Retry the same explicit ID
  and reconcile a duplicate; do not generate a new ID for the retry.

`StartWorkflowByName` uses `encoding/json`, including for `json.RawMessage`;
whitespace may be compacted and HTML characters escaped. For byte-preserving
submission, use `producer.StartWorkflowEncoded(ctx, name, json.RawMessage(data),
opts...)`. It rejects invalid JSON before insertion and stores a copy of the
bytes verbatim on SQLite and PostgreSQL, including whitespace, key order, escapes,
and number spelling. Do not mutate the input concurrently with the call.

Automatic migration is enabled by default. `WithAutoMigrate(false)` validates
configuration and connectivity, but **does not check schema compatibility**.
Apply the current embedded migrations externally before using that handle.
`WithMigrationsFS` selects custom migrations when automatic migration is enabled.
All initialization database work uses the initialization context; cancelling it
after a successful open has no effect on later operations.

`Shutdown` rejects new operations, drains admitted operations, then closes storage.
Give each operation its own deadline to bound draining. Concurrent and repeated
shutdown calls join the same cleanup. Cancelling a shutdown waiter or reaching
`WithShutdownTimeout` interrupts only that wait; cleanup continues. Use
`errors.Is(err, selvedge.ErrShutdownInterrupted)` to identify interrupted waiting.
That error also wraps `context.Canceled` or `context.DeadlineExceeded`. A final
cleanup failure may itself wrap a context error, so checking only that cause
does not establish whether cleanup finished.
Call `Shutdown` again to observe completion and any storage-close error. A closed
producer cannot reopen; create another with `OpenProducer`.

Worker `App.Shutdown` uses the same interrupted-wait marker and retains the final
cleanup result for repeated calls. Completed cleanup takes precedence over a
cancelled waiting context. Observe the result before restarting an App, since
`Start` begins a new lifecycle after cleanup completes.

Database, migration, notification publishing and shutdown timeout options apply
to producers. Worker options do not enable execution. `WithListenNotify` controls
PostgreSQL notification publishing so separate workers can wake promptly; producers
do not listen. Workflow lifecycle hooks supplied through `WithHooks`, including
`OnWorkflowStart`, never fire in a producer. Wrap submission and lookup calls with
application metrics or tracing for client observability; no client-specific hook
interface is currently provided.

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
