# Activities

[Documentation](README.md) · [Project overview](../README.md)

Activities perform external work, such as API calls or database writes. Workflows use their recorded results during replay.

## Execution and concurrency

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
Earlier development databases require a fresh schema. See [storage and migrations](operations.md#storage-and-migrations).
