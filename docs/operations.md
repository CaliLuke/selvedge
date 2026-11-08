# Storage, replay checks, and observability

[Documentation](README.md) · [Project overview](../README.md)

This guide covers database setup, compatibility checks for workflow changes, and lifecycle tracing.

## Storage and migrations

SQLite and PostgreSQL are the supported storage backends. PostgreSQL also supports LISTEN/NOTIFY for workflow wakeups.

`WithDatabase` accepts a SQLite path, a `file:` URL, a `sqlite://` URL, or a PostgreSQL connection URL.

`App.Start` applies embedded migrations by default. `WithAutoMigrate(false)` disables this behavior. `WithMigrationsFS` supplies an alternative migration filesystem.

`internal/migrations/sql/` is the schema source of truth. The module embeds the migration files for each dialect, and tests apply those same files. Released migrations are immutable. Add schema changes as new timestamped migrations for both SQLite and PostgreSQL.

Version 0.7.0 is the first Selvedge release and establishes its schema baseline.
Automatic upgrades from Romancy or earlier development schemas are not supported;
use a fresh database for this release. Those schemas can share the initial
migration version while lacking required tables, columns, or constraints, so
`App.Start` cannot upgrade them by reapplying the initial migration.

Error envelopes use snake_case JSON fields, including panic and nondeterminism details. Older development envelope formats have no compatibility shim.

Schema application is transactional: SQLite uses `BEGIN IMMEDIATE`; PostgreSQL takes a transaction-scoped advisory lock before checking `schema_migrations`. SQL errors abort and roll back the schema application.

Migration lock waits use the context passed to `App.Start`. Without a deadline, startup can wait indefinitely unless that context is canceled. This applies to SQLite writer locks and PostgreSQL advisory locks.

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

## Worker routing and diagnostics

Dispatch filters use registered workflow names. The storage `FindDispatchable` variadic names filter treats omitted names or a nil slice as all workflows, and a non-nil empty slice as none.

Workers sharing a database must use distinct workflow names for different implementations. Dispatch does not scope names by `owner_service`; workers that register the same name can execute each other's instances. Use separate databases when services need independent namespaces.

`WorkflowContext.Storage()` exposes read-only history lookup for diagnostics. It does not expose database connections or storage mutations.

## Observability

Workflow hooks and `hooks/otel` provide observability.

`hooks/otel` emits a completed span for each lifecycle callback. Start, retry,
and cache-hit callbacks are markers; completion spans use the reported duration.
Correlate callbacks through instance and command ID attributes. Spans inherit the
callback context and do not remain open across suspension or worker handoff.
