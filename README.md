# Selvedge

Selvedge is an embedded durable-execution engine for Go. It runs inside your application and stores workflow state in a database.

Workflows coordinate activities. Activities perform I/O and record their results for replay. Signal and timer waits suspend execution until work can resume.

## Derived from Romancy

Selvedge is derived from Yasushi Itoh's [Romancy](https://github.com/i2y/romancy) v0.6.2, licensed under the MIT License. Selvedge is a separate project that will diverge from Romancy.

Removed from Romancy: LLM and MCP integrations, public channels and groups, Saga compensation, transactional outbox, CloudEvents and HTTP handlers, recur, MySQL, CLI tools, examples, and website.

## Engine core

- `App` manages workflow execution, storage, and background tasks.
- `DefineWorkflow` and `RegisterWorkflow` define and register typed workflows.
- `DefineActivity` creates activities with retry and timeout options.
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

`CancelWorkflow` stores a durable request and wakes the instance. `Sleep` and `Receive` observe cancellation at their next yield. Activities already in progress continue. A recorded result takes precedence during replay. At a new receive, cancellation takes precedence over a pending signal.

An uncaught `WorkflowCancelledError` closes the instance as cancelled. A workflow that catches cancellation and returns successfully completes normally. Cancellation does not immediately change the instance status.

Signals have no idempotency key. Receive timeouts, asynchronous receive, and pending-signal inspection are not available.

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

Activities run without a database transaction around user code. Their results and failures are recorded in history before being returned to the workflow. `GetWorkflowContext(ctx)` provides workflow metadata inside an activity.

Workflow code must return suspension signals from signal and timer waits so that the replay engine can suspend the instance.

## Storage and migrations

SQLite and PostgreSQL are the supported storage backends. PostgreSQL also supports LISTEN/NOTIFY for workflow wakeups.

`WithDatabase` accepts a SQLite path, a `file:` URL, a `sqlite://` URL, or a PostgreSQL connection URL.

`App.Start` applies embedded migrations by default. `WithAutoMigrate(false)` disables this behavior. `WithMigrationsFS` supplies an alternative migration filesystem.

`internal/migrations/sql/` is the schema source of truth. The module embeds one initial schema file for each dialect, and tests apply those same files. Before the first release, edit both schema files in place. Numbered migrations start at S20.

Pre-S7 databases must be recreated: histories without command positions fail on first history access.

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

The development Compose file also includes Jaeger for OpenTelemetry traces.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
