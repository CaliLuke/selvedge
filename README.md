# Selvedge

Selvedge is a Go library for background work that needs to survive crashes and
restarts. It runs inside your application and saves progress in SQLite or
PostgreSQL, so a worker can resume unfinished work after a restart.

Use it for jobs with several steps, such as processing uploads or calling
external APIs with retries. Workflows can also wait for user approval. Workflows can wait for signals
or timers, and you can run workers separately from the service that submits jobs.

You write the steps as a **workflow** and put external work, such as API calls,
in **activities**. On recovery, Selvedge replays the workflow using recorded
activity results. An interrupted activity can run again, so activities must be
safe to retry.

## Install

Use Go 1.27.3 or later.

```sh
go get github.com/CaliLuke/selvedge
```

## A workflow in Go

```go
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
```

An `App` connects to the database and runs registered workflows and activities.
The [getting started guide](docs/getting-started.md) shows the complete program,
including registration, startup, and reading the result.

## Documentation

The [manual](docs/README.md) covers:

- [Workflows](docs/workflows.md): signals, timers, cancellation, and child workflows.
- [Activities](docs/activities.md): concurrent work, retries, timeouts, and heartbeats.
- [Clients and queries](docs/clients.md): submit jobs and inspect their progress.
- [Operations](docs/operations.md): databases, migrations, replay checks, and tracing.
- [Development](docs/development.md): local setup and contributor checks.

## License and attribution

[MIT](LICENSE). Derived from Yasushi Itoh’s [Romancy](https://github.com/i2y/romancy) v0.6.2.
