# Getting started

[Documentation](README.md) · [Project overview](../README.md)

This example runs a workflow with one activity and stores its history in a local SQLite database.

## Install

Use Go 1.27.3 or later.

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

## Engine core

- `App` manages workflow execution, storage, and background tasks.
- `DefineWorkflow` and `RegisterWorkflow` define and register typed workflows.
- `DefineActivity` creates activities with retry and timeout options. `RegisterActivity` makes them available to recovery workers.
- `WorkflowContext` provides execution state and activity identifiers.
- The replay engine reads stored history and returns recorded results.
- `Receive`, `Sleep`, and `SleepUntil` provide signal and timer waits.
- `CancelWorkflow` requests cancellation of an instance. `GetWorkflowResult` reads its status and output.
- Workflow hooks and `hooks/otel` provide observability.

Next: [workflow rules and waits](workflows.md) and [activities](activities.md).
