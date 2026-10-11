---
title: "Lifecycle Hooks"
weight: 5
---

Selvedge provides an interface-based hook system that allows you to integrate custom observability and monitoring tools without coupling the framework to specific vendors.

## Overview

The hook system enables you to:

- ✅ Add distributed tracing (OpenTelemetry, Jaeger, etc.)
- ✅ Send metrics to monitoring systems (Datadog, Prometheus)
- ✅ Track errors (Sentry, custom logging)
- ✅ Audit workflow execution
- ✅ Combine multiple observability backends

**Design Philosophy:** Selvedge focuses on workflow orchestration, while observability is delegated to users through a flexible hook system.

## Quick Start

### 1. Implement WorkflowHooks Interface

```go
package main

import (
	"context"
	"log"

	"github.com/CaliLuke/selvedge"
)

type LoggingHooks struct{}

func (h *LoggingHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	log.Printf("workflow.start instance_id=%s workflow_name=%s", instanceID, workflowName)
}

func (h *LoggingHooks) OnActivityComplete(ctx context.Context, instanceID, activityID, activityName string, result any, cacheHit bool) {
	log.Printf("activity.complete instance_id=%s activity_id=%s activity_name=%s cache_hit=%v",
		instanceID, activityID, activityName, cacheHit)
}

// Implement other methods as needed...
```

### 2. Pass Hooks to App

```go
app := selvedge.NewApp(
	selvedge.WithDatabase("workflow.db"),
	selvedge.WithWorkerID("worker-1"),
	selvedge.WithHooks(&LoggingHooks{}), // Your hook implementation
)

ctx := context.Background()
if err := app.Start(ctx); err != nil {
	log.Fatal(err)
}
```

### 3. Run Your Workflows

All lifecycle events are automatically captured:

- ✅ Workflow start, complete, failure, cancellation
- ✅ Activity execution (with cache hit/miss tracking)
- ✅ Event send/receive

## Available Hooks

The `WorkflowHooks` interface defines these methods (all optional when using `HooksBase`):

| Hook Method | Parameters | Description |
|-------------|------------|-------------|
| `OnWorkflowStart` | `instanceID`, `workflowName`, `inputData` | Called when a workflow starts execution |
| `OnWorkflowComplete` | `instanceID`, `workflowName`, `result` | Called when a workflow completes successfully |
| `OnWorkflowFailed` | `instanceID`, `workflowName`, `err` | Called when a workflow fails with an error |
| `OnWorkflowCancelled` | `instanceID`, `workflowName` | Called when a workflow is cancelled |
| `OnActivityStart` | `instanceID`, `activityID`, `activityName`, `isReplaying` | Called before an activity executes |
| `OnActivityComplete` | `instanceID`, `activityID`, `activityName`, `result`, `cacheHit` | Called after an activity completes successfully |
| `OnActivityFailed` | `instanceID`, `activityID`, `activityName`, `err` | Called when an activity fails with an error |
| `OnEventSent` | `eventType`, `eventSource`, `eventData` | Called when an event is sent (transactional outbox) |
| `OnEventReceived` | `instanceID`, `eventType`, `eventData` | Called when a workflow receives an awaited event |

## Best Practices

### 1. Scrub Sensitive Data

Always remove sensitive information from logs:

```go
var sensitiveFields = map[string]bool{
	"password":    true,
	"api_key":     true,
	"token":       true,
	"ssn":         true,
	"credit_card": true,
}

func scrubData(data map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range data {
		if sensitiveFields[strings.ToLower(k)] {
			result[k] = "***REDACTED***"
		} else {
			result[k] = v
		}
	}
	return result
}

type SecureHooks struct {
	selvedge.HooksBase
}

func (h *SecureHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	if data, ok := inputData.(map[string]any); ok {
		safeData := scrubData(data)
		log.Printf("workflow.start input_data=%v", safeData)
	}
}
```

### 2. Handle Hook Errors Gracefully

Don't let hook failures break your workflows:

```go
type RobustHooks struct {
	selvedge.HooksBase
}

func (h *RobustHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	defer func() {
		if r := recover(); r != nil {
			// Log but don't panic (workflow should continue)
			log.Printf("Hook error: %v", r)
		}
	}()

	// Send metrics (might fail)
	sendMetrics(instanceID, workflowName)
}
```

### 3. Use Sampling in Production

For high-throughput systems, sample traces:

```go
import "math/rand"

type SampledHooks struct {
	selvedge.HooksBase
	sampleRate float64
}

func NewSampledHooks(sampleRate float64) *SampledHooks {
	return &SampledHooks{sampleRate: sampleRate}
}

func (h *SampledHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	if rand.Float64() < h.sampleRate {
		log.Printf("workflow.start instance_id=%s", instanceID)
	}
}

func (h *SampledHooks) OnWorkflowFailed(ctx context.Context, instanceID, workflowName string, err error) {
	// Always log errors (100% sampling)
	log.Printf("workflow.failed instance_id=%s error=%v", instanceID, err)
}
```

## Integration Examples

### OpenTelemetry

```go
package main

import (
	"context"

	"github.com/CaliLuke/selvedge"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type OTelHooks struct {
	selvedge.HooksBase
	tracer trace.Tracer
}

func NewOTelHooks() *OTelHooks {
	return &OTelHooks{
		tracer: otel.Tracer("selvedge"),
	}
}

func (h *OTelHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	_, span := h.tracer.Start(ctx, "workflow.start",
		trace.WithAttributes(
			attribute.String("workflow.instance_id", instanceID),
			attribute.String("workflow.name", workflowName),
		),
	)
	defer span.End()
}

func (h *OTelHooks) OnActivityComplete(ctx context.Context, instanceID, activityID, activityName string, result any, cacheHit bool) {
	_, span := h.tracer.Start(ctx, "activity.complete",
		trace.WithAttributes(
			attribute.String("activity.id", activityID),
			attribute.String("activity.name", activityName),
			attribute.Bool("activity.cache_hit", cacheHit),
		),
	)
	defer span.End()
}

// Usage
app := selvedge.NewApp(
	selvedge.WithDatabase("workflow.db"),
	selvedge.WithHooks(NewOTelHooks()),
)
```

**What you get:**

- Distributed tracing across workflows
- Activity execution spans with cache hit/miss
- OpenTelemetry-compatible (works with Jaeger, Grafana, etc.)

### Prometheus

```go
package main

import (
	"context"

	"github.com/CaliLuke/selvedge"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	workflowStarted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "selvedge_workflow_started_total",
			Help: "Total workflows started",
		},
		[]string{"workflow_name"},
	)
	activityExecuted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "selvedge_activity_executed_total",
			Help: "Activities executed",
		},
		[]string{"activity_name", "cache_hit"},
	)
)

type PrometheusHooks struct {
	selvedge.HooksBase
}

func (h *PrometheusHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	workflowStarted.WithLabelValues(workflowName).Inc()
}

func (h *PrometheusHooks) OnActivityComplete(ctx context.Context, instanceID, activityID, activityName string, result any, cacheHit bool) {
	cacheHitStr := "false"
	if cacheHit {
		cacheHitStr = "true"
	}
	activityExecuted.WithLabelValues(activityName, cacheHitStr).Inc()
}
```

### Datadog

```go
package main

import (
	"context"
	"fmt"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/CaliLuke/selvedge"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type DatadogHooks struct {
	selvedge.HooksBase
	statsd *statsd.Client
}

func NewDatadogHooks(statsdAddr string) (*DatadogHooks, error) {
	client, err := statsd.New(statsdAddr)
	if err != nil {
		return nil, err
	}
	return &DatadogHooks{statsd: client}, nil
}

func (h *DatadogHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	h.statsd.Incr("selvedge.workflow.started",
		[]string{fmt.Sprintf("workflow:%s", workflowName)}, 1)

	span := tracer.StartSpan("workflow.start",
		tracer.ServiceName("selvedge"),
		tracer.Tag("workflow.name", workflowName),
		tracer.Tag("instance.id", instanceID),
	)
	defer span.Finish()
}

func (h *DatadogHooks) OnActivityComplete(ctx context.Context, instanceID, activityID, activityName string, result any, cacheHit bool) {
	h.statsd.Incr("selvedge.activity.completed",
		[]string{
			fmt.Sprintf("activity:%s", activityName),
			fmt.Sprintf("cache_hit:%v", cacheHit),
		}, 1)
}
```

### Sentry Error Tracking

```go
package main

import (
	"context"

	"github.com/getsentry/sentry-go"
	"github.com/CaliLuke/selvedge"
)

type SentryHooks struct {
	selvedge.HooksBase
}

func (h *SentryHooks) OnWorkflowFailed(ctx context.Context, instanceID, workflowName string, err error) {
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetContext("workflow", map[string]interface{}{
			"instance_id":   instanceID,
			"workflow_name": workflowName,
		})
		sentry.CaptureException(err)
	})
}

func (h *SentryHooks) OnActivityFailed(ctx context.Context, instanceID, activityID, activityName string, err error) {
	sentry.WithScope(func(scope *sentry.Scope) {
		scope.SetContext("activity", map[string]interface{}{
			"instance_id":   instanceID,
			"activity_id":   activityID,
			"activity_name": activityName,
		})
		sentry.CaptureException(err)
	})
}
```

## Complete Example

```go
package main

import (
	"context"
	"log"

	"github.com/CaliLuke/selvedge"
)

// Custom hooks implementation
type MyHooks struct {
	selvedge.HooksBase
}

func (h *MyHooks) OnWorkflowStart(ctx context.Context, instanceID, workflowName string, inputData any) {
	log.Printf("🚀 Workflow started: %s (%s)", workflowName, instanceID)
}

func (h *MyHooks) OnWorkflowComplete(ctx context.Context, instanceID, workflowName string, result any) {
	log.Printf("✅ Workflow completed: %s (%s)", workflowName, instanceID)
}

func (h *MyHooks) OnWorkflowFailed(ctx context.Context, instanceID, workflowName string, err error) {
	log.Printf("❌ Workflow failed: %s (%s) - %v", workflowName, instanceID, err)
}

func (h *MyHooks) OnActivityStart(ctx context.Context, instanceID, activityID, activityName string, isReplaying bool) {
	if isReplaying {
		log.Printf("⏪ Replaying activity: %s", activityName)
	} else {
		log.Printf("▶️ Executing activity: %s", activityName)
	}
}

func (h *MyHooks) OnActivityComplete(ctx context.Context, instanceID, activityID, activityName string, result any, cacheHit bool) {
	if cacheHit {
		log.Printf("📦 Activity cache hit: %s", activityName)
	} else {
		log.Printf("✅ Activity completed: %s", activityName)
	}
}

// Result type
type GreetingResult struct {
	Message string `json:"message"`
}

// Activity
var greetUser = selvedge.DefineActivity("greet_user",
	func(ctx context.Context, name string) (GreetingResult, error) {
		return GreetingResult{Message: "Hello, " + name}, nil
	},
)

// Workflow
var greetingWorkflow = selvedge.DefineWorkflow("greeting_workflow",
	func(ctx *selvedge.WorkflowContext, name string) (GreetingResult, error) {
		result, err := greetUser.Execute(ctx, name)
		if err != nil {
			return GreetingResult{}, err
		}
		return result, nil
	},
)

func main() {
	app := selvedge.NewApp(
		selvedge.WithDatabase("workflow.db"),
		selvedge.WithWorkerID("worker-1"),
		selvedge.WithHooks(&MyHooks{}),
	)

	ctx := context.Background()

	if err := app.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer app.Shutdown(ctx)

	// Start workflow - hooks will log all events
	instanceID, err := selvedge.StartWorkflow(ctx, app, greetingWorkflow, "World")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Workflow instance: %s", instanceID)
}
```

**Output:**

```
🚀 Workflow started: greeting_workflow (abc-123-def)
▶️ Executing activity: greet_user
✅ Activity completed: greet_user
✅ Workflow completed: greeting_workflow (abc-123-def)
Workflow instance: abc-123-def
```

## See Also

- **[Workflows and Activities](/docs/core-features/workflows-activities)**: Learn about basic workflow concepts
- **[Durable Execution](/docs/core-features/durable-execution/replay)**: Understand replay and recovery
- **[Examples](/docs/examples/simple)**: See workflows in action
