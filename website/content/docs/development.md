---
title: "Development"
weight: 100
---

This guide explains how to set up and use the Tilt-based development environment for Selvedge.

## Prerequisites

- Go 1.24+
- Docker (OrbStack recommended)
- [Tilt](https://tilt.dev/)

## Quick Start

1. Install Tilt:
   ```bash
   # macOS (Homebrew)
   brew install tilt-dev/tap/tilt

   # Linux/Windows - see https://docs.tilt.dev/install.html
   curl -fsSL https://raw.githubusercontent.com/tilt-dev/tilt/master/scripts/install.sh | bash
   ```

2. Start the development environment:
   ```bash
   tilt up
   ```

3. Open the Tilt UI: http://localhost:10350

## Running Specific Examples

You can run specific examples by passing their names as arguments:

```bash
# Run only the demo example
tilt up demo

# Run demo and booking examples
tilt up demo booking

# Run all examples
tilt up
```

## Available Examples

| Example | Port | Description |
|---------|------|-------------|
| simple | - | Basic workflow without events |
| demo | 8080 | Order processing with events |
| saga | - | Saga pattern with compensation |
| booking | 8083 | Human-in-the-loop approval |
| event_handler | 8082 | Event-driven workflows |
| retry | - | Retry policies demo |
| timer | 8081 | Sleep and timer workflows |
| mcp | - | MCP server integration (stdio) |
| channel | 8084 | Channel-based message passing |

## Services

### PostgreSQL
- **Host**: localhost
- **Port**: 5433
- **Database**: selvedge_test
- **User**: selvedge
- **Password**: selvedge

Connect via psql:
```bash
docker exec -it selvedge-postgres psql -U selvedge -d selvedge_test
```

### Jaeger (Tracing)
- **UI**: http://localhost:16686
- **OTLP gRPC**: localhost:4317
- **OTLP HTTP**: localhost:4318

## Live Reload

Code changes are automatically detected and examples restart:
- Changes to `*.go` files trigger rebuild
- Changes to example directories trigger rebuild
- Infrastructure (postgres, jaeger) remains running

## Environment Variables

When running via Tilt, the following environment variables are set:

| Variable | Value |
|----------|-------|
| `DATABASE_URL` | `postgres://selvedge:selvedge@localhost:5432/selvedge_test?sslmode=disable` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` |
| `OTEL_SERVICE_NAME` | `selvedge-{example_name}` |

## Stopping the Environment

```bash
tilt down
```

To also remove data volumes:
```bash
docker compose down -v
```

## Troubleshooting

### Port Already in Use
Stop any existing processes using the ports (8080-8084, 5433, 16686).

### Database Connection Issues
Ensure PostgreSQL is healthy:
```bash
docker ps
docker logs selvedge-postgres
```

### Traces Not Appearing in Jaeger
1. Ensure Jaeger is running: http://localhost:16686
2. Check that `OTEL_EXPORTER_OTLP_ENDPOINT` is set
3. Wait a few seconds for traces to be batched and sent
