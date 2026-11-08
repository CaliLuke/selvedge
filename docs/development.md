# Development

[Documentation](README.md) · [Project overview](../README.md)

The repository uses the same quality gates locally and in CI.

The full race-enabled gate requires a supported OS/architecture, cgo enabled,
and a C compiler where required by Go (for example GCC or Clang on Linux).
See [Go's race detector requirements](https://go.dev/doc/articles/race_detector#Requirements).
It also requires Docker or a dedicated PostgreSQL test URL, as described below.

```sh
make install-tools
uv tool install prek==0.5.10
make install-hooks
make check-fast # read-only build, vet/lint, formatting, hygiene and maintainability
make check      # full gate: adds fresh race tests and coverage floors
make fmt        # explicit formatting/import fixes
```

CI and the pre-push hook run `make check`; pre-commit runs `make check-fast`.
The full gate starts and removes an isolated PostgreSQL 17 container unless
`SELVEDGE_TEST_POSTGRES_URL` points to a dedicated test server. It resets the
server's test database, so never use an application database. Go 1.27.3, Python
3.9+, and Docker (or that test URL) are prerequisites. Prek 0.5.10 is the tested
hook version; the config rejects older versions.

See [quality-gate policy](quality-gates.md) for enforced checks, explicit
coverage debt, package floors, and the Halstead-based maintainability gate.

For PostgreSQL storage tests, start the development database and run:

```sh
docker compose up -d postgres
SELVEDGE_TEST_POSTGRES_URL='postgres://selvedge:selvedge@localhost:5433/selvedge_test?sslmode=disable' \
    go test -tags=postgres,pglocal ./internal/storage
```

Tests with the `integration` build tag use PostgreSQL testcontainers by default
and require Docker. Set `SELVEDGE_TEST_POSTGRES_URL` to reuse a test server instead;
each fixture creates and removes its own database. That test account must have
permission to create databases.

```sh
go test -race -tags=integration -count=1 -timeout=25m ./...
```

CI runs the race detector, both PostgreSQL storage suites, and the integration
suite against a PostgreSQL service with separate databases for each fixture.

The development Compose file also includes Jaeger for OpenTelemetry traces.
