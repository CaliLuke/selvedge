# Quality gates

`make check-fast` and `make check` are the canonical, non-mutating commands.
`make fmt` and `make tidy` are explicit repair operations. Checks never refresh a
baseline or rewrite source to hide a failure.

The pre-commit hook runs the fast command. The pre-push hook and CI run the full
command. Install hooks with `make install-hooks` after installing Prek 0.5.10.
For manual hook verification, use `prek run --all-files --hook-stage pre-commit`
or `prek run --all-files --hook-stage pre-push`; the latter is the full gate.

## Enforcement inventory

| Area | Blocking behavior |
| --- | --- |
| Build and vet | Build all packages with `integration,postgres,pglocal`; the normal golangci-lint run includes govet and type checking. |
| Formatting | golangci-lint checks gofmt and goimports without applying edits. |
| Lint and dead code | Existing standard and repository linters remain enabled, including `unused`. Exported library APIs remain reachable by external consumers; executable-root deadcode analysis must not delete them. |
| Module hygiene | `go mod tidy -diff` must succeed without dependency drift. |
| Duplication | A separate production-code golangci-lint pass rejects `dupl` findings at 150 syntax-tree tokens. Test wrappers are outside this duplication policy, but remain in correctness lint and tests. |
| Function maintainability | The same production pass rejects maintainability index below 20. This score combines Halstead volume, cyclomatic complexity and lines of code. No existing function violations are exempted. |
| Tests | Fresh race-enabled default tests, PostgreSQL storage tests (`postgres,pglocal`), and integration tests (`integration`), retaining their separate established scopes. |
| Coverage | Fresh atomic profiles from all three test runs, per-package floors, and targeted floors for new producer, SQLite initialization and gate code. |
| Gate integrity | Unit and rejection fixtures prove failures for low maintainability, duplication, missing source inventory, malformed profiles and incomplete test results. |

`golangci-lint` remains pinned at v2.15.0 in `scripts/quality/tools.env`.
The maintainability/duplication pass analyzes production Go files. Ordinary lint
still includes tests and the repository's existing narrow test-linter exclusions.
The coverage inventory uses the same build tags, parses all owned non-generated Go files,
and fails on package-list or syntax errors. Only the standard `Code generated ...
DO NOT EDIT.` header marks generated code; ordinary implementation and tool code
remain in scope. Current owned packages have no generated-source exceptions.

The full command uses `-coverpkg=./...` so tests in one package also measure shared
helpers. It requires completed results for every expected package and all three
profiles. It compares every positive-statement coverage block against fresh
instrumentation from the active Go toolchain, rejecting truncated profiles even
when they remain syntactically valid. Source hashes must match the inventory
captured before tests. New packages need an explicit entry in
`scripts/quality/policy.json`; they cannot disappear from the denominator.
`hooks` has no executable statements and is explicitly recorded as such.

Logs and fresh profiles are retained under `.cache/quality/run.*`. A supplied
PostgreSQL URL must identify a dedicated test database with create-database
permission. The storage tests reset it. Without a URL, the full command owns a
temporary PostgreSQL 17 container and removes it on success or failure. Tests and
analysis run serially to bound resource use.

## Coverage policy and accepted debt

The default package floor is 80%. The producer and gate source files also have
80% individual floors; SQLite busy-timeout restoration and the shared context-error classifier require 100%.
The initial complete test run measured these existing gaps:

| Package | Measured | Floor | Follow-up |
| --- | --- | --- | --- |
| `internal/notify` | 77.69% | 75% | Exercise connection loss/recovery, malformed payloads and handler panic recovery. The small margin accommodates concurrent listener paths. |
| `internal/storage/faultstore` | 72.27% | 72% | Cover unexercised delegation and injected-error boundaries through consumer behavior tests. |
| `internal/testkit` | 69.23% | 69% | Validate the alternate testcontainers startup path and infrastructure failures; canonical checks supply a dedicated PostgreSQL server. |

These are explicit accepted coverage debts, not excluded packages. Their floors
remain blocking and should rise as coverage improves. Any change requires review
of the measurements and reason in `scripts/quality/policy.json`.

## Maintainability policy

The pinned `maintidx` implementation uses Halstead volume, branching complexity
and source line count in a composite function score. It does not independently
cap Halstead difficulty or effort. The floor of 20 blocks low-scoring production
functions; there are no grandfathered function-score exceptions. Rejection tests
exercise both branch-heavy and large straight-line functions, plus duplication.
See the [analyzer documentation](https://github.com/yagipy/maintidx) for its metric
and [golangci-lint configuration](https://golangci-lint.run/docs/linters/configuration/#maintidx)
for threshold semantics. Scores are indicators to review, not proof of good design.

Architecture and cohesion belong in design review. Specialized structural tools
may support a concrete refactoring investigation, but are not required by these
hooks or CI. There are no automatic file-size, receiver-method-count or package-size
ceilings and no custom structural analyzer in the gate.

## Verification

Coverage-inventory tests cover real package loading, missing/invalid source and
generated-file boundaries. Policy fixtures verify missing or unreviewed packages,
coverage regressions, missing/failed test runs, valid-but-truncated profiles,
changed sources and failed tools. Real temporary Go modules prove the pinned
linter rejects low-maintainability functions and duplicated implementations.
A signal fixture verifies removal of the gate's owned PostgreSQL container.
These checks run in the fast gate.

A passing gate means configured checks pass with the accepted coverage debt above.
It does not establish architectural quality or replace independent code review.
