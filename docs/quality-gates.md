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
| Function maintainability | The same production pass rejects maintainability index below 20. No existing function violations are exempted. |
| Structural maintainability | The repository's AST inventory rejects new or worsening receiver, file, package and dependency-holder measurements against the reviewed policy. |
| Tests | Fresh race-enabled default tests, PostgreSQL storage tests (`postgres,pglocal`), and integration tests (`integration`), retaining their separate established scopes. |
| Coverage | Fresh atomic profiles from all three test runs, per-package floors, and targeted floors for new producer, SQLite initialization and gate code. |
| Gate integrity | Unit and rejection fixtures prove failures for new violations, growth, missing analysis, malformed profiles and incomplete test results. |

`golangci-lint` remains pinned at v2.15.0 in `scripts/quality/tools.env`.
The maintainability/duplication pass analyzes production Go files. Ordinary lint
still includes tests and the repository's existing narrow test-linter exclusions.
The inventory uses the same build tags, parses all owned non-generated Go files,
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
80% individual floors; SQLite busy-timeout restoration requires 100%.
The initial complete test run measured these existing gaps:

| Package | Measured | Floor | Follow-up |
| --- | --- | --- | --- |
| `internal/notify` | 77.69% | 75% | Exercise connection loss/recovery, malformed payloads and handler panic recovery. The small margin accommodates concurrent listener paths. |
| `internal/storage/faultstore` | 72.27% | 72% | Cover unexercised delegation and injected-error boundaries through consumer behavior tests. |
| `internal/testkit` | 69.23% | 69% | Validate the alternate testcontainers startup path and infrastructure failures; canonical checks supply a dedicated PostgreSQL server. |

These are explicit accepted coverage debts, not excluded packages. Their floors
remain blocking and should rise as coverage improves. Any change requires review
of the measurements and reason in `scripts/quality/policy.json`.

## Structural policy and accepted debt

Default limits are 600 source lines per file, one substantial disconnected group
in a file above that size, 30 declared methods / 600 method lines per receiver,
25 files / 6,000 lines per package, and eight external-package type qualifiers in
a struct's fields. These are review triggers, not evidence of a runtime defect.

The scanner groups declarations by references within each file, connects methods
of the same receiver, and measures substantial groups (at least 80 lines, or at
least two declarations totaling 40 lines). This is an AST signal: local identifier
shadowing can conservatively join unrelated groups, and the checker does not
resolve promoted method sets or cross-package semantic concern groups. Receiver,
file and package growth remain independently bounded even when that graph joins
all declarations. It does not invent cross-layer rules absent from this project.

A complete type-aware Lagotto audit remains unavailable on Go 1.27.3:
[Lagotto #58](https://github.com/CaliLuke/lagotto/issues/58) tracks the v0.2.7 export
data incompatibility. Its partial reports are never consumed as acceptance data.
The working AST check supplies narrower structural enforcement. Reevaluate it
when a released Lagotto version can completely load this module; remove the
fallback only after reviewing complete results and preserving the growth limits.
No dependency source or fork is used.

Initial reviewed hotspots:

| Identity | Accepted measurement | Classification and follow-up |
| --- | --- | --- |
| `App` | 39 methods, 972 method lines | Debt: lifecycle, election, dispatch and client ingress share one owner. Extract worker coordination behind a narrower owner while preserving public APIs and shutdown/lease ordering. |
| `app.go` | 902 lines | Debt: separate lifecycle and dispatch responsibilities by ownership, not by splitting methods into arbitrary files. |
| `SQLiteStorage` / `PostgresStorage` | 59 / 61 methods | Debt: adapters implement the broad Storage contract. First narrow consumers to existing manager interfaces; preserve transaction and fencing boundaries. |
| `internal/storage/sqlite.go` | 670 lines | Debt: pool setup, transaction handling, instance CRUD and search are combined. Separate connection setup and query responsibilities without splitting transaction ownership. |
| Root / storage packages | 29 / 26 production files | Debt: public APIs/runtime and multiple persistence responsibilities are broad. Refactor responsibility boundaries before adding package layers. |
| `queryStorage` | 57 methods | Intentional exception: the read-only wrapper must reject the mutation surface of Storage. Review new methods against that boundary. |
| `faultstore.Store` | 54 methods | Intentional exception: fault injection mirrors Storage to test failures around durable effects. Review delegation and hook ordering. |

These follow-up descriptions are local refactoring drafts, not authorization to
perform unrelated runtime redesign. The policy retains exact numeric ceilings
and individual reasons. A previously flagged receiver cannot keep growing simply
because it still produces one finding. New or increased violations fail;
improvements pass and are reported for tightening. Removed identities fail with
a stale-baseline error until their removal is reviewed. Updating a baseline is an explicit code-review operation:
show old/new measurements and rationale, and tighten resolved debt. There is no
automatic refresh command or blanket directory suppression.

## Verification

The scanner tests cover real package loading, missing/invalid source, generic
receiver identity, generated-file boundaries and disconnected declarations.
The policy fixtures verify new and worsening violations, improvements, narrow
exceptions, missing packages and metrics, coverage regressions, missing/failed
test runs, valid-but-truncated profiles, changed sources and failed tools.
Real temporary Go modules prove the pinned linter rejects both low-maintainability
functions and duplicated implementations. These checks run in the fast gate.

A passing gate means configured checks pass with the accepted debt above. It does
not establish that the architecture is healthy or replace independent code review.
