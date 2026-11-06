#!/usr/bin/env bash
# Non-mutating acceptance checks. Formatting fixes belong to `make fmt`.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/quality/tools.env
mode=${1:-full}
if [[ "$mode" != fast && "$mode" != full ]]; then
    echo 'Usage: scripts/check.sh [fast|full]' >&2
    exit 2
fi
command -v go >/dev/null
command -v python3 >/dev/null
GOLANGCI_LINT=${GOLANGCI_LINT:-$(command -v golangci-lint || printf '%s/bin/golangci-lint' "$(go env GOPATH)")}
export GOLANGCI_LINT
if ! "$GOLANGCI_LINT" version | grep -Eq "version ${GOLANGCI_LINT_VERSION#v}([[:space:]]|$)"; then
    echo "Install golangci-lint $GOLANGCI_LINT_VERSION with make install-tools" >&2
    exit 1
fi
mkdir -p .cache/quality
quality_dir=$(mktemp -d .cache/quality/run.XXXXXX)
container_id=
container_name=
cleanup() {
    status=$?
    if [[ -n "$container_name" ]] && docker container inspect "$container_name" >/dev/null 2>&1; then
        if ! docker rm -f "$container_name" >/dev/null; then
            echo "Failed to remove owned test container $container_id" >&2
            status=1
        fi
    fi
    echo "Quality logs: $quality_dir"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
run() {
    name=$1
    shift
    echo "RUN  $name"
    if "$@" >"$quality_dir/$name.stdout" 2>"$quality_dir/$name.stderr"; then
        echo "PASS $name"
    else
        status=$?
        echo "FAIL $name (exit $status)" >&2
        tail -60 "$quality_dir/$name.stdout" >&2
        tail -60 "$quality_dir/$name.stderr" >&2
        return "$status"
    fi
}
tags=integration,postgres,pglocal
run config "$GOLANGCI_LINT" config verify
run modules go mod tidy -diff
run build go build -tags="$tags" ./...
run lint "$GOLANGCI_LINT" run --build-tags="$tags"
run maintainability "$GOLANGCI_LINT" run --enable-only=maintidx,dupl --tests=false --build-tags="$tags"
run gate-unit-tests go test -count=1 ./internal/qualitygate
run gate-rejection-tests python3 -m unittest discover -s scripts/quality -p 'test_*.py'
run inventory go run ./internal/qualitygate
run coverage-policy python3 scripts/quality/gates.py inventory --inventory "$quality_dir/inventory.stdout" --policy scripts/quality/policy.json
cat "$quality_dir/coverage-policy.stdout"
if [[ "$mode" == fast ]]; then
    echo 'PASS fast checks (full tests and coverage not run)'
    exit 0
fi
if [[ -z "${SELVEDGE_TEST_POSTGRES_URL:-}" ]]; then
    command -v docker >/dev/null
    container_name="selvedge-quality-$$-$RANDOM"
    container_id=$(docker run --rm -d --name "$container_name" -e POSTGRES_USER=selvedge -e POSTGRES_PASSWORD=selvedge_test -e POSTGRES_DB=selvedge_test -p 127.0.0.1::5432 postgres:17)
    ready=false
    for _ in {1..60}; do
        if docker exec "$container_id" pg_isready -U selvedge >/dev/null 2>&1; then ready=true; break; fi
        sleep 1
    done
    if [[ "$ready" != true ]]; then echo 'PostgreSQL did not become ready' >&2; exit 1; fi
    address=$(docker port "$container_id" 5432/tcp)
    export SELVEDGE_TEST_POSTGRES_URL="postgres://selvedge:selvedge_test@$address/selvedge_test?sslmode=disable"
fi
# Keep the established test scopes separate. Integration fixtures isolate their DBs;
# postgres,pglocal tests deliberately use and reset the dedicated test server DB.
run unit-tests go test -json -race -count=1 -timeout=10m -coverpkg=./... -coverprofile="$quality_dir/unit.out" ./...
run postgres-storage go test -json -race -tags=postgres,pglocal -count=1 -timeout=10m -coverpkg=./... -coverprofile="$quality_dir/postgres.out" ./internal/storage
run integration-tests go test -json -race -tags=integration -count=1 -timeout=25m -coverpkg=./... -coverprofile="$quality_dir/integration.out" ./...
run coverage python3 scripts/quality/gates.py coverage --inventory "$quality_dir/inventory.stdout" --policy scripts/quality/policy.json \
    --profile "$quality_dir/unit.out" --profile "$quality_dir/postgres.out" --profile "$quality_dir/integration.out" \
    --events "$quality_dir/unit-tests.stdout" --events "$quality_dir/postgres-storage.stdout" --events "$quality_dir/integration-tests.stdout"
cat "$quality_dir/coverage.stdout"
echo 'PASS full quality gate'
