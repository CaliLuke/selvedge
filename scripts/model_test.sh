#!/usr/bin/env bash
# Checks the Selvedge TLA+ model (docs/formal/Selvedge.tla) with TLC.
#
# usage: scripts/model_test.sh [--long] /absolute/path/to/tla2tools.jar
#
# --long also runs SelvedgeTwoWorkersLong (two slots with cancellation and a
# signal: about 39M states, about 20 minutes).
#
# Every pass configuration must pass and still explore at least its floor of
# distinct states; every fault configuration must fail with its named
# invariant; every witness must be reachable; every liveness fault must
# violate its temporal property.
set -euo pipefail

long=0
jar=""
for arg in "$@"; do
  if [[ "${arg}" == --long ]]; then
    long=1
  else
    jar="${arg}"
  fi
done
if [[ ! -f "${jar}" ]]; then
  echo 'usage: scripts/model_test.sh [--long] /absolute/path/to/tla2tools.jar' >&2
  exit 1
fi
jar="$(cd "$(dirname "${jar}")" && pwd)/$(basename "${jar}")"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
# Keep TLC traces outside the checkout, including expected counterexamples.
cp "${root}/docs/formal/Selvedge.tla" "${root}/docs/formal/Selvedge"*.cfg "${work}/"
cd "${work}"

# run MODULE CONFIG prints TLC's exit code. Deadlock checking stays on: the
# module names its legitimate end state with an explicit stuttering action
# (Done), so any other state without a successor is a modelling error.
run() {
  local module="$1" config="$2"
  local result=0
  java -Xmx4g -XX:+UseParallelGC -cp "${jar}" tlc2.TLC -workers 4 \
    -metadir "${work}/states-${config}" -config "${config}.cfg" "${module}.tla" >"${config}.log" 2>&1 || result=$?
  echo "${result}"
}

fail() {
  cat "$2.log" >&2
  echo "$2: $1" >&2
  exit 1
}

# expect_pass MODULE CONFIG MIN_STATES checks a configuration that must pass,
# and that it still explores at least MIN_STATES distinct states, so a change
# that collapses the state space cannot pass silently.
expect_pass() {
  local module="$1" config="$2" floor="$3"
  local result states
  result="$(run "${module}" "${config}")"
  [[ "${result}" == 0 ]] || fail "expected no violation" "${config}"
  states="$(grep 'distinct states found' "${config}.log" | tail -1 | sed -E 's/.* ([0-9]+) distinct states found.*/\1/')"
  [[ "${states}" -ge "${floor}" ]] || fail "explored ${states} distinct states, fewer than ${floor}" "${config}"
  echo "${config}: passed (${states} distinct states)"
}

# Each fault configuration breaks one design rule; TLC must find the
# counterexample for the invariant that rule protects. A witness
# configuration states a scenario as a negated invariant, so the
# counterexample proves the scenario is reachable.
expect_violation() {
  local module="$1" config="$2" invariant="$3"
  local result
  result="$(run "${module}" "${config}")"
  if [[ "${result}" != 12 ]] || ! grep -q "Invariant ${invariant} is violated" "${config}.log"; then
    fail "expected a ${invariant} counterexample" "${config}"
  fi
  echo "${config}: reproduced the expected ${invariant} counterexample"
}

# expect_liveness_violation checks a liveness fault: TLC must report that a
# temporal property fails (exit code 13), so the property is not vacuous.
expect_liveness_violation() {
  local module="$1" config="$2"
  local result
  result="$(run "${module}" "${config}")"
  if [[ "${result}" != 13 ]] || ! grep -q "Temporal properties were violated" "${config}.log"; then
    fail "expected a liveness counterexample" "${config}"
  fi
  echo "${config}: reproduced the expected liveness counterexample"
}

m=Selvedge
# Pass gates. The floors sit just under the measured distinct-state counts.
expect_pass ${m} ${m} 5070000
expect_pass ${m} ${m}Channels 6820000
expect_pass ${m} ${m}TwoWorkers 4580000
expect_pass ${m} ${m}TwoWorkersSignals 15830000
expect_pass ${m} ${m}TwoWorkersCrash 1520000
expect_pass ${m} ${m}Liveness 648000
expect_pass ${m} ${m}LivenessCancel 648000
expect_pass ${m} ${m}LivenessCrash 3640000

# Liveness faults.
expect_liveness_violation ${m} ${m}LivenessUnsafeCancelIgnoredWhileReceiving
expect_liveness_violation ${m} ${m}LivenessUnsafeCancelIgnoredWhileSleeping
expect_liveness_violation ${m} ${m}LivenessUnsafeCancelIgnoredWhileAwaitingChild
expect_liveness_violation ${m} ${m}LivenessUnsafeLostWakeup
expect_liveness_violation ${m} ${m}LivenessUnsafeAbandonWithoutWake

# Witnesses.
expect_violation ${m} ${m}WitnessTakeoverCompletes WitnessTakeoverCompletes
expect_violation ${m} ${m}WitnessZombieFenced WitnessZombieFenced
expect_violation ${m} ${m}WitnessActivityRetried WitnessActivityRetried
expect_violation ${m} ${m}WitnessCancelWhileSleeping WitnessCancelWhileSleeping
expect_violation ${m} ${m}WitnessParentCloseCancelsChild WitnessParentCloseCancelsChild
expect_violation ${m} ${m}WitnessReadyBLater WitnessReadyBLater
expect_violation ${m} ${m}WitnessPendingSeen WitnessPendingSeen
expect_violation ${m} ${m}WitnessReadyAfterWake WitnessReadyAfterWake

# Lease faults.
expect_violation ${m} ${m}UnsafeNoLeaseCheck NoConcurrentOwners
expect_violation ${m} ${m}UnsafeMarginBelowSkew NoConcurrentOwners
expect_violation ${m} ${m}UnsafeAcquirerClockExpiry NoConcurrentOwners
expect_violation ${m} ${m}UnsafeProcessReentrantAcquire NoConcurrentOwners
expect_violation ${m} ${m}UnsafeProcessFence WritesFenced
expect_violation ${m} ${m}UnsafeUnfencedWrite WritesFenced
expect_violation ${m} ${m}UnsafeUnfencedDelivery ReplayDeterministic

# Replay faults.
expect_violation ${m} ${m}UnsafeUnrecordedWakeOrder ReplayDeterministic
expect_violation ${m} ${m}UnsafeUnrecordedNow ReplayDeterministic
expect_violation ${m} ${m}UnsafeLiveIsReady ReplayDeterministic
expect_violation ${m} ${m}UnsafeLivePending ReplayDeterministic
expect_violation ${m} ${m}UnsafeLiveReceiveAsync PendingMatchesReceive
expect_violation ${m} ${m}UnsafePendingLiveCursor ReplayDeterministic
expect_violation ${m} ${m}UnsafeCompletionOrderKeys ReplayDeterministic
expect_violation ${m} ${m}UnsafeLiveFutureValue ReplayDeterministic
expect_violation ${m} ${m}UnsafeScheduleWithoutLookup CompletedStepNotReplayedAsNew

# History faults.
expect_violation ${m} ${m}UnsafeSilentDuplicateKey ResultRecordedOnce
expect_violation ${m} ${m}UnsafeLastWriteWins ResultRecordedOnce

# Signal faults.
expect_violation ${m} ${m}UnsafeCursorFromMax SignalsNotLost
expect_violation ${m} ${m}UnsafeSharedCursor SignalsNotLost

# Cancellation and child faults.
expect_violation ${m} ${m}UnsafeTimerRevivesCancelled CancelledStaysCancelled
expect_violation ${m} ${m}UnsafeNoParentClose ChildrenFollowParent
expect_violation ${m} ${m}UnsafeCleanupInCancelledScope CleanupRunsAfterCancel
expect_violation ${m} ${m}UnsafeAbandonWithoutWake ParkedOnlyWhenWaiting

if [[ "${long}" == 1 ]]; then
  expect_pass ${m} ${m}TwoWorkersLong 39540000
fi
