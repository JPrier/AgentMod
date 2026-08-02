#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
gate_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-harness-overlap.XXXXXX")
cleanup() {
    rm -rf -- "$gate_root"
}
trap cleanup EXIT INT TERM

cd "$repository"
cargo build -p agentmod-harness -p agentmod-runtime
AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
AGENTMOD_HARNESS_TEST_GATE_ROOT="$gate_root"
export AGENTMOD_HARNESS_PROGRAM AGENTMOD_HARNESS_TEST_GATE_ROOT
output=$("$repository/target/debug/agentmod-runtime" harness-overlap-smoke)
printf '%s' "$output" | grep -q '"status":"ok"'
printf '%s' "$output" | grep -q '"boundary":"runtime_service_to_bounded_harness_pool"'
printf '%s' "$output" | grep -q '"started_before_release":2'
printf '%s' "$output" | grep -q '"released":2'
printf '%s\n' "runtime/harness bounded-overlap E2E passed"
