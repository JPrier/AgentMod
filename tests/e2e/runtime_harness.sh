#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-harness -p agentmod-runtime
AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_HARNESS_PROGRAM
output=$("$repository/target/debug/agentmod-runtime" harness-smoke)
printf '%s' "$output" | grep -q '"status":"ok"'
printf '%s' "$output" | grep -q '"boundary":"runtime_service_to_harness_process"'
printf '%s' "$output" | grep -q '"text":"alpha beta runtime-harness-ok"'
printf '%s\n' "runtime↔harness process E2E passed"
