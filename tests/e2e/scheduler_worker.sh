#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-scheduler
root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-scheduler-e2e.XXXXXX")
export AGENTMOD_SCHEDULER_ROOT="$root"
export AGENTMOD_SCHEDULER_AUTH_TOKEN="0123456789abcdef0123456789abcdef"
trap 'rm -rf -- "$root"' EXIT INT TERM

negotiate='{"command":"negotiate","value":{"protocol_version":1,"capabilities":["durable_schedules"],"authentication_token":"0123456789abcdef0123456789abcdef"}}'
once='{"schedule_id":"once","session_id":"session:1","idempotency_id":"idempotency:once","style":"persistent-chat","workspace":"workspace","permission_policy":"safe-background","provider":"deterministic-mock","model":"mock","token_budget":100,"cost_budget_micros":0,"trigger":{"kind":"at_millis","value":0},"payload":{"kind":"prompt","value":{"prompt":"scheduled work"}},"active":true}'
event='{"schedule_id":"event","session_id":"session:1","idempotency_id":"idempotency:event","style":"persistent-chat","workspace":"workspace","permission_policy":"safe-background","provider":"deterministic-mock","model":"mock","token_budget":100,"cost_budget_micros":0,"trigger":{"kind":"runtime_event","value":{"event_type":"tool.execution_completed"}},"payload":{"kind":"prompt","value":{"prompt":"scheduled work"}},"active":true}'
output=$(printf '%s\n' \
    "$negotiate" \
    "{\"command\":\"upsert\",\"value\":{\"schedule\":$once}}" \
    "{\"command\":\"upsert\",\"value\":{\"schedule\":$once}}" \
    "{\"command\":\"upsert\",\"value\":{\"schedule\":$event}}" \
    '{"command":"list","value":{"limit":10}}' \
    '{"command":"claim_due","value":{"limit":10}}' \
    '{"command":"claim_due","value":{"limit":10}}' \
    '{"command":"fire_runtime_event","value":{"event_id":"event:1","event_type":"tool.execution_completed"}}' \
    '{"command":"fire_runtime_event","value":{"event_id":"event:1","event_type":"tool.execution_completed"}}' |
    "$repository/target/debug/agentmod-scheduler")
printf '%s' "$output" | grep -F '"result":"negotiated"' >/dev/null
printf '%s' "$output" | grep -F '"replayed":true' >/dev/null
test "$(printf '%s' "$output" | grep -c '"execution_id":')" -eq 2
execution_id=$(printf '%s' "$output" | sed -n \
    's/.*"execution_id":"\([0-9a-f]*\)".*/\1/p' | head -n 1)
test -n "$execution_id"

completion=$(printf '%s\n' \
    "$negotiate" \
    "{\"command\":\"complete_execution\",\"value\":{\"execution_id\":\"$execution_id\",\"succeeded\":true}}" \
    "{\"command\":\"complete_execution\",\"value\":{\"execution_id\":\"$execution_id\",\"succeeded\":true}}" \
    '{"command":"claim_due","value":{"limit":10}}' |
    "$repository/target/debug/agentmod-scheduler")
printf '%s' "$completion" | grep -F '"changed":true' >/dev/null
printf '%s' "$completion" | grep -F '"changed":false' >/dev/null
test "$(find "$root/executions" -name '*.json' | wc -l)" -eq 2

echo "durable scheduler worker E2E passed"
