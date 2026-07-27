#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-cli

run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-receipt-recovery-e2e.XXXXXX")"
workspace="$run_root/workspace"
mkdir -p "$workspace"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
filesystem="$repository/target/debug/agentmod-filesystem-host"
cli="$repository/target/debug/agentmod"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$filesystem"
export AGENTMOD_TOOL_RECEIPT_DELAY_MS=8000

cleanup() {
    for pid in "${approval_pid:-}" "${daemon_pid:-}"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-receipt-recovery-e2e.*) rm -rf -- "$run_root" ;;
        *) printf 'refusing unexpected cleanup path: %s\n' "$run_root" >&2 ;;
    esac
}
trap cleanup EXIT

(cd "$run_root" && "$runtime" serve) &
daemon_pid=$!
for _ in $(seq 1 50); do
    "$cli" doctor --json >/dev/null 2>&1 && break
    sleep 0.1
done
"$cli" doctor --json >/dev/null
session_id="$("$cli" session create --workspace "$workspace" \
    --style persistent-chat --json |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
turn="$("$cli" run "write once across a crash" --session "$session_id" \
    --option 'mock_scenario="approval_write"' --json)"
continuation="$(printf '%s' "$turn" |
    sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')"
[[ -n "$continuation" ]]

"$cli" approval resolve "$session_id" "$continuation" approve --json \
    >"$run_root/approval.stdout" 2>"$run_root/approval.stderr" &
approval_pid=$!
receipt_root="$run_root/sessions/$session_id/artifacts/tool-receipts"
for _ in $(seq 1 80); do
    receipt_count="$(find "$receipt_root" -maxdepth 1 -name '*.json' 2>/dev/null |
        wc -l | tr -d ' ')"
    [[ "$receipt_count" == 1 ]] && break
    kill -0 "$approval_pid" 2>/dev/null
    sleep 0.1
done
[[ "$(find "$receipt_root" -maxdepth 1 -name '*.json' | wc -l | tr -d ' ')" == 1 ]]

kill -9 "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
kill "$approval_pid" 2>/dev/null || true
wait "$approval_pid" 2>/dev/null || true
approval_pid=
unset AGENTMOD_TOOL_RECEIPT_DELAY_MS
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$run_root/host-must-not-be-spawned"

(cd "$run_root" && "$runtime" serve) &
daemon_pid=$!
for _ in $(seq 1 50); do
    "$cli" doctor --json >/dev/null 2>&1 && break
    sleep 0.1
done
recovered="$("$cli" approval resolve "$session_id" "$continuation" approve --json)"
printf '%s' "$recovered" | grep -q 'continued after durable approval decision'
[[ "$(cat "$workspace/approved.txt")" == "executed once" ]]

python3 - "$run_root/sessions/$session_id/events.jsonl" <<'PY'
import json
import sys

events = [json.loads(line)["event"] for line in open(sys.argv[1], encoding="utf-8")]
types = [event["metadata"]["event_type"] for event in events]
for expected in (
    "approval.resolved",
    "tool.execution_dispatched",
    "tool.execution_started",
    "tool.execution_completed",
):
    assert types.count(expected) == 1, (expected, types.count(expected))
assert "tool.execution_failed" not in types
PY

printf 'runtime post-dispatch receipt recovery E2E passed\n'
