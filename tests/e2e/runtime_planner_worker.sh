#!/usr/bin/env bash
set -euo pipefail

repository=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repository"

cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-planner-e2e.XXXXXX")
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
daemon=""

cleanup() {
    if [[ -n "$daemon" ]]; then
        kill "$daemon" 2>/dev/null || true
        wait "$daemon" 2>/dev/null || true
    fi
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-planner-e2e.*) rm -rf -- "$run_root" ;;
        *) echo "refusing to remove unexpected E2E root: $run_root" >&2 ;;
    esac
}
trap cleanup EXIT

start_runtime() {
    "$runtime" serve >"$run_root/runtime.stdout.log" 2>"$run_root/runtime.stderr.log" &
    daemon=$!
    for _ in $(seq 1 100); do
        if "$cli" doctor --json >/dev/null 2>&1; then
            return
        fi
        sleep 0.1
    done
    echo "runtime did not become ready" >&2
    exit 1
}

start_runtime
session_id=$("$cli" session create --workspace "$repository" \
    --style planner-worker@1.2.0 --json |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')
"$cli" run "verify modular child orchestration" --session "$session_id" \
    --option 'mock_scenario="planner_worker"' --json >/dev/null
python3 - "$run_root" "$session_id" <<'PY'
import json, pathlib, sys
root, session = pathlib.Path(sys.argv[1]), sys.argv[2]
events = [json.loads(line) for line in
          (root / "sessions" / session / "events.jsonl").read_text().splitlines()]
types = [event["metadata"]["event_type"] for event in events]
assert types.count("child_agent.created") == 3
assert types.count("child_agent.join_completed") == 2
assert types.count("style.reviewer_findings_committed") == 2
PY
kill "$daemon"
wait "$daemon" 2>/dev/null || true
daemon=""
start_runtime
"$cli" session inspect "$session_id" --json |
    python3 -c 'import json,sys; state=json.load(sys.stdin)["state"]; assert state["lifecycle"]=="completed"; assert len(state["planner_worker"]["reviews"])==2'
echo "runtime planner-worker child/join/reject-once/restart E2E passed"
