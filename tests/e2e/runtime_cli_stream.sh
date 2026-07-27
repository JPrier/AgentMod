#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-cli-stream-e2e.XXXXXX")"
workspace="$run_root/workspace"
mkdir -p "$workspace"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_HARNESS_FRAME_PACING_MS=500

cleanup() {
    if [[ -n "${daemon_pid:-}" ]]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-cli-stream-e2e.*) rm -rf -- "$run_root" ;;
        *) printf 'refusing to remove unexpected path: %s\n' "$run_root" >&2 ;;
    esac
}
trap cleanup EXIT

(cd "$run_root" && "$runtime" serve) &
daemon_pid=$!
for _ in $(seq 1 50); do
    if "$cli" doctor --json >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done
"$cli" doctor --json >/dev/null

session_id="$("$cli" session create --workspace "$workspace" \
    --style persistent-chat --json |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
[[ -n "$session_id" ]]

fifo="$run_root/stream.fifo"
mkfifo "$fifo"
"$cli" run "emit committed frames" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="live-cli"' --stream-json >"$fifo" &
turn_pid=$!
exec 3<"$fifo"
if ! IFS= read -r -t 5 first_line <&3; then
    printf 'first stream item was not emitted promptly\n' >&2
    exit 1
fi
if ! kill -0 "$turn_pid" 2>/dev/null; then
    printf 'CLI buffered output until turn completion\n' >&2
    exit 1
fi
printf '%s\n' "$first_line" >"$run_root/items.jsonl"
cat <&3 >>"$run_root/items.jsonl"
wait "$turn_pid"

python3 - "$run_root/items.jsonl" <<'PY'
import json
import sys

items = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
events = [item for item in items if item["command"] == "run_event"]
terminal = [item for item in items if item["command"] == "run_complete"]
assert events[0]["event"]["event"] == "started"
assert len(terminal) == 1 and items[-1]["command"] == "run_complete"
sequences = [event["committed_sequence"] for event in events]
assert sequences == sorted(set(sequences))
visible = "".join(
    event["event"]["text"]
    for event in events
    if event["event"]["event"] == "text"
)
assert visible == "alpha beta live-cli", visible
assert terminal[0]["last_committed_sequence"] == 10
PY

printf 'runtime live CLI streaming JSON E2E passed\n'
