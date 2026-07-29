#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-session-reconnect-e2e.XXXXXX")"
workspace="$run_root/workspace"
mkdir -p "$workspace"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"

cleanup() {
    if [[ -n "${daemon_pid:-}" ]]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-session-reconnect-e2e.*) rm -rf -- "$run_root" ;;
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
session_id="$("$cli" session create --workspace "$workspace" \
    --style persistent-chat --json |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
"$cli" run "create reconnect history" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' --json >/dev/null

"$cli" session events "$session_id" --limit 3 --json >"$run_root/first.json"
first_after="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["last_delivered_sequence"])' "$run_root/first.json")"
"$cli" session events "$session_id" --after "$first_after" --limit 4 --json >"$run_root/second.json"
second_after="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["last_delivered_sequence"])' "$run_root/second.json")"
"$cli" session events "$session_id" --after "$second_after" --limit 4 --json >"$run_root/third.json"
third_after="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["last_delivered_sequence"])' "$run_root/third.json")"
"$cli" session events "$session_id" --after "$third_after" --limit 4 --json >"$run_root/fourth.json"
fourth_after="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["last_delivered_sequence"])' "$run_root/fourth.json")"
"$cli" session events "$session_id" --after "$fourth_after" --limit 4 --json >"$run_root/fifth.json"
fifth_after="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["last_delivered_sequence"])' "$run_root/fifth.json")"
"$cli" session events "$session_id" --after "$fifth_after" --limit 4 --json >"$run_root/caught-up.json"

python3 - "$run_root/first.json" "$run_root/second.json" "$run_root/third.json" \
    "$run_root/fourth.json" "$run_root/fifth.json" "$run_root/caught-up.json" <<'PY'
import json
import sys

pages = [json.load(open(path, encoding="utf-8")) for path in sys.argv[1:]]
sequences = [event["sequence"] for page in pages[:-1] for event in page["events"]]
assert sequences == list(range(1, 20)), sequences
assert all(page["has_more"] for page in pages[:4])
assert not pages[4]["has_more"]
assert pages[4]["head_sequence"] == pages[4]["last_delivered_sequence"] == 19
assert pages[5]["events"] == [] and not pages[5]["has_more"]
assert pages[5]["head_sequence"] == pages[5]["last_delivered_sequence"] == 19
PY

printf 'runtime credit-window reconnect-from-sequence E2E passed\n'
