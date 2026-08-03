#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-scheduler \
    -p agentmod-harness -p agentmod-cli

configured_target=${CARGO_TARGET_DIR:-$repository/target}
case "$configured_target" in
    /*) target_root=$configured_target ;;
    *) target_root=$repository/$configured_target ;;
esac
debug_root=$target_root/debug

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-automatic-memory-e2e.XXXXXX")
succeeded=0
workspace="$run_root/workspace"
style_root="$run_root/styles/user"
mkdir -p "$workspace" "$style_root"
cp "$repository/tests/fixtures/styles/automatic-memory-file.toml" \
    "$style_root/automatic-memory.toml"

# The style graph uses a fixed `filesystem.read` gate; make the read succeed.
mkdir -p "$workspace/src"
printf '%s\n' 'pub fn fixture() -> bool { true }' > \
    "$workspace/src/lib.rs"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$debug_root/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$debug_root/agentmod-filesystem-host"
export AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS=10000
runtime="$debug_root/agentmod-runtime"
cli="$debug_root/agentmod"

cleanup() {
    if [ -n "${turn_pid:-}" ]; then
        kill "$turn_pid" 2>/dev/null || true
        wait "$turn_pid" 2>/dev/null || true
    fi
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    case "$succeeded:$run_root" in
        0:*)
            echo "retained failed E2E root: $run_root" >&2
            ;;
        1:"${TMPDIR:-/tmp}"/agentmod-automatic-memory-e2e.*)
            rm -rf -- "$run_root"
            ;;
        *)
            echo "refusing to remove unexpected E2E root: $run_root" >&2
            ;;
    esac
}
trap cleanup EXIT INT TERM

start_runtime() {
    (cd "$run_root" && "$runtime" serve) &
    daemon_pid=$!
    attempt=0
    until "$cli" doctor --json >/dev/null 2>&1; do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 100 ]; then
            echo "runtime did not become ready" >&2
            exit 1
        fi
        sleep 0.1
    done
}

event_count() {
    grep -c -F "\"event_type\":\"$1\"" "$journal" || true
}

start_runtime
"$cli" style validate "$style_root/automatic-memory.toml" --json |
    grep -F '"valid":true' >/dev/null
created=$("$cli" session create --workspace "$workspace" \
    --style e2e-automatic-memory --json)
session_id=$(printf '%s' "$created" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"
run_id=018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d3e
journal="$run_root/sessions/$session_id/events.jsonl"
memory_file="$run_root/memory/file.jsonl"

"$cli" run "remember the automatic memory boundary" --session "$session_id" \
    --cancellation-id "$run_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="automatic-memory-output"' --json \
    >"$run_root/cut-turn.stdout.log" 2>"$run_root/cut-turn.stderr.log" &
turn_pid=$!

attempt=0
while :; do
    if [ -f "$journal" ] && [ -f "$memory_file" ] &&
        [ "$(event_count memory.write_dispatched)" -eq 1 ] &&
        [ "$(wc -l <"$memory_file" | tr -d ' ')" -eq 1 ]; then
        break
    fi
    if ! kill -0 "$turn_pid" 2>/dev/null; then
        if wait "$turn_pid"; then
            turn_status=0
        else
            turn_status=$?
        fi
        turn_pid=
        echo "cut turn exited before the crash window (status $turn_status)" >&2
        echo "--- cut turn stdout ---" >&2
        sed -n '1,120p' "$run_root/cut-turn.stdout.log" >&2
        echo "--- cut turn stderr ---" >&2
        sed -n '1,120p' "$run_root/cut-turn.stderr.log" >&2
        exit 1
    fi
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 200 ]; then
        echo "automatic memory process did not reach the post-persist crash cut" >&2
        exit 1
    fi
    sleep 0.05
done
test "$(event_count memory.write_completed)" -eq 0

kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
wait "$turn_pid" 2>/dev/null || true
turn_pid=

export AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS=0
export AGENTMOD_HARNESS_PROGRAM="$run_root/harness-must-not-be-spawned"
export AGENTMOD_FIXTURE_HARNESS_PROGRAM="$run_root/fixture-harness-must-not-be-spawned"
start_runtime
recovered=$("$cli" run "remember the automatic memory boundary" \
    --session "$session_id" --cancellation-id "$run_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="automatic-memory-output"' --json)
printf '%s' "$recovered" | grep -F '"events":[]' >/dev/null

test "$(event_count memory.write_proposed)" -eq 1
test "$(event_count memory.write_approved)" -eq 1
test "$(event_count memory.write_dispatched)" -eq 1
test "$(event_count memory.write_completed)" -eq 1
test "$(event_count model.request_proposed)" -eq 1
test "$(event_count model.request_approved)" -eq 1
test "$(event_count model.request_started)" -eq 1
test "$(event_count model.response_completed)" -eq 1
test "$(event_count model.request_failed)" -eq 0
test "$(event_count model.request_cancelled)" -eq 0
test "$(wc -l <"$memory_file" | tr -d ' ')" -eq 1

before=$("$cli" session inspect "$session_id" --json)
printf '%s' "$before" >"$run_root/before-inspect.json"
python3 - "$journal" "$memory_file" "$session_id" "$run_id" <<'PY'
import json
import re
import sys

journal_path, memory_path, session_id, run_id = sys.argv[1:]
events = [json.loads(line)["event"] for line in open(journal_path, encoding="utf-8")]
memory = [
    event for event in events
    if event["metadata"]["event_type"].startswith("memory.write_")
]
assert [event["metadata"]["event_type"] for event in memory] == [
    "memory.write_proposed",
    "memory.write_approved",
    "memory.write_dispatched",
    "memory.write_completed",
]
payloads = [event["payload"]["payload"] for event in memory]
identity = payloads[0]["identity"]
assert all(payload["identity"] == identity for payload in payloads)
digest = payloads[1]["action_digest"]
assert payloads[2]["action_digest"] == digest
assert payloads[3]["action_digest"] == digest
assert identity["provider"] == "file"
assert identity["policy"] == "turn_completion"
assert identity["run_id"] == run_id
assert identity["scope"] == f"session:{session_id}"
assert payloads[3]["retained"] is True

with open(memory_path, encoding="utf-8") as source:
    records = [json.loads(line) for line in source]
assert len(records) == 1
record = records[0]
assert record["schema_version"] == 1
assert record["id"] == payloads[3]["reference"]
assert record["scope"] == identity["scope"]
assert record["source"] == identity["source"]
assert record["source"] == f"runtime.automatic_memory:turn_completion:{run_id}"
assert record["content"] == payloads[0]["content"]
assert record["created_at_millis"] == identity["created_at_millis"]
assert len(record["content"].encode("utf-8")) == identity["byte_size"]
assert re.fullmatch(r"[0-9a-f]{64}", record["checksum"])
typed = json.loads(record["content"])
assert typed["schema"] == "agentmod.context-summary.v1"
assert "remember the automatic memory boundary" in record["content"]
assert "automatic-memory-output" in record["content"]
PY
printf '%s' "$before" | grep -F '"automatic_memory_writes":{' >/dev/null
printf '%s' "$before" | grep -F '"state":"completed"' >/dev/null
printf '%s' "$before" | grep -F '"retained":true' >/dev/null
printf '%s' "$before" | grep -F "\"run_id\":\"$run_id\"" >/dev/null
printf '%s' "$before" | grep -F '"policy":"turn_completion"' >/dev/null
printf '%s' "$before" | grep -F 'agentmod.context-summary.v1' >/dev/null
before_count=$(wc -l <"$journal" | tr -d ' ')
before_journal_hash=$(sha256sum "$journal" | cut -d' ' -f1)
before_memory_hash=$(sha256sum "$memory_file" | cut -d' ' -f1)
before_head=$(printf '%s' "$before" |
    sed -n 's/.*"last_sequence":\([0-9][0-9]*\).*/\1/p')
test -n "$before_head"

kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
start_runtime
replayed=$("$cli" session replay "$session_id" --json)
printf '%s' "$replayed" >"$run_root/replayed.json"
printf '%s' "$replayed" | grep -F "\"last_sequence\":$before_head" >/dev/null
printf '%s' "$replayed" | grep -F '"state":"completed"' >/dev/null
test "$(wc -l <"$journal" | tr -d ' ')" -eq "$before_count"
test "$(sha256sum "$journal" | cut -d' ' -f1)" = "$before_journal_hash"
test "$(sha256sum "$memory_file" | cut -d' ' -f1)" = "$before_memory_hash"
python3 - "$run_root/before-inspect.json" "$run_root/replayed.json" <<'PY'
import json
import sys

before = json.load(open(sys.argv[1], encoding="utf-8"))["state"]
replayed = json.load(open(sys.argv[2], encoding="utf-8"))["state"]
assert before["last_sequence"] == replayed["last_sequence"]
assert before["automatic_memory_writes"] == replayed["automatic_memory_writes"]
PY

kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
export AGENTMOD_MEMORY_WRITE_PERMISSION_MODE=ask
export AGENTMOD_HARNESS_PROGRAM="$debug_root/agentmod-harness"
start_runtime

ask_created=$("$cli" session create --workspace "$workspace" \
    --style e2e-automatic-memory --json)
ask_session_id=$(printf '%s' "$ask_created" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
ask_run_id=018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d4e
ask_journal="$run_root/sessions/$ask_session_id/events.jsonl"
ask_waiting=$("$cli" run "approve native automatic memory" \
    --session "$ask_session_id" --cancellation-id "$ask_run_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="native-ask-output"' --json)
ask_continuation=$(printf '%s' "$ask_waiting" |
    sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
test -n "$ask_continuation"
test "$(grep -c -F '"event_type":"memory.write_proposed"' "$ask_journal")" -eq 1
test "$(grep -c -F '"event_type":"approval.requested"' "$ask_journal")" -eq 1
test "$(grep -c -F '"event_type":"memory.write_approved"' "$ask_journal" || true)" -eq 0
test "$(wc -l <"$memory_file" | tr -d ' ')" -eq 1

kill "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
start_runtime
ask_approved=$("$cli" approval resolve "$ask_session_id" \
    "$ask_continuation" approve --json)
printf '%s' "$ask_approved" | grep -F '"transitioned":true' >/dev/null
test "$(grep -c -F '"event_type":"approval.resolved"' "$ask_journal")" -eq 1
test "$(grep -c -F '"event_type":"memory.write_approved"' "$ask_journal")" -eq 1
test "$(grep -c -F '"event_type":"memory.write_dispatched"' "$ask_journal")" -eq 1
test "$(grep -c -F '"event_type":"memory.write_completed"' "$ask_journal")" -eq 1
test "$(wc -l <"$memory_file" | tr -d ' ')" -eq 2
ask_before_duplicate=$(wc -l <"$ask_journal" | tr -d ' ')
ask_duplicate=$("$cli" approval resolve "$ask_session_id" \
    "$ask_continuation" approve --json)
printf '%s' "$ask_duplicate" | grep -F '"transitioned":false' >/dev/null
test "$(wc -l <"$ask_journal" | tr -d ' ')" -eq "$ask_before_duplicate"
test "$(wc -l <"$memory_file" | tr -d ' ')" -eq 2

deny_created=$("$cli" session create --workspace "$workspace" \
    --style e2e-automatic-memory --json)
deny_session_id=$(printf '%s' "$deny_created" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
deny_run_id=018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d5e
deny_journal="$run_root/sessions/$deny_session_id/events.jsonl"
deny_waiting=$("$cli" run "deny native automatic memory" \
    --session "$deny_session_id" --cancellation-id "$deny_run_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="native-deny-output"' --json)
deny_continuation=$(printf '%s' "$deny_waiting" |
    sed -n 's/.*"awaiting_continuation":"\([^"]*\)".*/\1/p')
test -n "$deny_continuation"
deny_result=$("$cli" approval resolve "$deny_session_id" \
    "$deny_continuation" deny --json)
printf '%s' "$deny_result" | grep -F '"transitioned":true' >/dev/null
test "$(grep -c -F '"event_type":"approval.resolved"' "$deny_journal")" -eq 1
test "$(grep -c -F '"event_type":"memory.write_failed"' "$deny_journal")" -eq 1
test "$(grep -c -F '"event_type":"memory.write_dispatched"' "$deny_journal" || true)" -eq 0
test "$(wc -l <"$memory_file" | tr -d ' ')" -eq 2
deny_inspect=$("$cli" session inspect "$deny_session_id" --json)
printf '%s' "$deny_inspect" | grep -F '"state":"failed"' >/dev/null
printf '%s' "$deny_inspect" | grep -F '"failure_code":"user_denied"' >/dev/null

succeeded=1
echo "runtime automatic-memory crash/restart/replay/native-ask E2E passed"
