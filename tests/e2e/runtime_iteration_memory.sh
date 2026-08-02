#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-iteration-memory-e2e.XXXXXX")
style_root="$run_root/styles/user"
mkdir -p "$style_root"
cat >"$style_root/iteration-memory.toml" <<'TOML'
schema_version = 1
kind = "custom"
required_capabilities = ["approval", "artifacts", "context", "model", "tools"]
allowed_tool_groups = ["filesystem"]
allowed_providers = ["deterministic-mock"]
allowed_plugins = ["runtime.security"]

[identity]
id = "e2e-iteration-memory"
version = "1.0.0"
runtime_api = "^1.0"

[graph]
kind = "inline"
source = '''
format_version = 1
entry = "fresh-context"

[budget]
max_steps = 500
max_tokens = 750000
max_cost_micros = 75000000
max_duration_ms = 2700000

[declarations]
capabilities = ["artifacts", "context", "model", "tools"]
tools = ["filesystem.read"]
providers = ["deterministic-mock"]

[[variables]]
name = "model_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "research"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "model_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "research"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "research_receipt"
type = { kind = "node_result_reference" }
scope = "run"
producer = "tool-batch"
consumers = ["persist"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "receipt_artifact"
type = { kind = "artifact_reference" }
scope = "run"
producer = "persist"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "confidential"

[[variables]]
name = "iteration"
type = { kind = "map", value_type = { kind = "boolean" }, max_entries = 1 }
scope = "run"
producer = "repeat"
consumers = ["repeat"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[nodes]]
id = "fresh-context"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "fresh" }

[[nodes]]
id = "research"
kind = "model_call"
provider = "deterministic-mock"
write_variables = ["model_disposition", "model_result"]
retry_limit = 2
configuration = { type = "model_request", disposition_output = "model_disposition", result_output = "model_result" }

[[nodes]]
id = "tool-batch"
kind = "tool_execution_gate"
read_variables = ["model_disposition", "model_result"]
write_variables = ["research_receipt"]
read_scopes = ["workspace"]
configuration = { type = "provider_tool_batch_execution", request_reference_variable = "model_result", disposition_variable = "model_disposition", maximum_calls = 32, allowed_tools = ["filesystem.read"] }

[[nodes]]
id = "persist"
kind = "persist_artifact"
read_variables = ["research_receipt"]
write_variables = ["receipt_artifact"]
configuration = { type = "persist_artifact", content = { kind = "provider_result_text", reference_variable = "research_receipt" }, mime_type = "text/markdown", security = "private", retention = "session" }

[[nodes]]
id = "repeat"
kind = "loop"
read_variables = ["iteration"]
write_variables = ["iteration"]
max_iterations = 3

[[nodes]]
id = "done"
kind = "complete_session"
read_variables = ["receipt_artifact"]

[[edges]]
from = "fresh-context"
to = "research"

[[edges]]
from = "research"
to = "tool-batch"

[[edges]]
from = "tool-batch"
to = "persist"

[[edges]]
from = "persist"
to = "repeat"

[[edges]]
from = "repeat"
to = "fresh-context"
condition = "iteration.remaining == true"
label = "continue"

[[edges]]
from = "repeat"
to = "done"
condition = "iteration.remaining == false"
label = "complete"
'''

[[interceptors]]
id = "runtime-style-policy"
owner = "runtime.security"
event = "action.proposed"
stage = 10
priority = 100
before = []
after = []
supported_decisions = [
  "continue",
  "replace",
  "reject",
  "require_approval",
  "cancel",
]
required_capabilities = ["approval"]

[memory]
provider = "file"
scopes = ["session"]
retrieval_timing = "iteration_start"
max_items = 64
max_injected_bytes = 524288
write_policy = "iteration_completion"
injection_location = "before_current_input"

[memory.query]
source = "session_goal"
include_active_artifacts = true
include_style_context = true
max_query_bytes = 32768

[compaction]
strategy = "none"
reserved_context_tokens = 0
max_provider_projection_tokens = 0
preserve_unresolved_tasks = true
preserve_active_processes = true
preservation_requirements = [
  "system_instructions",
  "current_input",
  "pending_control_state",
  "artifact_references",
  "memory_provenance",
  "active_graph_state",
  "tool_call_correlation",
]

[approvals]
default = "ask"
groups = { "filesystem.read" = "allow" }

[budgets]
max_iterations = 16
max_steps = 500
max_tokens = 750000
max_cost_micros = 75000000
max_duration_ms = 2700000

[child_agents]
max_children = 0
max_concurrent = 0
max_depth = 0
per_child_token_budget = 0

[retry]
max_attempts = 4
initial_backoff_ms = 0
max_backoff_ms = 0
retryable_failures = ["provider.unavailable"]

[termination]
allowed_outcomes = ["complete_session", "fail"]
on_hard_limit = "fail"
require_explicit_terminal_node = true

[selection]
requires_explicit_selection = true
model_may_select = false
TOML

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS=10000
runtime="$repository/target/debug/agentmod-runtime"
cli="$repository/target/debug/agentmod"

stop_runtime() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
        daemon_pid=
    fi
}

cleanup() {
    if [ -n "${turn_pid:-}" ]; then
        kill "$turn_pid" 2>/dev/null || true
        wait "$turn_pid" 2>/dev/null || true
        turn_pid=
    fi
    stop_runtime
    unset AGENTMOD_RUNTIME_ENDPOINT AGENTMOD_RUNTIME_AUTH_TOKEN
    unset AGENTMOD_HARNESS_PROGRAM AGENTMOD_FIXTURE_HARNESS_PROGRAM
    unset AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-iteration-memory-e2e.*)
            rm -rf -- "$run_root"
            ;;
        *)
            echo "refusing to remove unexpected E2E root: $run_root" >&2
            ;;
    esac
}
trap cleanup EXIT INT TERM

start_runtime() {
    (cd "$run_root" && "$runtime" serve) \
        >"$run_root/runtime.stdout.log" \
        2>"$run_root/runtime.stderr.log" &
    daemon_pid=$!
    attempt=0
    until doctor=$("$cli" doctor --json 2>/dev/null); do
        attempt=$((attempt + 1))
        if [ "$attempt" -ge 100 ]; then
            cat "$run_root/runtime.stderr.log" >&2 || true
            echo "runtime did not become ready" >&2
            exit 1
        fi
        sleep 0.1
    done
    printf '%s' "$doctor" | python3 -c '
import json
import sys
assert json.load(sys.stdin)["state"] == "ready"
'
}

event_count() {
    grep -c -F "\"event_type\":\"$1\"" "$journal" || true
}

start_runtime
"$cli" style validate "$style_root/iteration-memory.toml" --json |
    python3 -c 'import json, sys; assert json.load(sys.stdin)["valid"] is True'
created=$("$cli" session create --workspace "$repository" \
    --style e2e-iteration-memory --json)
session_id=$(printf '%s' "$created" |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["session_id"])')
run_id=018f0f5d-8c2a-7d31-9e42-4f6b8a1c2d3e
journal="$run_root/sessions/$session_id/events.jsonl"
memory_file="$run_root/memory/file.jsonl"

"$cli" run "map the repository architecture" --session "$session_id" \
    --cancellation-id "$run_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="generic research finding"' --json \
    >"$run_root/cut-turn.stdout.log" 2>"$run_root/cut-turn.stderr.log" &
turn_pid=$!

attempt=0
while :; do
    if ! kill -0 "$turn_pid" 2>/dev/null; then
        cat "$run_root/cut-turn.stderr.log" >&2 || true
        echo "turn exited before the iteration-memory crash cut" >&2
        exit 1
    fi
    if [ -f "$journal" ] && [ -f "$memory_file" ] &&
        [ "$(event_count memory.write_dispatched)" -eq 1 ] &&
        [ "$(wc -l <"$memory_file" | tr -d ' ')" -eq 1 ]; then
        break
    fi
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 300 ]; then
        echo "iteration memory did not reach the post-persist crash cut" >&2
        exit 1
    fi
    sleep 0.05
done
test "$(event_count memory.write_completed)" -eq 0
for event_type in model.request_proposed model.request_approved \
    model.request_started model.response_completed
do
    test "$(event_count "$event_type")" -eq 3
done

stop_runtime
wait "$turn_pid" 2>/dev/null || true
turn_pid=

export AGENTMOD_MEMORY_WRITE_POST_PERSIST_DELAY_MS=0
export AGENTMOD_HARNESS_PROGRAM="$run_root/harness-must-not-be-spawned"
export AGENTMOD_FIXTURE_HARNESS_PROGRAM="$run_root/fixture-harness-must-not-be-spawned"
start_runtime
"$cli" run "map the repository architecture" --session "$session_id" \
    --cancellation-id "$run_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="generic research finding"' --json \
    >"$run_root/recovered.json"
python3 - "$run_root/recovered.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    assert json.load(source)["events"] == []
PY

for event_type in model.request_proposed model.request_approved \
    model.request_started model.response_completed
do
    test "$(event_count "$event_type")" -eq 3
done
for event_type in memory.write_proposed memory.write_approved \
    memory.write_dispatched memory.write_completed
do
    test "$(event_count "$event_type")" -eq 3
done
test "$(event_count model.request_failed)" -eq 0
test "$(event_count model.request_cancelled)" -eq 0
test "$(wc -l <"$memory_file" | tr -d ' ')" -eq 3

"$cli" session inspect "$session_id" --json >"$run_root/inspection.json"
python3 - "$journal" "$memory_file" "$run_root/inspection.json" \
    "$session_id" "$run_id" <<'PY'
import json
import re
import sys
from collections import Counter, defaultdict

journal_path, memory_path, inspection_path, session_id, run_id = sys.argv[1:]
with open(journal_path, encoding="utf-8") as source:
    events = [json.loads(line)["event"] for line in source]
with open(memory_path, encoding="utf-8") as source:
    file_records = [json.loads(line) for line in source]
with open(inspection_path, encoding="utf-8") as source:
    inspection = json.load(source)

event_types = [event["metadata"]["event_type"] for event in events]
for event_type in (
    "model.request_proposed",
    "model.request_approved",
    "model.request_started",
    "model.response_completed",
    "memory.write_proposed",
    "memory.write_approved",
    "memory.write_dispatched",
    "memory.write_completed",
):
    assert event_types.count(event_type) == 3, event_type
assert event_types.count("model.request_failed") == 0
assert event_types.count("model.request_cancelled") == 0

transitions = {
    event["metadata"]["sequence"]: event
    for event in events
    if event["metadata"]["event_type"] == "style.transition_selected"
    and event["payload"]["payload"]["from_node_id"] == "repeat"
}
assert len(transitions) == 3

memory_events = [
    event
    for event in events
    if event["metadata"]["event_type"].startswith("memory.write_")
]
groups = defaultdict(list)
for event in memory_events:
    identity = event["payload"]["payload"]["identity"]
    groups[identity["write_id"]].append(event)
assert len(groups) == 3
assert len(file_records) == 3
expected_lifecycle = [
    "memory.write_proposed",
    "memory.write_approved",
    "memory.write_dispatched",
    "memory.write_completed",
]
sources = set()
iterations = set()
for write_id, lifecycle in groups.items():
    lifecycle.sort(key=lambda event: event["metadata"]["sequence"])
    assert [event["metadata"]["event_type"] for event in lifecycle] == (
        expected_lifecycle
    )
    payloads = [event["payload"]["payload"] for event in lifecycle]
    identity = payloads[0]["identity"]
    assert all(payload["identity"] == identity for payload in payloads)
    digest = payloads[1]["action_digest"]
    assert payloads[2]["action_digest"] == digest
    assert payloads[3]["action_digest"] == digest
    assert identity["policy"] == "iteration_completion"
    assert identity["provider"] == "file"
    assert identity["scope"] == f"session:{session_id}"
    assert identity["run_id"] == run_id
    assert identity.get("session_completion") is None
    boundary = identity["iteration_completion"]
    assert boundary is not None

    source_pattern = re.compile(
        "^runtime[.]automatic_memory:iteration_completion:"
        + re.escape(run_id)
        + ":[0-9a-f]{64}$"
    )
    assert source_pattern.fullmatch(identity["source"])
    assert identity["source"] not in sources
    sources.add(identity["source"])
    assert re.fullmatch(r"[0-9a-f]{64}", write_id)

    transition = transitions[boundary["sequence"]]
    selected = transition["payload"]["payload"]
    assert boundary["loop_node_id"] == selected["from_node_id"]
    assert boundary["destination_node_id"] == selected["to_node_id"]
    assert boundary["attempt"] == selected["attempt"]
    assert boundary["loop_iteration"] == selected["loop_iteration"]
    assert boundary["step"] == selected["step"]
    assert boundary["event_checksum"] == transition["integrity_checksum"]
    assert boundary["loop_iteration"] not in iterations
    iterations.add(boundary["loop_iteration"])

    typed = json.loads(payloads[0]["content"])
    assert typed["schema"] == "agentmod.iteration-completion-memory.v1"
    assert typed["successful_iteration"] == boundary
    assert typed["artifact_references"]
    persist = [
        output
        for output in typed["node_outputs"]
        if output["node_id"] == "persist"
        and output["loop_iteration"] == boundary["loop_iteration"]
        and output["artifact_reference"] is not None
    ]
    assert len(persist) == 1
    assert persist[0]["artifact_reference"] in typed["artifact_references"]

    matching_file = [
        record for record in file_records
        if record["source"] == identity["source"]
    ]
    assert len(matching_file) == 1
    record = matching_file[0]
    assert payloads[3]["retained"] is True
    assert record["schema_version"] == 1
    assert record["id"] == payloads[3]["reference"]
    assert record["scope"] == identity["scope"]
    assert record["content"] == payloads[0]["content"]
    assert record["created_at_millis"] == identity["created_at_millis"]
assert iterations == {0, 1, 2}

state = inspection["state"]
assert len(state["successful_iteration_completions"]) == 3
assert len(state["automatic_memory_writes"]) == 3
for record in state["automatic_memory_writes"].values():
    assert record["state"] == "completed"
    assert record["retained"] is True
PY

cp "$journal" "$run_root/journal.before"
cp "$memory_file" "$run_root/memory.before"
cp "$run_root/inspection.json" "$run_root/inspection.before"
stop_runtime
start_runtime
"$cli" session replay "$session_id" --json >"$run_root/replayed.json"
python3 - "$run_root/inspection.before" "$run_root/replayed.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as source:
    before = json.load(source)["state"]
with open(sys.argv[2], encoding="utf-8") as source:
    replay = json.load(source)
assert replay["command"] == "session_replay"
after = replay["state"]
assert after["last_sequence"] == before["last_sequence"]
assert after["automatic_memory_writes"] == before["automatic_memory_writes"]
assert (
    after["successful_iteration_completions"]
    == before["successful_iteration_completions"]
)
PY
cmp -s "$run_root/journal.before" "$journal"
cmp -s "$run_root/memory.before" "$memory_file"

echo "runtime iteration-memory 3-loop/v2/crash/restart/replay E2E passed"
