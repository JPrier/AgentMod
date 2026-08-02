#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness \
  -p agentmod-scheduler -p agentmod-cli

target_dir="${CARGO_TARGET_DIR:-target}"
if [[ "$target_dir" != /* ]]; then
  target_dir="$repository/$target_dir"
fi
runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
scheduler="$target_dir/debug/agentmod-scheduler"
cli="$target_dir/debug/agentmod"
run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-graph-schedule-e2e.XXXXXX")"
workspace="$run_root/workspace"
user_styles="$run_root/styles/user"
mkdir -p "$workspace" "$user_styles"

wake_timestamp="$(python3 - <<'PY'
from datetime import datetime, timedelta, timezone
value = datetime.now(timezone.utc) + timedelta(seconds=1)
print(value.isoformat(timespec="milliseconds").replace("+00:00", "Z"))
PY
)"
sed "s/2099-01-01T00:00:00Z/$wake_timestamp/" \
  "$repository/tests/fixtures/styles/arbitrary-graph-schedule.toml" \
  >"$user_styles/arbitrary-graph-schedule.toml"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_PROGRAM="$scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"
export AGENTMOD_PERMISSION_MODE=allow
export AGENTMOD_SCHEDULER_POLL_MS=0
daemon_pid=""
launch_counter=0
succeeded=false

stop_runtime() {
  if [[ -n "$daemon_pid" ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    kill "$daemon_pid" 2>/dev/null || true
    wait "$daemon_pid" 2>/dev/null || true
  fi
  daemon_pid=""
}

cleanup() {
  stop_runtime
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-graph-schedule-e2e.*)
      if [[ "$succeeded" == true ]]; then
        rm -rf -- "$run_root"
      else
        echo "retained failed schedule graph E2E root: $run_root" >&2
      fi
      ;;
    *) echo "refusing unexpected schedule graph E2E root: $run_root" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM

start_runtime() {
  launch_counter=$((launch_counter + 1))
  runtime_log="$run_root/runtime-$launch_counter.stderr.log"
  (cd "$run_root" && exec "$runtime" serve) \
    >"$run_root/runtime-$launch_counter.stdout.log" \
    2>"$runtime_log" &
  daemon_pid=$!
  for _ in $(seq 1 100); do
    if "$cli" doctor --json >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  cat "$runtime_log" >&2
  echo "runtime did not become ready" >&2
  exit 1
}

event_count() {
  grep -c "\"event_type\":\"$1\"" "$journal" || true
}

assert_event_count() {
  actual=$(event_count "$1")
  if [[ "$actual" -ne "$2" ]]; then
    echo "expected $2 $1 events, found $actual" >&2
    exit 1
  fi
}

assert_plan() {
  python3 -c '
import json, sys
state = json.load(sys.stdin)["state"]
binding = state["style_binding"]
plan = binding["execution_plan"]
assert binding["id"] == "user-graph-schedule"
assert binding["version"] == "1.0.0"
assert plan["compilation"]["compiler"] == "agentmod-runtime-node-plan@3"
assert binding["execution_plan_hash"]
assert plan["registry_hash"]
expected = {
    "await-wake": "runtime.schedule",
    "finish": "runtime.session-completion",
}
assert len(plan["nodes"]) == len(expected)
for node in plan["nodes"]:
    assert expected[node["node_id"]] == node["executor_id"]
    assert node["executor_version"] == "1.0.0"
    assert node["source"] == {"kind": "runtime"}
    assert node["boundary"] == "runtime_logic"
' <<<"$1"
}

start_runtime
validation=$("$cli" style validate \
  "$user_styles/arbitrary-graph-schedule.toml" --json)
if ! python3 -c 'import json,sys; assert json.load(sys.stdin)["valid"]' \
  <<<"$validation"; then
  echo "arbitrary schedule graph did not validate: $validation" >&2
  exit 1
fi
style=$("$cli" style inspect user-graph-schedule@1.0.0 --json)
python3 -c '
import json,sys
summary=json.load(sys.stdin)["summary"]
assert summary["availability"] == "available"
assert summary["source"] == "user"
' <<<"$style"

session=$("$cli" session create --workspace "$workspace" \
  --style user-graph-schedule@1.0.0 --json)
session_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])' \
  <<<"$session")
journal="$run_root/sessions/$session_id/events.jsonl"
style_lock="$run_root/sessions/$session_id/style.lock"
created=$("$cli" session inspect "$session_id" --json)
assert_plan "$created"
readarray -t plan_identity < <(python3 -c '
import json,sys
binding=json.load(sys.stdin)["state"]["style_binding"]
print(binding["execution_plan_hash"])
print(binding["execution_plan"]["registry_hash"])
' <<<"$created")
plan_hash="${plan_identity[0]}"
registry_hash="${plan_identity[1]}"
style_lock_before=$(sha256sum "$style_lock" | cut -d " " -f 1)

waiting=$("$cli" run "execute exact one-time schedule graph" \
  --session "$session_id" --provider deterministic-mock \
  --model mock-model --json)
continuation_id=$(python3 -c '
import json,sys
value=json.load(sys.stdin).get("awaiting_continuation")
assert value
print(value)
' <<<"$waiting")
stored_inspection=$("$cli" session inspect "$session_id" --json)
assert_plan "$stored_inspection"
PLAN_HASH="$plan_hash" REGISTRY_HASH="$registry_hash" \
CONTINUATION_ID="$continuation_id" python3 -c '
import json, os, sys
state=json.load(sys.stdin)["state"]
contract=state["style_execution"]["execution_contract"]
assert contract["execution_plan_hash"] == os.environ["PLAN_HASH"]
assert contract["registry_hash"] == os.environ["REGISTRY_HASH"]
assert len(contract["node_executors"]) == 2
records=list(state["style_execution"]["graph_schedules"].values())
assert len(records) == 1
record=records[0]
assert record["state"] == "stored"
assert record["wait_for_trigger"] is True
assert record["identity"]["continuation_id"] == os.environ["CONTINUATION_ID"]
' <<<"$stored_inspection"

for expectation in \
  "style.execution_initialized 1" \
  "graph.schedule_resolved 1" \
  "graph.schedule_approved 1" \
  "graph.schedule_dispatched 1" \
  "graph.schedule_stored 1" \
  "scheduler.fired 0" \
  "graph.node_wait_resolved 0"
do
  # shellcheck disable=SC2086
  assert_event_count $expectation
done
PLAN_HASH="$plan_hash" STORED_INSPECTION="$stored_inspection" \
CONTINUATION_ID="$continuation_id" python3 - "$journal" <<'PY'
import json, os, pathlib, sys
events=[json.loads(line)["event"] for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
payloads={event["metadata"]["event_type"]: event["payload"]["payload"] for event in events}
resolved=payloads["graph.schedule_resolved"]
approved=payloads["graph.schedule_approved"]
dispatched=payloads["graph.schedule_dispatched"]
stored=payloads["graph.schedule_stored"]
binding=json.loads(os.environ["STORED_INSPECTION"])["state"]["style_binding"]
plan_node=next(node for node in binding["execution_plan"]["nodes"] if node["node_id"] == "await-wake")
assert resolved["identity"]["work"]["node_id"] == "await-wake"
assert resolved["identity"]["execution_plan_hash"] == os.environ["PLAN_HASH"]
assert resolved["identity"]["configuration_hash"] == plan_node["adapter_configuration_reference"]
assert resolved["trigger"]["kind"] == "at_millis"
assert resolved["wait_for_trigger"] is True
assert resolved["consequential"] is True
assert resolved["cancellation"] == "cancel_trigger"
assert resolved["identity"]["schedule_id"] == approved["identity"]["schedule_id"]
assert resolved["identity"]["schedule_id"] == dispatched["identity"]["schedule_id"]
assert resolved["identity"]["schedule_id"] == stored["identity"]["schedule_id"]
assert resolved["identity"]["idempotency_id"] == approved["identity"]["idempotency_id"]
assert resolved["identity"]["continuation_id"] == os.environ["CONTINUATION_ID"]
assert approved["action_digest"]
assert approved["action_digest"] == dispatched["action_digest"] == stored["action_digest"]
assert stored["replayed"] is False
PY

stop_runtime
export AGENTMOD_SCHEDULER_POLL_MS=25
start_runtime
completed=""
for _ in $(seq 1 300); do
  completed=$("$cli" session inspect "$session_id" --json)
  if python3 -c '
import json,sys
raise SystemExit(0 if json.load(sys.stdin)["state"]["lifecycle"] == "completed" else 1)
' <<<"$completed"; then
    break
  fi
  sleep 0.05
done
python3 -c '
import json,sys
state=json.load(sys.stdin)["state"]
assert state["lifecycle"] == "completed"
assert state["style_execution"]["termination_reason"] == "complete_session"
completed=[node["node_id"] for node in state["style_execution"]["completed_nodes"]]
assert completed.count("await-wake") == 1
assert completed.count("finish") == 1
' <<<"$completed"
assert_plan "$completed"
PLAN_HASH="$plan_hash" REGISTRY_HASH="$registry_hash" python3 -c '
import json,os,sys
binding=json.load(sys.stdin)["state"]["style_binding"]
assert binding["execution_plan_hash"] == os.environ["PLAN_HASH"]
assert binding["execution_plan"]["registry_hash"] == os.environ["REGISTRY_HASH"]
' <<<"$completed"
test "$(sha256sum "$style_lock" | cut -d " " -f 1)" = "$style_lock_before"

for expectation in \
  "style.execution_initialized 1" \
  "graph.schedule_resolved 1" \
  "graph.schedule_approved 1" \
  "graph.schedule_dispatched 1" \
  "graph.schedule_stored 1" \
  "scheduler.fired 1" \
  "graph.node_wait_resolved 1"
do
  # shellcheck disable=SC2086
  assert_event_count $expectation
done
execution_id=$(python3 - "$journal" <<'PY'
import json, pathlib, sys
events=[json.loads(line)["event"] for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
fired=[event for event in events if event["metadata"]["event_type"] == "scheduler.fired"]
resolved=[event for event in events if event["metadata"]["event_type"] == "graph.node_wait_resolved"]
assert len(fired) == len(resolved) == 1
assert resolved[0]["payload"]["payload"]["disposition"] == "resumed"
execution_id=fired[0]["payload"]["payload"]["execution_id"]
assert execution_id
print(execution_id)
PY
)
test -f "$AGENTMOD_SCHEDULER_ROOT/executions/$execution_id.succeeded"

before_replay_count=$(wc -l <"$journal" | tr -d " ")
stop_runtime
start_runtime
sleep 0.25
pending=$("$cli" schedule claim --limit 4 --json)
python3 -c 'import json,sys; assert json.load(sys.stdin)["executions"] == []' \
  <<<"$pending"
replayed=$("$cli" session replay "$session_id" --json)
assert_plan "$replayed"
PLAN_HASH="$plan_hash" python3 -c '
import json,os,sys
state=json.load(sys.stdin)["state"]
assert state["lifecycle"] == "completed"
assert state["style_binding"]["execution_plan_hash"] == os.environ["PLAN_HASH"]
' <<<"$replayed"
test "$(wc -l <"$journal" | tr -d " ")" -eq "$before_replay_count"

succeeded=true
echo "runtime arbitrary schedule graph exact-plan/policy/outbox/restart/wake-once/replay E2E passed"
