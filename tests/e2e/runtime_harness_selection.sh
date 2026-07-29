#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-harness-e2e.XXXXXX")"
workspace="$run_root/workspace"
mkdir -p "$workspace" "$run_root/styles/user"
cp "$repository/tests/fixtures/styles/fixture-harness-chat.toml" "$run_root/styles/user/"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_FIXTURE_HARNESS_PROGRAM="$harness"
daemon=""

cleanup() {
  if [[ -n "$daemon" ]] && kill -0 "$daemon" 2>/dev/null; then
    kill "$daemon" 2>/dev/null || true
    wait "$daemon" 2>/dev/null || true
  fi
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-harness-e2e.*) rm -rf -- "$run_root" ;;
  esac
}
trap cleanup EXIT

start_runtime() {
  "$runtime" serve >"$run_root/runtime.stdout.log" 2>"$run_root/runtime.stderr.log" &
  daemon=$!
  for _ in $(seq 1 100); do
    if "$cli" doctor --json >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  cat "$run_root/runtime.stderr.log"
  return 1
}

stop_runtime() {
  kill "$daemon"
  wait "$daemon" 2>/dev/null || true
  daemon=""
}

start_runtime
catalog="$("$cli" harness list --json)"
python3 - "$catalog" <<'PY'
import json, sys
rows = {row["id"]: row for row in json.loads(sys.argv[1])["harnesses"]}
assert rows["native"]["availability"] == "available"
assert rows["fixture"]["availability"] == "available"
assert "tool_calls" in rows["fixture"]["capabilities"]
assert "images" not in rows["fixture"]["capabilities"]
PY

native="$("$cli" session create --workspace "$workspace" --style persistent-chat --harness native --json)"
fixture="$("$cli" session create --workspace "$workspace" --style persistent-chat --harness fixture --json)"
native_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])' <<<"$native")"
fixture_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])' <<<"$fixture")"

if "$cli" session create --workspace "$workspace" --style fixture-harness-incompatible --json >/dev/null 2>&1; then
  echo "incompatible style/harness combination was accepted" >&2
  exit 1
fi

for item in "$native_id:native:native-ok" "$fixture_id:fixture:fixture-ok"; do
  IFS=: read -r session_id harness_id output <<<"$item"
  inspection="$("$cli" session inspect "$session_id" --json)"
  python3 - "$inspection" "$harness_id" <<'PY'
import json, sys
binding = json.loads(sys.argv[1])["state"]["style_binding"]
assert binding["harness"] == sys.argv[2]
assert binding["harness_version"] == "1.0.0"
assert binding["harness_capability_set_hash"]
PY
  "$cli" run "run through $harness_id" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' --option "mock_text=\"$output\"" --json >/dev/null
  grep -q "\"harness\":\"$harness_id\"" "$run_root/sessions/$session_id/events.jsonl"
done

stop_runtime
start_runtime
recovered="$("$cli" session inspect "$fixture_id" --json)"
python3 - "$recovered" <<'PY'
import json, sys
state = json.loads(sys.argv[1])["state"]
assert state["style_binding"]["harness"] == "fixture"
assert state["style_compatibility"]["status"] == "compatible"
PY
"$cli" run "fixture after restart" --session "$fixture_id" \
  --option 'mock_scenario="streaming_text"' --option 'mock_text="fixture-restarted"' --json >/dev/null
echo "runtime harness registry/selection/capability/restart E2E passed"
