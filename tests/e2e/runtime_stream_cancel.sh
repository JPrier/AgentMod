#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-stream-cancel-e2e-XXXXXX")"
daemon_pid=""
turn_pid=""
cleanup() {
  if [[ -n "$turn_pid" ]] && kill -0 "$turn_pid" 2>/dev/null; then
    kill "$turn_pid" 2>/dev/null || true
  fi
  if [[ -n "$daemon_pid" ]] && kill -0 "$daemon_pid" 2>/dev/null; then
    kill "$daemon_pid" 2>/dev/null || true
  fi
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-stream-cancel-e2e-*) rm -rf -- "$run_root" ;;
  esac
}
trap cleanup EXIT

workspace="$run_root/workspace"
mkdir -p "$workspace"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_HARNESS_FRAME_PACING_MS=750

(
  cd "$run_root"
  "$runtime" serve
) &
daemon_pid=$!

ready=false
for _ in $(seq 1 50); do
  if "$cli" doctor --json >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 0.1
done
[[ "$ready" == true ]] || { echo "runtime did not become ready" >&2; exit 1; }

created="$("$cli" session create --workspace "$workspace" --style persistent-chat --json)"
session_id="$(printf '%s' "$created" | python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')"
cancellation_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
turn_stdout="$run_root/turn.stdout"
turn_stderr="$run_root/turn.stderr"
"$cli" run stream-until-cancelled \
  --session "$session_id" \
  --option mock_scenario=streaming_text \
  --cancellation-id "$cancellation_id" \
  --json >"$turn_stdout" 2>"$turn_stderr" &
turn_pid=$!

sleep 1.1
cancelled=false
for _ in $(seq 1 20); do
  if "$cli" cancel "$cancellation_id" --reason "E2E cancellation" --json \
      >"$run_root/cancel.json" 2>/dev/null; then
    cancelled=true
    break
  fi
  sleep 0.1
done
[[ "$cancelled" == true ]] || { echo "active cancellation was not accepted" >&2; exit 1; }
wait "$turn_pid"
turn_pid=""

python3 - "$turn_stdout" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as handle:
    result = json.load(handle)
kinds = [event["event"] for event in result["events"]]
assert "text" in kinds, kinds
assert "cancelled" in kinds, kinds
assert "completed" not in kinds, kinds
PY

journal="$run_root/sessions/$session_id/events.jsonl"
grep -q '"event_type":"model.output_delta_observed"' "$journal"
grep -q '"event_type":"model.request_cancelled"' "$journal"
if grep -q '"event_type":"model.response_completed"' "$journal"; then
  echo "cancelled request committed completion" >&2
  exit 1
fi

next="$("$cli" run new-request-after-cancellation --session "$session_id" --json)"
printf '%s' "$next" | grep -q '"event":"completed"'
echo "runtime incremental stream cancellation E2E passed"
