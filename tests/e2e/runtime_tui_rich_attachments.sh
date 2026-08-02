#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_root="$(mktemp -d)"
runtime_pid=""

stop_runtime() {
  if [[ -n "$runtime_pid" ]]; then
    kill "$runtime_pid" 2>/dev/null || true
    wait "$runtime_pid" 2>/dev/null || true
    runtime_pid=""
  fi
}

cleanup() {
  status=$?
  stop_runtime
  if [[ $status -ne 0 ]]; then
    for log in "$run_root"/*.log; do
      if [[ -f "$log" ]]; then
        echo "--- $(basename "$log") ---" >&2
        cat "$log" >&2
      fi
    done
  fi
  rm -rf -- "$run_root"
}
trap cleanup EXIT
cd "$repository"

cargo build --locked -p agentmod-runtime -p agentmod-harness \
  -p agentmod-cli -p agentmod-tui
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
tui="$repository/target/debug/agentmod-tui"
driver="$repository/tests/e2e/runtime_tui_rich_attachments_driver.py"
workspace="$run_root/workspace"
image="$workspace/pixel.png"
blob="$workspace/evidence.bin"
state="$run_root/rich-state.json"
mkdir -p "$workspace"
python3 - "$image" "$blob" "$run_root/outside.bin" <<'PY'
import base64
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_bytes(base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
))
pathlib.Path(sys.argv[2]).write_bytes(b"tui-rich-blob-evidence")
pathlib.Path(sys.argv[3]).write_bytes(b"outside-workspace")
PY
ln -s "$run_root/outside.bin" "$workspace/linked.bin"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_POLL_MS=0

start_runtime() {
  local log_stem="$1"
  (
    cd "$run_root"
    exec "$runtime" serve
  ) >"$run_root/$log_stem.log" 2>&1 &
  runtime_pid="$!"
  for _ in $(seq 1 100); do
    if "$cli" doctor --json >/dev/null 2>&1; then
      return
    fi
    if ! kill -0 "$runtime_pid" 2>/dev/null; then
      echo "runtime stopped before becoming ready" >&2
      return 1
    fi
    sleep 0.1
  done
  echo "runtime did not become ready" >&2
  return 1
}

start_runtime runtime-initial
created="$($cli session create --workspace "$workspace" \
  --style persistent-chat --json)"
session_id="$(printf '%s' "$created" |
  sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
test -n "$session_id"
if "$tui" --smoke-session-command "$session_id" \
  "/attach ../outside.bin" >/dev/null 2>&1; then
  echo "TUI accepted a workspace escape" >&2
  exit 1
fi
if "$tui" --smoke-session-command "$session_id" \
  "/attach linked.bin" >/dev/null 2>&1; then
  echo "TUI accepted a symbolic-link attachment" >&2
  exit 1
fi

turn="$($tui --smoke-attachment-turn \
  "inspect the TUI image and blob" "pixel.png" "evidence.bin")"
grep -q "attachments=2" <<<"$turn"
grep -q "pending_after_submit=0" <<<"$turn"
grep -q "deterministic response" <<<"$turn"
grep -q "turn committed" <<<"$turn"
python3 "$driver" --phase execute --root "$run_root" \
  --session "$session_id" --image "$image" --blob "$blob" \
  --state "$state" --cli "$cli" --tui "$tui"

stop_runtime
start_runtime runtime-restarted
python3 "$driver" --phase replay --root "$run_root" \
  --session "$session_id" --image "$image" --blob "$blob" \
  --state "$state" --cli "$cli" --tui "$tui"

echo "runtime TUI rich-attachment/restart E2E passed"
