#!/usr/bin/env bash
# Runtime-supervised live-provider fixture E2E (Unix): starts a deterministic
# local OpenAI-compatible HTTP fixture, configures the supervised native harness
# to use the live `local` provider adapter through the curated
# `AGENTMOD_PROVIDER_*` environment channel, and drives a real runtime session
# through the full service -> runtime -> harness -> live-provider path.
# Requires no network and no credentials.
set -euo pipefail 2>/dev/null || set -eu

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"
run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-live-fixture.XXXXXX")"
target_dir="$run_root/target"
export CARGO_TARGET_DIR="$target_dir"
cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-scheduler

runtime="$target_dir/debug/agentmod-runtime"
harness="$target_dir/debug/agentmod-harness"
cli="$target_dir/debug/agentmod"
scheduler="$target_dir/debug/agentmod-scheduler"
workspace="$run_root/workspace"
sessions="$run_root/sessions"
styles_user="$run_root/styles/user"
mkdir -p "$workspace" "$sessions" "$styles_user"
cp "$repository/tests/fixtures/styles/live-provider-chat.toml" "$styles_user/"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"
export AGENTMOD_SCHEDULER_PROGRAM="$scheduler"

# Deterministic local OpenAI-compatible fixture server in a background shell.
PYTHON="${PYTHON:-python3}"
"$PYTHON" "$repository/tests/fixtures/live_provider_fixture_server.py"   "$run_root/fixture-port" >"$run_root/fixture.log" 2>&1 &
fixture_pid=$!
daemon=""

cleanup() {
  kill "$fixture_pid" 2>/dev/null || true
  if [[ -n "$daemon" ]] && kill -0 "$daemon" 2>/dev/null; then
    kill "$daemon" 2>/dev/null || true
    wait "$daemon" 2>/dev/null || true
  fi
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-live-fixture.*) rm -rf -- "$run_root" ;;
  esac
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  [[ -s "$run_root/fixture-port" ]] && break
  sleep 0.2
done
fixture_port="$(cat "$run_root/fixture-port")"
if [[ -z "$fixture_port" ]]; then
  echo "fixture server did not start" >&2
  exit 1
fi

# Configure the live `local` provider for the supervised harness process.
# These variables are forwarded through the runtime's curated env channel.
export AGENTMOD_PROVIDER_LOCAL_BASE_URL="http://127.0.0.1:${fixture_port}/v1"
export AGENTMOD_PROVIDER_LOCAL_MODELS="fixture-model"

start_runtime() {
  (
    cd "$run_root"
    exec "$runtime" serve
  ) >"$run_root/runtime.stdout.log" 2>"$run_root/runtime.stderr.log" &
  daemon=$!
  for _ in $(seq 1 150); do
    if "$cli" doctor --json >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  cat "$run_root/runtime.stderr.log"
  return 1
}

start_runtime

session="$("$cli" session create --workspace "$workspace" \
  --style live-provider-chat --harness native \
  --memory none --compaction none \
  --max-iterations 1 --max-steps 20 --max-tokens 10000 \
  --max-cost-micros 1000000 --max-duration-ms 60000 --json \
  | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')"

result="$("$cli" run "hello from live fixture" --session "$session" \
  --provider local --model fixture-model \
  --option "base_url=http://127.0.0.1:${fixture_port}/v1" --json)"

text="$(printf '%s' "$result" | python3 -c 'import json,sys
d=json.load(sys.stdin)
print("".join(e.get("text","") for e in d.get("events",[]) if e.get("event")=="text"))')"

if [[ "$text" != *"runtime-live-fixture-ok"* ]]; then
  echo "runtime-supervised live fixture did not produce provider output" >&2
  echo "result: $result" >&2
  echo "runtime stderr:" >&2
  cat "$run_root/runtime.stderr.log" >&2 || true
  exit 1
fi

echo "runtime-supervised live-provider fixture E2E passed: $text"
