#!/usr/bin/env sh
set -eu

repository="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli
run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-large-branch-e2e.XXXXXX")"
workspace="$run_root/workspace"
mkdir -p "$workspace"
runtime="$repository/target/debug/agentmod-runtime"
harness="$repository/target/debug/agentmod-harness"
cli="$repository/target/debug/agentmod"
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN='0123456789abcdef0123456789abcdef0123456789abcdef'
export AGENTMOD_HARNESS_PROGRAM="$harness"
daemon_pid=''
cleanup() {
  [ -z "$daemon_pid" ] || kill "$daemon_pid" 2>/dev/null || true
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-large-branch-e2e.*) rm -rf -- "$run_root" ;;
  esac
}
trap cleanup EXIT INT TERM
(cd "$run_root" && "$runtime" serve >runtime.out 2>runtime.err) &
daemon_pid=$!
attempt=0
until "$cli" doctor --json >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  [ "$attempt" -lt 50 ] || exit 1
  sleep 0.1
done
created="$("$cli" session create --workspace "$workspace" --style persistent-chat --json)"
parent="$(printf '%s' "$created" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
turn_index=1
while [ "$turn_index" -le 17 ]; do
  "$cli" run "parent-$turn_index" --session "$parent" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"answer-$turn_index\"" --json >/dev/null
  turn_index=$((turn_index + 1))
done
parent_before="$("$cli" session inspect "$parent" --json)"
head="$(printf '%s' "$parent_before" | sed -n 's/.*"head_sequence":\([0-9][0-9]*\).*/\1/p')"
branched="$("$cli" session branch "$parent" --at "$head" --json)"
child="$(printf '%s' "$branched" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
child_before="$("$cli" session inspect "$child" --json)"
printf '%s' "$child_before" | grep -F '"method":"branch_artifact_handoff"' >/dev/null
artifact_id="$(printf '%s' "$child_before" | sed -n 's/.*"artifact_id":"\([^"]*\)".*/\1/p' | head -n 1)"
test -n "$artifact_id"
artifact="$run_root/sessions/$child/artifacts/$artifact_id.json"
test -f "$artifact"
grep -F "\"parent_session_id\":\"$parent\"" "$artifact" >/dev/null
"$cli" run child-after-artifact --session "$child" \
  --option 'mock_scenario="streaming_text"' \
  --option 'mock_text="bounded-child-ok"' --json >/dev/null
printf 'runtime artifact-backed bounded branch E2E passed\n'
