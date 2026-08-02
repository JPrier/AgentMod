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

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-typed-summary-e2e.XXXXXX")
succeeded=0
workspace="$run_root/workspace"
style_root="$run_root/styles/user"
mkdir -p "$workspace" "$style_root"
sed \
    -e 's/id = "e2e-persistent-sliding"/id = "e2e-persistent-summary"/' \
    -e 's/strategy = "sliding_window"/strategy = "summary"/' \
    tests/fixtures/styles/persistent-none-sliding.toml \
    >"$style_root/persistent-summary.toml"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$debug_root/agentmod-harness"
runtime="$debug_root/agentmod-runtime"
cli="$debug_root/agentmod"

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    case "$succeeded:$run_root" in
        0:*)
            echo "retained failed E2E root: $run_root" >&2
            ;;
        1:"${TMPDIR:-/tmp}"/agentmod-typed-summary-e2e.*)
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

start_runtime
"$cli" style validate "$style_root/persistent-summary.toml" --json |
    grep -F '"valid":true' >/dev/null
created=$("$cli" session create --workspace "$workspace" \
    --style e2e-persistent-summary --json)
session_id=$(printf '%s' "$created" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$session_id"
turn=$("$cli" run "summarize the canonical context" --session "$session_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="summary-output"' --json)
printf '%s' "$turn" | grep -F '"text":"alpha "' >/dev/null
printf '%s' "$turn" | grep -F '"text":"beta "' >/dev/null
printf '%s' "$turn" | grep -F '"text":"summary-output"' >/dev/null

journal="$run_root/sessions/$session_id/events.jsonl"
test "$(grep -c -F '"event_type":"conversation.entry_committed"' "$journal")" -eq 2
test "$(grep -F '"event_type":"conversation.entry_committed"' "$journal" |
    grep -c -F '"kind":"context_summary"' || true)" -eq 0
test "$(grep -F '"event_type":"context.projection_replaced"' "$journal" |
    grep -c -F '"method":"summary"')" -eq 1
grep -F '"event_type":"context.projection_replaced"' "$journal" |
    grep -F 'agentmod.context-summary.v1' >/dev/null
summary_line=$(grep -n -F '"method":"summary"' "$journal" |
    head -n 1 | cut -d: -f1)
proposal_line=$(grep -n -F '"event_type":"model.request_proposed"' "$journal" |
    head -n 1 | cut -d: -f1)
test "$summary_line" -lt "$proposal_line"

before=$("$cli" session inspect "$session_id" --json)
printf '%s' "$before" | grep -F '"strategy":"summary"' >/dev/null
printf '%s' "$before" | grep -F '"kind":"context_summary"' >/dev/null
printf '%s' "$before" | grep -F '"method":"summary"' >/dev/null
plan_hash=$(printf '%s' "$before" |
    sed -n 's/.*"execution_plan_hash":"\([^"]*\)".*/\1/p')
test -n "$plan_hash"
before_count=$(wc -l <"$journal" | tr -d ' ')

kill "$daemon_pid"
wait "$daemon_pid" || true
daemon_pid=
start_runtime
after=$("$cli" session inspect "$session_id" --json)
printf '%s' "$after" | grep -F "\"execution_plan_hash\":\"$plan_hash\"" >/dev/null
printf '%s' "$after" | grep -F '"kind":"context_summary"' >/dev/null
printf '%s' "$after" | grep -F 'agentmod.context-summary.v1' >/dev/null
replayed=$("$cli" session replay "$session_id" --json)
printf '%s' "$replayed" | grep -F "\"execution_plan_hash\":\"$plan_hash\"" >/dev/null
printf '%s' "$replayed" | grep -F '"kind":"context_summary"' >/dev/null
printf '%s' "$replayed" | grep -F 'agentmod.context-summary.v1' >/dev/null
test "$(wc -l <"$journal" | tr -d ' ')" -eq "$before_count"

succeeded=1
echo "runtime typed-summary compaction/restart/replay E2E passed"
