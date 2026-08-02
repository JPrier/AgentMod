#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_root="$(mktemp -d)"
runtime_pid=""
cleanup() {
  if [[ -n "$runtime_pid" ]]; then
    kill "$runtime_pid" 2>/dev/null || true
    wait "$runtime_pid" 2>/dev/null || true
  fi
  rm -rf -- "$run_root"
}
trap cleanup EXIT
cd "$repository"

cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-tui
export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_SCHEDULER_POLL_MS=0
(
  cd "$run_root"
  exec "$repository/target/debug/agentmod-runtime" serve
) >"$run_root/runtime.log" 2>&1 &
runtime_pid="$!"

for _ in $(seq 1 50); do
  if "$repository/target/debug/agentmod" doctor --json >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
"$repository/target/debug/agentmod" doctor --json >/dev/null
created="$("$repository/target/debug/agentmod" session create \
  --workspace "$run_root" --style persistent-chat --json)"
session_id="$(printf '%s' "$created" | sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
test -n "$session_id"
smoke="$("$repository/target/debug/agentmod-tui" --smoke)"
grep -q "ready=true" <<<"$smoke"
grep -q "sessions=1" <<<"$smoke"
style_smoke="$("$repository/target/debug/agentmod-tui" --smoke-command \
  "/style ephemeral-turn")"
grep -q "style_details=ephemeral-turn@1.1.0" <<<"$style_smoke"
turn="$("$repository/target/debug/agentmod-tui" --smoke-turn \
  "verify committed TUI streaming")"
grep -q "deterministic response" <<<"$turn"
grep -q "turn committed" <<<"$turn"
journal="$run_root/sessions/$session_id/events.jsonl"
test "$(grep -c '"event_type":"model.response_completed"' "$journal")" -eq 1
test "$(grep -c '"event_type":"conversation.entry_committed"' "$journal")" -ge 2
parent_before="$("$repository/target/debug/agentmod" session inspect \
  "$session_id" --json)"
parent_head="$(printf '%s' "$parent_before" |
  sed -n 's/.*"head_sequence":\([0-9][0-9]*\).*/\1/p')"
branch_smoke="$("$repository/target/debug/agentmod-tui" --smoke-command \
  "/branch 1 ephemeral-turn")"
branch_id="$(printf '%s' "$branch_smoke" |
  sed -n 's/.*branched \([0-9a-f-]*\) from.*/\1/p')"
test -n "$branch_id"
branch_inspection="$("$repository/target/debug/agentmod" session inspect \
  "$branch_id" --json)"
parent_after="$("$repository/target/debug/agentmod" session inspect \
  "$session_id" --json)"
grep -q '"id":"ephemeral-turn"' <<<"$branch_inspection"
grep -q "\"parent_session_id\":\"$session_id\"" <<<"$branch_inspection"
grep -q "\"head_sequence\":$parent_head" <<<"$parent_after"
grep -q '"id":"persistent-chat"' <<<"$parent_after"
component_smoke="$("$repository/target/debug/agentmod-tui" --smoke-command \
  "/new . ephemeral-turn native sqlite-fts sliding_window 3 40 100000 1000000 60000")"
component_id="$(printf '%s' "$component_smoke" |
  sed -n 's/.*selected=\([0-9a-f-]*\).*/\1/p')"
test -n "$component_id"
component_inspection="$("$repository/target/debug/agentmod" session inspect \
  "$component_id" --json)"
grep -q '"provider":"sqlite-fts"' <<<"$component_inspection"
grep -q '"strategy":"sliding_window"' <<<"$component_inspection"
grep -q '"max_iterations":3' <<<"$component_inspection"
grep -q '"max_tokens":100000' <<<"$component_inspection"
cli_selected="$("$repository/target/debug/agentmod" session create \
  --workspace "$run_root" --style ephemeral-turn --memory file \
  --compaction tool_output_eviction --max-iterations 4 --max-steps 50 \
  --max-tokens 120000 --max-cost-micros 1200000 --max-duration-ms 70000 --json)"
cli_selected_id="$(printf '%s' "$cli_selected" |
  sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')"
cli_inspection="$("$repository/target/debug/agentmod" session inspect \
  "$cli_selected_id" --json)"
grep -q '"provider":"file"' <<<"$cli_inspection"
grep -q '"strategy":"tool_output_eviction"' <<<"$cli_inspection"
grep -q '"max_iterations":4' <<<"$cli_inspection"
grep -q '"max_tokens":120000' <<<"$cli_inspection"
echo "runtime TUI smoke/branch/component/budget-selection E2E passed"
