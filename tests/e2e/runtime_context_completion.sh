#!/usr/bin/env bash
# Unix process E2E for TASK-05 live context completion: typed-summary
# compaction, artifact-handoff compaction, and automatic memory writes.
# Mirrors runtime_context_completion.ps1. Syntax-checked only in CI
# environments that do not execute Windows process tests.
set -euo pipefail

repository="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repository"

cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli -p agentmod-scheduler

runtime="target/debug/agentmod-runtime"
harness="target/debug/agentmod-harness"
cli="target/debug/agentmod"
run_root="$(mktemp -d)/agentmod-context-completion-e2e-$$"
workspace="$run_root/workspace"
style_root="$run_root/styles/user"
mkdir -p "$workspace" "$style_root"
cp tests/fixtures/styles/persistent-file-summary.toml "$style_root"
cp tests/fixtures/styles/persistent-file-artifact.toml "$style_root"
cp tests/fixtures/styles/persistent-file-auto-write.toml "$style_root"
cp tests/fixtures/styles/persistent-none-none.toml "$style_root"

export AGENTMOD_RUNTIME_ENDPOINT="agentmod-context-completion-e2e-$$"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$harness"

cleanup() {
  if [[ -n "${daemon_pid:-}" ]]; then kill "$daemon_pid" 2>/dev/null || true; fi
  rm -rf "$run_root"
}
trap cleanup EXIT

"$runtime" serve >"$run_root/runtime.stdout.log" 2>&1 &
daemon_pid=$!
for _ in $(seq 1 80); do
  if "$cli" doctor --json >/dev/null 2>&1; then break; fi
  sleep 0.1
done

for fixture in persistent-file-summary persistent-file-artifact \
  persistent-file-auto-write persistent-none-none; do
  "$cli" style validate "$style_root/$fixture.toml" --json >/dev/null
done

summary_session="$("$cli" session create --workspace "$workspace" \
  --style e2e-persistent-summary --json | jq -r .session_id)"
artifact_session="$("$cli" session create --workspace "$workspace" \
  --style e2e-persistent-artifact --json | jq -r .session_id)"
auto_session="$("$cli" session create --workspace "$workspace" \
  --style e2e-persistent-auto-write --json | jq -r .session_id)"
none_session="$("$cli" session create --workspace "$workspace" \
  --style e2e-persistent-none --json | jq -r .session_id)"

run_turn() {
  local session="$1" prompt="$2" output="$3"
  "$cli" run "$prompt" --session "$session" \
    --option 'mock_scenario="streaming_text"' \
    --option "mock_text=\"$output\"" --json >/dev/null
}

summary_compacted=0
artifact_compacted=0
for turn in $(seq 1 22); do
  run_turn "$summary_session" "equivalent compaction pressure turn $turn" "stable-output-$turn"
  run_turn "$artifact_session" "equivalent compaction pressure turn $turn" "stable-output-$turn"
  run_turn "$none_session" "equivalent compaction pressure turn $turn" "stable-output-$turn"
  run_turn "$auto_session" "equivalent compaction pressure turn $turn" "stable-output-$turn"
  summary_journal="$run_root/sessions/$summary_session/events.jsonl"
  artifact_journal="$run_root/sessions/$artifact_session/events.jsonl"
  if grep -q '"context.summary_completed"' "$summary_journal"; then summary_compacted=1; fi
  if grep -q '"context.artifact_completed"' "$artifact_journal"; then artifact_compacted=1; fi
  if [[ "$summary_compacted" -eq 1 && "$artifact_compacted" -eq 1 ]]; then break; fi
done
[[ "$summary_compacted" -eq 1 ]] || { echo "summary compaction never executed"; exit 1; }
[[ "$artifact_compacted" -eq 1 ]] || { echo "artifact compaction never executed"; exit 1; }

summary_inspection="$("$cli" session inspect "$summary_session" --json)"
artifact_inspection="$("$cli" session inspect "$artifact_session" --json)"
echo "$summary_inspection" | jq -e \
  '.state.conversation.projection_provenance.method == "summary"' >/dev/null
echo "$summary_inspection" | jq -e '
  [.state.conversation.provider_projection[] | select(.kind == "context_summary")] | length >= 1' >/dev/null
echo "$artifact_inspection" | jq -e '
  .state.conversation.projection_provenance.method == "artifact_handoff"' >/dev/null
echo "$artifact_inspection" | jq -e '
  [.state.conversation.provider_projection[] | select(.kind == "artifact_reference")] | length >= 1' >/dev/null

auto_journal="$run_root/sessions/$auto_session/events.jsonl"
grep -q '"memory.write_proposed"' "$auto_journal"
grep -q '"memory.write_completed"' "$auto_journal"

# Deduplication: the identical turn must not add a write proposal.
before_count="$(grep -c '"memory.write_proposed"' "$auto_journal" || true)"
run_turn "$auto_session" "auto-write dedup probe" "dedup-output"
run_turn "$auto_session" "auto-write dedup probe" "dedup-output"
after_count="$(grep -c '"memory.write_proposed"' "$auto_journal" || true)"
[[ "$after_count" -eq "$before_count" ]] || { echo "auto-write duplicated"; exit 1; }

echo "runtime context completion (summary/artifact/auto-write) process E2E passed"
