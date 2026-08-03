#!/usr/bin/env bash
# Independent harness conformance E2E (Unix): builds and drives the genuinely
# independent agentmod-harness-fixture binary over bounded JSONL stdio and
# verifies protocol negotiation, distinct identity/capabilities, deterministic
# streaming, negative capability guards, and in-flight cancellation.
# Requires no network and no credentials.
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"

cargo build -p agentmod-harness-fixture

fixture="target/debug/agentmod-harness-fixture"
if [[ "$(uname -s)" == "MINGW"* || "$(uname -s)" == "MSYS"* ]]; then
  fixture="${fixture}.exe"
fi

export AGENTMOD_HARNESS_AUTH_KEY="$(printf '11%.0s' {1..32})"
export AGENTMOD_FIXTURE_DEV_MODE=1

# Start the fixture process with stdin/stdout pipes.
coproc FIXTURE { "$fixture"; }
trap 'kill "$FIXTURE_PID" 2>/dev/null || true' EXIT

send() {
  printf '%s\n' "$1" >&"${FIXTURE[1]}"
}

read_reply() {
  IFS= read -r line <&"${FIXTURE[0]}"
  printf '%s' "$line"
}

health="$(send '{"command":"health"}' && read_reply)"
if [[ "$health" != *'"status":"ok"'* || "$health" != *'"streaming"'* || "$health" == *'"images"'* ]]; then
  echo "health negotiation did not report distinct capabilities" >&2
  exit 1
fi

catalog="$(send '{"command":"catalog"}' && read_reply)"
if [[ "$catalog" != *'"id":"independent-fixture"'* || "$catalog" != *'"version":"2.0.0"'* ]]; then
  echo "catalog identity is incorrect" >&2
  exit 1
fi

session="018f6f83-7b80-7000-8000-000000000001"
cancel="018f6f83-7b80-7000-8000-000000000010"
# Streaming scenario: 5 event frames (started, 3 text deltas, completed).
send '{"command":"execute","value":{"session_id":"'"$session"'","provider":"fixture-deterministic","model":"fixture-model","entries":[{"kind":"user","value":{"text":"hello"}}],"options":{"fixture_scenario":"streaming_text","fixture_text":"e2e"},"authorization_grant":"grant","cancellation_id":"'"$cancel"'"}}'
stream_frames=""
for _ in 1 2 3 4 5; do
  stream_frames="${stream_frames}$(read_reply)\n"
done
if [[ "$stream_frames" != *'"event":"started"'* || "$stream_frames" != *'"event":"completed"'* ]]; then
  echo "streaming scenario did not start and complete" >&2
  exit 1
fi
delta_count="$(printf '%b' "$stream_frames" | grep -c 'text_delta' || true)"
if [[ "$delta_count" -ne 3 ]]; then
  echo "expected 3 streamed text deltas, got $delta_count" >&2
  exit 1
fi

# Negative capability guard: image input is rejected.
send '{"command":"execute","value":{"session_id":"'"$session"'","provider":"fixture-deterministic","model":"fixture-model","entries":[{"kind":"image","value":{"media_type":"image/png","data_base64":"aGVsbG8="}}],"options":{"fixture_scenario":"text"},"authorization_grant":"grant","cancellation_id":"'"$cancel"'"}}'
rejected="$(read_reply)"
if [[ "$rejected" != *'"event":"failed"'* || "$rejected" != *'"unsupported_capability"'* ]]; then
  echo "image input was not rejected" >&2
  exit 1
fi

# In-flight cancellation of the slow streaming scenario.
slow_cancel="018f6f83-7b80-7000-8000-000000000011"
send '{"command":"execute","value":{"session_id":"'"$session"'","provider":"fixture-deterministic","model":"fixture-model","entries":[{"kind":"user","value":{"text":"wait"}}],"options":{"fixture_scenario":"slow_stream"},"authorization_grant":"grant","cancellation_id":"'"$slow_cancel"'"}}'
sleep 0.3
send '{"command":"cancel","value":{"cancellation_id":"'"$slow_cancel"'"}}'
cancelled_frames=""
for _ in 1 2 3; do
  cancelled_frames="${cancelled_frames}$(read_reply)\n"
done
if [[ "$cancelled_frames" != *'"event":"cancelled"'* ]]; then
  echo "slow stream was not cancelled" >&2
  exit 1
fi

echo "independent harness conformance E2E passed"
