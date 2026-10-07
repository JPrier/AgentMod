#!/usr/bin/env bash
# Native end-to-end test: real runtime + real plugin processes, OpenRouter
# replaced by a local mock. Covers a tool loop, approval + sub-agent, hot
# config apply (and rejection), hard stop, plugin crash isolation, SIGKILL of
# the runtime mid-stream with recovery from the log, and full log verification.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${AGENTMOD_BIN:-target/debug/agentmod}
DATA=$(mktemp -d)
PORT=7799
B=http://127.0.0.1:$PORT/api
CFG=$(mktemp --suffix=.toml -p .)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$DATA" "$CFG"' EXIT
sed -e "s|max_tokens = 1024 }|max_tokens = 1024, base_url = \"http://127.0.0.1:8766/api/v1\" }|" \
    -e "s|port = 7700|port = $PORT|" agentmod.toml > "$CFG"
MOCK_DELAY_MS=150 node tests/mock-openrouter.mjs 8766 2>/dev/null &
export OPENROUTER_API_KEY=test-key

fail() { echo "FAIL: $*" >&2; exit 1; }
py() { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }
start() { "$BIN" serve --config "$CFG" --data "$DATA" > "$DATA/serve.log" 2>&1 & RT=$!; for _ in $(seq 50); do curl -sf $B/info >/dev/null && return; sleep 0.2; done; cat "$DATA/serve.log"; fail "runtime did not start"; }
wait_for() { for _ in $(seq ${3:-100}); do if curl -s "$B/sessions/$1" | py "$2" >/dev/null 2>&1; then return; fi; sleep 0.2; done; fail "timed out waiting on $1: $2"; }

echo "1. key is mandatory"
if env -u OPENROUTER_API_KEY "$BIN" serve --config "$CFG" --data "$DATA/nokey" > "$DATA/nokey.log" 2>&1; then fail "started without a key"; fi
grep -q "OpenRouter API key required" "$DATA/nokey.log" || fail "no key message"

start
echo "2. tool loop"
curl -s -XPOST $B/sessions -d '{"definition":"chat","text":"calculate (12+30)*7"}' >/dev/null
wait_for s0001 "assert any(e['event_name']=='assistant-message' and '294' in e['payload']['text'] for e in d['events'])"

echo "3. approval gate + sub-agent session"
curl -s -XPOST $B/sessions/s0001/messages -d '{"text":"delegate: summarize the plan"}' >/dev/null
wait_for s0001 "assert any(e['event_name']=='approval-requested' for e in d['events'])"
AR=$(curl -s $B/sessions/s0001 | py "print([e['event_id'] for e in d['events'] if e['event_name']=='approval-requested'][-1])")
curl -s -XPOST $B/sessions/s0001/actions -d "{\"reply_to\":\"$AR\",\"action\":\"approve\"}" >/dev/null
wait_for s0001 "assert any(e['event_name']=='tool-result' and e['payload']['name']=='delegate' for e in d['events'])" 200
curl -s $B/sessions | py "assert any(s['definition']=='worker' and s['parent']=='s0001' for s in d)" || fail "no worker session"

echo "4. hot apply and whole-config rejection"
CONF=$(curl -s $B/config | py "c=d['config']; c['plugins']['approval-gate']['config']={'require':'*'}; print(json.dumps({'config':c}))")
curl -s -XPOST $B/config/apply -d "$CONF" | py "assert d['ok']" || fail "apply"
BAD=$(curl -s $B/config | py "c=d['config']; c['definitions']['chat']['subscribers'].append({'plugin':'ghost'}); print(json.dumps({'config':c}))")
curl -s -XPOST $B/config/apply -d "$BAD" | py "assert not d['ok'] and any(x['code']=='unknown-plugin' for x in d['diagnostics'])" || fail "rejection"

echo "5. hard stop mid-stream"
curl -s -XPOST $B/sessions -d '{"definition":"chat","text":"tell me a story"}' >/dev/null
wait_for s0003 "assert any(e['event_name']=='stream-chunk' for e in d['events'])"
curl -s -XPOST $B/sessions/s0003/commands -d '{"command":"hard-stop"}' | py "assert d['status']['state']=='halted' and not d['status']['open_invocations']" || fail "hard stop"

echo "6. plugin crash is isolated and retried"
curl -s -XPOST $B/sessions -d '{"definition":"chat","text":"hello again"}' >/dev/null
wait_for s0004 "assert any(e['event_name']=='stream-chunk' for e in d['events'])"
PID=$(grep -o "started plugin .openrouter-model. (pid Some([0-9]*)" "$DATA/serve.log" | tail -1 | grep -o "[0-9]*" | tail -1)
kill -9 "$PID"
wait_for s0004 "assert any(e['event_name']=='assistant-message' for e in d['events'])" 200
curl -s $B/sessions/s0004 | py "assert any(i['attempts']>1 for e in d['events'] for i in e['invocations'] if i['plugin']=='openrouter-model')" || fail "no retry"

echo "7. SIGKILL the runtime mid-stream; recover from the log"
curl -s -XPOST $B/sessions -d '{"definition":"chat","text":"one more long answer please"}' >/dev/null
wait_for s0005 "assert any(e['event_name']=='stream-chunk' for e in d['events'])"
kill -9 $RT; wait $RT 2>/dev/null || true
start
wait_for s0005 "assert any(e['event_name']=='assistant-message' for e in d['events'])" 200
curl -s $B/sessions/s0005 | py "
chunks=[e for e in d['events'] if e['event_name']=='stream-chunk']
idx=[e['payload']['index'] for e in chunks]
assert idx==list(range(len(idx))), idx
assert any(i['attempts']>1 for e in d['events'] for i in e['invocations'])" || fail "recovery"
kill $RT; wait $RT 2>/dev/null || true

echo "8. every log replays through a fresh kernel"
"$BIN" verify --data "$DATA"
echo "e2e: all checks passed"
