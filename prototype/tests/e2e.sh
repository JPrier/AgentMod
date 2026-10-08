#!/usr/bin/env bash
# Native end-to-end test: real runtime + real plugin processes, OpenRouter
# replaced by a local mock. Covers a tool loop, approval + sub-agent, hot
# config apply (and rejection), hard stop, plugin crash isolation, SIGKILL of
# the runtime mid-stream with recovery from the log, the coding tools through
# a real session (local-workspace), and full log verification.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=${AGENTMOD_BIN:-target/debug/agentmod}
DATA=$(mktemp -d)
PORT=7799
B=http://127.0.0.1:$PORT/api
CFG=$(mktemp --suffix=.toml -p .)
trap 'kill $(jobs -p) 2>/dev/null || true; [ -n "${KEEP:-}" ] || rm -rf "$DATA"; rm -f "$CFG"' EXIT
sed -e "s|max_tokens = 4096 }|max_tokens = 4096, base_url = \"http://127.0.0.1:8766/api/v1\" }|" \
    -e "s|port = 7700|port = $PORT|" \
    -e "s#root = \".agentmod/workspace\"#root = \"$DATA/ws\"#" agentmod.toml > "$CFG"
MOCK_DELAY_MS=150 node tests/mock-openrouter.mjs 8766 2>"$DATA/mock.log" &
export OPENROUTER_API_KEY=test-key

fail() { echo "FAIL: $*" >&2; echo "data: $DATA" >&2; exit 1; }
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

echo "8. coding tools through a real session (coder: local-workspace + policy)"
msg() { python3 -c "import json,sys; print(json.dumps({'text':sys.argv[1]}))" "$1"; }
newsess() { python3 -c "import json,sys; print(json.dumps({'definition':sys.argv[1],'text':sys.argv[2]}))" "$1" "$2" | curl -s -XPOST $B/sessions -d @- | py "print(d['session_id'])"; }
ev() { curl -s "$B/sessions/$1" | py "$2"; }
C=$(newsess coder 'tool apply_patch {"changes":[{"action":"create","path":"hello.sh","content":"echo hello from the workspace\n"}]}')
wait_for $C "assert any(e['event_name']=='tool-result' and e['payload']['name']=='apply_patch' and not e['payload'].get('error') for e in d['events'])" 100
wait_for $C "assert any(e['event_name']=='workspace-change' and '+echo hello' in e['payload']['unified'] for e in d['events'])" 20
wait_for $C "assert any(e['event_name']=='checkpoint-created' for e in d['events'])" 20
wait_for $C "assert any(e['event_name']=='assistant-message' for e in d['events'])" 100
test -f "$DATA/ws/hello.sh" || fail "apply_patch did not create the file"
curl -s -XPOST $B/sessions/$C/messages -d "$(msg 'tool shell {"command":"bash hello.sh && pwd"}')" >/dev/null
wait_for $C "assert any(e['event_name']=='tool-result' and e['payload']['name']=='shell' and 'hello from the workspace' in e['payload']['output'] and e['payload']['exit_code']==0 for e in d['events'])" 100
curl -s -XPOST $B/sessions/$C/messages -d "$(msg 'tool read_file {"path":"../../etc/passwd"}')" >/dev/null
wait_for $C "assert any(e['event_name']=='tool-result' and e['payload']['name']=='read_file' and e['payload']['error'] and 'outside the workspace' in e['payload']['output'] for e in d['events'])" 100
# The model saw only the compact core tool set (lazy discovery), in a stable order.
ev $C "
r=[e for e in d['events'] if e['event_name']=='model-response']
m=r[-1]['payload']['metrics']
assert m['tools_sent']<=12 and m['tools_deferred']>=5, m
assert m['tool_schema_tokens']>0 and m['cached_tokens']==600, m" || fail "tool tiering metrics"
grep "MOCK " "$DATA/mock.log" | tail -1 | grep -q '"tools":\["shell","process","read_file","list_dir","search_files","search_text","apply_patch","update_plan","ask_user","tool_search"' || fail "core tools not sent in the fixed order"

echo "9. a whole coding task: plan, search, read, patch, test, server, finish"
mkdir -p "$DATA/ws"
printf 'function add(a, b) {\n  return a - b;\n}\nmodule.exports = { add };\n' > "$DATA/ws/calc.js"
printf 'const { add } = require("./calc");\nif (add(2, 3) !== 5) { console.error("FAIL add"); process.exit(1); }\nconsole.log("ok");\n' > "$DATA/ws/test.js"
T=$(newsess coder 'script:fix the failing test in this repo')
wait_for $T "assert any(e['event_name']=='assistant-message' and 'Fixed add()' in e['payload']['text'] for e in d['events'])" 300
grep -q "return a + b;" "$DATA/ws/calc.js" || fail "patch not applied"
ev $T "
names=[e['event_name'] for e in d['events']]
for n in ['plan-updated','workspace-info','checkpoint-created','workspace-change','process-started']: assert n in names, n
res=[e['payload'] for e in d['events'] if e['event_name']=='tool-result']
assert any(r['name']=='shell' and r['exit_code']==0 and 'ok' in r['output'] for r in res)
assert any(r['name']=='search_text' and 'calc.js' in r['output'] for r in res)
assert any(r['name']=='read_file' and 'sha256' in r['output'] for r in res)
assert any(r['name']=='process' and 'listening on' in r['output'] for r in res), [r['output'][:80] for r in res if r['name']=='process']
assert any(e['event_name']=='assistant-message' and 'Server is listening' in e['payload']['text'] for e in d['events'])
plans=[e['payload']['items'] for e in d['events'] if e['event_name']=='plan-updated']
assert all(i['status']=='completed' for i in plans[-1])" || fail "coding task"
FIRSTCP=$(ev $T "print([e['event_id'] for e in d['events'] if e['event_name']=='checkpoint-created'][0])")

echo "10. rewind the workspace to before the fix (reversible), from the UI"
curl -s -XPOST $B/sessions/$T/actions -d "{\"reply_to\":\"$FIRSTCP\",\"action\":\"restore\"}" >/dev/null
wait_for $T "assert any(e['event_name']=='workspace-restored' for e in d['events'])" 100
grep -q "return a - b;" "$DATA/ws/calc.js" || fail "restore did not rewind the file"
PREV=$(ev $T "print([e['payload']['previous'] for e in d['events'] if e['event_name']=='workspace-restored'][0])")
curl -s -XPOST $B/sessions/$T/messages -d "$(msg 'tool tool_search {"query":"select:checkpoints"}')" >/dev/null
wait_for $T "assert any(e['event_name']=='tool-result' and e['payload']['name']=='tool_search' and 'Loaded checkpoints' in e['payload']['output'] for e in d['events'])" 100
sleep 1
curl -s -XPOST $B/sessions/$T/messages -d "$(msg "tool checkpoints {\"action\":\"restore\",\"checkpoint\":\"$PREV\"}")" >/dev/null
wait_for $T "assert len([e for e in d['events'] if e['event_name']=='workspace-restored'])>=2" 100
grep -q "return a + b;" "$DATA/ws/calc.js" || fail "undoing the rewind failed"

echo "11. branch from before the fix: an isolated workspace at that point"
SEQ=$(ev $T "print([e['sequence'] for e in d['events'] if e['event_name']=='tool-call' and e['payload']['name']=='apply_patch'][0]-1)")
BR=$(python3 -c "import json; print(json.dumps({'definition':'coder','text':'script:peek','fork_from':{'session_id':'$T','sequence':$SEQ}}))" | curl -s -XPOST $B/sessions -d @- | py "print(d['session_id'])")
wait_for $BR "assert any(e['event_name']=='assistant-message' and 'branch sees' in e['payload']['text'] for e in d['events'])" 200
ev $BR "
t=[e for e in d['events'] if e['event_name']=='assistant-message'][-1]['payload']['text']
assert 'a - b' in t and 'worktrees/$BR' in t, t
w=[e for e in d['events'] if e['event_name']=='workspace-info'][0]['payload']
assert w['mode']=='isolated' and w['base']['session']=='$T', w" || fail "branch workspace"
grep -q "return a + b;" "$DATA/ws/calc.js" || fail "the branch changed the parent's workspace"
curl -s $B/sessions | py "assert any(s['session_id']=='$BR' and s['fork_of']['session_id']=='$T' for s in d)" || fail "branch not listed with its ancestry"

echo "12. policy: ask, approve; re-validation denies after a policy change"
N=$(newsess coder 'script:net')
wait_for $N "assert any(e['event_name']=='approval-requested' for e in d['events'])" 100
ev $N "assert any(e['event_name']=='policy-decision' and e['payload']['effect']=='ask' and e['payload']['rule']=='mode:default' for e in d['events'])" || fail "no explained ask"
AR=$(ev $N "print([e['event_id'] for e in d['events'] if e['event_name']=='approval-requested'][-1])")
curl -s -XPOST $B/sessions/$N/actions -d "{\"reply_to\":\"$AR\",\"action\":\"approve\"}" >/dev/null
wait_for $N "assert any(e['event_name']=='tool-result' and e['payload']['name']=='shell' and 'curl-ran' in e['payload']['output'] for e in d['events'])" 100
N2=$(newsess coder 'script:net')
wait_for $N2 "assert any(e['event_name']=='approval-requested' for e in d['events'])" 100
CONF=$(curl -s $B/config | py "c=d['config']; c['plugins']['policy']['config']['rules']=[{'id':'no-curl','tool':'shell','when':{'command':'curl'},'effect':'deny','reason':'curl is banned'}]; print(json.dumps({'config':c}))")
curl -s -XPOST $B/config/apply -d "$CONF" | py "assert d['ok']" || fail "apply deny rule"
AR2=$(ev $N2 "print([e['event_id'] for e in d['events'] if e['event_name']=='approval-requested'][-1])")
curl -s -XPOST $B/sessions/$N2/actions -d "{\"reply_to\":\"$AR2\",\"action\":\"approve\"}" >/dev/null
wait_for $N2 "assert any(e['event_name']=='tool-result' and 'policy changed while the approval was pending' in e['payload']['output'] for e in d['events'])" 100
ev $N2 "assert not any(e['event_name']=='tool-call' and e['payload'].get('approved') for e in d['events'])" || fail "denied call ran"

echo "13. ask_user is a recorded continuation; the user's next message answers it"
Q=$(newsess coder 'script:ask')
wait_for $Q "assert any(e['event_name']=='user-input-requested' for e in d['events'])" 100
curl -s -XPOST $B/sessions/$Q/messages -d "$(msg 'blue')" >/dev/null
wait_for $Q "assert any(e['event_name']=='assistant-message' and 'You chose: blue' in e['payload']['text'] for e in d['events'])" 100
ev $Q "assert any(e['status']=='vetoed' and e['event_name']=='user-message' for e in d['events'])" || fail "answer was also a new turn"

echo "14. delegation: an isolated child, evidence back, adoption"
D=$(newsess coder 'script:delegate')
wait_for $D "assert any(e['event_name']=='assistant-message' and 'adopted' in e['payload']['text'] for e in d['events'])" 300
test -f "$DATA/ws/greeting.txt" || fail "adoption did not bring the child's file"
CH=$(curl -s $B/sessions | py "print([s['session_id'] for s in d if s['parent']=='$D'][0])")
ev $CH "
w=[e for e in d['events'] if e['event_name']=='workspace-info'][0]['payload']
assert w['mode']=='isolated' and 'worktrees/$CH' in w['root'], w
u=[e for e in d['events'] if e['event_name']=='user-message'][0]['payload']
assert u['delegation']['tools'] and 'delegate' not in u['delegation']['tools']" || fail "child isolation"
ev $D "
r=[e['payload'] for e in d['events'] if e['event_name']=='tool-result' and e['payload']['name']=='delegate'][0]
assert r['trust']=='external' and r['evidence']['files_changed']==['greeting.txt'], r
assert 'Child $CH finished' in r['output']" || fail "child evidence"

echo "15. children cannot use tools outside their allowlist; unknown tools are answered"
F=$(newsess coder 'script:readonlychild')
wait_for $F "assert any(e['event_name']=='tool-result' and e['payload']['name']=='delegate' for e in d['events'])" 300
FC=$(curl -s $B/sessions | py "print([s['session_id'] for s in d if s['parent']=='$F'][0])")
ev $FC "
r=[e['payload'] for e in d['events'] if e['event_name']=='tool-result' and e['payload']['name']=='apply_patch'][0]
assert r['error'] and 'Denied by policy' in r['output'], r
tp=[c for c in d['events'] if c['event_name']=='policy-decision']
assert any(p['payload']['scope'] in ('child','mode') for p in tp)" || fail "child allowlist"
test ! -f "$DATA/ws/nope.txt" || fail "forbidden write happened"
B2=$(newsess coder 'script:bad')
wait_for $B2 "assert any(e['event_name']=='assistant-message' and 'recovered' in e['payload']['text'] for e in d['events'])" 100
ev $B2 "assert any(e['event_name']=='tool-result' and 'There is no tool named' in e['payload']['output'] for e in d['events'])" || fail "unknown tool"

echo "16. provider hot swap mid-session: the next model request uses the new provider"
H=$(newsess coder 'first turn on the original provider')
wait_for $H "assert any(e['event_name']=='assistant-message' for e in d['events'])" 100
SWAP=$(curl -s $B/config | py "
c=d['config']
for df in c['definitions'].values():
    df['subscribers']=[{'plugin':'openai-model'} if s['plugin']=='openrouter-model' else s for s in df['subscribers']]
c['plugins']['openai-model']['config']={'base_url':'http://127.0.0.1:8766/api/v1','api_key':'test-key','model':'mock/swapped'}
print(json.dumps({'config':c}))")
curl -s -XPOST $B/config/apply -d "$SWAP" | py "assert d['ok'], d" || fail "provider swap apply"
curl -s -XPOST $B/sessions/$H/messages -d "$(msg 'second turn after the swap')" >/dev/null
wait_for $H "assert len([e for e in d['events'] if e['event_name']=='assistant-message'])>=2" 100
ev $H "
inv=[i['plugin'] for e in d['events'] if e['event_name']=='model-request' for i in e['invocations'] if i['plugin'].endswith('-model')]
assert inv[0]=='openrouter-model' and inv[-1]=='openai-model', inv
assert any(c['kind']=='config-applied' for c in d['control'])
r=[e['payload'] for e in d['events'] if e['event_name']=='model-response']
assert r[-1]['model']=='mock/swapped' and r[-1]['provider']=='openai-compatible', r[-1]" || fail "provider hot swap"
# Swap back so later steps run on the original provider.
BACK=$(curl -s $B/config | py "
c=d['config']
for df in c['definitions'].values():
    df['subscribers']=[{'plugin':'openrouter-model'} if s['plugin']=='openai-model' else s for s in df['subscribers']]
print(json.dumps({'config':c}))")
curl -s -XPOST $B/config/apply -d "$BACK" | py "assert d['ok']" || fail "swap back"

echo "17. a running process survives a runtime SIGKILL and is reconciled"
L=$(newsess coder 'script:long')
wait_for $L "assert any(e['event_name']=='assistant-message' and 'started p' in e['payload']['text'] for e in d['events'])" 100
kill -9 $RT; wait $RT 2>/dev/null || true
start
curl -s -XPOST $B/sessions/$L/messages -d "$(msg 'script:procs')" >/dev/null
wait_for $L "assert any(e['event_name']=='tool-result' and e['payload']['name']=='process' and '[ticker]: running' in e['payload']['output'] for e in d['events'])" 100
PIDF=$(ls "$DATA"/ws/.agentmod/state/procs/*/pid | head -1); kill -- -$(cat "$PIDF") 2>/dev/null || pkill -P "$(cat "$PIDF")" || true
kill $RT; wait $RT 2>/dev/null || true

echo "18. every log replays through a fresh kernel"
"$BIN" verify --data "$DATA"
echo "19. metrics derive from the logs alone"
"$BIN" metrics --data "$DATA" "$T" --json | py "
m=d['$T']
assert m['model_requests']>=8 and m['edits']>=1 and m['cached_tokens']>0 and m['tool_calls']>=8, m
assert m['time_to_first_edit_ms'] is not None and m['tool_schema_tokens']>0 and m['checkpoints']>=1, m" || fail "metrics"
"$BIN" metrics --data "$DATA" "$N" --json | py "assert d['$N']['permission_prompts']==1" || fail "prompt metric"
echo "e2e: all checks passed"
