#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-style-context-e2e.XXXXXX")
workspace="$run_root/workspace"
style_root="$run_root/styles/user"
mkdir -p "$workspace" "$style_root"
cp tests/fixtures/styles/persistent-file-none.toml "$style_root/"
cp tests/fixtures/styles/persistent-none-none.toml "$style_root/"
cp tests/fixtures/styles/persistent-none-sliding.toml "$style_root/"
sed \
    -e 's/id = "e2e-persistent-file"/id = "e2e-persistent-sqlite"/' \
    -e 's/provider = "file"/provider = "sqlite-fts"/' \
    tests/fixtures/styles/persistent-file-none.toml \
    >"$style_root/persistent-sqlite-none.toml"
sed \
    -e 's/id = "e2e-persistent-file"/id = "e2e-persistent-file-bounded"/' \
    -e 's/max_items = 8/max_items = 1/' \
    -e 's/max_injected_bytes = 16384/max_injected_bytes = 1024/' \
    -e 's/max_query_bytes = 16384/max_query_bytes = 12/' \
    tests/fixtures/styles/persistent-file-none.toml \
    >"$style_root/persistent-file-bounded.toml"
sed \
    -e 's/id = "e2e-persistent-sliding"/id = "e2e-persistent-tight"/' \
    -e 's/reserved_context_tokens = 1024/reserved_context_tokens = 128/' \
    -e 's/max_provider_projection_tokens = 8192/max_provider_projection_tokens = 256/' \
    tests/fixtures/styles/persistent-none-sliding.toml \
    >"$style_root/persistent-tight.toml"
sed \
    -e 's/id = "e2e-persistent-file"/id = "e2e-persistent-context-node"/' \
    -e 's/retrieval_timing = "before_model_request"/retrieval_timing = "context_node"/' \
    tests/fixtures/styles/persistent-file-none.toml \
    >"$style_root/persistent-context-node.toml"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
runtime="$repository/target/debug/agentmod-runtime"
cli="$repository/target/debug/agentmod"

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    case "$run_root" in
        "${TMPDIR:-/tmp}"/agentmod-style-context-e2e.*)
            rm -rf -- "$run_root"
            ;;
        *)
            echo "refusing to remove unexpected E2E root: $run_root" >&2
            ;;
    esac
}
trap cleanup EXIT INT TERM

(cd "$run_root" && "$runtime" serve) &
daemon_pid=$!
attempt=0
until "$cli" doctor --json >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 80 ]; then
        echo "runtime did not become ready" >&2
        exit 1
    fi
    sleep 0.1
done

for fixture in \
    persistent-file-none.toml \
    persistent-none-none.toml \
    persistent-none-sliding.toml
do
    "$cli" style validate "tests/fixtures/styles/$fixture" --json |
        grep -F '"valid":true' >/dev/null
done
"$cli" style validate "$style_root/persistent-sqlite-none.toml" --json |
    grep -F '"valid":true' >/dev/null
for fixture in \
    persistent-file-bounded.toml \
    persistent-tight.toml \
    persistent-context-node.toml
do
    "$cli" style validate "$style_root/$fixture" --json |
        grep -F '"valid":true' >/dev/null
done

create_session() {
    style=$1
    "$cli" session create --workspace "$workspace" --style "$style" --json |
        sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p'
}

file_id=$(create_session e2e-persistent-file)
isolated_file_id=$(create_session e2e-persistent-file)
sqlite_id=$(create_session e2e-persistent-sqlite)
bounded_file_id=$(create_session e2e-persistent-file-bounded)
tight_id=$(create_session e2e-persistent-tight)
unsupported_timing_id=$(create_session e2e-persistent-context-node)
none_id=$(create_session e2e-persistent-none)
none_compaction_id=$(create_session e2e-persistent-none)
sliding_id=$(create_session e2e-persistent-sliding)
test -n "$file_id"
test -n "$isolated_file_id"
test -n "$sqlite_id"
test -n "$bounded_file_id"
test -n "$tight_id"
test -n "$unsupported_timing_id"
test -n "$none_id"
test -n "$none_compaction_id"
test -n "$sliding_id"

AGENTMOD_TEST_MEMORY_PROVIDER=file \
AGENTMOD_TEST_MEMORY_PATH="$run_root/memory/file.jsonl" \
AGENTMOD_TEST_MEMORY_SCOPE="session:$file_id" \
AGENTMOD_TEST_MEMORY_SOURCE="process-e2e-fixture" \
AGENTMOD_TEST_MEMORY_CONTENT="orchid memory probe retained only for the file-backed session" \
AGENTMOD_TEST_MEMORY_CREATED_AT_MS=1000 \
    cargo test -p agentmod-runtime-dependency --locked \
        --test e2e_memory_seed \
        seed_file_memory_for_process_e2e -- --ignored --exact
AGENTMOD_TEST_MEMORY_PROVIDER=sqlite-fts \
AGENTMOD_TEST_MEMORY_PATH="$run_root/memory/sqlite-fts.sqlite3" \
AGENTMOD_TEST_MEMORY_SCOPE="session:$sqlite_id" \
AGENTMOD_TEST_MEMORY_SOURCE="sqlite-e2e-fixture" \
AGENTMOD_TEST_MEMORY_CONTENT="orchid memory probe retained only for the sqlite-backed session" \
AGENTMOD_TEST_MEMORY_CREATED_AT_MS=1001 \
    cargo test -p agentmod-runtime-dependency --locked \
        --test e2e_memory_seed \
        seed_file_memory_for_process_e2e -- --ignored --exact
AGENTMOD_TEST_MEMORY_PROVIDER=file \
AGENTMOD_TEST_MEMORY_PATH="$run_root/memory/file.jsonl" \
AGENTMOD_TEST_MEMORY_SCOPE="session:$bounded_file_id" \
AGENTMOD_TEST_MEMORY_SOURCE="bounded-e2e-one" \
AGENTMOD_TEST_MEMORY_CONTENT="orchid probe first bounded record" \
AGENTMOD_TEST_MEMORY_CREATED_AT_MS=1002 \
    cargo test -p agentmod-runtime-dependency --locked \
        --test e2e_memory_seed \
        seed_file_memory_for_process_e2e -- --ignored --exact
AGENTMOD_TEST_MEMORY_PROVIDER=file \
AGENTMOD_TEST_MEMORY_PATH="$run_root/memory/file.jsonl" \
AGENTMOD_TEST_MEMORY_SCOPE="session:$bounded_file_id" \
AGENTMOD_TEST_MEMORY_SOURCE="bounded-e2e-two" \
AGENTMOD_TEST_MEMORY_CONTENT="orchid probe second bounded record" \
AGENTMOD_TEST_MEMORY_CREATED_AT_MS=1003 \
    cargo test -p agentmod-runtime-dependency --locked \
        --test e2e_memory_seed \
        seed_file_memory_for_process_e2e -- --ignored --exact

file_turn=$("$cli" run "orchid memory probe" --session "$file_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="context-output"' --json)
none_turn=$("$cli" run "orchid memory probe" --session "$none_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="context-output"' --json)
isolated_file_turn=$("$cli" run "orchid memory probe" \
    --session "$isolated_file_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="context-output"' --json)
sqlite_turn=$("$cli" run "orchid memory probe" --session "$sqlite_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="context-output"' --json)
"$cli" run "orchid probe and deliberately longer query text" \
    --session "$bounded_file_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="bounded-output"' --json >/dev/null
if "$cli" run "oversized-current-input-$(printf '%01024d' 0)" \
    --session "$tight_id" \
    --option 'mock_scenario="streaming_text"' --json >/dev/null 2>&1
then
    echo "oversized first projection was dispatched" >&2
    exit 1
fi
if "$cli" run "unsupported timing must fail preflight" \
    --session "$unsupported_timing_id" \
    --option 'mock_scenario="streaming_text"' --json >/dev/null 2>&1
then
    echo "unsupported retrieval timing executed" >&2
    exit 1
fi
printf '%s' "$file_turn" | grep -F '"text":"alpha "' >/dev/null
printf '%s' "$file_turn" | grep -F '"text":"beta "' >/dev/null
printf '%s' "$file_turn" | grep -F '"text":"context-output"' >/dev/null
test "$file_turn" = "$none_turn" ||
    {
        # Sequence identities differ; visible event streams must not.
        file_events=$(printf '%s' "$file_turn" |
            sed 's/.*"events":\(\[[^]]*\]\).*/\1/')
        none_events=$(printf '%s' "$none_turn" |
            sed 's/.*"events":\(\[[^]]*\]\).*/\1/')
        test "$file_events" = "$none_events"
    }

file_journal="$run_root/sessions/$file_id/events.jsonl"
none_journal="$run_root/sessions/$none_id/events.jsonl"
isolated_file_journal="$run_root/sessions/$isolated_file_id/events.jsonl"
sqlite_journal="$run_root/sessions/$sqlite_id/events.jsonl"
bounded_file_journal="$run_root/sessions/$bounded_file_id/events.jsonl"
tight_journal="$run_root/sessions/$tight_id/events.jsonl"
unsupported_timing_journal="$run_root/sessions/$unsupported_timing_id/events.jsonl"
grep -F '"event_type":"context.projection_replaced"' "$file_journal" |
    grep -F '"method":"memory:file"' |
    grep -F '"kind":"retrieved_memory"' |
    grep -F '"query":"orchid memory probe"' |
    grep -F "\"scope\":\"session:$file_id\"" |
    grep -F '"source":"process-e2e-fixture"' |
    grep -F '"reference":"' |
    grep -F '"injection_event":"' >/dev/null
grep -F '"event_type":"context.projection_replaced"' "$file_journal" |
    grep -E '"kind":"retrieved_memory".*"kind":"user_message"' >/dev/null
if grep -F '"event_type":"context.projection_replaced"' "$none_journal" >/dev/null; then
    echo "no-memory session received a context replacement" >&2
    exit 1
fi
if grep -F '"kind":"retrieved_memory"' "$isolated_file_journal" >/dev/null; then
    echo "file memory crossed its session scope" >&2
    exit 1
fi
grep -F '"event_type":"context.projection_replaced"' "$sqlite_journal" |
    grep -F '"method":"memory:sqlite-fts"' |
    grep -F '"provider":"sqlite-fts"' |
    grep -F "\"scope\":\"session:$sqlite_id\"" |
    grep -F '"source":"sqlite-e2e-fixture"' >/dev/null
bounded_replacement=$(grep -F '"event_type":"context.projection_replaced"' \
    "$bounded_file_journal" | tail -n 1)
test "$(printf '%s' "$bounded_replacement" |
    grep -o '"kind":"retrieved_memory"' | wc -l | tr -d ' ')" -eq 1
printf '%s' "$bounded_replacement" |
    grep -F '"query":"orchid probe"' >/dev/null
if grep -F '"event_type":"model.request_proposed"' "$tight_journal" >/dev/null; then
    echo "oversized projection crossed the provider proposal boundary" >&2
    exit 1
fi
test "$(wc -l <"$unsupported_timing_journal" | tr -d ' ')" -eq 1

file_proposal=$(grep -F '"event_type":"model.request_proposed"' \
    "$file_journal" | tail -n 1)
none_proposal=$(grep -F '"event_type":"model.request_proposed"' \
    "$none_journal" | tail -n 1)
file_hash=$(printf '%s' "$file_proposal" |
    sed -n 's/.*"projection_hash":"\([^"]*\)".*/\1/p')
none_hash=$(printf '%s' "$none_proposal" |
    sed -n 's/.*"projection_hash":"\([^"]*\)".*/\1/p')
test -n "$file_hash"
test -n "$none_hash"
test "$file_hash" != "$none_hash"
printf '%s' "$file_proposal" | grep -F '"provider":"deterministic-mock"' >/dev/null
printf '%s' "$none_proposal" | grep -F '"provider":"deterministic-mock"' >/dev/null

memory_line=$(grep -n -m 1 -F \
    '"event_type":"context.projection_replaced"' "$file_journal" |
    cut -d: -f1)
proposal_line=$(grep -n -m 1 -F \
    '"event_type":"model.request_proposed"' "$file_journal" |
    cut -d: -f1)
test "$memory_line" -lt "$proposal_line"

# Restart and prove style-selected memory routes reconstruct from durable state.
kill "$daemon_pid"
wait "$daemon_pid"
daemon_pid=
(cd "$run_root" && "$runtime" serve) &
daemon_pid=$!
attempt=0
until "$cli" doctor --json >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 80 ]; then
        echo "runtime did not become ready after restart" >&2
        exit 1
    fi
    sleep 0.1
done
"$cli" session inspect "$file_id" --json |
    grep -F '"provider":"file"' >/dev/null
"$cli" session inspect "$sqlite_id" --json |
    grep -F '"provider":"sqlite-fts"' >/dev/null
"$cli" session inspect "$none_id" --json |
    grep -F '"provider":"none"' >/dev/null
"$cli" run "orchid memory probe" --session "$file_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="file-after-restart"' --json >/dev/null
"$cli" run "orchid memory probe" --session "$sqlite_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="sqlite-after-restart"' --json >/dev/null
"$cli" session inspect "$file_id" --json |
    grep -F '"source":"process-e2e-fixture"' |
    grep -F '"injection_event":"' >/dev/null
"$cli" session inspect "$sqlite_id" --json |
    grep -F '"source":"sqlite-e2e-fixture"' |
    grep -F '"injection_event":"' >/dev/null

# Branch the memory-backed projection into an explicit no-memory style.
parent_before=$("$cli" session inspect "$file_id" --json)
fork_sequence=$(printf '%s' "$parent_before" |
    sed -n 's/.*"last_sequence":\([0-9][0-9]*\).*/\1/p')
test -n "$fork_sequence"
branch_json=$("$cli" session branch "$file_id" --at "$fork_sequence" \
    --style e2e-persistent-none --json)
branch_id=$(printf '%s' "$branch_json" |
    sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p')
test -n "$branch_id"
"$cli" session inspect "$branch_id" --json |
    grep -F '"kind":"retrieved_memory"' >/dev/null
"$cli" run "branch must not inherit parent memory" --session "$branch_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="branch-output"' --json >/dev/null
if "$cli" session inspect "$branch_id" --json |
    grep -F '"kind":"retrieved_memory"' >/dev/null
then
    echo "no-memory branch leaked inherited parent memory" >&2
    exit 1
fi
parent_after=$("$cli" session inspect "$file_id" --json)
test "$parent_before" = "$parent_after"
branch_journal="$run_root/sessions/$branch_id/events.jsonl"
cleanup_line=$(grep -n -F '"method":"memory:none"' "$branch_journal" |
    head -n 1 | cut -d: -f1)
branch_proposal_line=$(grep -n -F '"event_type":"model.request_proposed"' \
    "$branch_journal" | head -n 1 | cut -d: -f1)
test -n "$cleanup_line"
test "$cleanup_line" -lt "$branch_proposal_line"

turn_number=1
while [ "$turn_number" -le 18 ]; do
    prompt="equivalent context pressure turn $turn_number"
    output="stable-output-$turn_number"
    none_result=$("$cli" run "$prompt" --session "$none_compaction_id" \
        --option 'mock_scenario="streaming_text"' \
        --option "mock_text=\"$output\"" --json)
    sliding_result=$("$cli" run "$prompt" --session "$sliding_id" \
        --option 'mock_scenario="streaming_text"' \
        --option "mock_text=\"$output\"" --json)
    printf '%s' "$none_result" | grep -F "\"text\":\"$output\"" >/dev/null
    printf '%s' "$sliding_result" | grep -F "\"text\":\"$output\"" >/dev/null
    turn_number=$((turn_number + 1))
done

sliding_journal="$run_root/sessions/$sliding_id/events.jsonl"
none_journal="$run_root/sessions/$none_compaction_id/events.jsonl"
none_committed=$(grep -c -F \
    '"event_type":"conversation.entry_committed"' "$none_journal")
sliding_committed=$(grep -c -F \
    '"event_type":"conversation.entry_committed"' "$sliding_journal")
test "$none_committed" -eq "$sliding_committed"

turn_number=1
while [ "$turn_number" -le 18 ]; do
    prompt="equivalent context pressure turn $turn_number"
    output="stable-output-$turn_number"
    grep -F "\"text\":\"$prompt\"" "$none_journal" >/dev/null
    grep -F "\"text\":\"$prompt\"" "$sliding_journal" >/dev/null
    grep -F "alpha beta $output" "$none_journal" >/dev/null
    grep -F "alpha beta $output" "$sliding_journal" >/dev/null
    turn_number=$((turn_number + 1))
done

if grep -F '"method":"sliding_window"' "$none_journal" >/dev/null; then
    echo "none compaction strategy replaced provider context" >&2
    exit 1
fi
last_compaction=$(grep -F '"event_type":"context.projection_replaced"' \
    "$sliding_journal" | grep -F '"method":"sliding_window"' | tail -n 1)
test -n "$last_compaction"
first_compaction_line=$(grep -n -m 1 -F '"method":"sliding_window"' \
    "$sliding_journal" | cut -d: -f1)
first_proposal_line=$(grep -n -m 1 -F '"event_type":"model.request_proposed"' \
    "$sliding_journal" | cut -d: -f1)
test "$first_compaction_line" -lt "$first_proposal_line"
replacement_count=$(printf '%s' "$last_compaction" |
    grep -o '"kind":"' | wc -l | tr -d ' ')
test "$replacement_count" -lt "$sliding_committed"

last_compaction_line=$(grep -n -F '"event_type":"context.projection_replaced"' \
    "$sliding_journal" | grep -F '"method":"sliding_window"' |
    tail -n 1 | cut -d: -f1)
next_proposal_line=$(awk -v after="$last_compaction_line" \
    'NR > after && /"event_type":"model.request_proposed"/ { print NR; exit }' \
    "$sliding_journal")
test -n "$next_proposal_line"

"$cli" session inspect "$none_compaction_id" --json |
    grep -F '"projection_provenance":null' >/dev/null
"$cli" session inspect "$sliding_id" --json |
    grep -F '"method":"sliding_window"' >/dev/null
last_boundary=$(grep -F '"event_type":"context.boundary_completed"' \
    "$sliding_journal" | grep -F '"boundary":"before_model_request"' |
    tail -n 1)
estimated_tokens=$(printf '%s' "$last_boundary" |
    sed -n 's/.*"estimated_tokens":\([0-9][0-9]*\).*/\1/p')
serialized_bytes=$(printf '%s' "$last_boundary" |
    sed -n 's/.*"serialized_bytes":\([0-9][0-9]*\).*/\1/p')
test -n "$estimated_tokens"
test -n "$serialized_bytes"
test "$estimated_tokens" -le 7168
test "$serialized_bytes" -le 16777216

echo "runtime style-selected memory/compaction process E2E passed"
