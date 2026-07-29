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

create_session() {
    style=$1
    "$cli" session create --workspace "$workspace" --style "$style" --json |
        sed -n 's/.*"session_id":"\([^"]*\)".*/\1/p'
}

file_id=$(create_session e2e-persistent-file)
none_id=$(create_session e2e-persistent-none)
none_compaction_id=$(create_session e2e-persistent-none)
sliding_id=$(create_session e2e-persistent-sliding)
test -n "$file_id"
test -n "$none_id"
test -n "$none_compaction_id"
test -n "$sliding_id"

AGENTMOD_TEST_MEMORY_FILE="$run_root/memory/file.jsonl" \
AGENTMOD_TEST_MEMORY_SCOPE="session:$file_id" \
AGENTMOD_TEST_MEMORY_SOURCE="process-e2e-fixture" \
AGENTMOD_TEST_MEMORY_CONTENT="orchid memory probe retained only for the file-backed session" \
AGENTMOD_TEST_MEMORY_CREATED_AT_MS=1000 \
    cargo test -p agentmod-runtime-dependency --locked \
        --test e2e_memory_seed \
        seed_file_memory_for_process_e2e -- --ignored --exact

file_turn=$("$cli" run "orchid memory probe" --session "$file_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="context-output"' --json)
none_turn=$("$cli" run "orchid memory probe" --session "$none_id" \
    --option 'mock_scenario="streaming_text"' \
    --option 'mock_text="context-output"' --json)
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

echo "runtime style-selected memory/compaction process E2E passed"
