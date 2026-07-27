#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-process-host -p agentmod-cli

run_root=$(mktemp -d "${TMPDIR:-/tmp}/agentmod-coding-e2e.XXXXXX")
workspace="$run_root/workspace"
mkdir -p "$workspace/src"
printf '%s\n' '[package]' 'name = "coding-fixture"' 'version = "0.1.0"' \
    'edition = "2024"' > "$workspace/Cargo.toml"
printf '%s\n' \
    'pub fn add(left: i32, right: i32) -> i32 {' \
    '    left + right + 1' \
    '}' \
    '' \
    '#[cfg(test)]' \
    'mod tests {' \
    '    use super::add;' \
    '    #[test]' \
    '    fn adds_two_numbers() {' \
    '        assert_eq!(add(2, 3), 5);' \
    '    }' \
    '}' > "$workspace/src/lib.rs"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$repository/target/debug/agentmod-filesystem-host"
export AGENTMOD_PROCESS_HOST_PROGRAM="$repository/target/debug/agentmod-process-host"
export AGENTMOD_PROCESS_ALLOWED_EXECUTABLES="cargo"
export AGENTMOD_PERMISSION_MODE="allow"

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    rm -rf -- "$run_root"
}
trap cleanup EXIT INT TERM

(cd "$run_root" && "$repository/target/debug/agentmod-runtime" serve) &
daemon_pid=$!
attempt=0
until "$repository/target/debug/agentmod" doctor --json >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
        echo "runtime did not become ready" >&2
        exit 1
    fi
    sleep 0.1
done

created=$("$repository/target/debug/agentmod" session create \
    --workspace "$workspace" --style persistent-chat --json)
session_id=$(printf '%s' "$created" | sed -n \
    's/.*"session_id":"\([^"]*\)".*/\1/p')
turn=$("$repository/target/debug/agentmod" run "fix add and verify it" \
    --session "$session_id" --option 'mock_scenario="coding_task"' --json)
printf '%s' "$turn" | grep -F \
    '"text":"implemented the fix and verified the tests pass"' >/dev/null
grep -F 'left + right' "$workspace/src/lib.rs" >/dev/null
! grep -E 'left \+ right \+ [12]' "$workspace/src/lib.rs" >/dev/null

journal="$run_root/sessions/$session_id/events.jsonl"
test "$(grep -c '"event_type":"tool.call_proposed"' "$journal")" -eq 5
grep -F '"call_id":"coding-test-failing"' "$journal" | \
    grep -F '"success":false' >/dev/null
grep -F '"call_id":"coding-test-passing"' "$journal" | \
    grep -F '"success":true' >/dev/null
cargo test --quiet --manifest-path "$workspace/Cargo.toml"
echo "complete coding-task E2E passed"
