#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build -p agentmod-runtime -p agentmod-harness \
    -p agentmod-filesystem-host -p agentmod-process-host \
    -p agentmod-scheduler -p agentmod-cli -p agentmod-tui

run_root=$(mktemp -d "${TMPDIR:-/tmp}/am-process-e2e.XXXXXX")
workspace="$run_root/workspace"
mkdir -p "$workspace"
fixture="$run_root/agentmod-interactive"
cat >"$run_root/interactive.rs" <<'RUST'
use std::io::{self, BufRead, Write};
fn main() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(180));
        std::process::exit(124);
    });
    println!("ready");
    io::stdout().flush().expect("flush");
    for line in io::stdin().lock().lines() {
        let line = line.expect("line");
        println!("echo:{line}");
        io::stdout().flush().expect("flush");
        if line == "exit" { break; }
    }
}
RUST
rustc "$run_root/interactive.rs" -o "$fixture"

export AGENTMOD_RUNTIME_ENDPOINT="$run_root/runtime.sock"
export AGENTMOD_RUNTIME_AUTH_TOKEN="0123456789abcdef0123456789abcdef0123456789abcdef"
export AGENTMOD_HARNESS_PROGRAM="$repository/target/debug/agentmod-harness"
export AGENTMOD_FILESYSTEM_HOST_PROGRAM="$repository/target/debug/agentmod-filesystem-host"
export AGENTMOD_PROCESS_HOST_PROGRAM="$repository/target/debug/agentmod-process-host"
export AGENTMOD_SCHEDULER_PROGRAM="$repository/target/debug/agentmod-scheduler"
export AGENTMOD_SCHEDULER_ROOT="$run_root/scheduler"
export AGENTMOD_PROCESS_ALLOWED_EXECUTABLES="$fixture"
export AGENTMOD_PROCESS_IDLE_TIMEOUT_MS=750
export AGENTMOD_PERMISSION_MODE=allow
cli="$repository/target/debug/agentmod"
runtime="$repository/target/debug/agentmod-runtime"
tui="$repository/target/debug/agentmod-tui"
process_action_sequence=0
last_process_response=
succeeded=false

cleanup() {
    if [ -n "${daemon_pid:-}" ]; then
        kill -9 "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    pkill -f "^$fixture$" 2>/dev/null || true
    case "$run_root" in
        "${TMPDIR:-/tmp}"/am-process-e2e.*)
            if [ "$succeeded" = true ]; then
                rm -rf -- "$run_root"
            else
                echo "retained failed process restart E2E root: $run_root" >&2
            fi
            ;;
        *) echo "refusing unexpected process restart E2E root: $run_root" >&2 ;;
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
        sleep 0.05
    done
}

process_action() {
    session=$1
    tool=$2
    arguments=$3
    process_action_sequence=$((process_action_sequence + 1))
    response=$("$cli" run "execute deterministic process action" \
        --session "$session" \
        --option 'mock_scenario="process_action"' \
        --option "mock_process_tool=$tool" \
        --option "mock_process_arguments=$arguments" \
        --option "mock_process_call_id=process-action-$process_action_sequence" \
        --json)
    last_process_response=$response
    if printf '%s' "$response" | grep -F '"event":"failed"' >/dev/null; then
        printf 'process action %s failed: %s\n' "$tool" "$response" >&2
        exit 1
    fi
}

start_runtime
created=$("$cli" session create --workspace "$workspace" \
    --style persistent-chat --json)
session_id=$(printf '%s' "$created" | sed -n \
    's/.*"session_id":"\([^"]*\)".*/\1/p')
process_action "$session_id" process.start_pty \
    "{\"executable\":\"$fixture\",\"arguments\":[],\"working_directory\":null,\"environment\":{},\"timeout_ms\":null,\"output_limit_bytes\":65536,\"cleanup\":\"retain\",\"terminal\":{\"columns\":80,\"rows\":24,\"pixel_width\":0,\"pixel_height\":0}}"

process_root="$workspace/.agentmod/process-logs"
if [ ! -d "$process_root" ]; then
    printf 'process.start_pty did not create process logs: %s\n' \
        "$last_process_response" >&2
    exit 1
fi
process_count=$(find "$process_root" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
[ "$process_count" = 1 ]
process_id=$(find "$process_root" -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)

kill -9 "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
start_runtime

process_action "$session_id" process.reattach "{\"process_id\":\"$process_id\"}"
"$cli" schedule add on-process-echo --session "$session_id" \
    --prompt "inspect the newly observed process output" \
    --process-id "$process_id" --contains "echo:hello-after-runtime-restart" \
    --json >/dev/null
process_action "$session_id" process.input \
    "{\"process_id\":\"$process_id\",\"content\":\"hello-after-runtime-restart\\r\\n\",\"close\":false}"
journal="$run_root/sessions/$session_id/events.jsonl"
attempt=0
until grep -F 'echo:hello-after-runtime-restart' "$journal" >/dev/null 2>&1; do
    process_action "$session_id" process.read \
        "{\"process_id\":\"$process_id\",\"stream\":\"terminal\",\"offset\":0,\"length\":65536}"
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
        echo "reattached PTY output did not enter canonical history" >&2
        exit 1
    fi
    sleep 0.05
done
attempt=0
until [ "$(grep '"event_type":"scheduler.fired"' "$journal" 2>/dev/null |
    grep -c '"schedule_id":"on-process-echo"' || true)" -eq 1 ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
        echo "matching process output did not execute its schedule" >&2
        exit 1
    fi
    sleep 0.05
done
process_action "$session_id" process.read \
    "{\"process_id\":\"$process_id\",\"stream\":\"terminal\",\"offset\":0,\"length\":65536}"
sleep 0.25
[ "$(grep '"event_type":"scheduler.fired"' "$journal" |
    grep -c '"schedule_id":"on-process-echo"')" -eq 1 ]

kill -9 "$daemon_pid"
wait "$daemon_pid" 2>/dev/null || true
daemon_pid=
start_runtime
[ "$(grep '"event_type":"scheduler.fired"' "$journal" |
    grep -c '"schedule_id":"on-process-echo"')" -eq 1 ]
process_action "$session_id" process.reattach "{\"process_id\":\"$process_id\"}"
process_action "$session_id" process.input \
    "{\"process_id\":\"$process_id\",\"content\":\"exit\\r\\n\",\"close\":false}"
process_action "$session_id" process.wait "{\"process_id\":\"$process_id\"}"

process_count=$(find "$process_root" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')
[ "$process_count" = 1 ]
for tool in process.start_pty process.reattach process.input process.read process.wait; do
    grep -F "$tool" "$journal" >/dev/null
done
[ "$(grep -c '"event_type":"process.reconciliation_started"' "$journal")" = 2 ]
[ "$(grep -c '"event_type":"process.reconciliation_completed"' "$journal")" = 2 ]
grep -F '"status":"live"' "$journal" >/dev/null
resources=$("$tui" --smoke-session-command "$session_id" /resources)
printf '%s' "$resources" | grep -F "selected=$session_id" >/dev/null
printf '%s' "$resources" | grep -F 'resources=0/0/2' >/dev/null
succeeded=true
echo "runtime restart preserved one PTY, delivered output once, and committed exit without redispatch"
