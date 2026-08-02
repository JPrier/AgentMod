#!/usr/bin/env bash
set -u

step_id=${1:?step id is required}
shift
if [[ ! "$step_id" =~ ^[0-9]{2}[a-z0-9]*-[a-z0-9-]+$ ]]; then
  echo "invalid Linux final-validation step id: $step_id" >&2
  exit 2
fi
if [[ $# -eq 0 ]]; then
  echo "Linux final-validation command is required" >&2
  exit 2
fi

copy_root=${AGENTMOD_LINUX_FINAL_COPY_ROOT:?copy root is required}
cargo_target_dir=${AGENTMOD_LINUX_FINAL_TARGET_DIR:-"$copy_root/.cargo-target"}
evidence_root=/mnt/c/Users/jkpri/AgentMod/.omx/validation
prefix="$evidence_root/2026-07-31-linux-final-$step_id"
stdout_path="$prefix.stdout.log"
stderr_path="$prefix.stderr.log"
result_path="$prefix.result.json"

for path in "$stdout_path" "$stderr_path" "$result_path"; do
  if [[ -e "$path" ]]; then
    echo "refusing to overwrite existing Linux final-validation evidence: $path" >&2
    exit 2
  fi
done

started_at=$(date --iso-8601=seconds --utc)
started_ns=$(date +%s%N)
set +e
(
  cd "$copy_root"
  CARGO_TARGET_DIR="$cargo_target_dir" \
    CARGO_TERM_COLOR=never \
    RUST_BACKTRACE=1 \
    "$@"
) >"$stdout_path" 2>"$stderr_path"
exit_code=$?
set -e
completed_ns=$(date +%s%N)
completed_at=$(date --iso-8601=seconds --utc)
elapsed_ns=$((completed_ns - started_ns))

python3 - \
  "$result_path" "$step_id" "$exit_code" "$elapsed_ns" \
  "$started_at" "$completed_at" "$copy_root" "$cargo_target_dir" \
  "$stdout_path" "$stderr_path" "$@" <<'PY'
import json
import pathlib
import sys

(
    result_path,
    step_id,
    exit_code,
    elapsed_ns,
    started_at,
    completed_at,
    copy_root,
    cargo_target_dir,
    stdout_path,
    stderr_path,
    *command,
) = sys.argv[1:]
repository = pathlib.Path("/mnt/c/Users/jkpri/AgentMod")
result = {
    "schema_version": 1,
    "step_id": step_id,
    "command": " ".join(command),
    "arguments": command[1:],
    "exit_code": int(exit_code),
    "passed": int(exit_code) == 0,
    "elapsed_seconds": int(elapsed_ns) / 1_000_000_000,
    "started_at_utc": started_at,
    "completed_at_utc": completed_at,
    "source_head": "abbf97b82687a4d1e7463aab33382258d6d38fd9",
    "source_working_tree_dirty": True,
    "isolated_copy_root": copy_root,
    "cargo_target_dir": cargo_target_dir,
    "stdout_path": str(pathlib.Path(stdout_path).relative_to(repository)),
    "stderr_path": str(pathlib.Path(stderr_path).relative_to(repository)),
}
pathlib.Path(result_path).write_text(
    json.dumps(result, indent=2) + "\n", encoding="utf-8"
)
print(json.dumps(result, indent=2))
PY

if [[ "$exit_code" -ne 0 ]]; then
  echo "--- stdout tail ---"
  tail -n 80 "$stdout_path" 2>/dev/null || true
  echo "--- stderr tail ---"
  tail -n 120 "$stderr_path" 2>/dev/null || true
fi
exit "$exit_code"
