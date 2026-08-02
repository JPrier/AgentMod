#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-scheduler \
    -p agentmod-harness -p agentmod-cli

configured_target=${CARGO_TARGET_DIR:-$repository/target}
case "$configured_target" in
    /*) target_root=$configured_target ;;
    *) target_root=$repository/$configured_target ;;
esac
debug_root=$target_root/debug
python3 tests/e2e/artifact_handoff_finalize_e2e.py \
    --repository "$repository" \
    --runtime "$debug_root/agentmod-runtime" \
    --cli "$debug_root/agentmod" \
    --harness "$debug_root/agentmod-harness" \
    --platform linux
