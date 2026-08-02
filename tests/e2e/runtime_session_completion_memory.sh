#!/usr/bin/env sh
set -eu

repository=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repository"
cargo build --locked -p agentmod-runtime -p agentmod-harness -p agentmod-cli
python3 tests/e2e/session_completion_memory_e2e.py \
    --repository "$repository" \
    --runtime "$repository/target/debug/agentmod-runtime" \
    --cli "$repository/target/debug/agentmod" \
    --harness "$repository/target/debug/agentmod-harness" \
    --platform linux
