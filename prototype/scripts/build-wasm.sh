#!/usr/bin/env bash
# Build the core as WebAssembly for the in-browser runtime (ui/pkg).
#
# Requirements: the wasm32-unknown-unknown target and wasm-bindgen-cli 0.2.100:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-bindgen-cli --version 0.2.100 --locked
# Set CARGO / CARGO_FLAGS to use another toolchain (e.g. build-std).
set -euo pipefail
cd "$(dirname "$0")/.."
CARGO="${CARGO:-cargo}"
"$CARGO" build --release --target wasm32-unknown-unknown -p agentmod-wasm ${CARGO_FLAGS:-}
TARGET_DIR="${CARGO_TARGET_DIR:-target}"
wasm-bindgen --target web --no-typescript --out-dir ui/pkg "$TARGET_DIR/wasm32-unknown-unknown/release/agentmod_wasm.wasm"
ls -la ui/pkg
