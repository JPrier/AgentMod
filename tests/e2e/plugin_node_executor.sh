#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository"

target_dir="${CARGO_TARGET_DIR:-target}"
if [[ "$target_dir" != /* ]]; then
  target_dir="$repository/$target_dir"
fi
run_root="$(mktemp -d "${TMPDIR:-/tmp}/agentmod-graph-c-e2e.XXXXXX")"
succeeded=false
cleanup() {
  case "$run_root" in
    "${TMPDIR:-/tmp}"/agentmod-graph-c-e2e.*)
      if [[ "$succeeded" == true ]]; then
        rm -rf -- "$run_root"
      else
        echo "retained failed Graph C E2E root: $run_root" >&2
      fi
      ;;
    *) echo "refusing unexpected Graph C E2E root: $run_root" >&2 ;;
  esac
}
trap cleanup EXIT HUP INT TERM

if ! cargo build --locked -p agentmod-plugin-host -p agentmod-plugin-fixture-worker; then
  echo "plugin process fixture build failed" >&2
  exit 1
fi
export AGENTMOD_TEST_PLUGIN_HOST="$target_dir/debug/agentmod-plugin-host"
export AGENTMOD_TEST_PLUGIN_WORKER="$target_dir/debug/agentmod-plugin-fixture-worker"
if ! cargo test --locked -p agentmod-integration-tests \
  --test plugin_node_process \
  -- --ignored 2>&1 | tee "$run_root/plugin-node-process.log"; then
  echo "plugin-node process tests failed" >&2
  exit 1
fi
succeeded=true
echo "plugin Graph C registration/invocation/validation/restart/timeout/ambiguity E2E passed"
