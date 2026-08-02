#!/usr/bin/env sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
AGENTMOD_GRAPH_B_CANCELLATION_ONLY=1
export AGENTMOD_GRAPH_B_CANCELLATION_ONLY
exec sh "$script_dir/runtime_arbitrary_graph_b.sh"
