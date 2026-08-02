# Architecture Overview

This document describes the repository as it exists at this revision. It is not
the target-product feature list.

## Implemented foundation

The workspace currently contains:

- dependency-light core crates for primitives, events, continuations, expressions,
  graph compilation, protocol support, and event pipelines;
- versioned wire-contract crates for runtime, harness, tools, plugins, and frontends;
- four-layer runtime, harness, and CLI process slices;
- a checksummed JSONL journal, content-addressed artifact dependency, canonical
  conversation structures, validated snapshot data/dependency adapters, reducers,
  action proposals, and permission evaluation;
- an authenticated long-running runtime with durable sessions, replay and
  branching, streamed turns, approvals, continuations, schedules, receipts, and
  crash recovery;
- isolated harness, plugin-host, and first-party tool-host processes connected by
  versioned protocols;
- an N-tier session-style registry, immutable per-session style bindings, and a
  generic compiled-graph executor used by arbitrary admitted graphs and current
  built-in modes;
- an N-tier harness registry with per-session adapter identity and capability
  negotiation;
- replay-derived style/graph introspection consumed by CLI JSON and the TUI
  Graph view without dispatching effects;
- per-session runtime- and plugin-provided memory retrieval and compaction plus
  ordered plugin context transforms with canonical projection provenance,
  durable receipts, and fail-closed recovery;
- protocol-only CLI, TUI, and ACP frontends;
- deterministic architecture enforcement and intentional violating fixtures.

The runnable runtime is an authenticated local daemon. The CLI and TUI connect
over local IPC and can discover and select styles, create and inspect sessions,
run streamed turns, replay and branch history, resolve approvals, cancel work,
and manage schedules. Deterministic provider, harness, plugin, memory, and tool
fixtures keep the default suite credential-free.

## Dependency topology

```text
CLI service -> CLI logic -> CLI data -> CLI dependency
                                      -> runtime wire DTOs

runtime service -> runtime logic -> runtime data -> runtime dependency
runtime service -> runtime wire DTOs

harness service -> harness logic -> harness data -> harness dependency
harness service -> harness wire DTOs
```

Composition roots may construct every layer in their own process. Process-layer
crates may not import another process's internal layer crates.

## Partial product integrations

Exact persisted executor dispatch is live for arbitrary admitted user graphs,
the migrated built-ins, planner-worker v1.4, and isolated plugin nodes,
including an exact plugin-host executor inside a bounded parallel region. Runtime
logic owns variable, transition, effect, receipt, budget, and recovery
validation; plugin context transforms and plugin-provided memory retrieval and
compaction use the same proposal/dispatch/receipt/application discipline.

Product completion remains broader than this foundation. Independent planner
worker turns, branch workspaces, and child-owned diff/test evidence are
cross-platform process-tested. The complete workspace-mode/write-denial matrix,
additional plugin action and nested-parallel classes, TUI LSP management,
broader semantic DLP, and macOS process evidence remain open. Immediate-parent
child-session MCP inheritance is process-tested on Windows and Ubuntu/WSL2 but
does not establish transitive or grandchild inheritance. Consult `STATUS.md` for
the evidence level of each tool host, protocol, recovery path, and frontend
surface.

See [Initial maps](initial-maps.md), [N-tier rules](n-tier.md), and
[Process boundaries](process-boundaries.md).
