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
  generic compiled-graph executor used by persistent chat;
- per-session memory retrieval and compaction with canonical projection
  provenance;
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

The process topology is live, but session-style execution is not complete.
Persistent-chat compatible graphs execute generically; ephemeral turn, research
loop, planner-worker-reviewer, arbitrary declarative graphs, runtime-managed
child sessions, plugin-composed pipelines, and per-session harness capability
selection remain active development. Consult `STATUS.md` for the evidence level
of each tool host, protocol, recovery path, and frontend surface.

See [Initial maps](initial-maps.md), [N-tier rules](n-tier.md), and
[Process boundaries](process-boundaries.md).
