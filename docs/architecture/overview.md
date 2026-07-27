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
- deterministic architecture enforcement and intentional violating fixtures.

The runnable binaries are presently health/doctor vertical slices. The runtime
prints a health response and exits; it is not yet a daemon. The harness reports a
deterministic provider catalog; it does not call a model API. The CLI implements
only `doctor` and uses a deterministic runtime client rather than local IPC.

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

## Target topology, not yet implemented

The intended topology has a long-running runtime coordinating separate harness,
tool-host, plugin-host, scheduler, and frontend processes through authenticated
versioned protocols. TUI, ACP, tool hosts, plugin host, scheduler, provider
adapters, MCP, LSP, browser, Git, memory providers, session-style execution, and
multi-agent orchestration remain planned.

See [Initial maps](initial-maps.md), [N-tier rules](n-tier.md), and
[Process boundaries](process-boundaries.md).
