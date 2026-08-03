# AgentMod North Star

This document states the end-state **goal** of AgentMod: what it is, what it must be able to do, what binds it, and what it is not. It is deliberately design-free — it says *what*, never *how*. A separate High-Level Design proposes the approach and may be revised without contaminating this goal. Each design level below it builds consensus at its altitude, then recurses into lower-level designs until the design is implementation-grade; nothing is implemented directly from this document.

## Goal

Today's agent harnesses hardcode the workflows they support; AgentMod exists because those workflows should belong to the user.

AgentMod is a general-purpose agent runtime. Users define their own agent experiences — interactive chat, scheduled jobs, background agents, gated workflows — and the runtime executes them deterministically, exactly as defined, every time. Everything that happens is persisted in an immutable history that is never rewritten, visible to and reachable by every part of the system. Any capability a harness would hardcode — the loop, memory, compaction, tools, UI — is user-replaceable and swappable without interrupting live sessions. It runs equally as a developer's local harness or as a cloud-deployed runtime driven by automated systems.

## Requirements

### Determinism and the model boundary

- The system is deterministic everywhere except the LLM itself. The runtime, workflow execution, history reads and writes, and plugin dispatch are reproducible and dependable; all nondeterminism lives outside the core, in plugins.
- The core contains no LLM client. Model providers are plugins like everything else. The core is fully functional — routable, testable, replayable — with no model attached.
- If a plugin is configured to run, it runs, observably, every time. No silent skips.

### Expressibility — the universality bar

The core ships no agent features; instead, its schemas must make every workflow and feature below implementable **as a plugin or workflow definition, without modifying the core**. Existing harnesses are the floor for expressibility, not the ceiling.

Must be expressible:

- Interactive coding sessions: terminal-based, streaming, with lifecycle hooks that can block, rewrite, or inject.
- Approval gates and sandboxing, for users operating in unsafe spaces. The runtime itself assumes trusted operation; approval is a plugin, never a default.
- Spec-driven development (requirements → design → tasks) and strict gated (LangGraph-style) workflows, as workflow definitions.
- Frontends as plugins over one schema, including plugin-defined UIs that frontends decide how to render. ACP-style editor attachment is just one frontend plugin, with no special status.
- Triggers as plugins: cron/at/every schedules, heartbeats, file events, webhooks, automated systems starting sessions.
- Background and scheduled agents: heartbeat agents, memory injection into fresh sessions, session auto-titling, Discord/Slack send-and-wait.
- Tools as plugins. The core has no concept of "tool": a tool-call request is an event; a plugin that answers it is a tool. Wrapping arbitrary MCP servers must be expressible as a plugin. Tool availability is controllable per session through configuration, and what the model is told about available tools is itself context assembled by plugins, observably.
- Provider-side tool execution is not precluded: a provider plugin may let its provider run tools within a single call for latency and token savings; such activity is internal to that plugin and disclosed through its recorded outputs.
- Subagents; agents starting sessions for other agents; agent-built plugins available to subsequent sessions.
- Multiple simultaneous memory systems; tunable ephemerality.

### Plugins

- Plugins are uncategorized. One generic shape: event in → contributed context + output events. "Memory", "compaction", "introspection", "frontend", "tool", "provider" are things plugins happen to do — never runtime concepts.
- Plugin authorship is language-agnostic. Writing a plugin must not require Rust, so existing tools can be wrapped as plugins.
- Hot-swap at runtime: plugins and integrations change without core restarts, while 10–100 concurrent sessions keep running through the experiment.
- State changes only through plugin invocations. A plugin may leave state unchanged; when state changes, the change is attributable to exactly one recorded invocation.

### History and observability

- The persisted history is the source of truth. Every chat, tool call, and event is recorded; history is append-only — never rewritten; oversized payloads spill to files but remain part of the record.
- Everything is explainable from the record at the plugin boundary: for any event, the record shows its pipeline — which plugins ran, in what order, and each step's input and output state. Plugins are internal black boxes; the boundary record is the disclosure, and internal logging is the plugin author's business.
- Live sessions are inspectable in flight, not just post-hoc.

### Reliability

- History-is-truth recovery: if the runtime crashes or restarts, every session resumes from history — recoverable even mid-pipeline, because every step is durably recorded. No session state exists only in memory. The persisted record is, implicitly, a checkpointing system.
- Plugin fault isolation: a misbehaving plugin (crash, hang, resource exhaustion) can fail its own work but must not corrupt history, halt the runtime, or take down other sessions. Isolation mechanics are lower-design decisions; the tension between isolation and per-event overhead is acknowledged and left to the High-Level Design to resolve.
- The core targets effectively-perfect reliability. It is written so exhaustive verification is feasible — deterministic, idempotent, functional style; every branch testable; every input and output validated — and it is tested to that standard.

### Performance

- Runtime overhead is imperceptible against model latency: the core's own event path — routing, pipeline traversal, plugin dispatch, history writes — adds no more than ~10 ms end-to-end versus a direct API call, sustained at 100 concurrent sessions on developer hardware. Plugin execution time is outside this budget and is the plugin author's responsibility.
- Interactive streaming remains token-smooth.

### Deployment

- One runtime serves multiple invocation methods and compute patterns: local interactive use, cloud-hosted deployment, and automated triggers, without distinct runtimes per pattern.

## Daily-driver milestone (acceptance test)

The proof of generality: the day Claude Code, Codex, Kiro, the Zed agent, and the private OpenClaw fork are retired, the operator's actual workflows run on AgentMod alone — as plugins and workflow definitions, in daily use:

- Terminal-interactive coding sessions with hooks and MCP tools.
- Cron/scheduled background agents, heartbeat agents, memory injection, session auto-titling.
- Discord/Slack send-and-wait agents.
- Subagents and sessions spawned by other agents.

## Constraints

- **Core in Rust.** The determinism, reliability, and scale requirements demand its memory safety and performance; the language is a consequence of the requirements, not a preference.
- **Single-tenant deployments.** A deployment serves one trusting person or team. If siloing between humans is required, run additional runtime instances.
- **This document stays at the highest level.** Its size limit is semantic: goal, requirements, constraints, and non-goals only. Any sentence explaining *how* is cut or pushed down the design tree.

## Non-goals

- **Multi-tenant isolation.** No per-user auth, quotas, or data isolation inside one runtime; separation is achieved by running more instances.
- **Anything built into the core.** No built-in memory, compaction, frontend, scheduler, model client, tools, or approval flow. The distribution may ship a bundle of default plugins, but they use exactly the public plugin interface with zero privileged access — batteries included, all removable, none special.
- **Feature-for-feature harness parity.** Existing harnesses set the expressibility floor; their shapes do not constrain the design.
- **Preserving the existing codebase.** Reuse of the current implementation is a cost decision made at audit time; the design owes it nothing.
- **Safety-by-default approval friction.** The runtime assumes trusted operation; approval and sandboxing exist only where a user plugs them in.
