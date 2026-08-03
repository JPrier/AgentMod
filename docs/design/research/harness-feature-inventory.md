# Harness Feature Inventory

Research for [issue #4](https://github.com/JPrier/AgentMod/issues/4), part of the wayfinder map ([#1](https://github.com/JPrier/AgentMod/issues/1)). Surveyed from public documentation only, 2026-08-03.

Harnesses surveyed: **Claude Code** (Anthropic), **OpenAI Codex CLI**, **Zed Agent Panel**, **Curo**, **OpenClaw** (public proxy for the private fork). For each capability area: what the harness offers, and — the payload of this doc — what it does **not** let a user control. AgentMod's premise is that those fixed internals become swappable plugins.

> **Curo caveat.** Three targeted web searches (2026-08-03) found no substantive public documentation, package registry entry, or repository for a harness named "Curo." Its rows below are marked *no public docs*; its column should be filled from first-hand notes by its user. Treat its absence itself as a data point: at least one harness in the replacement set is documentable only by its operator, which is exactly the situation the AgentMod requirements list has to survive.

---

## 1. Session / context management

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | One conversation per session; `--resume`/`--continue`, session forking via subagents; startup context assembled from system prompt, memory, CLAUDE.md hierarchy, environment info, deferred MCP tool schemas; `/context` shows exactly what loaded | [context window](https://code.claude.com/docs/en/context-window), [memory](https://code.claude.com/docs/en/memory) |
| Codex CLI | `codex resume` reopens or searches local chats per-repo; token budget shown live ("100% context left"); cloud environments via `codex cloud` with results applied back locally | [Codex CLI docs](https://developers.openai.com/codex/cli/) |
| Zed | Multiple concurrent threads, each with independent agent, context window, and history; Threads Sidebar grouped by project; archive/restore; checkpoints ("Restore Checkpoint" reverts pre-edit state); `@`-mention files/dirs/symbols/prior threads | [agent panel](https://zed.dev/docs/ai/agent-panel) |
| Curo | *No public docs* | — |
| OpenClaw | Gateway process owns all session state; SQLite session rows + append-only transcript event tree; session keys per channel/agent/subagent; reset via `/new`, daily-reset hour, idle expiry; pruning with age cutoff, entry cap, disk budget (`session.maintenance`) | [session deep dive](https://docs.openclaw.ai/reference/session-management-compaction) |

**Not user-controllable:** Claude Code — the startup assembly order and the system prompt itself (append-only via `--append-system-prompt`); what counts as a session boundary. Codex — session storage format and lifecycle. Zed — thread storage internals; external-agent threads' feature set "varies depending on the agent." OpenClaw — the Gateway owning state end-to-end (no alternate session store), session-key scheme.

## 2. Memory

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Two systems: CLAUDE.md hierarchy (managed-policy → user → project → local, plus `.claude/rules/` with path-scoped frontmatter, `@` imports) and **auto memory** — Claude-written `MEMORY.md` index (first 200 lines/25KB auto-loaded) plus on-demand topic files, per-repo, machine-local; relocatable via `autoMemoryDirectory`; toggleable | [memory](https://code.claude.com/docs/en/memory) |
| Codex CLI | `AGENTS.md` instruction files (via `/init`) — static, user-authored. No harness-written persistent memory documented | [Codex CLI docs](https://developers.openai.com/codex/cli/) |
| Zed | Rules files for instructions; no agent-written memory system documented for native threads; external agents bring their own | [agent panel](https://zed.dev/docs/ai/agent-panel), [external agents](https://zed.dev/docs/ai/external-agents) |
| Curo | *No public docs* | — |
| OpenClaw | Four workspace files (USER.md, MEMORY.md, daily `memory/YYYY-MM-DD.md`, DREAMS.md); hybrid vector+keyword `memory_search` with pluggable embedding providers; **swappable memory backends** (builtin SQLite, QMD, Honcho, LanceDB); "dreaming" consolidation sweep promotes facts to MEMORY.md; pre-compaction memory flush | [memory overview](https://docs.openclaw.ai/concepts/memory) |

**Not user-controllable:** Claude Code — *when* Claude decides to save/recall (model judgment), the 200-line/25KB index load limit, the injection position of memory in context. Codex — no memory to control; users wanting memory must build it into AGENTS.md manually. Zed — n/a natively. OpenClaw — the most swappable of the set (backends and embedders are pluggable), but scoring gates, consolidation thresholds, and taint-gating logic "operate automatically without user visibility or adjustment parameters."

## 3. Compaction / context editing

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Auto-compact near the limit; `/compact` with free-text focus instructions; documented survival table (project-root CLAUDE.md re-injected, skill descriptions not, path-scoped rules summarized away until re-triggered); `PreCompact`/`PostCompact` hooks fire around it | [context window](https://code.claude.com/docs/en/context-window), [hooks](https://code.claude.com/docs/en/hooks) |
| Codex CLI | Context tracked and surfaced; compaction exists but strategy is not a documented configuration surface | [Codex CLI docs](https://developers.openai.com/codex/cli/) |
| Zed | Automatic summarization when threads near token thresholds ("Context Compacted" entry); manual `/compact` | [agent panel](https://zed.dev/docs/ai/agent-panel) |
| Curo | *No public docs* | — |
| OpenClaw | Richest public knob set: `compaction.enabled`, `keepRecentTokens`, **`compaction.provider` (pluggable summarizer)**, `maxActiveTranscriptBytes` preflight guard, mid-turn overflow precheck, `memoryFlush.*` (silent agentic turn writes durable state before summarizing), `thinkingLevel` for the summary call | [session deep dive](https://docs.openclaw.ai/reference/session-management-compaction) |

**Not user-controllable:** Claude Code — the compaction *algorithm* and summary prompt (only per-run focus text), the auto-trigger threshold, what the summarizer keeps. Zed — threshold and summarization strategy. Codex — all of it. OpenClaw — even with a pluggable provider: the built-in context-window headroom reserve, tool-call/tool-result pairing preservation, and chunk-split logic are hardcoded. **In no surveyed harness can a user replace compaction wholesale with a different context-editing strategy (e.g. selective eviction, retrieval-backed reconstruction) — the closest is OpenClaw swapping the summarizer inside a fixed pipeline.**

## 4. Scheduling & background agents

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Background bash tasks; background subagents with completion notifications; scheduling exists as product-level skills/cloud routines rather than a core runtime primitive | [hooks](https://code.claude.com/docs/en/hooks) (TaskCreated/TaskCompleted events) |
| Codex CLI | `codex cloud`: submit work to a configured cloud environment, apply result locally; no local cron/scheduler documented | [Codex CLI docs](https://developers.openai.com/codex/cli/) |
| Zed | None documented — interactive panel only | [agent panel](https://zed.dev/docs/ai/agent-panel) |
| Curo | *No public docs* | — |
| OpenClaw | Full scheduler in the Gateway: `at`/`every`/`cron`/`on-exit`/`stream` triggers; payload types (system event, agent turn, shell command, headless script); session targeting (main/isolated/current/custom); delivery via announce/webhook/none; per-job model, thinking, tool restrictions, timeouts, failure alerts; heartbeat migrated onto the same jobs | [automations](https://docs.openclaw.ai/automation/cron-jobs) |

**Not user-controllable:** OpenClaw — jobs only run while the Gateway runs; 60-min watchdog on isolated agent-turn jobs; 30s minimum condition-trigger interval; auto-disable after 10 consecutive failures; SQLite persistence. Others — scheduling simply isn't a runtime concept; users bolt on OS cron/CI. **This is the widest capability gap between OpenClaw and the coding harnesses, and the one Josh's private fork exists for (background/scheduled agents, heartbeat).**

## 5. Subagents / orchestration

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Named agent types via `.claude/agents/*.md` frontmatter (model, effort, tools, isolation incl. git worktree); background-by-default with completion notification; continue an agent via message; forks inherit parent context; per-subagent auto memory opt-in; `SubagentStart`/`SubagentStop` hooks | [sub-agents](https://code.claude.com/docs/en/sub-agents), [hooks](https://code.claude.com/docs/en/hooks) |
| Codex CLI | Delegation of "focused work to specialized agents" with findings returned to the main session | [Codex CLI docs](https://developers.openai.com/codex/cli/) |
| Zed | No native subagents; orchestration happens inside external agents connected over ACP | [external agents](https://zed.dev/docs/ai/external-agents) |
| Curo | *No public docs* | — |
| OpenClaw | `sessions_spawn` background runs in isolated sessions; depth limit 5 (orchestrator pattern at depth 2); concurrency caps (`maxConcurrent` 8, `maxChildrenPerAgent` 5); per-spawn model/thinking overrides, isolated-vs-fork context mode; push-based announce with retry/queue fallback | [subagents](https://docs.openclaw.ai/tools/subagents) |

**Not user-controllable:** Claude Code — the orchestration topology (parent→child tree only; no peer-to-peer, no agent starting sessions for other agents); result-return format. OpenClaw — hardcoded tool strip lists (subagents *never* get `message`; leaves lose spawn tools); subagent sessions hit context limits and abort instead of compacting ([open issue #6042](https://github.com/openclaw/openclaw/issues/6042)); context injection limited to AGENTS.md. **No surveyed harness lets a user redefine the orchestration model itself — e.g. LangGraph-style gated workflows between sessions.**

## 6. Tool & MCP integration

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Built-in tool suite; MCP servers (stdio/HTTP) at user/project scope; deferred tool schemas with on-demand ToolSearch; skills as packaged instructions; hooks can act as tools (`mcp_tool` hook type) | [context window](https://code.claude.com/docs/en/context-window), [hooks](https://code.claude.com/docs/en/hooks) |
| Codex CLI | `codex mcp` connects local/remote MCP servers, tool inspection before execution; `config.toml` `mcp_servers` blocks | [Codex CLI docs](https://developers.openai.com/codex/cli/) |
| Zed | Built-in tools (search, edit, terminal); MCP servers for external tools, forwarded over ACP to external agents; per-model MCP compatibility warnings | [agent panel](https://zed.dev/docs/ai/agent-panel), [external agents](https://zed.dev/docs/ai/external-agents) |
| Curo | *No public docs* | — |
| OpenClaw | Tools from core + plugins; per-agent `tools.allow`/`tools.deny` profiles; plugins can add whole tool families (browser, media, speech, web) | [plugins](https://docs.openclaw.ai/tools/plugin), [security](https://docs.openclaw.ai/gateway/security) |

**Not user-controllable:** All four documented harnesses fix the *built-in* toolset — MCP adds tools but cannot replace or reshape core tools (edit semantics, search behavior) or the tool-result formatting injected into context. Claude Code's deferred-schema mechanics are tunable only via coarse env flags.

## 7. Approval / permission gating

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Layered settings (managed policy → user → project → local) with allow/deny permission rules; permission modes; hooks as programmable policy: `PreToolUse` can deny/allow/ask/defer *and rewrite tool input*; `PermissionRequest`/`PermissionDenied` events; sandbox settings enforceable by managed policy | [hooks](https://code.claude.com/docs/en/hooks), [memory §managed](https://code.claude.com/docs/en/memory) |
| Codex CLI | `approval_policy` (untrusted / on-request / never / granular) orthogonal to `sandbox_mode` (read-only / workspace-write / danger-full-access); named profiles bundle both; `/permissions` inspects active sandbox and writable roots | [Codex CLI docs](https://developers.openai.com/codex/cli/), [approval/sandbox explainer](https://vladimirsiedykh.com/blog/codex-cli-approval-modes-2025) |
| Zed | Tool permissions: allowed / denied / confirmed per action; ACP-based permissions apply to external agents | [agent panel](https://zed.dev/docs/ai/agent-panel) |
| Curo | *No public docs* | — |
| OpenClaw | Per-agent tool allow/deny profiles; exec approvals bound to exact request context; opt-in Docker/Podman sandbox with workspace visibility none/ro/rw; per-sender tool restriction (defense-in-depth only) | [security](https://docs.openclaw.ai/gateway/security) |

**Not user-controllable:** Codex — approval is a mode picker, not programmable (no hook that can inspect a call and decide). Zed — the fixed three-state model. OpenClaw — single-trusted-operator model; per-sender restrictions don't isolate hostile users. Claude Code is the outlier: hooks make gating genuinely programmable — the benchmark AgentMod must at least match, but even there the *prompt-side* permission UX (how asks render) is fixed.

## 8. UI surfaces

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Terminal TUI, desktop app, IDE integrations, cloud/web, SDK for embedding; `MessageDisplay` hook can rewrite displayed text (transcript unchanged) | [Codex/Claude comparison contexts]; [hooks](https://code.claude.com/docs/en/hooks) |
| Codex CLI | CLI, IDE extension, desktop app, web/cloud | [Codex CLI docs](https://developers.openai.com/codex/cli/) |
| Zed | Editor-native Agent Panel + Threads Sidebar; also acts as a *frontend for other harnesses* via ACP — the closest existing thing to AgentMod's "frontends are plugins" tenet, but editor-bound and ACP-shaped | [external agents](https://zed.dev/docs/ai/external-agents) |
| Curo | *No public docs* | — |
| OpenClaw | Channel plugins: WebChat, Discord, Telegram, and other messaging surfaces, all over one Gateway | [plugins](https://docs.openclaw.ai/tools/plugin) |

**Not user-controllable:** In every harness the rendering schema is fixed — an agent (or plugin) cannot define a novel UI element and have arbitrary frontends render it. Zed/ACP standardizes the frontend↔agent wire but the vocabulary is ACP's, not user-extensible.

## 9. Extensibility points

| Harness | What it offers | Sources |
|---|---|---|
| Claude Code | Hooks (30+ lifecycle events; command/HTTP/MCP-tool/prompt/agent handler types; can block, rewrite input/output, inject context), skills, agents, MCP, plugins, output styles, statusline, settings layers | [hooks](https://code.claude.com/docs/en/hooks) |
| Codex CLI | `config.toml` + profiles + `-c` overrides; MCP; managed `requirements.toml` (e.g. `allow_managed_hooks_only`); notify hook. No general lifecycle-hook system comparable to Claude Code's documented | [Codex CLI docs](https://developers.openai.com/codex/cli/), [config guide](https://majesticlabs.dev/blog/202607/codex-cli-configuration-guide) |
| Zed | MCP servers; custom ACP agents via `agent_servers` (any command speaking ACP); rules files. Native agent internals not extensible | [external agents](https://zed.dev/docs/ai/external-agents) |
| Curo | *No public docs* | — |
| OpenClaw | Broadest plugin surface: channels, model providers, **agent harnesses**, tools/skills, speech/media, web ops, **memory backends**, lifecycle/message hooks (`api.on()` typed events), plugin-owned CLI commands; installed from registry/npm/git/local; policy allow/deny | [plugins](https://docs.openclaw.ai/tools/plugin) |

**Not user-controllable — the core loop itself, in all of them:** no harness lets a user replace the agentic loop, the event model, the transcript/log format, or the context-assembly pipeline. Extensions decorate a fixed engine: Claude Code hooks fire *around* fixed events but "cannot modify session configuration during runtime, tool execution order or parallelism, or the model's response generation"; OpenClaw plugins register into a fixed Gateway lifecycle. Hot-swapping extensions without restart is also absent (OpenClaw requires Gateway restart on plugin install; Claude Code loads settings at session start).

---

## Capability checklist

Test the North Star requirements list against this. For each item: does a requirement cover it, deliberately exclude it (non-goal), or was it missed? Items marked **(gap)** are things *no* surveyed harness lets users control — AgentMod's differentiation lives there.

**Session & context**
- [ ] Create / resume / fork / archive sessions; list and search history
- [ ] Multiple concurrent sessions (Zed threads; AgentMod target: 10–100)
- [ ] Inspect exactly what is in context (`/context`, token meters)
- [ ] Checkpoint / revert session state (Zed checkpoints; append-only log gives this free)
- [ ] **(gap)** User-defined session boundary & startup-assembly pipeline

**Memory**
- [ ] User-authored instruction files, hierarchical/scoped (CLAUDE.md, AGENTS.md, rules)
- [ ] Agent-written persistent memory with an index + topic files
- [ ] Memory search (vector + keyword), pluggable embedders
- [ ] Swappable memory backends (OpenClaw only)
- [ ] Consolidation / promotion passes (OpenClaw dreaming)
- [ ] **(gap)** Multiple simultaneous memory systems; user-visible/tunable consolidation logic

**Compaction / context editing**
- [ ] Auto-compact on threshold + manual with focus instructions
- [ ] Pre-compaction durable-state flush (OpenClaw memoryFlush)
- [ ] Declared survival semantics (what re-injects after compaction)
- [ ] Pluggable summarizer (OpenClaw `compaction.provider`)
- [ ] **(gap)** Wholesale-replaceable compaction *strategy*; user-set thresholds everywhere; alternatives to summarization

**Scheduling & background**
- [ ] at / every / cron / on-event triggers; heartbeat
- [ ] Per-job session targeting (main / isolated / named), model/tool/timeout overrides
- [ ] Delivery routing (chat announce / webhook / none); failure alerting
- [ ] Background tool + agent runs with completion notification
- [ ] **(gap)** Scheduling as a first-class primitive of the same runtime as interactive chat (only OpenClaw approaches; AgentMod tenet: same compiled graphs)

**Subagents / orchestration**
- [ ] Named agent definitions (model, tools, isolation, memory)
- [ ] Isolated vs fork context modes; worktree isolation
- [ ] Depth/concurrency limits; orchestrator pattern; continue-agent messaging
- [ ] Result announce/return with retry semantics
- [ ] **(gap)** User-defined orchestration topologies (gated workflows, peer sessions, agents starting sessions for other agents)

**Tools & MCP**
- [ ] Built-in tool suite; MCP client (stdio + remote); deferred schema loading
- [ ] Skills / packaged workflows loaded on demand
- [ ] **(gap)** Replaceable built-in tools and tool-result formatting

**Approval / permissions**
- [ ] Layered policy (org → user → project → local); allow/deny rules
- [ ] Orthogonal sandbox axis (Codex modes; OpenClaw containers; workspace ro/rw)
- [ ] Programmable gating: inspect + deny/allow/rewrite per call (Claude Code hooks)
- [ ] Exec approvals bound to exact request context
- [ ] **(gap)** Pluggable permission *engine* rather than a fixed mode set

**UI surfaces**
- [ ] Terminal, IDE, desktop, web, messaging channels over one runtime
- [ ] Frontend-agnostic protocol (ACP precedent)
- [ ] **(gap)** Frontends as plugins over one schema; plugin-defined UI elements

**Extensibility**
- [ ] Lifecycle hooks: typed events, blocking + mutating + context-injecting (Claude Code breadth: pre/post tool, prompt, compact, session, subagent, config)
- [ ] Plugin registries/install sources; policy allow/deny for plugins
- [ ] Plugins able to add channels, providers, tools, memory backends, CLI commands (OpenClaw breadth)
- [ ] **(gap)** Hot-swap plugins without restarting the runtime, with sessions surviving
- [ ] **(gap)** Replaceable core loop: event routing, log format, context assembly — in every surveyed harness these are the engine, not extension points

**Meta**
- [ ] Every capability above expressible as "event in → appended context + output event" (AgentMod plugin tenet) — use each checked row as a test case
- [ ] Observability/determinism: if configured to run, it runs, visibly (no surveyed harness documents a determinism guarantee for extensions)
