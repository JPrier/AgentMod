# AgentMod prototype — the High-Level Design, running

This directory is a working prototype of [the High-Level Design](../docs/design/high-level-design.md)
for the [North Star](../docs/design/north-star.md). It is a standalone Cargo workspace: it neither
depends on nor changes the existing implementation in the repository root.

**Try it in the browser:** <https://jprier.github.io/AgentMod/> — the Rust kernel compiled to
WebAssembly, every plugin in its own Web Worker, the model reached through the OpenRouter plugin.
It asks for the model provider's settings (an OpenRouter API key by default) before the runtime
starts; the form comes from the provider plugin's declared settings, the key stays in your
browser, is sent only to the provider, and is omitted from exported logs.

**Code with it:** start a `coder` session. It is a complete coding harness built from plugins:
shell and background processes, structured file/search/patch tools, workspace checkpoints and
rewind, branching, a plan, questions to the user, layered permissions, isolated child agents,
diagnostics, repo map, skills, web/MCP/browser tools found on demand, and metrics. In the browser
the workspace is an x86 Linux VM in your tab (CheerpX, [plugins/linux-sandbox](plugins/linux-sandbox/README.md));
natively it is a local directory (`local-workspace`). The design and its decisions are in
[docs/design/coding-harness.md](../docs/design/coding-harness.md).

## What is here

```text
prototype/
├── crates/
│   ├── agentmod-core      deterministic, sans-I/O kernel (no clock, no I/O, no LLM client)
│   ├── agentmod-runtime   native host: `agentmod serve | compile | inspect | verify | config`
│   └── agentmod-wasm      the same kernel for the in-browser host
├── plugins/               bundled plugins (JS run under Node or as Web Workers; one in Python)
├── ui/                    the web frontend + in-browser runtime host
├── tests/                 native e2e, browser e2e, mock OpenRouter
├── bench/                 the harness against a minimal loop (and external CLIs) on fixed tasks
├── scripts/               build-wasm.sh, build-site.sh
└── agentmod.toml          the default deployment: plugins + session definitions
```

| HLD concept | Where it lives |
| --- | --- |
| The compiler ([compiler.md](../docs/design/compiler.md)): manifests + one flattened definition → one compiled graph, which is also the routing table; its hash is the session's revision | `core/src/compiler.rs` (graph, every §6 check, report sections), `core/src/shape.rs` (typed shapes), `core/src/manifest.rs` (manifests, definitions, fragments), `core/src/core_manifest.rs` (the runtime's own events, versioned) |
| Envelope (`event_id`, `session_id`, `event_name`, `sequence`, `cause`, `lane`, `arrived_at`) plus exactly the declared keys and slots | `core/src/types.rs`; projection in `Kernel::start_invocation` |
| One publish primitive: pipeline output vs cited deferred publish; `start-session` is a runtime call | `Kernel::publish`, `Kernel::start_session` — every start compiles first (a failure creates no session and returns the diagnostics, also as `session-start-failed`); on success only the core emits `session-started`, carrying the caller's input; refused publishes are recorded, never silent |
| Blocking chain in phase order (return keys / contribute / veto) + async observers | `Kernel::advance` along compiled edges; anything that contradicts a declaration is a recorded `breach` |
| Two FIFO lanes, chosen per publish among the lanes an emit declares (the UI's steer and queue are one `user-message` on two lanes); soft stop / hard stop / resume are commands, not events | `Kernel::publish`, `Kernel::command` |
| Per-session append-only log projecting into session → events → invocations | `core/src/record.rs`, `core/src/projection.rs`, JSONL + content-addressed spill in `runtime/src/store.rs` |
| History-is-truth recovery: orphan scan bounded by the active-set index | `Kernel::load_session` + `Kernel::recover`; deterministic output ids make re-published outputs idempotent |
| Live, layered, stamped configuration; revisions are per session | `Kernel::apply_config` (targets sessions, rejected whole on any error, applied at the next event boundary); every invocation records its revision, code and config hashes |
| Plugins are supervised processes over one JSON-RPC wire protocol | `runtime/src/host.rs` (stdio) and `ui/runtime/browser-host.js` (Web Workers) |
| Frontends as plugins with UI-hint payloads | `plugins/web-ui` (gateway) and the page itself; vocabulary: text, markdown, tool, choice, progress, diff, form (old logs may also carry `stream-chunk`) |
| Live model output outside the log | `core/src/stream.rs`: normalized provider frames, coalescing, recovery state, byte-bounded clients; one canonical `model-response` per turn ([hot-paths.md](../docs/design/hot-paths.md)) |
| Tool calls as an event family | `tool-call/*` is resolved at compile time into the members the definition's tool plugins subscribe to (an MCP bridge declares one per server tool); each plugin gets its resolved emit set; the model is offered exactly those tools and an unknown name is answered by `chat-context`; any failed invocation becomes the core's `invocation-failed` |

The core ships no agent features. Chat shape, tools, memory, approval, titling, sub-agents,
triggers, and the model itself are all plugins:

| Plugin | Does |
| --- | --- |
| `openrouter-model` | The model. OpenRouter chat completions with streaming and tool calls; serves its model catalog and credential check to frontends. **Requires a key.** |
| `openai-model` | Alternative provider for any OpenAI-compatible endpoint. |
| `chat-context` | Folds messages into context and drives the model ↔ tool loop. Stateless. |
| `redactor` | Blocking subscriber that returns a credential-masked `text` for user messages. |
| `policy` | Layered permissions: tool visibility, deny > ask > allow across scopes, explained decisions, digest-bound approvals revalidated on arrival, permission modes. |
| `approval-gate` | The older tool-name approval gate (used by `chat`). |
| `local-workspace` | The coding toolkit in a local directory. Native runtime only; not a sandbox. |
| `linux-sandbox` | The coding toolkit in an x86 Linux VM inside the browser (CheerpX host device). |
| `plan` | `update_plan`: a tiny, visible plan. |
| `ask-user` | `ask_user`: a question to the user whose answer continues the tool call. |
| `tool-discovery` | `tool_search`: finds and loads deferred tools. |
| `budget` | Model-request, tool-call, token and wall-time bounds (used for children); as `loop-limit`, the per-turn bound on the agent loop (the core has no loop limit). |
| `subagent`, `subagent-reporter` | `delegate`: isolated child sessions (tool allowlist, budget, worktree, evidence); `adopt_changes`. |
| `memory` | `remember`/`recall`; notes injected with provenance. |
| `web-fetch` | `web_fetch` (deferred), untrusted, no private hosts. |
| `mcp-bridge` | MCP servers (stdio/HTTP) as deferred `mcp__server__tool` tools. |
| `browser-control` | Browser automation over the Chrome DevTools Protocol (deferred). |
| `minimal-shell` | One `shell` tool: the minimal loop the benchmark compares against. |
| `tool-clock`, `tool-calc`, `py-wordcount` (Python) | Tools: a tool call is an event; a plugin that answers it is a tool. |
| `titler` | Async session auto-titling. |
| `heartbeat` | Trigger plugin with a journal session (`every_seconds` in config; off by default). |
| `web-ui` | The web frontend: HTTP + SSE gateway natively, the page itself in the browser. |

The coding toolkit (`plugins/sdk/coding/`) is shared by every execution target:

| Tool | Tier | Does |
| --- | --- | --- |
| `shell` | core | command with exit code, separate streams, timeout, bounded output (full output saved), parsed diagnostics; checkpoint first unless read-only |
| `process` | core | start / status / read / write / kill / list long-running processes; they survive the runtime |
| `read_file`, `list_dir`, `search_files`, `search_text` | core | structured, bounded, deterministic reads and searches |
| `apply_patch` | core | atomic multi-file create/update/delete/move; refuses stale edits; checkpoint first |
| `update_plan`, `ask_user`, `tool_search` | core | from the plugins above |
| `checkpoints`, `view_image`, `repo_map`, `import_repo`, `load_skill`, `adopt_changes`, `delegate`, web, MCP, browser | deferred | loaded with `tool_search` |

Session definitions in `agentmod.toml`: `chat` (general assistant), `coder` (the coding harness),
`worker` (what `delegate` starts), `minimal` (provider + chat loop + one shell tool), `heartbeat`.

### Manifests and definitions

A plugin declares, at handshake, what it receives, returns, emits and may call; every key has a
shape. A definition lists the plugins a session runs, the phases that order its blocking
subscribers, tie-breaks where the compiler finds an observable tie, the events it accepts from
other sessions, and its slots. It never lists edges. Session slots are requested in a
subscription's `slots`, a sibling of `receives`.

```js
// plugins/redactor/main.js (manifest)
subscribe: [{ event: 'user-message', mode: 'blocking',
              receives: { text: { shape: 'string' } },
              returns:  { text: { shape: 'string' }, redacted: { shape: 'integer' } } }],
```

```toml
[definitions.chat]
runtime = "0.1"                                   # selects the core manifest
phases = ["offer", "intake", "guard", "loop"]

[definitions.chat.plugins]
redactor = { phase = "intake" }
approval-gate = { phase = "guard" }
chat-context = { phase = "loop" }
openrouter-model = {}                             # async only: no phase needed

[definitions.coder.order.session-started]         # several contribute context: order them
offer = ["linux-sandbox", "local-workspace", "memory"]

[definitions.chat.inbound.subagent-result]        # accepted from child sessions
call_id = { shape = "string" }
```

`agentmod compile` prints each definition's graph (one line per emitter and event class: the
blocking chain with phases, async destinations), every diagnostic with the file and line it
traces to, and the report sections (internal cycles and cycles through external input, resolved
families, capabilities, interrupt use, unlisted keys, the external surface, orders fixed by
name).

## Run it natively

Requirements: Rust (stable), Node 20+, Python 3 (for `py-wordcount`), and an OpenRouter key.

```shell
cd prototype
export OPENROUTER_API_KEY=sk-or-...        # required: the runtime will not start without it
cargo run -p agentmod-runtime -- serve     # web UI on http://127.0.0.1:7700/
```

The key is read from the environment by the plugin and never written to the log. Without it the
`openrouter-model` plugin refuses its handshake, the compiler rejects the configuration, and
`serve` exits with a message saying so.

Other commands:

For coding against a local directory, start a `coder` session; the workspace is
`local-workspace`'s `root` (default `.agentmod/workspace`; set it in `agentmod.toml`). Project instructions come from
`AGENTS.md`, `CLAUDE.md`, or `.agentmod/instructions.md`; repository policy from
`.agentmod/policy.json` (it can only restrict); skills from `.agentmod/skills/*/SKILL.md`.
Harness state (checkpoints, processes, full outputs) lives in `<root>/.agentmod/state/`.

```shell
cargo run -p agentmod-runtime -- compile            # handshake plugins, print the compiled graphs, diagnostics, report
cargo run -p agentmod-runtime -- inspect [s0001]    # replay-as-reading: runs no plugins
cargo run -p agentmod-runtime -- verify             # replay every log through a fresh kernel
cargo run -p agentmod-runtime -- metrics [s0001] [--json] [--turns]   # model efficiency + control-plane amplification, from the logs
cargo run -p agentmod-runtime -- config > ui/runtime/agentmod.config.json   # browser config
```

Data lives in `.agentmod/`: `sessions/*.jsonl` (the logs), `spill/` (content-addressed payloads),
`configs/` (compiled configs by hash), `revisions/` (revisions compiled at start-session or apply),
`journal.jsonl`, and `index.json` (derived, rebuildable).

The hosted UI can attach to your local runtime: choose **Local runtime** and enter
`http://127.0.0.1:7700` (the gateway sends CORS and Private-Network-Access headers).

## Build the browser runtime and site

```shell
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.100 --locked
./scripts/build-wasm.sh      # → ui/pkg
./scripts/build-site.sh      # → dist/ (what GitHub Pages serves)
```

## Tests

```shell
cargo test --workspace                 # compiler (every §6 check, every §11 example), families, kernel scenarios, randomized replay equivalence, store
(cd tests && npm install)              # fake-indexeddb, ws (test-only)
node --test tests/*.test.mjs           # coding toolkit, policy, projection, browser runtime, streaming, turn ergonomics, MCP/web, CDP, sandbox (fake CheerpX)
./tests/e2e.sh                         # native: real runtime + plugin processes, mock OpenRouter
node tests/browser.mjs                 # headless Chrome against dist/
node bench/run.mjs --selftest          # benchmark plumbing; real runs need OPENROUTER_API_KEY (see bench/README.md)
node bench/kilo.mjs --assert           # the Kilo regression benchmark, offline (add --before-ref <ref> to compare builds)
node tests/sandbox-browser.mjs         # real CheerpX in headless Chrome (needs network)
```

`tests/e2e.sh` covers: key enforcement, a tool loop, approval + sub-agent, hot apply and
whole-config rejection, hard stop mid-stream (the live stream ends with its invocation; no token
fragment reaches the log), a killed plugin being retried while other sessions continue, SIGKILL of
the runtime mid-stream (recovery state on disk, a fresh attempt on restart, no concatenated text,
recovery state dropped once the response is logged), parallel tool calls settling into one model
request, and a whole coding task through a real session (`local-workspace`): plan,
search, patch, tests, a background server, rewind, branching, policy approval and revalidation,
`ask_user`, delegation with adoption, child allowlists, a provider hot swap, a process surviving a
runtime SIGKILL, `verify` over every log, and metrics. The kernel tests include randomized interleavings of
publishes, completions, retries, commands, and config applies, checking after every step that a
fresh kernel rebuilt from the log matches the live one exactly.

## Prototype decisions on open subdesign questions

These are the choices the prototype made where tickets #5–#13 are still open. They are
experiments to inform those decisions, not resolutions of them.

- **Output visibility (#6).** A pipeline output is recorded and dispatchable as soon as it is
  published; a later veto vetoes only the event being processed. (Token streaming no longer
  relies on this: live frames travel outside the pipeline; see docs/design/hot-paths.md.)
- **Idempotence (#8).** Pipeline output ids are `{invocation}.o{n}`. A restarted invocation that
  re-publishes its n-th output gets the same id and is deduplicated.
- **Failures.** A failed blocking subscriber settles its event as `failed` (fail-closed). Async
  failures are recorded on the invocation only. Orphans are retried up to `max_attempts`.
- **Process sharing (#5).** One process per plugin version, shared by all sessions. Hard stop
  sends `cancel`; a plugin that ignores it for 3 s is killed, and its other in-flight
  invocations are retried from the record.
- **Hot swap (#12).** A new plugin version runs alongside the old one; old processes drain when no
  revision in use references them. A global apply targets every loaded session that follows the
  global layer (and becomes the config new sessions start from); a session-scoped apply is a
  per-session layer. Either is rejected whole if any target's candidate revision fails to compile.
- **Recursion (#9).** Cycles are legal and reported as info. The core has no recursion or depth
  limit; the `loop-limit` instance of the `budget` plugin bounds each turn's agent loop.
- **Context.** The `context` slot holds attributed items in named sections (`messages`, `tools`,
  `plan`, …). Contributions are ops (`add`, `replace`, `remove`, `clear-section`, `restore`) folded
  in log order. `restore` returns context to its state at an earlier record — as a new record,
  so it can itself be undone. Branching copies context into the new log (`context-seeded`).
- **Redaction.** A blocking subscriber's `returns` change what later subscribers and the settled
  event carry; the arrival record keeps what was published, because history is never rewritten.

## Known limits

Single process, single machine; no sandboxing of plugins themselves (by design, approval is a
plugin; code the agent runs can be sandboxed by choosing `linux-sandbox`); `local-workspace` is
not a sandbox; tool descriptions contributed before a plugin is removed stay in context until
edited out. The browser runtime stores sessions in IndexedDB, with the platform's limits
(eviction, one writing tab). The harness's own known limits are listed in
[docs/design/coding-harness.md](../docs/design/coding-harness.md#known-limits).
