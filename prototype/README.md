# AgentMod prototype — the High-Level Design, running

This directory is a working prototype of [the High-Level Design](../docs/design/high-level-design.md)
for the [North Star](../docs/design/north-star.md). It is a standalone Cargo workspace: it neither
depends on nor changes the existing implementation in the repository root.

**Try it in the browser:** <https://jprier.github.io/AgentMod/> — the Rust kernel compiled to
WebAssembly, every plugin in its own Web Worker, the model reached through the OpenRouter plugin.
It asks for an OpenRouter API key before the runtime starts (kept in your browser, sent only to
openrouter.ai, omitted from exported logs).

**Code in the browser:** start a `coder` session. The `linux-sandbox` plugin runs an x86 Linux VM
in your tab (CheerpX), so the agent writes, compiles, and runs code on your own machine; see
[plugins/linux-sandbox](plugins/linux-sandbox/README.md).

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
├── scripts/               build-wasm.sh, build-site.sh
└── agentmod.toml          the default deployment: plugins + session definitions
```

| HLD concept | Where it lives |
| --- | --- |
| Config-compiled event bus; graph is a compile-time artifact | `core/src/compiler.rs` — demand/supply matching, dead listeners, starved consumers, supply walked through transforms, wildcard placement, cycle report, external surface |
| Envelope (`event_id`, `session_id`, `event_name`, `sequence`, `cause`, `lane`, `arrived_at`, `payload`, `context`) | `core/src/types.rs` |
| One publish primitive: pipeline output vs cited deferred publish vs `start-session` | `Kernel::publish`, `Kernel::start_session` — blocked publishes are recorded, never silent |
| Blocking chain (contribute / transform / veto) + async observers | `Kernel::advance`; transforms are checked against their declaration at runtime |
| Two FIFO lanes; soft stop / hard stop / resume are commands, not events | `Kernel::command` |
| Per-session append-only log projecting into session → events → invocations | `core/src/record.rs`, `core/src/projection.rs`, JSONL + content-addressed spill in `runtime/src/store.rs` |
| History-is-truth recovery: orphan scan bounded by the active-set index | `Kernel::load_session` + `Kernel::recover`; deterministic output ids make re-published outputs idempotent |
| Live, layered, stamped configuration | `Kernel::apply_config` (global or one session, applied at the next event boundary); every invocation records code + config hashes |
| Plugins are supervised processes over one JSON-RPC wire protocol | `runtime/src/host.rs` (stdio) and `ui/runtime/browser-host.js` (Web Workers) |
| Frontends as plugins with UI-hint payloads | `plugins/web-ui` (gateway) and the page itself; vocabulary: text, markdown, stream-chunk, tool, choice, progress, diff, form |

The core ships no agent features. Chat shape, tools, memory, approval, titling, sub-agents,
triggers, and the model itself are all plugins:

| Plugin | Does |
| --- | --- |
| `openrouter-model` | The model. OpenRouter chat completions with streaming and tool calls. **Requires a key.** |
| `openai-model` | Optional alternative provider for any OpenAI-compatible endpoint. |
| `chat-context` | Folds messages into context and drives the model ↔ tool loop. Stateless. |
| `redactor` | Blocking transformer that masks credentials in user messages. |
| `approval-gate` | Vetoes guarded tool calls until a human answers a `choice` UI hint. |
| `tool-clock`, `tool-calc`, `py-wordcount` (Python) | Tools: a tool call is an event; a plugin that answers it is a tool. |
| `memory` | `remember`/`recall` tools and memory injection into new sessions. |
| `subagent`, `subagent-reporter` | `delegate` starts a worker session; the worker reports back cross-session. |
| `linux-sandbox` | Coding tools (`run`, `read_file`, `write_file`, `edit_file`, `list_files`, `import_repo`) in an x86 Linux VM inside the browser (CheerpX). Browser runtime only. |
| `local-workspace` | The same coding tools in a local directory. Native runtime only; not a sandbox, so in no default definition. |
| `titler` | Async session auto-titling. |
| `heartbeat` | Trigger plugin with a journal session (`every_seconds` in config; off by default). |
| `web-ui` | The web frontend: HTTP + SSE gateway natively, the page itself in the browser. |

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

```shell
cargo run -p agentmod-runtime -- compile            # handshake plugins, print pipelines + diagnostics
cargo run -p agentmod-runtime -- inspect [s0001]    # replay-as-reading: runs no plugins
cargo run -p agentmod-runtime -- verify             # replay every log through a fresh kernel
cargo run -p agentmod-runtime -- config > ui/runtime/agentmod.config.json   # browser config
```

Data lives in `.agentmod/`: `sessions/*.jsonl` (the logs), `spill/` (content-addressed payloads),
`configs/` (compiled configs by hash), `journal.jsonl`, and `index.json` (derived, rebuildable).

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
cargo test --workspace                 # compiler, kernel scenarios, randomized replay equivalence, store
node --test tests/*.test.mjs           # coding tools, the sandbox target (fake CheerpX), plugin protocol
./tests/e2e.sh                         # native: real runtime + plugin processes, mock OpenRouter
(cd tests && npm install) && node tests/browser.mjs   # headless Chrome against dist/
node tests/sandbox-browser.mjs         # real CheerpX in headless Chrome (needs network)
```

`tests/e2e.sh` covers: key enforcement, a tool loop, approval + sub-agent, hot apply and
whole-config rejection, hard stop mid-stream, a killed plugin being retried while other sessions
continue, SIGKILL of the runtime mid-stream with recovery from the log (no duplicate or missing
stream chunks), the coding tools through a real session (`local-workspace`), and `verify` over
every log. The kernel tests include randomized interleavings of
publishes, completions, retries, commands, and config applies, checking after every step that a
fresh kernel rebuilt from the log matches the live one exactly.

## Prototype decisions on open subdesign questions

These are the choices the prototype made where tickets #5–#13 are still open. They are
experiments to inform those decisions, not resolutions of them.

- **Output visibility (#6).** A pipeline output is recorded and dispatchable as soon as it is
  published; a later veto vetoes only the event being processed. This is what lets streaming
  chunks reach frontends while the provider invocation is still open (the provider is an async
  subscriber, so the session's serial dispatcher is not blocked).
- **Idempotence (#8).** Pipeline output ids are `{invocation}.o{n}`. A restarted invocation that
  re-publishes its n-th output gets the same id and is deduplicated.
- **Failures.** A failed blocking subscriber settles its event as `failed` (fail-closed). Async
  failures are recorded on the invocation only. Orphans are retried up to `max_attempts`.
- **Process sharing (#5).** One process per plugin version, shared by all sessions. Hard stop
  sends `cancel`; a plugin that ignores it for 3 s is killed, and its other in-flight
  invocations are retried from the record.
- **Hot swap (#12).** A new plugin version runs alongside the old one; old processes drain when no
  installed-and-used config references them. Sessions adopt a global apply at their next event
  boundary; a session-scoped apply is a per-session layer.
- **Recursion (#9).** Cycles are legal and reported as info; a runtime `max_causal_depth` bounds
  every causal chain, including chains through `start-session`.
- **Context.** Contributions are ops (`add`, `replace`, `remove`, `clear-slot`, `restore`) folded
  in log order. `restore` returns context to its state at an earlier record — as a new record,
  so it can itself be undone. Branching copies context into the new log (`context-seeded`).
- **Redaction.** A transformer changes what later subscribers and the settled event carry; the
  arrival record keeps what was published, because history is never rewritten.

## Known limits

Single process, single machine; no sandboxing of plugins themselves (by design, approval is a
plugin; code the agent runs can be sandboxed by choosing `linux-sandbox`); the browser runtime
keeps logs in memory (export them from the top bar); tool descriptions contributed before a plugin
is removed stay in context until edited out.
