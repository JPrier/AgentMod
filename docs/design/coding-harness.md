# AgentMod as a coding harness

Status: implemented in `prototype/` (see section 5, "Implementation status").
Parents: [North Star](north-star.md), [High-Level Design](high-level-design.md).

This document maps the coding-harness specification onto the redesign that the
`prototype/` workspace implements, records the audit that preceded the work, and
states the architectural decisions the specification introduced. The HLD stays
authoritative: the kernel ships no agent features, and everything below is a
plugin, a shared plugin-SDK module, or a small generic host addition.

## 1. Audit (before this work)

The audit covered `docs/design/*`, `prototype/` (kernel, native host, browser
host, every bundled plugin, UI, tests), the last merged PR (#20,
`prototype/sandbox-control`), and the root implementation's `STATUS.md`. The
root workspace (`apps/`, `core/`, …) is the pre-redesign implementation; the
HLD's "Preserving the existing codebase" non-goal applies to it, so this work
targets `prototype/` and leaves the root untouched.

### Implemented and usable

| Capability | Where |
|---|---|
| Deterministic sans-I/O kernel, append-only per-session logs, write-ahead before dispatch | `crates/agentmod-core/src/kernel.rs`, `runtime/src/store.rs` |
| History-is-truth recovery, orphan retry, idempotent pipeline outputs (`{invocation}.o{n}`) | `Kernel::recover`, `Kernel::retry`, kernel tests incl. randomized replay equivalence |
| Process-isolated plugins (Node/Python processes natively, Web Workers in the browser) | `runtime/src/host.rs`, `ui/runtime/browser-host.js` |
| Compiled pipelines with demand/supply validation, blocking veto/transform, async observers | `core/src/compiler.rs` |
| Live layered config apply (global or per session) at event boundaries, stamped invocations | `Kernel::apply_config` |
| Context as attributed ops with `restore` to any earlier sequence (auditable restoration) | `core/src/context.rs` |
| Session fork (`fork_from`) seeding context from any recorded point | `Kernel::start_session` |
| Hard stop / soft stop / resume as dispatcher commands | `Kernel::command` |
| Browser runtime = same kernel in WASM | `crates/agentmod-wasm` |
| Linux VM in the browser (CheerpX) as a host device behind a plugin | `plugins/linux-sandbox`, `ui/runtime/devices/cheerpx-vm.js` |
| Provider plugins (OpenRouter, OpenAI-compatible) with streaming and tool calls | `plugins/openrouter-model`, `plugins/sdk/openai-compat.js` |
| Approval as a blocking plugin; frontend answers with `ui-action` | `plugins/approval-gate` |

### Implemented but incomplete

| Capability | Gap |
|---|---|
| Coding tools (`run`, `read_file`, `write_file`, `edit_file`, `list_files`, `import_repo`) | No text/glob search, no structured multi-file patch, no stale-read detection (no hashes), no persistent processes, no checkpoints, no diagnostics; `list_files` dumps whole trees up to 600 entries; `run` output truncation discards the rest |
| Sub-agents (`delegate`) | Child gets a fixed `worker` definition, inherits nothing *but also* has no workspace, no capability scoping, no budget, no cancellation, no evidence beyond final text; parent hard stop does not reach the child |
| Approval | Tool-name allowlist only; no deny rules, no scopes, no explanation of which rule matched, no action digest, no re-validation when the approval arrives |
| Redaction | Regex masking of user messages only; tool outputs and command environments are not covered |
| Memory | Works; injected notes carry no provenance (source, reason) |
| Browser persistence | Logs live in memory; a page reload loses every session (only the VM disk persists) |
| Plugin identity | `binary_hash` covers the entry file only, so a change to a shared SDK module does not change the stamp |

### Architecture exists, no practical tool or UI

| Capability | Missing surface |
|---|---|
| Context restore / rewind | No user-facing rewind; no workspace (file) rewind at all |
| Session branching | `fork_from` exists in the API; no branch from an earlier event in the UI, no workspace isolation for branches, no branch listing |
| History inspection | Inspector shows raw events/invocations; no high-level views of checkpoints, processes, children, plan |
| Hot model/provider swap | Possible through raw config apply; UI is OpenRouter-specific |

### Missing

Persistent processes; `search_files`/`search_text`; `apply_patch`; plan state;
`ask_user`; workspace checkpoints; child isolation/adoption; lazy tool
discovery; context trust classes; layered policy; scoped secrets; diagnostics;
repository map; artifact (image) inspection; browser automation; web fetch; MCP;
metrics; a benchmark against a minimal loop.

### Conflicts with the specification

1. **Provider-specific frontend (drift).** `ui/runtime/models.js` and most of
   the provider panel in `ui/app.js` call OpenRouter's `/models` and store
   OpenRouter keys directly. Model-catalog discovery belongs to the provider
   plugin.
2. **Ambient environment leak.** `local-workspace` runs commands with the
   runtime's entire `process.env`, so every exported secret (including the
   provider key) is visible to the agent's shell.
3. **All tool schemas sent every turn.** `openai-compat.js` sends every `tools`
   slot item on every request; there is no tiering.
4. **Tool output is indistinguishable from instructions.** Tool results, file
   contents and web content enter the prompt with the same authority as user
   text.

## 2. Shape of the solution

The normal loop stays the bundled chat convention:

```text
user-message → chat-context → model-request → provider → model-response
   → tool-call* ─(policy: blocking veto/ask)→ tool plugins → tool-result* → model-request …
```

Nothing below adds a kernel concept. The new pieces are:

| Piece | Kind | Purpose |
|---|---|---|
| `sdk/coding/*` | shared SDK modules | the coding toolkit over an *execution target*: shell, processes, files, search, patch, checkpoints, diagnostics, repo map, images |
| `local-workspace`, `linux-sandbox` | execution-target plugins | the same toolkit against a local directory or the in-browser VM |
| `sdk/projection.js` | shared SDK module | turns context into a provider request: authority labels, tool tiering, plan, instructions, dangling-call repair, metrics |
| `policy` | blocking plugin | layered deny > ask > allow rules, scoped grants, explanations, action digests, re-validation, tool visibility, secret grants |
| `plan`, `ask-user`, `tool-discovery` | tool plugins | tiny planning tool, user questions as continuations, lazy capability discovery |
| `subagent` / `subagent-reporter` | tool plugins | isolated child sessions with scoped tools, budget, workspace isolation, evidence, adoption |
| `budget` | blocking plugin | turn/token bounds (used for children) |
| `web-fetch`, `mcp-bridge`, `browser-control` | deferred-tier tool plugins | network, MCP servers, browser automation, all policy-gated and untrusted |
| host `service` calls | generic host addition | read-only plugin services (model catalog) for frontends |
| browser IndexedDB store | browser host addition | durable logs and compilations in the browser |

## 3. Architectural decisions

### 3.1 World state versus logical replay; workspace checkpoints

The log reconstructs *logical* state exactly (context, events, invocations). It
cannot reconstruct the *world*: files, processes, remote systems. Replay never
re-executes effects, so the world is recorded, not re-derived:

* **Checkpoints.** Before every `apply_patch` and before a shell command that
  is not classified read-only, the toolkit snapshots the workspace into a
  *shadow* Git repository that lives outside the project (`GIT_DIR` under the
  target's state directory, `GIT_WORK_TREE` = workspace root). A snapshot is a
  tree object; identical trees are deduplicated, so a run of read-only work
  costs nothing. Each snapshot is published as a `checkpoint-created` event
  (tree id, reason, the tool call it precedes), so the log binds logical history
  to world state.
* **Rewind** is new history, never deletion. Restoring (the `checkpoints` tool's `restore` action, or the UI's
  *restore files* `ui-action`) puts files back
  to a checkpoint's tree after taking a checkpoint of the current state (so the
  rewind is itself reversible) and publishes `workspace-restored`. Conversation
  rewind is the kernel's existing context `restore` op. "Both" is the two in one
  user action. Branching from an old point is `start-session` with `fork_from`
  plus a workspace materialized from the checkpoint in effect at that point.
* The project's own `.git` is never touched; session branches and Git branches
  are different identities (`session_id` vs repository HEAD), both recorded.

### 3.2 Persistent processes

Processes are started by the toolkit through the target's shell, detached, with
stdout/stderr/exit status written to files in the target's state directory
(`<state>/procs/<id>/`). The process id is derived from the tool call id, so a
retried invocation finds the existing process instead of starting a second one.
`process-started` / `process-exited` events record lifecycle in the session log.

If the runtime or plugin dies, the process keeps running (it is detached) and
its files keep filling. On the next use the toolkit *reconciles* from those
files and reports `reconciled: true` and whether the pid is still alive; it
never claims to have replayed a process. If the execution environment itself is
gone (VM restarted, machine rebooted), the files survive only if they are on
persistent storage; otherwise the process is reported `lost`. Hard stop cancels
the *tool call*, not background processes; killing a process is an explicit
action (`process` with `action: "kill"`, or the UI).

### 3.3 Context authority classes

Every projected context item gets one authority class, decided by the projection
from *who contributed it and through which slot*, never from its text:

| Class | Source |
|---|---|
| `system` | `system` slot from plugins listed in the projection's trusted set, the projection's own preamble |
| `user` | `user-message` events published by a frontend (`web-ui`), and `ask_user` answers |
| `workspace` | project instruction files (`AGENTS.md`, `CLAUDE.md`, `.agentmod/instructions.md`), repository skills |
| `agent` | the model's own messages and plan |
| `tool-data` | structured tool metadata produced by a trusted tool plugin (exit codes, paths, hashes) |
| `untrusted` | tool output text, file contents, web pages, MCP output, child-agent conclusions, logs |

`untrusted` content is wrapped in delimiters carrying its source, and the
system preamble states that such content is data. Authority is also structural:
permissions change only through `ui-action` events whose origin is a frontend
plugin listed as an approver, or through config apply (control capability).
Tool plugins cannot emit `ui-action` (the kernel blocks undeclared emits), so
text inside a tool result cannot grant anything no matter what it says.
Workspace instructions are below user and system: they can ask, never authorize.

### 3.4 Layered security: visibility, authorization, containment, recovery

1. **Visibility.** The `policy` plugin contributes a `tool-policy` context item
   listing tools that are denied for the session; the projection omits them
   from the model's tool list and from `tool_search`.
2. **Authorization.** The same plugin is a blocking subscriber on `tool-call`
   and vetoes or asks *regardless* of visibility (a model can still emit a call
   to a hidden tool name).
3. **Containment.** Execution targets confine paths, strip the environment to an
   allowlist, inject only granted secrets, bound output and time; the VM adds a
   hardware boundary with no network.
4. **Recovery.** Checkpoints before mutation; rewind.

Rule precedence: within the effective rule set, `deny > ask > allow`. Scopes,
highest authority first: `runtime` (deployment config) → `user` (user policy
file) → `workspace` (repository `.agentmod/policy.json`, may only add `deny`/
`ask`) → `session` (per-session config layer) → `child` (the delegation's
allowlist) → `grant` ("allow for this session" approvals recorded in the log)
→ `invocation` (a one-time approval) → `mode` (the permission mode's default:
`read-only`, `default`, `auto`). A
lower scope can never turn a higher-scope `deny` into `allow`; a session grant
("always allow this for the session") turns `ask` into `allow` only where no
higher scope says `deny`. Every decision is published as `policy-decision` with
the matched rule, its scope, the effect, and the rules it overrode.

An approval binds the **action digest** (SHA-256 of tool name + canonical
arguments). When the answer arrives, policy is evaluated again against the
*current* configuration before the call is re-published; a deny added while the
question was open wins, and a digest mismatch is rejected.

### 3.5 Lazy capability discovery and tool-schema budget

Tool specs carry a `tier`: `core` (always sent) or `deferred`. The projection
sends core tools plus deferred tools the session has loaded (`tools-loaded`
slot), in a deterministic order (fixed core order, then name), so the tool
block is byte-stable across turns for prompt caching. `tool_search` (core)
searches every offered spec by name, description, and group and loads matches.
The projection reports `tool_schema_tokens` (estimated) and `tools_sent` with
each model request, and the benchmark tracks them.

Core set: `shell`, `process` (one tool with an `action` field), `read_file`,
`list_dir`, `search_files`, `search_text`, `apply_patch`, `update_plan`,
`ask_user`, `tool_search`. Everything else (`delegate`, `view_image`,
`repo_map`, `checkpoints`, web, MCP, browser) is deferred.

### 3.6 Secrets are capabilities

Secrets are configured on the execution-target plugin (`secrets` map: name →
source env var + allowed command patterns). A shell call names the secrets it
needs (`secrets: ["GITHUB_TOKEN"]`); policy decides (default `ask`); the value
is injected into that one command's environment only; every occurrence of the
value in output is replaced with `«secret:NAME»` before anything is published;
the tool result records `secrets_used` by name. Commands otherwise get an
allowlisted environment. Child sessions get no secrets unless the delegation
names them and policy allows it.

### 3.7 Provider-neutral frontend

Frontends discover providers through a generic read-only **service** call: a
plugin manifest may declare `services` (e.g. `model-catalog`); the host routes
`call_service(plugin, service, args)` to it and returns the result, without
recording (it changes no state). Choosing a model is a config apply, which is
recorded. Providers also declare `settings` (with `secret: true` fields), so the
UI renders a generic settings form; nothing in `ui/` names OpenRouter.

### 3.8 Child capability and context isolation

A child is an ordinary session (`start-session`, cause = the parent's
`delegate` invocation) from a configured definition. It receives only: the task
text, explicitly passed context snippets, an allowlist of tools, a budget, and a
workspace choice (`shared` read-only view, or `isolated` worktree materialized
from a parent checkpoint). It inherits no memory, secrets, or tools by default.
Its result returns as `subagent-result` with a concise conclusion plus
evidence (commands run and exit codes, files changed, final checkpoint, diff
stat); the full transcript stays in the child's log. Adoption is a parent tool
(`adopt_changes`) that applies the child's change set file by file, refusing any
file whose parent content no longer matches the child's base (stale-state
protection). Parent hard stop cascades to running children.

### 3.9 Benchmarking against a minimal loop

`prototype/bench/` runs the same tasks under two definitions with the same
model: `coder` (full harness) and `minimal` (provider + chat loop + one `shell`
tool). It records success, tests passed, tokens, cached tokens, calls, wall
time, time to first edit, context size per turn, static tool-schema tokens,
permission prompts, child usage, and recovery events, all derived from the
session logs (no telemetry backend). External harnesses can be added as
command-line adapters; none is a test dependency.

## 4. Further decisions made during implementation

### 4.1 Skills

A skill is a directory with a `SKILL.md` (front matter: `name`, `description`)
under `.agentmod/skills/`, `.claude/skills/`, or a configured extra directory.
The workspace plugin contributes only an index (name + description, ordered by
name) to the `skills-index` slot; `load_skill` (deferred, auto-offered when the
index is non-empty) returns the body. Skills come from the repository, so they
are `workspace` authority: they can instruct, never authorize, and their text
is wrapped like any other workspace instruction.

### 4.2 External capabilities are contained by policy and labelling

`web-fetch`, `mcp-bridge`, and `browser-control` are ordinary deferred tool
plugins. Each declares `group` and `effects` on its specs (`network`, `mcp`,
`browser`), so policy rules can target them without knowing tool names. Their
results carry `trust: "external"`; the projection wraps them in
`<untrusted source=…>`. `web-fetch` refuses private and loopback hosts unless
configured. MCP tools are namespaced `mcp__<server>__<tool>` and default to
`ask`. `browser-control` drives a Chrome it launches with its own profile
(`DEFAULT_RULES` asks before using a browser on the local machine).

### 4.3 Browser persistence

The browser host now writes every record to IndexedDB *before* dispatching it
(the same write-ahead rule as the native store), keeps compiled configs there,
and on boot rebuilds sessions from the store and runs the kernel's recovery.
Differences from the native host that remain by nature of the platform: a
browser can evict storage under pressure (the host requests persistent storage
but cannot force it); a private window may offer no IndexedDB (the runtime then
runs in memory and says so); two tabs share one store, so the first tab holds a
Web Lock as the only writer and a second tab runs in memory and says so; plugin
workers cannot outlive the tab, so an in-flight invocation is retried after
reload instead of continuing.

### 4.4 Interruption and cancellation

| Action | Effect |
|---|---|
| Soft stop | finish the current event chain, start no new model turn |
| Hard stop | cancel in-flight invocations (`cancel` to plugins, kill after 3 s); cascades to running children; background processes keep running |
| Interrupted tool call | the projection pairs every dangling call with a synthetic "interrupted" result, so the next request is valid for every provider |
| Interrupted mutating shell command | an in-flight marker survives; the next shell result says the command may have changed files, and the checkpoint taken before it allows rewind |
| Runtime killed | processes survive (detached) and are reconciled from their files; logs recover per the HLD |
| Kill a process | `process kill` or the UI; the command's own pid tree is signalled leaf-first (never the waiting wrapper, which crashes CheerpX) |

### 4.5 Observability without a telemetry backend

Providers record per-call metrics on `model-response` (latency, time to first
token, tokens, cached tokens, cost, retries, finish reason); the projection
records `tool_schema_tokens`, `tools_sent`, and context size on
`model-request`. `agentmod metrics [SESSION] [--json]` and the UI's Harness tab
derive everything from the logs, so the numbers survive export and replay.

### 4.6 Containment by target

`linux-sandbox` (CheerpX VM in the tab, no network) is the contained target;
`local-workspace` is not a sandbox. It confines tool paths, runs commands with
an allowlisted environment, injects granted secrets only, and pairs with
`policy` in `coder`. A local container/VM target would be one more execution
target plugin over the same toolkit (`runner` + `target` interface); none is
bundled.

## 5. Implementation status

Every item of the specification, where it lives, and how it is checked.
"e2e n" refers to the numbered steps of `prototype/tests/e2e.sh` (native
runtime, real plugin processes, a mock OpenRouter that only plays back model
turns).

| Requirement | Implementation | Checked by |
|---|---|---|
| Shell | `shell` in `sdk/coding/toolkit.js`: exit code, separate streams, timeout, head+tail bounding with full output saved, diagnostics, read-only classification | `coding-toolkit.test.mjs`, e2e 8 |
| Persistent processes | `process` (`sdk/coding/processes.js`): start (idempotent per call), status, read with cursor, write stdin, kill, list; durable state dir; reconciliation | `coding-toolkit.test.mjs`, e2e 17 (runtime SIGKILL) |
| read_file / list_dir / search_files / search_text | toolkit + `sdk/coding/search.js`; deterministic order, bounded, content hashes | `coding-toolkit.test.mjs` |
| apply_patch | `sdk/coding/patch.js`: create/update/delete/move, exact edits or `*** Begin Patch`, atomic, stale-read refusal, nearest-match hints, diff | `coding-toolkit.test.mjs`; randomized diffs verified with `patch(1)` in `workspace-tools.test.mjs` |
| Plan tool | `plan` plugin (`update_plan`), rendered as a trailing system message and in the UI | `harness.test.mjs`, e2e 9 |
| ask_user as continuation | `ask-user` plugin: the question is a `choice`/free-text hint; the answer becomes the tool result, not a new user turn | `harness.test.mjs`, e2e 12 |
| Checkpoints and rewind | shadow Git (`sdk/coding/checkpoints.js`), nested repos grafted; restore checkpoints first; UI *rewind files / conversation / both* | `coding-toolkit.test.mjs`, e2e 10 |
| Session branching | UI *branch here* = `start-session` with `fork_from` + isolated worktree at the checkpoint in effect | e2e 11 |
| Child agents | `subagent` (`delegate`): definitions, tool allowlist, read-only children, budget, isolated worktrees, evidence, hard-stop cascade; `adopt_changes` with stale-base conflicts | `harness.test.mjs`, e2e 13–14 |
| Artifact inspection | `view_image` → image attachments, bounded per request | `coding-toolkit.test.mjs`, `harness.test.mjs` |
| Browser automation | `browser-control` over CDP (navigate, snapshot, click, type, screenshot, eval) | `browser-control.test.mjs` (fake CDP) |
| Web / network | `web-fetch` (fetch + HTML→text, private-host refusal) | `mcp-web.test.mjs` |
| MCP | `mcp-bridge` (stdio + HTTP), namespaced tools, deferred | `mcp-web.test.mjs` (fixture server) |
| Provider-neutral UI | plugin `services` (`model-catalog`, `check-credentials`) + `settings`; `ui/runtime/models.js` removed | `browser-runtime.test.mjs`, `tests/browser.mjs` |
| Git awareness | `workspace-info` (branch, dirty files, nested repos), checkpoints never touch `.git`, policy protects VCS internals | `coding-toolkit.test.mjs`, `harness.test.mjs` |
| Diagnostics | `sdk/coding/diagnostics.js` (rustc, tsc, gcc/clang, python, pytest, eslint, TAP) → `diagnostics` event + UI hint | `coding-toolkit.test.mjs` |
| Lazy discovery, measured schemas | tiers + `tool_search`; deterministic order; `tool_schema_tokens` per request | `harness.test.mjs`, bench |
| Context authority | `sdk/projection.js` authority classes, untrusted wrapping, workspace instructions below user | `harness.test.mjs` |
| Layered security, permissions | `policy` (`sdk/policy.js`): visibility, deny > ask > allow, scopes, explanations, digest-bound approvals revalidated on arrival, permission modes | `harness.test.mjs`, e2e 12 |
| Secrets | target `secrets` + grants with command patterns; injection per command; redaction `«secret:NAME»`; no secrets for children | `coding-toolkit.test.mjs`, `harness.test.mjs` |
| Provider abstraction | `sdk/openai-compat.js` + `catalog.js`; OpenRouter and OpenAI-compatible plugins; hot swap by config apply | e2e 15 |
| Context management | projection: tool-output elision with a deterministic summary, image bounds, budget from the model's context length | `harness.test.mjs` |
| Repo map | `repo_map` (`sdk/coding/repomap.js`) | `coding-toolkit.test.mjs` |
| Skills | 4.1 | `coding-toolkit.test.mjs` |
| Hot config | unchanged kernel mechanism; plugin stamps now hash transitive imports | Rust `stamps_cover_imported_modules`, e2e |
| Observable memory | `memory` notes carry provenance; injection recorded as `memory-injected` | `harness.test.mjs` |
| History inspection | UI Harness tab (plan, checkpoints, processes, children, policy decisions, metrics); `agentmod inspect` | `browser-runtime.test.mjs` (`harnessSummary`) |
| Interrupt / cancel | 4.4 | e2e 5, 17; `harness.test.mjs` |
| Browser persistence | 4.3 | `browser-runtime.test.mjs` (fake IndexedDB, crash between write and dispatch) |
| Stale-state protection | read hashes, `expected_sha256`, adoption base check, approval digests | `coding-toolkit.test.mjs`, `harness.test.mjs` |
| Output bounding | shell head+tail, list/search caps, image caps, elision | `coding-toolkit.test.mjs` |
| Observability | 4.5 | e2e 19 |
| Benchmark | `prototype/bench/` (coder vs minimal, hidden checks, regressions, adapters for external CLIs) | `node bench/run.mjs --selftest` |

The self-test (one task, mock model playing the same turns for both
definitions) shows the plumbing, not model quality: `coder` passes and
`minimal` fails because the scripted turns use the harness tools; the static
tool-schema cost is about 1.9k tokens for `coder` against about 80 for
`minimal`. Real comparisons need `OPENROUTER_API_KEY` and
`node bench/run.mjs --model <id>`.

### Known limits

- Not verified in this change against real services: a real model (no key in
  the build environment) and real CheerpX (its CDN was unreachable). The
  in-browser runtime was built to WebAssembly and `tests/browser.mjs` passes in
  headless Chromium; the VM device and CDP client are tested in Node against
  fakes, and `tests/sandbox-browser.mjs` runs against real CheerpX in CI.
- A definition whose plugins a host cannot run (e.g. `minimal`, which is
  native-only, in the browser) is skipped on that host with a
  `definition-unavailable` warning instead of failing the whole config.
- Shell classification is a heuristic; anything not recognised as read-only is
  treated as mutating (checkpoint first, policy may ask). On `local-workspace`
  shell commands are not path-confined.
- Repositories nested inside nested repositories are snapshotted as plain
  files of the outer nested repository.
- No LSP; diagnostics come from compiler and test output. GitHub access is
  through the shell (`gh`, with a granted token) or an MCP server.
- A child's isolated worktree is materialized at its first tool call, from
  the parent checkpoint in effect when it was delegated.
- Under CheerpX, grandchildren of a killed process may run to completion
  (`pgrep -P` cannot see them).
