# Hot paths: live streams, compiled tool routing, and turn economy

Status: implemented in `prototype/` (October 2026). Companion to
[high-level-design.md](high-level-design.md) and [coding-harness.md](coding-harness.md).

AgentMod stays generic where it is configured and compiled, and becomes direct
where it runs. Two hot paths used to go through the generic event pipeline even
though nothing generic was happening on them:

1. **Model output.** Every provider token delta was a canonical `stream-chunk`
   event: appended to the log, fsynced, run through a pipeline, dispatched to
   every async subscriber (the frontend among them), and broadcast to watchers —
   one full control-plane round per token.
2. **Tool calls.** Every `tool-call` event was delivered to every plugin that
   consumes `tool-call` — about ten in the `coder` definition — and all but one
   returned without doing anything. Ownership was already known from
   configuration.

This document describes what replaced them, how the harness now avoids model
turns that carry no information, how the cost of the control plane is
measured, and how the change was benchmarked.

## 1. Three kinds of state

| Kind | What it is | Where it lives | Lifetime |
|---|---|---|---|
| **Canonical semantic state** | The source of truth: what was asked, which model ran, what it answered, which tool ran with which arguments under which policy and config, what came back, what changed, what recovered. | The per-session append-only log (`sessions/*.jsonl`, IndexedDB `records`). | Forever; replayable. |
| **Recoverable partial state** | Enough of an in-flight stream to survive a page refresh, a reconnect, or a runtime restart: per attempt, the blocks so far, usage, status, sequence. | The recovery store: `streams/*.jsonl` natively, IndexedDB `stream-ops` in the browser. Written as compact segments, then compacted snapshots — never cumulative copies (`H`, `He`, `Hel`…). | Until the stream's invocation completes and its canonical response is durable; then discarded. |
| **Ephemeral presentation state** | Coalesced live frames on their way to a screen; the UI's materialized view of a stream. | Client queues in the stream hub; the page's `StreamStore`. | Until rendered; dropped (and re-hydrated) when a client falls behind. |

The canonical log never contains a token fragment, a coalesced frame, or a
"plugin X saw this tool call and ignored it" record.

## 2. The semantic event pipeline (what stays canonical)

Unchanged: one publish primitive, blocking chain then async observers, two
lanes, deterministic replay, recorded stamps, write-ahead before dispatch.
What changed is *what is allowed to be an event*, and *who an event is
delivered to*.

### Event classification

Every event family in the bundled plugins, classified:

| Family | Class | Frequency | Notes |
|---|---|---|---|
| `session-started`, `config-applied` | canonical semantic | per session / per config change | core lifecycle |
| `dispatch-failed` | canonical semantic | rare | core: a keyed event had no owner, or its owner failed without answering |
| `user-message`, `assistant-message` | canonical semantic | per turn | |
| `model-request`, `model-response` | canonical semantic | per model turn | the response carries text, tool calls, model/provider identity, normalized usage, and the stream's own counts |
| `tool-call`, `tool-result` | canonical semantic | per tool call | the approved re-publish of a call is its own event |
| `policy-decision`, `approval-requested`, `ui-action`, `context-edit` | canonical semantic | per decision / user action | a quiet `allow` leaves only the policy invocation record |
| `plan-updated`, `user-input-requested`, `session-titled`, `memory-injected` | canonical semantic | per call | |
| `workspace-info`, `workspace-change`, `checkpoint-created`, `workspace-restored`, `process-started`, `process-exited`, `diagnostics` | canonical semantic | bounded per tool call | world-state facts that rewind and audit depend on |
| `workspace-status` | ephemeral presentation, recorded | ≤ a few per tool call (boot, import progress) | low volume, so left as events; a candidate for the stream channel if it grows |
| `subagent-started`, `subagent-result`, `budget-exhausted` | canonical semantic | per child | child *progress* is the child's own session, not parent events |
| heartbeat journal records | canonical semantic | per fire (off by default) | |
| **provider token deltas** (was `stream-chunk`) | **ephemeral presentation + durable recovery** | per token | now frames on the live stream channel (§3) |
| process output | durable recovery (files in the workspace state dir) | per byte | never events; read through the `process` tool |
| runtime counters, timings | telemetry | per operation | `/api/metrics`, `agentmod metrics`; never labelled by session or stream id |

Only token deltas were high-frequency; nothing else needed moving.

### Who an event is delivered to

* The web frontend no longer subscribes to `*`. It needed one standing
  invocation per session to cite for user actions, and reads everything else
  as a watcher, so it now consumes `session-started` only. Before, every event
  cost two extra records and an IPC round trip to the gateway.
* Tool calls go to their compiled owner (§4).

## 3. The live stream subsystem

```text
provider wire format
   ↓  provider adapter (plugin): SSE parser → normalized frames → StreamWriter
   ↓  `stream` method (batched notifications; the closing batch is a request)
StreamHub  (agentmod-core::stream, same code natively and in WASM)
   ├── live, coalesced frames → attached clients (cursor + byte-bounded queue)
   ├── recovery ops → recovery store (segments, compacted snapshots, discard)
   └── materialized partial state → snapshots for hydration
provider adapter publishes ONE canonical `model-response` → semantic pipeline
```

### Normalized frames

`open`, `block-start` (`text` | `reasoning` | `tool-call`, with tool name and
call id), `text-delta`, `reasoning-delta`, `tool-args-delta`, `metadata`,
`usage`, `block-end`, `boundary` (served model/provider changed), `error`
(`retryable` or not), `cancelled`, `complete`; clients also receive
`snapshot`, `finalized`, and `resync-required`. Provider specifics stay in
the adapter (`plugins/sdk/openai-compat.js`, `plugins/sdk/sse.js`).

### Identity and sequence

* `session_id` and `stream_id`: the stream *is* the provider's invocation
  (`s0003/i17`), so execution owns it — not a tab, not a connection.
* `attempt_id` (`<invocation attempt>.<n>`): every retry and every provider or
  model fallback opens a new attempt. A new attempt supersedes the previous
  one's partial output; it is never concatenated to it. Frames from a
  superseded attempt are ignored and counted as late.
* An AgentMod-assigned sequence, monotonic per stream across attempts; the
  provider's own sequence (`pseq`) when it has one. Duplicates are detected by
  sequence, never by text equality; gaps are counted.

### Coalescing

Adjacent compatible deltas (same attempt, same block) are merged and flushed
after `flush_ms` (default 33) or `flush_bytes` (default 512), at any block
boundary, and on completion, error, or cancellation. Runs never cross
content blocks, tool calls, reasoning/text boundaries, attempts, or
fallback boundaries. **The first delta of each block goes out at once** (in
the plugin's batcher and in the hub), so first-token latency is not traded
away: in the Kilo benchmark, median time from model request to first visible
text is 15 ms after vs 19 ms before, while live frames per provider event
fell to 0.56. Settings live under `[runtime.streaming]`
(`flush_ms`, `flush_bytes`, `client_queue_bytes`, `max_stream_bytes`,
`compact_after_segments`).

### Terminal states, errors, retries

A stream's status is monotonic: once `complete`, `failed`, or `cancelled`,
later frames are ignored and counted. The adapter distinguishes an HTTP error
before streaming (retried by `post`), an error event or dropped connection
after HTTP 200, a truncated stream (no finish reason, no `[DONE]`), and a
malformed final frame — the last three retry as new attempts
(`stream_retries`, default 1), then `fallback_models` are tried. A UI
disconnect cancels nothing. A plugin crash interrupts the open attempt and the
kernel's invocation retry opens a new one. A hard stop finalizes the stream
as cancelled. A runtime crash leaves recovery segments; on restart the stream
is restored as `interrupted` for reconnecting clients and superseded by the
retry's new attempt; if the crash came after the canonical response was
logged, the leftover recovery state is discarded.

### Usage

Adapters report usage as they get it (`final`, `cumulative`, or `delta`);
`UsageAccumulator` normalizes per attempt (cumulative reports replace, deltas
add — cumulative counters are never summed). The canonical response carries
`usage` (the final attempt) and, when there were several attempts,
`usage_total` (everything billed); metrics use the total.

### Tool-call streaming

Tool arguments stream per tool block as presentation only — the UI shows
"preparing `shell` …". Nothing executes until the provider's block is
complete and the response assembled; arguments that are not a complete JSON
object come back to the model as an error result and are never passed on;
then chat-context checks them against the tool's schema, and policy decides as
before.

### Backpressure, clients, reconnect

Every boundary is bounded: the SSE parser (per event), the plugin's batch
(bytes), the plugin→host link (watcher stdin backlog), the hub's per-client
queue (bytes), the gateway's per-SSE-connection socket buffer, and the
materialized text per stream. A client that falls behind is dropped to
`resync-required` instead of growing memory, and re-hydrates from a snapshot.
Hydration is race-free: `attach` returns snapshots atomically with
subscribing; over SSE the client subscribes first, buffers, fetches
`/api/streams`, and applies only buffered frames with a greater sequence.
Any number of clients attach independently, a second client can join
mid-stream, and one disconnecting affects nothing.

### Rendering

`ui/runtime/stream-store.js` ingests messages as they arrive — also in a
hidden tab — and `renderScheduler` renders at most once per animation frame
while visible, nothing while hidden, and once (the latest state) when the tab
becomes visible again. Live bubbles update in place; they give way to the
canonical events once those arrive.

## 4. Compiled tool routing

Plugins declare the tools they own, from their configuration, at handshake:

```js
consumes: [ownTools(['read_file', 'list_dir', …])],   // { event: 'tool-call', keyed: { key: 'name', values } }
tools: declareTools(specs),                           // names, parameters, required, tier
```

MCP servers own families (`mcp__github__*`). The compiler builds, per
definition and keyed event, one owner table and rejects: duplicate ownership
and overlapping prefix families (unless the definition pins an owner with
`route_owners`), pins naming a plugin that does not own the value (stale or
disabled), tools declared without owning them, invalid tool schemas, keyed
consumers that are blocking or wildcards, and plugin versions other than the
one a config requires (`version = "0.2"`). `agentmod compile` prints each
table; `routes_revision` hashes them.

At runtime the kernel dispatches each settled `tool-call` to exactly the
owner its name selects, plus the observers (the policy gate before it, any
async observers after). The invocation record carries `route` — the tool name
that selected the owner — and the stamp (binary + config hash) bound at
dispatch. Retries and cancellations use that recorded executor identity, so
a hot configuration change can never move an effect to another executor
mid-flight; the next call uses the new revision. A call no owner claims, or
whose owner fails without answering, becomes `dispatch-failed`, which
chat-context answers so the loop never waits forever.

Lazy discovery binds to the table: `tool_search` loads only tools with a
compiled owner, loading is session-sticky (the `tools-loaded` slot), and
there is no runtime scanning.

## 5. Tool batch settlement

Chat-context publishes every call of one assistant turn and asks the model
again only when **all** of them are answered — whatever order results arrive
in, whether some failed, whether one waited for an approval or a user's
answer. A hard stop while a batch is pending asks nothing further. Covered by
`tests/e2e.sh` step 16b (four parallel calls, out of order, one actionable
failure, one approval → exactly one next model request).

## 6. Turn economy

The harness should not make a model spend a turn to learn something the
harness already knows. Changes:

| Avoidable turn | What the harness does now |
|---|---|
| Probing the environment (`uname`, `which gcc`, versions) | OS, architecture, CPUs, installed tools with versions, and network reachability are probed once per session and shown in the system prompt. |
| A discovery turn for an obvious capability | Deferred tools are listed with their argument names in `tool_search`'s description and are callable directly; a user message matching a tool's declared `intents` ("import the GitHub repo…") loads it before the first request. Full schemas still ship only when loaded: ~15 tokens per deferred tool instead of a turn. |
| A wrong-path or wrong-argument retry chain | Errors name the fix: a file passed as a search directory returns the exact `path` + `include` call; a missing path names the nearest directory and its entries; arguments are checked against the schema before dispatch with a corrected example; unknown tool names get the closest matches. |
| One read per turn | The preamble asks for independent calls in one response (they run concurrently and settle together, §5), and never for dependent mutations together. |
| Plan-only turns | `update_plan` is coarse (3–6 steps), recorded once, and updated alongside other calls — never as a turn of its own. |

Planning and lazy discovery stay; they were not removed to save a call.

**Measured trade-off.** The environment line, the deferred-tool signatures, and
the longer preamble add about 7% prompt tokens per request when the model
does *not* change its behaviour (Kilo, fixed trajectory: 131.6k vs 123.3k
prompt tokens over 23 requests, mostly cached). When it uses them, requests
fall from 23 to 10 and prompt tokens by 57% (§8).

## 7. Metrics

`agentmod-core::metrics::session_metrics(records)` computes, from a log alone
(any log — pre-hub logs count each `stream-chunk` as a provider event, a frame,
and a live delivery, so before/after use one definition):

* **control plane** — records and bytes, events (semantic vs `stream-chunk`),
  pipeline starts/settles, plugin invocations (blocking, async, no-op, failed,
  retried), tool-call invocations split into exact-owner and broadcast
  dispatches, provider events → frames → live frames, recovery writes/bytes;
* **model efficiency** — requests, prompt / cached / uncached / completion
  tokens, cost, latency, provider retries, recovered invocations, compactions,
  child-agent calls, failed tools, dispatch failures, context size per turn,
  time to first edit, wall time;
* **ratios** — pipelines per semantic event, invocations per semantic event,
  plugin invocations per tool call, live frames per provider event, canonical
  events and records per model response, journal bytes per useful output
  byte, no-op share of invocations;
* **per turn** — records, bytes, events, pipelines, invocations, tool calls,
  and the response's tokens and context, keyed by the request's event id.

Where: `agentmod metrics [SESSION] [--json] [--turns]`; the gateway's
`/api/sessions/<id>/metrics` and `/api/metrics` (runtime and gateway counters:
records, fsyncs, routed invocations, stream messages, live deliveries,
resyncs); WASM `session_metrics`; the **Hot path** section of the harness
inspector, which also shows the page's own stream messages, renders, and
resyncs. Metric labels are kinds, never session or stream ids.

`bench/turns.mjs` answers *why* each model request happened (initial, final
answer, bad arguments, sandbox recovery, tool discovery, plan only,
environment probing, redundant reinspection, build/test iteration, error
recovery, avoidable serial step, repository navigation, tool result
follow-up), marking the avoidable ones, for a native data directory or a
browser **Export logs** file.

## 8. Benchmark: the Kilo task

`node bench/kilo.mjs --before-ref <ref>` runs *"Import the GitHub repo
antirez/kilo, build it with make, and explain how it draws the screen."*
offline against two built trees — the working tree and a git worktree of
`<ref>` built on the side — with a fake GitHub serving a vendored snapshot of
antirez/kilo, real tools, and a real `make`. The model is a script
(`bench/kilo-world.mjs`):

* **fixed** — the same trajectory on both builds (23 requests reproducing
  habits that cost turns: a plan-only turn, a discovery turn, two probes, one
  read per turn, a file passed as a search directory, a re-read). Differences
  are what the harness itself costs.
* **adaptive** — takes a shortcut only when the harness makes it visible in
  the request (environment facts, signatures, guidance, an actionable error).
  This is a *simulated* model, not a measurement of any real one.

Token usage is simulated from request sizes with prefix caching. Every metric
is computed from the logs by the current `agentmod metrics`; UI numbers come
from a client subscribed to the gateway's SSE stream. Results for
`d073dd7` (before) vs this change ([raw report](../benchmarks/kilo-offline-raw.md)):

**Same trajectory (control plane):**

| Metric | Before | After | Change |
|---|---|---|---|
| Task success / build / answer rubric | yes / yes / 4/4 | yes / yes / 4/4 | — |
| Model calls · tool calls | 23 · 22 | 23 · 22 | 0% |
| Canonical events | 415 (311 `stream-chunk`) | 104 | −75% |
| Pipeline executions | 415 | 104 | −75% |
| Plugin invocations | 739 (635 no-op) | 150 (45 no-op) | −80% |
| Plugin invocations per tool call | 11 | 2 (policy + owner) | −82% |
| Tool dispatch | 220 broadcast | 22 exact owner, 0 broadcast | |
| Journal records · bytes | 2,724 · 936 KB | 613 · 370 KB | −77% · −61% |
| Events per model response | 18.0 | 4.5 | −75% |
| Journal bytes per useful output byte | 36.5 | 14.4 | −61% |
| UI deliveries · bytes | 2,724 · 993 KB | 735 · 433 KB | −73% · −56% |
| First visible text, median · max | 19 ms · 105 ms | 15 ms · 75 ms | comparable |
| Prompt tokens (simulated) | 123.3k | 131.6k | +7% (see §6) |

**Adaptive script:** 23 → 10 model requests, 13 → 0 avoidable (classifier),
prompt tokens −57%, simulated cost −52%, wall time −26%, journal records
−85%; same success, build, and answer rubric.

**Minimal loop** (provider + loop + one `shell` tool, same task): 13 requests,
all successful; the coder harness used 10 requests but 2.4× the prompt tokens
(larger system prompt and tool schemas) and more control-plane records (409 vs
266). For this small task the harness pays for itself in turns, not in tokens;
its other features (checkpoints, policy, isolation) are not exercised here.

**Worse metrics, investigated.** Prompt tokens +7% on the fixed trajectory
(the price of §6's information, measured above). "Stream provider events" is
not comparable across builds: before, only text deltas were counted (tool
argument fragments never became events); after, every SSE event is counted.
The 45 remaining no-op invocations are policy `allow` decisions — real gates,
and the audit record of what policy allowed.

**What this does not show.** The original browser run of this task used about
40 model requests with a real model in the CheerpX sandbox. That log was not
available here and this environment has no model access, so its turns were not
classified; the 23-request trajectory above is a reconstruction of common
habits, not that run. To classify a real run: export the session from the
browser UI and run `node bench/turns.mjs export.json`; to re-measure with a
real model: `node bench/kilo.mjs --real --model <id>` (needs
`OPENROUTER_API_KEY` and GitHub access).

CI runs `node bench/kilo.mjs --assert`, which fails if any run fails, if a
token fragment reaches the log, if any tool call is not dispatched to exactly
one owner, or if records per model response, events per model response,
invocations per tool call, or the adaptive script's turn count regress.

## 9. Auditability

| Question | Where the answer is |
|---|---|
| Which model ran? | `model-response.model` (served), `requested_model`, `provider`, `attempt_id`; the provider invocation's stamp |
| Which tool, with what arguments? | the `tool-call` event (and its approved re-publish) |
| Which plugin executed it? | the owner's `invocation-started` record: `plugin`, `route`, `stamp` |
| Under what config? | `session-created` / `config-applied` hashes; the stamp on every invocation; `routes_revision` |
| What policy allowed it? | `policy-decision` for every non-quiet decision; the policy invocation (quiet allows); approvals and `ui-action`s |
| What came back? | `tool-result`, or `dispatch-failed` |
| What files changed? | `workspace-change`, `checkpoint-created`, `workspace-restored` |
| What child acted? | `subagent-started` and the child's own session |
| What recovery occurred? | `invocation-retried`, attempt counts, `usage_total`, stream attempt counts in the response's metrics |
| What response was committed? | the single `model-response` per turn |

## 10. Where things live and how they are tested

| Piece | Code | Tests |
|---|---|---|
| Stream hub (frames, coalescing, recovery, broker) | `crates/agentmod-core/src/stream.rs` | its unit tests: byte splits, coalescing, attempts, late/duplicate/missing/invalid frames, tool JSON per block, slow client + resync, mid-stream join, disconnect, recovery replay and compaction, crash restore, large responses |
| Native host integration | `crates/agentmod-runtime/src/host.rs`, `store.rs` | `tests/e2e.sh` steps 5–7 (hard stop mid-stream, plugin crash mid-stream, runtime SIGKILL mid-stream with recovery files and no concatenation) |
| Browser host integration | `ui/runtime/browser-host.js`, `persist.js`, `crates/agentmod-wasm` | `tests/browser-runtime.test.mjs`; `tests/browser.mjs` (headless Chrome, real WASM: live text renders mid-stream, nothing in the log) |
| Provider side | `plugins/sdk/sse.js`, `stream.js`, `openai-compat.js` | `tests/streaming.test.mjs`: every byte split, UTF-8, CRLF/LF/CR, malformed final frames, usage modes, retry after partial output, truncated streams, fallback, cancellation, tool arguments |
| Client store | `ui/runtime/stream-store.js` | `tests/streaming.test.mjs`: hydration barrier, gaps, duplicates, new attempts, hidden tabs |
| Keyed dispatch | `compiler.rs`, `kernel.rs`, `plugins/sdk/agentmod.js` (`ownTools`, `declareTools`) | `crates/agentmod-core/tests/routing.rs`: exact owner, duplicates, pins, stale owners, prefixes, schema, versions, no-owner/owner-failed, config revision, retry executor identity, replay |
| Batch settlement | `plugins/chat-context` | `tests/e2e.sh` step 16b |
| Turn economy | `plugins/sdk/coding/toolkit.js`, `toolargs.js`, `projection.js`, `tool-discovery` | `tests/turn-ergonomics.test.mjs`, `tests/harness.test.mjs` |
| Metrics | `crates/agentmod-core/src/metrics.rs` | `routing.rs`, e2e step 19 |
| Benchmark | `bench/kilo.mjs`, `kilo-world.mjs`, `turns.mjs` | CI `kilo.mjs --assert` |
