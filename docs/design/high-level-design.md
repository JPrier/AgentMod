# AgentMod High-Level Design

This document proposes the **approach** to the [North Star](north-star.md): how the runtime is shaped so that every requirement there is reachable. It is revisable without touching the goal. Each subsystem named here recurses into its own subcomponent design; nothing is implemented directly from this page.

## The spine: a config-compiled event bus

The runtime's sole execution construct is the **per-event pipeline**. An event arrives; the core looks up the compiled pipeline for its name; subscribed plugins run; everything is appended to the session's log; output events dispatch to their own pipelines.

The "workflow graph" is not an executor — it is the **compile-time artifact**. When configuration loads, the compiler walks every subscription and emission declared by the configured plugins and produces a validated graph: every consume matched to a publisher, every demand matched to a supply, recursion bounded, ordering fixed. Control flow lives entirely in plugins and configuration; the core guarantees only that dispatch is deterministic and observable. Gated workflows, agent loops, and shapes not yet imagined are config the compiler merely validates.

## Events

An event is an **envelope** — the universal supply, keys every consumer may demand without declaration:

- **`event_id`** — stable, unique; the idempotency key for at-least-once delivery.
- **`session_id`** — the session whose log this event lives in.
- **`event_name`** — the routing key.
- **`sequence`** — position in the session log; order-of-record is replay truth.
- **`cause`** — the invocation that published this (or, for a deferred publish, the cited standing trigger; for a session start, the starting invocation or true root). The causal chain is walkable from any event back to a root.
- **`lane`** — normal or priority.
- **`arrived_at`** — arrival timestamp, a recorded fact (the core stamps history; it never acts on time).
- **`payload`** — the event-specific keys.
- **`context`** — the session's assembled context as of dispatch: the ordered, invocation-attributed contributions from the log. The envelope guarantees this structure only; the *chat shape* (messages, tool descriptions, token counts) is a convention owned by the bundled default plugins — universal in practice, invisible to the core, replaceable in principle.

Beyond the envelope, payload contracts are **demand/supply declarations, not schemas**: an emitter declares, per event name, the payload keys it supplies; a consumer declares the keys it requires. The compiler verifies, per edge, that supply covers demand; extra keys always flow freely — consumers state needs, never exhaustive shapes. There is no central schema registry and no type documents. Richer validation, where wanted, is a validator plugin's business, not the core's.

The runtime itself publishes a small set of **core lifecycle events** (`session-started`, and siblings named in the control-plane subdesign) that plugins may pipeline off like any other.

## Plugins

A plugin is a **supervised long-running process** speaking one versioned JSON-RPC-style wire protocol over a local transport. One model, no hybrid: the process boundary simultaneously provides fault isolation, language-agnostic authorship, hot-swap, and the observability boundary — every request and response crossing it is recorded, which *is* the pipeline record. Identity is **connection-derived**: plugin instance, binary hash, and config hash bind at handshake, so attribution is structural and unforgeable — there is no sender field to lie in. Wrapping an MCP server is a thin bridge plugin; the core never knows MCP exists.

**Publishing.** There is one way to publish an event: `publish(event, target_session?)`. Each invocation the runtime dispatches carries an **invocation id** — every subscriber, blocking or async, is invoked under one (one invocation per plugin per event pipeline). The runtime classifies every publish by that id:

- **Pipeline output** — the publish references an invocation currently open on that connection: it becomes part of that invocation's record, ordered within the pipeline, `cause` stamped automatically.
- **Deferred publish** — no open invocation: the event enters a lane as a fresh arrival, and it **must cite a prior invocation dispatched to this same plugin** — its standing trigger (the send-and-wait reply cites the send; the file-change cites the watch registration). The runtime validates the citation against the record; an uncitable publish is blocked and recorded. Citations are validated against **plugin identity**, not process instance, so they survive plugin restarts and hot-swaps. The manifest flags, per emitted event name, whether it may be published deferred, so deferral is statically visible in the compiled graph.
- **`start-session`** — the sole rootless act: creates a new session (a new log) from a named definition, its payload carrying the initial event. The user's first message, cron's 02:00 job.

The deployment's complete external surface is the list of plugins holding the `deferred-publish` and `start-session` capabilities — small, declared, auditable — and every deferred entry is citation-chained back into the causal graph. Triggers (cron, webhooks, heartbeats, frontends) are just plugins holding these capabilities; the core has no scheduler and no clock. There is no core broadcast: "publish to all sessions" is a plugin looping over the session table — N ordinary recorded publishes.

**Sessions are the unit of concurrency.** A session's blocking dispatch is deliberately serial; parallel agents are **peer sessions** — each with its own log, own serial dispatcher, own definition, standard recovery. A sub-session is an ordinary session whose `cause` is another session's invocation; results return as publishes targeting the parent, which parks event-free until woken. Litmus: *needs its own event loop or its own context → session; otherwise → events in the parent.* Cross-session communication is solely recorded event publication (cross-session targeting is itself a manifest capability); parent–child and peer topologies are conventions, not runtime concepts. Mirrored records in both logs keep per-session replay self-contained.

The rest of the plugin contract:

- **Event in → contributed context + published events.**
- **Restart tolerance.** The runtime may kill and restart a plugin between any two invocations. In-memory state is disposable by contract; durable state lives in the log (replayed on init) or in external stores the plugin owns. The canonical pattern: a trigger or deferring plugin keeps a **journal session** — its durable memory is that session's log (obligation-opened on send, obligation-closed on delivery; one record per cron fire). On restart it reads the tail, re-establishes its external attachments, and applies its configured **catch-up policy** for input missed while down — the cron/anacron distinction is plugin config, not a runtime concern.
- **At-least-once delivery.** Every event carries a stable id; side-effecting plugins should be idempotent per event id.
- **Manifest.** Each plugin declares: consumed event names with demanded keys; emitted event names with supplied keys and per-event deferred flags; config schema; version; capabilities (`deferred-publish`, `start-session`, cross-session targeting). Declarations are enforced symmetrically at runtime: an undeclared emit, a missing supply key, or an uncitable deferred publish is blocked and recorded.

## Pipelines

A pipeline is an **ordered chain of blocking subscribers** plus a set of **async subscribers**.

- **Blocking subscribers** run sequentially in declared order. Each sees the event plus prior contributions, and may contribute context, publish events, **transform** the event in place, or **veto** it. A transformer declares the keys it preserves and adds; because the chain order is compiled, the compiler walks supply through each transform and verifies every downstream demand survives. Hooks, approval gates, redaction, and policy live here.
- **Async subscribers** observe the settled event after the blocking chain: read-only on the event, unable to veto; their publishes enter the bus as ordinary new events. Frontends, telemetry, and memory-writers live here. Asyncs may still be running when later events process; their outputs take log order at arrival.
- **Wildcards:** a subscriber may consume `*` (placed in every pipeline at its declared position). Wildcard consumers demand nothing beyond the envelope, so they are satisfiable by construction. Emit-wildcards do not exist — declared emits and supplies are the ground truth the graph is validated against.

Compilation validates **one-sidedly**: consuming an event no configured plugin emits is a compile error (a **dead listener** — a pipeline that can never fire), as is demanding a key no upstream supplies (a **starved consumer**). Emitting an event nobody consumes is legal — the log is the universal subscriber — and supplying keys nobody demands is the normal case, not a warning.

## Ordering and control

Per session, dispatchable traffic flows in **two FIFO lanes**: a priority lane (steering, config-apply, urgent deferred publishes) and a normal lane. The priority lane never interrupts a running pipeline — it wins only the *what's next* decision, draining fully before the normal lane resumes. Priority affects when an event runs, never how.

**Dispatcher commands** — soft stop, hard stop, resume — are not events and never queue. They are edge-triggered transitions of the session's dispatch state, applied the instant they arrive and recorded as history. Soft stop drains in-flight work then parks; hard stop kills in-flight invocations now — the only thing in the system that interrupts a running pipeline. The litmus: anything that must take effect while a pipeline runs cannot be an event.

## The log

Each session **is** its append-only log: typed records (event appended, invocation started, invocation completed with outputs, context contribution, config change, dispatcher command, spill pointer) under monotonic sequence numbers. Oversized payloads spill to content-addressed immutable files referenced by hash. Cross-session interaction is mirrored events referencing the peer session and sequence. Above the logs sits a derived, rebuildable index — the session table and event index; its loss is never data loss.

**The record projects into a tree of tables.** Every record is fully keyed — `session_id`, plus `event_id` and `invocation_id` where applicable — so the flat stream projects losslessly into a navigable hierarchy: the session table maps a session to its event ids (plus definition, dispatch state, watermark); an event's table holds its envelope and its invocation ids in pipeline order; an invocation's table holds start/completion, input and output state, context contributions, published event ids, and binary/config stamps. Published-event ids link forward (fan-out); `cause` links backward (ancestry) — every id is a lookup, every lookup points onward. The flat stream is the write model, for appending, recovery, and replay; the tree is the derived read model, for humans, frontends, and inspection — rebuildable from the streams in one pass.

**Recovery is one mechanism used everywhere.** Completed invocations never re-run, and a settled invocation's output set is closed — late arrivals queue as deferred publishes, never retroactively join old records. A crash triggers an orphan scan — invocation-starts without completions — bounded by the active-set index and per-session settled-watermarks, so dormant sessions cost nothing. Orphans restart; recorded outputs stand in for everything else. Hot recompiles, hard-stop resumes, and crash recovery are all this same scan. Replay-as-reading runs no plugins at all.

## Configuration

Configuration is **live, layered, and stamped**. Global config with per-session overrides; plugin versions likewise. Apply is explicit and immediate: dispatch pauses at the next event boundary, the compiler re-validates (rejecting the apply whole on failure), and the session resumes via standard recovery. Every invocation record stamps the plugin's binary hash and config hash, and the apply is itself a recorded event — nothing silent, everything attributable. Replaying into history whose stamps differ from current config is a surfaced choice, and the choice is recorded.

## Frontends

A frontend is an ordinary plugin at the human boundary: an async subscriber to what it renders, a publisher of user-action events (deferred publishes citing the session's standing invocations; the first message rides `start-session`), and a sender of dispatcher commands — the stop button reaches the dispatcher without traversing the pipeline it is stopping. Plugins may attach **UI hint payloads** (a small versioned vocabulary: text, diff, form, choice, progress, stream-chunk) to their events; each frontend renders what it understands and falls back to raw payload display. Terminal, web, and ACP-bridge frontends are peer plugins over one vocabulary; the runtime knows nothing about rendering.

## Performance and reliability posture

The event path — lane pop, dispatch, log append, IPC round-trip — is the entire core hot path, engineered to the ≤ ~10 ms budget at 100 concurrent sessions; plugin execution time is outside it. Long-running processes amortize spawn cost to zero; local IPC round-trips are ~100 µs. The core stays small enough to verify exhaustively: deterministic, idempotent, every branch testable — the reliability target is met by keeping the core nearly featureless.

## Subcomponent designs

This page's ideas recurse into child designs: wire protocol and plugin lifecycle; publishing classification and citation validation; plugin state survival patterns; log format, indexing, and recovery mechanics; the compiler — manifest validation and demand/supply matching (key nesting, optional type tags); lanes, dispatch, and the control plane (incl. the core lifecycle event set); frontend UI-hint vocabulary; session definitions and config layering; performance verification.
