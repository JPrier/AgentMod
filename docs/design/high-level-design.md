# AgentMod High-Level Design

This document proposes the **approach** to the [North Star](north-star.md): how the runtime is shaped so that every requirement there is reachable. It is revisable without touching the goal. Each subsystem named here recurses into its own subcomponent design; nothing is implemented directly from this page.

## The spine: a config-compiled event bus

The runtime's sole execution construct is the **per-event pipeline**. An event arrives; the core looks up the compiled pipeline for its name; subscribed plugins run; everything is appended to the session's log; output events dispatch to their own pipelines.

The "workflow graph" is not an executor — it is the **compile-time artifact**. When configuration loads, the compiler walks every subscription and emission declared by the configured plugins and produces one validated directed graph — which is also the table the runtime routes from: every consumer's required keys matched, by shape, against every emitter that can reach it; blocking order fixed by the definition's phases; no plugin consuming its own emits; cycles found and reported (agent loops are cycles, and termination is a plugin's business, not the core's). Control flow lives entirely in plugins and configuration; the core guarantees only that dispatch is deterministic and observable. Gated workflows, agent loops, and shapes not yet imagined are config the compiler merely validates.

## Events

An event carries an **envelope** — fixed metadata every subscriber receives without declaring it:

- **`event_id`** — stable, unique; the idempotency key for at-least-once delivery.
- **`session_id`** — the session whose log this event lives in.
- **`event_name`** — the routing key.
- **`sequence`** — position in the session log; order-of-record is replay truth.
- **`cause`** — the invocation that published this (or, for a deferred publish, the cited standing trigger; for a session start, the starting invocation or true root). The causal chain is walkable from any event back to a root.
- **`lane`** — normal or interrupt, chosen by the publisher from the lanes its emit declares.
- **`arrived_at`** — arrival timestamp, a recorded fact (the core stamps history; it never acts on time).
- **`payload`** — the event-specific keys; each subscriber receives only the keys it declared.

Context is **not** part of every envelope. It is the default **slot**: named, typed session state a definition declares, which plugins request and write through their declarations like payload keys, and receive only when they ask. A definition may replace it or declare other slots; an embedded workflow gets its own namespaced slots. The *chat shape* of context (messages, tool descriptions, token counts) is a convention owned by the bundled default plugins — invisible to the core, replaceable in principle. How slot writes combine is its own subdesign.

Payload contracts are **structural, typed shapes**: an emitter declares, per event name, the keys it sends and their shapes; a subscriber declares the keys it needs (`receives`). The compiler verifies, on every emitter→consumer edge, that each required key is supplied with a compatible shape. There is no central registry, no event ownership, and no dependency between plugins — shapes match structurally. Keys a declaration is silent about pass untyped, and reach only subscribers that opt into **unlisted keys**. Event names may form **families** (`tool-call/read_file` in `tool-call/*`) whose members are chosen at runtime. See [compiler.md](compiler.md).

The runtime itself publishes a small set of **core events** (`session-started`, `invocation-failed`, `session-start-failed`, inbound cross-session events, and siblings named in the control-plane subdesign), declared in a versioned **core manifest** the compiler treats like any plugin's; the runtime is the graph's boundary node.

## Plugins

A plugin is a **supervised long-running process** speaking one versioned JSON-RPC-style wire protocol over a local transport. One model, no hybrid: the process boundary simultaneously provides fault isolation, language-agnostic authorship, hot-swap, and the observability boundary — every request and response crossing it is recorded, which *is* the pipeline record. Identity is **connection-derived**: plugin instance, binary hash, and config hash bind at handshake, so attribution is structural and unforgeable — there is no sender field to lie in. Wrapping an MCP server is a thin bridge plugin; the core never knows MCP exists.

**Publishing.** There is one way to publish an event: `publish(event, target_session?)`. Each invocation the runtime dispatches carries an **invocation id** — every subscriber, blocking or async, is invoked under one (one invocation per plugin per event pipeline). The runtime classifies every publish by that id:

- **Pipeline output** — the publish references an invocation currently open on that connection: it becomes part of that invocation's record, ordered within the pipeline, `cause` stamped automatically.
- **Deferred publish** — no open invocation: the event enters a lane as a fresh arrival, and it **must cite a prior invocation dispatched to this same plugin** — its standing trigger (the send-and-wait reply cites the send; the file-change cites the watch registration). The runtime validates the citation against the record; an uncitable publish is blocked and recorded. Citations are validated against **plugin identity**, not process instance, so they survive plugin restarts and hot-swaps. The manifest flags, per emitted event name, whether it may be published deferred, so deferral is statically visible in the compiled graph.
- **`start-session`** — the sole rootless act, and a runtime API call rather than a publish: the caller (holding the capability) passes a definition and an initial input; the runtime compiles the definition, creates the session (a new log) and emits `session-started` into it, carrying the input. Only the core ever emits `session-started`. The user's first message, cron's 02:00 job, an orchestrator starting a detached session.

The deployment's complete external surface is the list of plugins holding the `deferred-publish` and `start-session` capabilities — small, declared, auditable — and every deferred entry is citation-chained back into the causal graph. Triggers (cron, webhooks, heartbeats, frontends) are just plugins holding these capabilities; the core has no scheduler and no clock. There is no core broadcast: "publish to all sessions" is a plugin looping over the session table — N ordinary recorded publishes.

**Sessions are the unit of concurrency.** A session's blocking dispatch is deliberately serial; parallel agents are **peer sessions** — each with its own log, own serial dispatcher, own definition, standard recovery. A sub-session is an ordinary session whose `cause` is another session's invocation; results return as publishes targeting the parent, which parks event-free until woken. Litmus: *needs its own event loop or its own context → session; otherwise → events in the parent.* Cross-session communication is solely recorded event publication: a sender declares its outbound events and holds the `cross-session` capability, a receiving definition declares the `inbound` events it accepts, and since targets are chosen at runtime the two are paired at publish time (a mismatch fails the publish back to the sender). Ids are globally unique and `cause` links cross sessions, so history across all sessions is one connected graph; parent–child and peer topologies are conventions, not runtime concepts. Mirrored records in both logs keep per-session replay self-contained.

The rest of the plugin contract:

- **Event in → contributed context + published events.**
- **Restart tolerance.** The runtime may kill and restart a plugin between any two invocations. In-memory state is disposable by contract; durable state lives in the log (replayed on init) or in external stores the plugin owns. The canonical pattern: a trigger or deferring plugin keeps a **journal session** — its durable memory is that session's log (obligation-opened on send, obligation-closed on delivery; one record per cron fire). On restart it reads the tail, re-establishes its external attachments, and applies its configured **catch-up policy** for input missed while down — the cron/anacron distinction is plugin config, not a runtime concern.
- **At-least-once delivery.** Every event carries a stable id; side-effecting plugins should be idempotent per event id.
- **Manifest.** Each plugin declares: subscriptions (event or family, mode, `receives`, `returns` for blocking ones, per-event actions); emits (event or family, key shapes, allowed lanes, deferred flag); outbound cross-session events; config schema; version; plugin-wide capabilities (`deferred-publish`, `start-session`, `cross-session`, `interrupt`, `control`, `observe`, queue and session-directory methods). The runtime enforces one rule: anything that **contradicts** a declaration — a missing required key, a wrong shape, an undeclared emit or call, an uncitable deferred publish — is a hard failure, recorded against the declaration it broke; anything a declaration is **silent** about passes.

## Pipelines

A pipeline is an **ordered chain of blocking subscribers** plus a set of **async subscribers**.

- **Blocking subscribers** run sequentially in the order the session definition fixes through **phases**. Each sees the event with every earlier subscriber's `returns` applied, and may return keys (overwrite or add, never remove, shapes preserved), contribute context, publish events, or **veto** it. The compiler walks supply through each `returns` in chain order and checks every later consumer against it. Where two subscribers in one phase could affect each other, the definition must order them. Hooks, approval gates, redaction, and policy live here.
- **Async subscribers** observe the settled event after the blocking chain: read-only on the event, unable to veto; their publishes enter the bus as ordinary new events. Frontends, telemetry, and memory-writers live here. Asyncs may still be running when later events process; their outputs take log order at arrival.
- **Event families:** a plugin written before its event names can be known declares a family emit (`tool-call/*`); the compiler resolves it into the concrete members the definition's subscribers name, so the compiled graph holds only concrete events. This is how tool calls reach their tool: each tool plugin subscribes to its own member (`tool-call/read_file`), the model is offered exactly those tools, and the emitter checks the model's chosen name against its resolved emit set before publishing — an unknown name is answered as an error, never published. A member's subscribers are ordinary subscribers; there is no single-handler rule. A failed invocation surfaces as the core `invocation-failed` event. See [compiler.md](compiler.md) §3.4.
- **Wildcards:** a subscriber may consume `*` (blocking ones take their position from the definition's phases). The compiler expands wildcards against actual emitters: a wildcard subscriber never receives its own plugin's emits, nor events emitted by any wildcard subscriber — so wildcard feedback loops cannot exist. Emits are declared events or declared families, never open wildcards.

Connectivity is judged per plugin: a plugin none of whose subscriptions can ever fire is a compile error; a single unfed subscription is a warning, as are a plugin with no effect (unless marked `sink`) and an unreachable island. Requiring a key some emitter does not supply (a **starved consumer**) is an error. Emitting an event nobody consumes is legal — the log is the universal subscriber — and supplying keys nobody requests is normal.

## Ordering and control

Per session, dispatchable traffic flows in **two FIFO lanes**: an interrupt lane and a normal lane. The lane is chosen per publish, from the lanes the emit declares (the interrupt lane requires the `interrupt` capability): a `user-message` on the interrupt lane steers, the same event on the normal lane queues. The interrupt lane never interrupts a running pipeline — it wins only the *what's next* decision, draining fully before the normal lane resumes. The lane affects when an event runs, never how it is handled.

**Dispatcher commands** — soft stop, hard stop, resume — are not events and never queue. They are edge-triggered transitions of the session's dispatch state, applied the instant they arrive and recorded as history. Soft stop drains in-flight work then parks; hard stop kills in-flight invocations now — the only thing in the system that interrupts a running pipeline. The litmus: anything that must take effect while a pipeline runs cannot be an event.

## The log

Each session **is** its append-only log: typed records (event appended, invocation started, invocation completed with outputs, context contribution, config change, dispatcher command, spill pointer) under monotonic sequence numbers. Oversized payloads spill to content-addressed immutable files referenced by hash. Cross-session interaction is mirrored events referencing the peer session and sequence. Above the logs sits a derived, rebuildable index — the session table and event index; its loss is never data loss.

**The record projects into a tree of tables.** Every record is fully keyed — `session_id`, plus `event_id` and `invocation_id` where applicable — so the flat stream projects losslessly into a navigable hierarchy: the session table maps a session to its event ids (plus definition, dispatch state, watermark); an event's table holds its envelope and its invocation ids in pipeline order; an invocation's table holds start/completion, input and output state, context contributions, published event ids, and binary/config stamps. Published-event ids link forward (fan-out); `cause` links backward (ancestry) — every id is a lookup, every lookup points onward. The flat stream is the write model, for appending, recovery, and replay; the tree is the derived read model, for humans, frontends, and inspection — rebuildable from the streams in one pass.

**Recovery is one mechanism used everywhere.** Completed invocations never re-run, and a settled invocation's output set is closed — late arrivals queue as deferred publishes, never retroactively join old records. A crash triggers an orphan scan — invocation-starts without completions — bounded by the active-set index and per-session settled-watermarks, so dormant sessions cost nothing. Orphans restart; recorded outputs stand in for everything else. Hot recompiles, hard-stop resumes, and crash recovery are all this same scan. Replay-as-reading runs no plugins at all.

## Configuration

Configuration is **live, layered, and stamped**. Definition files may be shared, but compiled **revisions are per session**. Every `start-session` compiles its definition first; a failed compile means the session never starts and the diagnostics return to the caller. Apply targets specific running sessions: the compiler builds the candidate from scratch, rejects it whole on any error (those sessions keep their revision), and on success dispatch pauses at the next event boundary and the session resumes via standard recovery. Every invocation record stamps the revision, the plugin's binary hash and config hash, and the apply is itself a recorded event — nothing silent, everything attributable. Replaying into history whose stamps differ from current config is a surfaced choice, and the choice is recorded.

## Frontends

A frontend is an ordinary plugin at the human boundary: a watcher of the sessions it renders (reading through the `observe` capability, off the per-event path) or an async subscriber where it needs to take part, a publisher of user-action events (deferred publishes citing the session's standing invocations, each on the lane the user chose — steer or queue; the first message rides `start-session`), and a sender of dispatcher commands — the stop button reaches the dispatcher without traversing the pipeline it is stopping. Plugins may attach **UI hint payloads** (a small versioned vocabulary: text, diff, form, choice, progress) to their events; token-by-token model output is *not* an event: providers send normalized frames to the runtime's live stream subsystem, which delivers coalesced frames to frontends outside the pipeline and the log, while the provider still publishes one canonical response per turn ([hot-paths.md](hot-paths.md)); each frontend renders what it understands and falls back to raw payload display. Terminal, web, and ACP-bridge frontends are peer plugins over one vocabulary; the runtime knows nothing about rendering.

## Performance and reliability posture

The event path — lane pop, dispatch, log append, IPC round-trip — is the entire core hot path, engineered to the ≤ ~10 ms budget at 100 concurrent sessions; plugin execution time is outside it. Long-running processes amortize spawn cost to zero; local IPC round-trips are ~100 µs. The core stays small enough to verify exhaustively: deterministic, idempotent, every branch testable — the reliability target is met by keeping the core nearly featureless.

## Subcomponent designs

This page's ideas recurse into child designs. Completed: [compiler.md](compiler.md) — manifests, typed shapes, and the compiled graph. Remaining: wire protocol and plugin lifecycle; publishing classification and citation validation; plugin state survival patterns; log format, indexing, and recovery mechanics; lanes, dispatch, and the control plane (incl. the core lifecycle event set); frontend UI-hint vocabulary; session definitions and config layering; performance verification.
