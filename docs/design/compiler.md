# AgentMod Compiler: manifests, shapes, and the compiled graph

Status: Draft for Josh's approval.

Decision ticket: [Subdesign: compiler — manifest validation and demand/supply matching](https://github.com/JPrier/AgentMod/issues/9). Parent: [AgentMod realignment map](https://github.com/JPrier/AgentMod/issues/1). Builds on the [High-Level Design](high-level-design.md) and is tested against the requirements in the [problem statement](problem-statement.md). Terms are defined in [GLOSSARY.md](../../GLOSSARY.md).

Where this document changed decisions in the High-Level Design, the HLD has been updated to match; [§12](#12-changes-to-the-high-level-design) lists what changed.

## 1. Posture

The compiler is modelled on `rustc`. What a definition says is what happens: there are no unknown side effects, and anything ambiguous is an error rather than a silent default. Where the compiler cannot decide something (most often ordering), the author must write it down, and only there.

The compiler is part of the core, not a plugin. It is deterministic: the same manifests, definition and runtime version always produce the same compiled graph and the same diagnostics, byte for byte. Plugins may be nondeterministic. Routing never is.

Compilation proves that **declarations are consistent with each other**. The runtime proves that **behaviour matches the declarations**. Nobody proves liveness: whether an event arrives, whether a loop ends, or whether a plugin's output means anything. [§10](#10-guarantee-matrix) sets out exactly which guarantee belongs to which party.

## 2. Inputs

- **Plugin manifests.** Each plugin hands the runtime its manifest at handshake ([§3](#3-the-manifest)).
- **The core manifest.** This is a built-in manifest, versioned with the runtime, that declares every event the runtime itself emits: `session-started`, the stop and lifecycle events, `dispatch-failed`, and inbound cross-session events. The compiler treats it exactly like a plugin's manifest. The runtime is the graph's **boundary node**.
- **One flattened session definition.** Definitions may be composed from reusable fragments. That composition is the session-definitions subdesign's job. The compiler always sees the fully expanded result, and every diagnostic traces back to the fragment and line that produced it. A definition names the runtime version it targets, and that version selects which core manifest the compiler loads.

## 3. The manifest

### 3.1 Shapes

Every declared key has a **shape** over JSON values:

- `string`, `integer`, `number`, `bool`, `null`
- `array<T>`
- `object` with nested keys
- literal enums such as `"ok" | "error"`
- unions

A declared key is required unless it is marked `optional = true`. Shapes are **structural**. There are no shared type declarations, no owning plugin for an event type, and no dependencies between plugins. An emitter declares the shape it sends, a subscriber declares the shape it needs, and the compiler matches the two. SDKs may generate native types from shapes.

### 3.2 Subscriptions

```toml
[[subscribe]]
event = "model-request"
mode  = "blocking"                    # or "async"
actions = ["contribute-context"]      # runtime calls scoped to this event (names provisional)
unlisted_keys = false                 # true → also receive every undeclared key, untyped
  [subscribe.receives]                # what it needs: the demand
  model    = { shape = "string" }
  messages = { shape = "array<object>" }
  [subscribe.receives.slots]
  context  = { shape = "object" }     # session slots are requested like keys
  [subscribe.returns]                 # blocking only: what it hands back to the chain
  temperature = { shape = "number" }
  messages    = { shape = "array<object>", optional = true }
```

- **Delivery is projected.** A subscriber receives the envelope (`event_id`, `session_id`, `event_name`, `sequence`, `cause`, `lane`, `arrived_at`) plus exactly the keys and slots it declared. Optional keys arrive only when they are present. With `unlisted_keys`, it also receives every other key, untyped and outside the compiler's guarantee. Every `unlisted_keys` use is listed in the compile report.
- **Returning keys.** A blocking subscriber's `returns` uses the same format as an emit. Returning a key that already exists overwrites it, and the value must keep the key's shape. Returning a new key adds it. Keys are never removed. A subscriber that declared `unlisted_keys` may rewrite unlisted values, but may not change their JSON kind.
- **Async subscribers** have no `returns` and cannot veto.
- **`actions`.** These are runtime calls scoped to one event's pipeline. `veto` and `contribute-context` are placeholder names: their naming and semantics are open and belong to other subdesigns.

### 3.3 Emits and event families

```toml
[[emit]]
event = "context-trimmed"
lane  = "normal"                      # "interrupt" requires the interrupt capability
deferred = false                      # may be published without an open invocation
  [emit.keys]
  removed = { shape = "integer" }

[[emit]]
event = "tool-call/*"                 # a family: members are chosen at runtime
exactly_one_handler = true
  [emit.keys]
  call_id = { shape = "string" }
  args    = { shape = "object" }
```

Event names may be structured as `family/member`. An emitter may declare a **family** when the member names are chosen at runtime, as with tool calls named by the model. Subscribers may subscribe to a single member (`tool-call/read_file`) or to the whole family (`tool-call/*`). A family marked `exactly_one_handler` must have exactly one subscriber that handles each member. Ordinary family observers, such as a policy gate, a frontend or telemetry, still receive every member. Sub-families nest (`tool-call/mcp__github__*`).

### 3.4 Plugin-wide declarations

- **`capabilities`** lists plugin-wide runtime powers: `deferred-publish`, `start-session`, `cross-session`, `interrupt`, the queue methods (list pending, cancel), and the session directory (`sessions.find`, possibly `sessions.read`). Any call that isn't declared is declined and recorded.
- **`outbound`** lists cross-session events the plugin may send, with their shapes. It requires `cross-session`.
- **`sink = true`** marks a plugin whose effect lies outside the bus, such as a frontend or a log exporter. Such a plugin is exempt from the no-effect warning.

## 4. The session definition (compiler-facing parts)

- **Plugins.** The definition lists the plugins it runs and their config. It never lists edges or pipelines; those are compiler output.
- **Phases.** The definition gives an ordered list of phase names and assigns every blocking subscriber a phase, either per plugin or overridden per event. A phase is only a name and has no meaning to the runtime.
- **Tie-breaks.** For each event, a definition may give an explicit order inside a phase. It must do so wherever the compiler reports an observable tie ([§6](#6-checks)).
- **Handler picks.** Where two plugins could handle the same member of an `exactly_one_handler` family, the definition names the winner.
- **`inbound`.** These are the events the session accepts from other sessions, with their shapes. They reach the session as emits of the core boundary node.
- **Slots.** These are named, typed items of session state that plugins read through `receives` and write through `returns`. `context` is a default slot that a definition may replace. An embedded workflow gets its own namespaced slots unless the definition maps them to the parent's. How slot writes combine (append or replace, and how context is assembled) is decided elsewhere.
- **Embedded workflows.** A definition with a declared receive/emit interface may be used as a plugin inside another definition. The compiler inlines it under a namespace (`research/model-request`), derives its manifest, and checks its interface like any other plugin's.
- **`deny_warnings = true`** promotes warnings to errors.

## 5. The compiled graph

The canonical output is one **directed graph**, and it is also the table the runtime routes from. Plugins and the boundary node are its nodes. Each edge reads *when A emits X, B is a destination*, and carries:

- **mode**: `blocking` or `async`
- **position**: phase, plus tie-break position, for blocking edges
- **lane**: normal or interrupt
- **supply**: the keys and shapes that will be present when the event reaches B

The event-bus pipelines are flattened into this graph. For one event, its blocking edges run one at a time in phase order. Each subscriber sees the event with all earlier `returns` applied. When the chain settles, all async edges start together, and the pipeline counts as complete at that point. A session dispatches one event at a time. Async results come back as new queued events.

The graph is **not a DAG**: agent loops are cycles. Fan-out to several async destinations is concurrent. Fan-in is a stateful plugin's job; the core has no join primitive.

### 5.1 How it is built

1. **Resolve.** Load the core manifest for the target runtime version and the manifests of every plugin listed, and inline any embedded workflows.
2. **Expand.** Expand wildcard and family subscriptions against the actual emitters, taking the emitter into account:
   - A wildcard subscriber never receives its own plugin's emits.
   - A wildcard subscriber never receives events emitted by any wildcard subscriber.

   So a `N --foo--> W2` edge exists while a `W1 --foo--> W2` edge does not, even though both carry `foo`.
3. **Order.** Place blocking edges by phase and tie-break, then check for observable ties.
4. **Supply walk.** For every emitter and consumer pair, walk the emitter's declared keys through each earlier blocking `returns` in the chain, then check the consumer's `receives` against what has accumulated.
5. **Analyse.** Find cycles (strongly connected components), reachability from roots, connectivity, and the holders of each capability.
6. **Hash.** Emit the graph and its diagnostics under a deterministic content hash, the **revision**.

## 6. Checks

| Check | Level |
|---|---|
| A subscriber's required key is missing from some emitter's supply, or a key's shape is incompatible. Checked **per emitter**: every emitter of the event must satisfy every consumer. | Error |
| A plugin emits and subscribes to the same event name, including through a family. | Error |
| A blocking subscriber has no phase. | Error |
| An observable tie inside a phase: one subscriber returns a key the other receives or returns, both contribute context, or either can veto. | Error, naming the pair, the event and the reason |
| Two fragments supply conflicting phase lists. | Error |
| A member of an `exactly_one_handler` family has two possible handlers and the definition picks none. | Error |
| A `returns` adds a key that some emitter of that event already supplies with a different shape. | Error |
| An action or capability is used without being declared, an interrupt-lane emit lacks `interrupt`, or an outbound event lacks `cross-session`. | Error |
| A plugin never fires (none of its subscriptions is fed). | Error |
| An unfed subscription (an individual event that no plugin emits). | Warning |
| A plugin has no effect: no consumed emits, no `returns`, no actions, and not a `sink`. | Warning |
| An unreachable island: plugins feed each other, but no root can reach them. | Warning |
| A cycle consisting only of interrupt edges (a starvation risk). | Warning |
| An unconsumed emit, or keys supplied that nobody requests. | Not reported (the log is the universal subscriber) |
| Cycles, capability holders, the interrupt section, `unlisted_keys` users, and the external surface. | Info |

**Roots**, for reachability, are:

- core lifecycle events and `start-session` initial events
- `inbound` events
- emits flagged `deferred`

**Diagnostics** name the definition, with the fragment and line it came from, plus the event, the key path, and the plugins involved. Warnings are recorded on the revision, and with `deny_warnings` they are errors.

## 7. Runtime enforcement

The runtime checks every invocation against the compiled declarations, under one rule: **anything that contradicts a declaration is a hard failure; anything the declaration is silent about passes.**

- **Hard failures:**
  - a required key is missing
  - any declared key arrives with the wrong shape
  - a required key from `returns` is missing
  - a return changes a key's kind or removes a key
  - an emit of an event that isn't declared (it has no edge, so it could not route anyway)
  - a call to an undeclared action or capability
- **Silence passes.** Extra undeclared keys travel untyped, and only `unlisted_keys` subscribers see them.
- **Cross-session publishes.** These are paired at runtime, against the target's compiled `inbound` declaration. A mismatch, or a target that doesn't accept the event, fails the publish with a recorded error returned to the sender. It does not stop the sender's session, because choosing the wrong target is a legitimate runtime outcome.
- **Unhandled family members.** A member of an `exactly_one_handler` family that nothing handles, or whose handler fails without answering, becomes the core `dispatch-failed` event.
- **Records.** Every runtime failure is recorded with: the session, the revision, the event and invocation ids, the plugin and its stamp (binary and config hash), **the exact compiled declaration it violated**, and the value actually observed. What the runtime then *does*, whether pausing, alerting or something else, is control-plane design.

## 8. Cycles and termination

Cycles are legal and are reported as information. The no-self-subscription rule and the wildcard expansion rules rule out the accidental, degenerate loops: one plugin re-triggering itself, and wildcard plugins feeding each other. Termination is **an explicit non-guarantee**. A cycle with no edge leading out of it is not proof that it runs forever, because any plugin may simply emit nothing. Loop limits are plugin policy, such as a stateful watchdog that emits a stop request when progress stalls. A hard stop is an operator's emergency control and doesn't count as a structural exit. The core has no recursion or depth limit.

## 9. Revisions and apply

- **Every start compiles.** Each `start-session` compiles its definition immediately before running. A failed compile means the session is never created, and the diagnostics are returned to the caller as an event, so an orchestrating agent can read and fix them.
- **Applying a change.** An apply targets specific running sessions. The definition file may be shared, but revisions belong to each session. The compiler builds the candidate from scratch. Any error rejects the whole apply, and those sessions keep their current revision. Success records the new revision in each session's log, and from then on every invocation stamps the revision it ran under.
- **Caching.** Identical inputs produce an identical revision, so a cached result may be reused. This is an optimisation only and never changes behaviour.
- **Timing.** When a new revision takes effect for events that are already queued or in flight belongs to the control-plane subdesign.

## 10. Guarantee matrix

Each property is assigned to the party that is responsible for it. The rubric IDs refer to the [problem statement](problem-statement.md).

| Property | Compile time | Runtime | Plugin contract | Non-guarantee | Rubric |
|---|---|---|---|---|---|
| Every consumed key is supplied, with a compatible shape, on every edge | ✔ | ✔ actual payloads match the declarations | | | C8 |
| Delivered data matches what was declared (projection) | | ✔ | | | C1, C8 |
| Blocking order is fully determined wherever it is observable | ✔ | ✔ the order is executed exactly | | | C2 |
| A plugin can do only what it declared (actions, capabilities, emits, returns) | ✔ capabilities granted | ✔ every call checked | | | C8 |
| No plugin consumes its own emits; no wildcard-to-wildcard feedback | ✔ | ✔ routes only along compiled edges | | | |
| Each member of a handled family has exactly one handler | ✔ | ✔ unhandled → `dispatch-failed` | | | |
| Cross-session events match what the target accepts | ✔ each side separately | ✔ paired at publish time | | | C5 |
| Interrupt use is explicit and visible | ✔ | ✔ | | Whether a given interrupt use is *justified* | C7 |
| A configuration that fails to compile never runs, and the failure leaves current sessions untouched | ✔ | ✔ | | | C6 |
| Every runtime failure traces back to its declaration | ✔ declarations identifiable | ✔ recorded | | | C3 |
| Plugins handle each event idempotently and survive restarts | | | ✔ | | |
| Untyped (`unlisted_keys`) data is used correctly | | | ✔ | | |
| An event will eventually be emitted or answered (liveness) | | | | ✔ (only reachability is reported) | |
| A loop terminates | | | | ✔ (cycles are reported) | |
| Freedom from deadlock or starvation | | | | ✔ (interrupt-only cycles warned) | |
| Payload *values* are meaningful or correct (e.g. a model's tool arguments) | | Shape only | ✔ | ✔ semantics | |

## 11. Examples

**Accepted: chat loop.**
- `chat-context` emits `model-request`; `openrouter` consumes it and emits `model-response`.
- `chat-context` consumes `model-response` and emits the `tool-call/*` family.
- `file-tools` handles `tool-call/read_file` and emits `tool-result`.
- `chat-context` consumes `tool-result` and emits `model-request`.

This compiles. The cycle `model-request → model-response → tool-call/* → tool-result → model-request` is reported as information. `web-ui` subscribes to the whole family as an async `sink`.

**Rejected: an emitter that falls short.** `shell` emits `tool-result {content, exit_code}`, `mcp-bridge` emits `tool-result {content}`, and `retry` requires `exit_code`. The error names the `mcp-bridge → retry` edge and the missing `exit_code` key. The fixes: make `exit_code` optional in `retry`, supply it from `mcp-bridge`, or use a separate event name.

**Rejected: an observable tie.** `policy-gate` and `redactor` are both in phase `guard` for `tool-call/*`. Both can veto, so the error asks for a tie-break: `order.tool-call.guard = ["policy-gate", "redactor"]`. If neither could veto and neither touched the other's keys, compilation would succeed, with a fixed order shown in the report.

**Rejected: self-subscription.** `summariser` consumes `context-updated` and emits `context-updated`. To fix it, emit a different event (`summary-ready`) and let another plugin react to it.

**Accepted, then failed at runtime.** `chat-context` declares `returns.messages` as `array<object>` but returns a string. This is a hard failure, recorded with the `returns` declaration it violated.

**Accepted, with a warning.** `ask-parent` subscribes to `answer`, and nothing in this definition emits it and no `inbound` declares it. The warning names the unfed subscription. Declaring `inbound answer {...}` in the definition removes it.

**Rejected: dynamic definition.** An orchestrator calls `start-session` with a definition it wrote moments earlier, and that definition has a starved consumer. No session is created, and the orchestrator receives the diagnostics as an event.

## 12. Changes to the High-Level Design

The High-Level Design has been updated in place for each of these; this list is the record of what changed and why.

1. **Payload contracts are typed shapes.** The HLD's "demand/supply, no schemas" becomes structural **shapes**. There is still no central registry, no event ownership and no dependencies between plugins, but every declared key has a type, and its optional "type tags" become mandatory shapes.
2. **The transform model is `receives` and `returns`.** Transforms are not "preserves/adds" lists: blocking subscribers declare `returns`, which are applied sequentially. Keys are never removed.
3. **Recursion is analysed, not bounded.** The HLD's "recursion bounded" becomes: self-subscription forbidden, wildcard feedback impossible by construction, cycles reported, and no core depth limit. The prototype's `max_causal_depth` has no place in the end state.
4. **The graph is the routing table.** It is not only a validation artifact. Pipelines are its per-event view.
5. **Event families exist.** The HLD forbids emit wildcards; this design allows declared families instead, which replace keyed owners (`keyed:` and the prototype's `ownTools`) for tool routing.
6. **Wildcard emitters are allowed,** under the expansion rules.
7. **Dead listeners are relaxed.** A dead listener is an error only at the plugin level (the plugin never fires). An individual unfed subscription is a warning.
8. **Order comes from the definition,** through phases. Plugins never declare their own order.
9. **Context is a slot.** Context is one of the definition's typed slots, delivered only on request, rather than an implicit part of every envelope.
10. **Revisions are per session,** and every start or apply compiles first.

## 13. Deferred to other work

- **Steering, cancellation, interrupt semantics and pipeline completion under in-flight work.** Worked through the handoff scenario, in the control plane.
- **How slot writes combine,** and how context is assembled.
- **The session directory API:** naming, labels and lookup.
- **Naming and semantics of `veto` and `contribute-context`.**
- **Trust for compiler extensions** (optional policy passes).
- **Compiler resilience:** malformed manifests, resource exhaustion, IR versioning, and the window between compiling and activating.
- **When a new revision takes effect** for events that are queued or in flight.
