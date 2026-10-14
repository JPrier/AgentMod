# AgentMod

AgentMod is an agent runtime. Plugins declare what they consume and emit, and session definitions select those plugins. A compiler turns these into one validated graph that the runtime executes deterministically.

## Definitions and compilation

**Session definition**:
The configuration a session runs: which plugins it uses, their config, phases, slots, and inbound events. It never lists edges.
_Avoid_: workflow graph, pipeline config

**Manifest**:
A plugin's complete declaration of what it receives, returns, emits, and may call. Anything it does beyond the manifest either is declined or is a failure.
_Avoid_: plugin schema, plugin spec

**Core manifest**:
The built-in, versioned manifest that declares the events the runtime itself emits.

**Compiled graph**:
The directed graph of plugins and event edges that the compiler derives from a session definition. The runtime routes from it.
_Avoid_: workflow graph, DAG

**Revision**:
The content hash identifying one compiled graph of one session. It is stamped on every invocation that runs under it.
_Avoid_: graph generation, version

**Boundary node**:
The runtime's own node in a compiled graph: the source of core and inbound events, and the destination of outbound events.

## Events and shapes

**Shape**:
The structural, typed description of a key's JSON value. Keys are required unless marked optional.
_Avoid_: schema, type

**Envelope**:
The fixed metadata every delivered event carries, whatever the subscriber declared.

**Event family**:
A declared group of event names `family/member`, such as `tool-call/*`, that the compiler resolves into the concrete members a definition's subscribers name.
_Avoid_: keyed owner, routed subscription, handler

**Resolved emit set**:
The concrete events a plugin may emit under one revision, after its family emits are resolved.

**Unlisted keys**:
The keys of an event a subscriber did not declare, received untyped only if the subscriber opts in.
_Avoid_: rest, opaque

**Inbound event**:
An event a session definition accepts from other sessions.

**Outbound event**:
An event a plugin may send to another session.

## Pipelines

**Blocking subscriber**:
A subscriber that runs in sequence in an event's chain, and may return keys to the subscribers after it.

**Async subscriber**:
A subscriber that runs concurrently once the blocking chain has settled. It cannot change the event.

**Phase**:
A named position in blocking order, declared by a session definition. It has no meaning to the runtime beyond its order.
_Avoid_: priority, stage

**Lane**:
The queue an event waits in: normal or interrupt. The publisher chooses it per publish, from the lanes its emit declares.
_Avoid_: priority

**Interrupt event**:
An event published on the interrupt lane, which is dispatched before ordinary queued work at the next pipeline boundary.
_Avoid_: priority event

**Steer / queue**:
The same user message published on the interrupt lane (steer) or the normal lane (queue).

**Watcher**:
A reader of a session's records from outside the bus. It is not a node in the compiled graph.

**Slot**:
A named, typed item of session state that plugins read and write through their declarations. Context is the default slot.
_Avoid_: global keys

**Embedded workflow**:
A session definition used as a single plugin inside another definition.
_Avoid_: sub-agent (as a runtime concept)

## Plugin processes

**Attempt**:
One dispatch of an invocation to a plugin process. An invocation interrupted by something other than its own failure is dispatched again as a new attempt, and every attempt after the first tells the plugin which attempt it is and which attempts came before.
_Avoid_: retry (as a noun for the record)

**Collateral invocation**:
An invocation stopped only because the process running it was killed for another invocation's sake.

**Process pool**:
The plugin processes that serve every session whose plugin has the same binary and the same process start config. Different versions of a plugin are different pools, running side by side.
_Avoid_: instance, worker

**Process start config**:
The config keys a plugin's author lists as sent once, when a process starts. Sessions that differ in any of them never share a process; every other key travels with each invocation.
_Avoid_: instance config, static config

**Plugin store**:
The runtime-owned set of installed plugin binaries, identified by binary hash. Name and version are labels from each binary's manifest; two binaries may share a label, and a reference to that label resolves to the most recently installed.
_Avoid_: registry, marketplace

**Deployment session**:
The session, started by the runtime at first boot, whose log records every change to the plugin store. It runs a deployment workflow.

**Deployment workflow**:
The replaceable session definition the deployment session runs: what happens when a plugin is requested, installed, or removed.
