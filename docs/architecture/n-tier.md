# N-Tier Architecture

Every deployable process uses `service -> logic -> data -> dependency`. A layer
may call only the layer immediately below it. Boundary mappings are owned by the
caller, and each layer defines separate request, result, and error types.

## Runtime

| Layer | Current responsibility and principal types |
|---|---|
| Service | Maps runtime protocol requests for health, styles, sessions, replay/branch, turns, streams, approvals, cancellation, continuations, and schedules into service-owned requests and responses. |
| Logic | Owns session-style selection and binding, graph interpretation, session and turn state, proposals, permission ordering, context composition, memory/compaction policy, replay, branching, continuations, scheduling, and recovery decisions. |
| Data | Normalizes style catalogs and caches, session and journal records, memory/context operations, artifacts, snapshots, action grants, continuations, receipts, schedules, and tool/provider routing before selecting dependencies. |
| Dependency | Owns filesystem and SQLite serialization, journal/snapshot/artifact stores, style manifest and cache files, process supervision, harness and tool-host protocols, plugin-host transport, memory adapters, and scheduler persistence. |

The composition root is `apps/runtime/bin`. It constructs concrete layers and
runs the authenticated local daemon, background scheduler recovery, and process
supervision. `agentmod-runtime-protocol` is the wire contract; protocol DTOs do
not enter runtime logic or data.

## Harness

| Layer | Current responsibility and principal types |
|---|---|
| Service | Maps harness health, provider execution, continuation, cancellation, stream, and recovery commands at the process boundary. |
| Logic | Owns provider-call lifecycle, proposal/continuation state, terminal receipt semantics, cancellation, and protocol-independent provider results. |
| Data | Normalizes provider requests, continuation records, stream fragments, usage, and terminal results before dependency dispatch. |
| Dependency | Owns provider SDKs and deterministic fixtures plus their external serialization and streaming behavior. |

The composition root is `apps/harness/bin`. Runtime and harness exchange only
the harness protocol; neither imports the other's internals.

## CLI

| Layer | Current responsibility and principal types |
|---|---|
| Service | Owns Clap parsing, command-specific service requests, rendering, output mode, and exit-code mapping. |
| Logic | Owns doctor, style, session, turn, replay/branch, approval, cancellation, and scheduling command decisions using CLI-owned types. |
| Data | Maps CLI logic operations into dependency requests and normalizes runtime availability and streamed results. |
| Dependency | Owns authenticated local runtime transport and runtime protocol DTO construction, plus deterministic fixtures used by layer tests. |

The composition root is `apps/cli/bin`. It assembles the layers and exposes the
implemented doctor, style, session, turn, replay/branch, approval, cancellation,
and scheduling commands.

## Allowed and prohibited dependencies

Allowed: service to logic and its endpoint protocol; logic to data; data to
dependency and stable core primitives; dependency to external APIs or outbound
protocols; composition root to its own four layers.

Prohibited: skipped or upward layer calls, protocol DTOs in logic/data,
cross-process internal imports, external SDKs above dependency, shared business
DTO crates, cross-layer aliases/re-exports, and lower-layer callbacks carrying
upper-layer types. `xtask architecture` enforces these rules.
