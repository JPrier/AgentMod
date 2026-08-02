# Immutable Node-Execution Plan

The node-execution plan is the immutable contract that fixes exactly how every
compiled graph node of a session will execute. It is resolved once from the
live node-executor registry at session creation (and at branch-with-style
creation), normalized deterministically, hash-bound to the session style and
compiled graph, and persisted with the session. A later runtime must never
reselect a different executor, silently upgrade a version, substitute the
native executor for a missing plugin executor, or drop a now-unavailable node.

```text
compiled style + live registry
        -> exact resolved execution plan
        -> immutable hash-bound session contract
        -> exact validation on restart
```

## Layer ownership

The plan follows the runtime `service -> logic -> data -> dependency` boundary:

- **service**: maps plan inspection endpoints only when an endpoint is added;
  it never contains plan decisions.
- **logic** (`apps/runtime/logic/src/execution_plan.rs`): owns compatibility
  and migration decisions. It derives the plan identity from the immutable
  `SessionStyleBinding`, validates the persisted plan file against that
  identity, classifies legacy sessions with typed "migration required"
  outcomes, and produces the pure inspection projection. Live-registry
  revalidation stays in `node_executor`.
- **data** (`apps/runtime/data/src/execution_plan.rs`): owns normalized plan
  records (`ExecutionPlanIdentityData`, `ExecutionPlanFileData`) and the
  selection of the plan-file persistence dependency. It builds the canonical
  checksummed payload and validates the loaded payload's structure.
- **dependency** (`apps/runtime/dependency/src/execution_plan.rs`): owns the
  external storage representation: one bounded, checksummed, atomically
  written `execution-plan.json` envelope per session. It validates only the
  envelope schema and BLAKE3 payload checksum; plan contents are opaque bytes.

Persistence structs are never passed into logic unchanged; each boundary maps
its own record types.

## Retained identity

The persisted plan binds, at minimum:

- execution-plan schema version (record `schema_version` and the frozen plan
  compiler identity `compiler`);
- style ID and exact style version;
- style content hash;
- compiled-style hash and compiled cache key;
- runtime API version;
- plugin-set hash and capability-set hash;
- node-executor registry hash;
- for every compiled node: node ID, serialized node kind, executor ID, exact
  executor version, executor source (`runtime` or exact plugin identity),
  execution boundary, executor runtime API requirement, node-required
  capabilities, selected executor capabilities, and executor configuration
  reference (the plan hash covers the canonical plan bytes);
- the complete execution-plan content hash.

External SDK types and live trait objects are never persisted.

## Persistence

At session creation, branch-with-style, and runtime-managed child creation,
logic resolves the compiled graph against the live registry
(`bind_runtime_execution_plan`), requires an executable report, normalizes the
resolution deterministically (nodes sorted by ID, registry hashed over the
sorted capability set), and computes the plan hash. The exact plan is
persisted in the canonical style binding (which travels through
`session.created` evidence) **and** as a dedicated checksummed
`execution-plan.json` envelope staged atomically inside the temporary session
directory before the final rename — a partially written plan can never leave a
valid-looking session.

The plan file payload is canonical JSON containing the schema version, the
identity record, and the exact plan JSON retained as a string so the plan
content hash survives the round trip byte-for-byte.

## Restart validation

On session load/resume, `validate_session_resume_plan` (or the strict
`validate_persisted_execution_plan` used by inspection/migration tooling):

1. reads and checksum-validates the persisted plan file;
2. verifies every style/compiled identity field (style ID/version/content
   hash, compiled hash/cache key, runtime API, plugin-set hash,
   capability-set hash, registry hash, plan hash, node count);
3. compares the canonical plan bytes with the binding plan;
4. reconstructs the live registry;
5. verifies every exact selected executor still exists, with exact version,
   source/plugin identity, boundary, capability set, and runtime API
   compatibility (`revalidate_runtime_execution_plan`);
6. fails closed with stable diagnostics when any exact identity changed.

The runtime never selects a newer or different compatible executor, never
removes a now-unavailable node, never substitutes the native executor for a
missing plugin executor, and never upgrades an old plan.

## Migration and branching

- A session without a plan file (or without a binding plan) is a typed
  "migration required" outcome; the runtime fails closed rather than
  executing on a reselected plan.
- `branch_with_recompiled_style` creates a new child with a newly resolved
  plan and its own plan file; the parent session and its plan file are left
  byte-for-byte unchanged.
- Deliberate migration tooling can inspect the plan file and use the typed
  `ExecutionPlanMigrationDiagnostic`; there is no unsafe in-place mutation.

## Replay and inspection

`inspect_execution_plan_file` reconstructs the plan identity projection
(plan hash, registry hash, node-to-executor mapping with exact versions,
sources, and boundaries) purely from canonical/session files. It performs no
live registry lookup and dispatches no effects. A separate
`availability_projection` pass reports per-node availability/compatibility
state against a caller-supplied live capability snapshot without altering the
pure projection.

## Failure and corruption behavior

Missing plan file, truncated file, invalid checksum, unknown schema version,
changed style hash, changed compiled graph, changed registry hash, missing
executor, changed executor version, changed capabilities, changed plugin
identity, duplicate node resolution, an extra live-registry registration that
must not alter the existing selection, and atomic branch failure are all
covered by dependency, data, logic, and integration tests. Corrupt files fail
closed with stable `EPLAN-3xx`/`EPLAN-4xx` diagnostics.
