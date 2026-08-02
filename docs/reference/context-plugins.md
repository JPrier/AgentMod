# Plugin Context Ports

The runtime exposes stable, bounded ports for plugin-provided context
behavior. These ports are typed contracts owned by runtime logic
(`agentmod_runtime_logic::context_ports`). Plugin-host transport adapts these
ports in a later workstream; this document describes the contracts and the
runtime-side validation guarantees.

## Plugin memory

`PluginMemoryPort` is the transport-free contract a plugin memory provider
implements:

- `retrieve` — bounded retrieval with full provenance (reference, content,
  score, creation time, byte size).
- `propose_write` — proposes one write; the runtime evaluates the proposal
  through the normal interceptor and policy chain before commit.
- `commit_write` — commits or rejects a previously proposed write and returns
  a terminal provider receipt (reference, retained, deduplicated).
- `health` — provider availability and a safe diagnostic label.
- `supported_scopes` — deterministic scope-key list.
- `bounds` — hard item/byte/query limits the provider enforces.

Every write still follows the canonical
`proposal -> interceptors -> user policy -> mandatory policy -> dispatch
evidence -> memory provider -> canonical reference` path. The model can never
write directly to memory storage.

## Plugin compaction

`PluginCompactionPort` lets a plugin propose a bounded replacement projection:

- `propose_replacement_projection` — returns a typed plan (replacement
  entries, provenance, artifact references, preserved-state declarations).
- `report_source_range_hash` — reports the exact compacted range and hash.
- `provide_artifacts` — serves immutable artifacts for an optional range.
- `declare_preserved_state` — declares which required runtime state the plan
  retains.

## Context transforms

`ContextTransformBoundary` names the lifecycle points where a plugin
transform may run:

- `before_memory_retrieval`
- `after_memory_retrieval`
- `before_compaction`
- `after_compaction`
- `before_provider_projection`
- `before_turn_completion`

## Runtime validation

`validate_plugin_context_effect` enforces that a plugin cannot:

- mutate canonical history (only the replaceable provider projection may be
  proposed);
- change session/style/workspace identity;
- fabricate user roles (`UserMessage`/`UserInstruction` entries not already in
  the projection are rejected);
- expose undeclared secrets (declared-secret scan);
- remove required pending state (`pending_control_state`, `current_input`,
  artifact references, memory provenance, active graph state, tool-call
  correlation);
- exceed configured context byte/token limits;
- produce duplicate projection entries.

`validate_plugin_memory_write` enforces identity, content-hash consistency,
byte bounds, and the same declared-secret scan for proposed memory writes.
