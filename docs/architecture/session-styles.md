# Session Styles

A session style is a selected, versioned execution contract. It is resolved and
compiled before the session is created, and its immutable binding is persisted
with the session. The binding includes the style identity and hashes, source,
runtime API and plugin/capability identities, graph cache key, harness
requirement, memory and compaction configuration, tool groups, approvals,
budgets, ordered plugin context-transform selections, child-agent policy, retry
policy, and termination policy. Restart and branch operations revalidate that
exact binding; the runtime does not silently substitute another style.

The runtime style registry follows the runtime service -> logic -> data ->
dependency boundary. It discovers built-in, user, project, and plugin-provided
records; supports list, inspect, validate, compile, disablement, and availability
diagnostics; and stores compiled styles in memory and in a persistent cache.
Validation and graph compilation remain owned by `agentmod-session-style-sdk`
and `agentmod-graph-engine`.

Session creation may explicitly select a memory provider, compaction strategy,
or any subset of the five hard execution budgets.
The SDK owns the manifest transform, including safe disabled/enabled lifecycle
controls; runtime data recompiles the transformed manifest through the ordinary
SDK validator. Logic restores the original source identity and binds the new
manifest, compiled descriptor, content hash, and cache key. Restart resolves
the exact base style, reapplies only the retained memory/compaction/budget selections,
recompiles, and compares the complete binding. Invalid or unavailable
components return SDK-derived diagnostics and are never silently replaced.

Budget transforms are SDK-owned. They update the style-wide ceilings and narrow
subordinate inline-graph, compaction, retry, and child-agent declarations before
normal compilation. Referenced graphs are not rewritten: a selected ceiling
below a referenced graph's declared budget is an explicit incompatibility.

## Immutable plugin context-transform selection

`context_transforms` is an ordered part of the compiled session-style contract,
not plugin discovery metadata. Every selected entry retains:

- exact plugin and transform IDs;
- exact semantic version and authoritative declaration hash;
- the `before_model_request` lifecycle; and
- the exact immutable configuration-reference hash.

Compilation verifies that the plugin is explicitly allowed by the style and
that exactly that transform declaration is available. The compiled vector is
persisted in the immutable style binding and its order becomes the runtime
phase order between memory and compaction. Restart and branch validation do not
select a newer compatible version, replace an unavailable transform, reorder
entries, or accept declaration-hash drift. Deliberate change requires a newly
compiled style binding.

## Immutable node-execution plan

Graph compilation and executor selection are separate contracts. After the SDK
compiles a graph, runtime logic resolves every compiled node through the single
composition-root node-executor registry and persists a `SessionExecutionPlan`
inside the immutable style binding. Each node record binds:

- the compiled node ID and serialized node kind;
- executor ID and version;
- runtime or exact plugin source;
- runtime-logic or plugin-host execution boundary;
- required and resolved capabilities;
- runtime API requirement;
- exact adapter-configuration reference; and
- the validated executor-declaration hash.

The plan also retains its compiler identity, the complete registry hash, and a
hash of the canonical plan. Plugin-backed records therefore bind both the
allowed plugin identity and the exact validated plugin executor declaration;
changing a declaration without changing its nominal version is still a
contract violation.

Session creation, branch creation, child-session creation, and turn preflight
all validate the retained plan. Restart reconstructs the live registry and
requires every exact selected implementation, version, capability set, runtime
API range, source, boundary, configuration reference, declaration hash,
registry hash, and plan hash to match. It never chooses a newer executor or a
different compatible executor. A missing legacy declaration hash or unavailable
selected implementation yields a stable migration diagnostic. Deliberate
movement requires a new branch/style compilation rather than mutation of the
existing binding.

`style.execution_initialized` copies the compiled graph, execution-plan and
registry hashes, exact node resolutions, initial node, immutable initial
variables, effective budgets, and run ID into canonical history. Replay reduces
that retained information without consulting the live registry, scheduler,
plugin host, harness, or tool hosts. Live component revalidation occurs before
new effects resume.

The generic style executor consumes the compiled SDK graph. It records
initialization, node entry, node completion or failure, and selected transitions
as canonical events and reconstructs its active node from replay. Current
`persistent-chat@1.2.0`, `ephemeral-turn@1.2.0`,
`research-loop@1.2.0`, `declarative-graph@1.2.0`, and arbitrary admitted user
graphs execute through this path while provider calls, tool proposals,
permission checks, receipts, continuations, artifacts, and recovery remain in
their existing runtime components. Planner-worker v1.4 also executes through
the generic path. Exact built-in `1.1.0` histories, including planner-worker
v1.1, retain explicit frozen versioned adapters.

Generic dispatch uses the exact executor ID, version, source, boundary, and
declaration/configuration hashes copied from the persisted plan. It does not
infer an executor from the style ID, node label, bundled fixture, or complete
graph topology. For execution-plan compiler generation
`agentmod-runtime-node-plan@3`, the dispatch mode itself is determined only by
that generation and the exact retained resolutions; adapter kind, variable
shape, and topology cannot trigger a legacy fallback. A generation-three plan
whose retained implementation has no supported exact generic handler fails
closed with `UnsupportedGenericExecutionPlan`. Adapter classification is
consulted only by generation `@2` historical bindings, which retain their
frozen replay behavior. For example, one arbitrary graph may deliberately
cross the runtime-logic and plugin-host boundaries:

```toml
format_version = 1
entry = "runtime_context"

[declarations]
capabilities = ["context", "model", "plugin.graph"]
providers = ["mock"]
plugins = ["fixture.node"]

[[nodes]]
id = "runtime_context"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "preserve_history" }

[[nodes]]
id = "renamed_plugin"
kind = "model_call"
provider = "mock"
required_capabilities = ["plugin.graph"]
configuration = { type = "plugin", plugin_id = "fixture.node", executor_id = "fixture.graph", executor_version = "1.0.0", node_kind = "model_call", input_schema = "fixture.graph.input", output_schema = "fixture.graph.output", configuration_reference = "fixture.graph.config", input = { value = "renamed" } }

[[nodes]]
id = "done"
kind = "complete_turn"

[[edges]]
from = "runtime_context"
to = "renamed_plugin"

[[edges]]
from = "renamed_plugin"
to = "done"
```

The first node resolves to
`runtime.context-construction@1.0.0`/`runtime_logic`; the second resolves to the
exact allowed `fixture.graph@1.0.0` declaration behind `plugin_host`. Runtime
logic still validates the plugin result, applies canonical variables and
budget charges, chooses only a declared transition, and commits the outcome.
The same exact plugin-host resolution may appear inside one admitted bounded
parallel region. Graph C process tests on Windows and Ubuntu/WSL2 prove its
validated action, persisted artifact, join propagation, correlated
cancellation, terminal receipt, and replay without redispatch. Nested parallel
regions remain unsupported.

The native tool namespace and provider aliases have one data-owned immutable
catalog. Its canonical hash is part of the current
`runtime.tool-gate@1.1.0` declaration identity, so adding or changing an alias
changes the executable ABI instead of silently changing an existing plan.
Historical `runtime.tool-gate@1.0.0` resolutions remain available for exact
restart validation. Provider aliases are normalized to canonical tool IDs
before allow-list validation, proposal commitment, approval continuation
creation, and host dispatch.

Style-selected context composition is live for persistent-chat and
ephemeral-turn compatible graphs. Memory retrieval, context replacement,
compaction, and ephemeral projection discard use the existing blocking proposal
pipeline and retain canonical provenance. Ephemeral turns build a fresh
provider projection from the current typed input plus only selected context,
then empty that projection before terminal node completion while preserving
canonical history. Exact context and graph events make restart recovery
fail-closed around both replacement boundaries. A graph whose selected context
implementation is unavailable or incompatible fails before a turn mutates the
journal.

Ordered plugin context transforms also execute in this generic context
boundary. Runtime logic drives each exact persisted ordinal through runtime
data and runtime dependency, then through plugin-host service → logic → data →
dependency into the isolated plugin process. The dependency boundary owns
keyed grants, exact invocation digests, nonces, timeouts, cancellation, bounded
protocol frames, and response audit validation. The isolated transform returns
a replacement proposal only.

Canonical events retain phase start, transform proposal, durable dispatch,
completed/failed/ambiguous terminal result, replacement approval, and the
runtime-owned projection replacement. Output deserialization alone is
insufficient: runtime logic validates the selected identity, declaration and
configuration hashes, output schema, typed entries, projection bounds,
preserved context, and replacement action. Mandatory policy is the final gate
and is revalidated before a previously approved replacement is applied after
restart.

The terminal plugin proposal is sealed into the generic durable plugin receipt
store before its canonical terminal event. Replay classifies a proposed
transform as safe to dispatch once; a dispatched transform waits for its exact
receipt; a completed transform awaits replacement authorization; and an
approved transform can apply without another plugin invocation. Missing or
mismatched receipts, unavailable or drifted declarations, and ambiguous
execution fail closed. Pure event reduction never queries the live plugin host.

The `artifact_handoff` compaction selection is a runtime-owned generic context
operation, not a style adapter. It writes the exact pre-compaction typed
projection through runtime logic → data → dependency, retains a canonical
proposal/approval/dispatch/receipt outbox and hash-only replacement approval,
and binds the stored object into both projection provenance and the replacement
event envelope. Approved/dispatched restart cuts reconcile without selecting a
different implementation or redispatching an ambiguous effect. Pure replay
does not inspect storage; live provider resumption verifies the retained object
first.

The memory profile's `turn_completion` write policy is live for approved
first-party file and SQLite providers. Runtime logic owns a canonical,
request-bound write outbox and revalidates mandatory policy before recovering
an approved or dispatched write. A dispatched file write is reconciled through
the same deterministic dependency identity rather than redispatched under a
new identity. Windows and Ubuntu/WSL2 process tests cover a kill after durable
file persistence and before the canonical terminal receipt.
`iteration_completion` is also live in runtime logic and is not inferred from
style identity or a topology profile. The reducer records each exact successful
loop transition, including its canonical sequence and checksum; a distinct
versioned write identity binds that boundary and bounded iteration-scoped
conversation, node-output, and artifact evidence. Focused Research tests and
dedicated Windows/Ubuntu process kill runs prove three exact writes,
first-write dispatched reconciliation, unavailable-harness restart without
provider redispatch, and byte-stable replay. Session-completion process cuts
also pass on both platforms. Native automatic-memory `ask` uses a distinct
continuation and exact action-digest approval subject; restart while pending,
approve-once, duplicate no-op, and denial-without-dispatch are process-proven on
Windows and Ubuntu/WSL2. Plugin continuation serialization remains unchanged.
Every retained automatic-memory entry carries a bounded runtime-owned
information-flow class. Common credential/path/URL/handle detection fails the
whole projection closed; broader semantic DLP remains a production limitation.

Planner-worker v1.4 executes its compiled plan through the same exact-executor
dispatcher as user graphs. Each worker is an atomically created child session
with a distinct runtime-owned `branch_workspace` lease and restricted immutable
style binding. Windows and Ubuntu/WSL2 process matrices hold both first-pass
workers concurrently at independent harness gates, release them in reverse task
order, and still integrate in canonical member order. Each child executes exact
filesystem edit, process test, and Git diff proposals once and persists its own
bounded result/diff/test artifacts. Integration and review consume exact
artifact references; structured rejection creates two bounded revision
children, the next review approves, and the parent workspace remains unchanged.
Daemon replacement is covered; the prior v1.3 matrix retains pure-replay
evidence. Exact v1.1 histories continue through their frozen compatibility
adapter.

Child-session MCP inheritance is separately controlled by the explicit,
style-wide `child_agents.inherit_mcp` policy. Omission or `false` binds the child
to an empty MCP configuration. When it is `true`, the runtime accepts only the
immediate parent's exact sanitized MCP binding and only when the parent binding
and child grant both include the `mcp` tool group. The child origin and immutable
style binding retain that exact selection; recovery requires the same parent
action, task, style, workspace lease, tool grant, and MCP binding. No compatible
substitution is selected. This immediate-parent rule does not imply transitive
or grandchild inheritance.

The Windows and Ubuntu/WSL2 child-MCP process fixtures use an immutable
`temporary_copy` workspace and the canonical task `invoke the inherited MCP
fixture`. They prove the explicit-true, omitted/default, and false policy paths,
exact recovery, and execute/restart/replay behavior. macOS was not run.

Workspace modes other than the proven branch-workspace/manual-review path and
the complete write-denial matrix are not process-tested. Arbitrary graphs are admitted by exact
per-node executor resolution plus graph, capability, parallel/recovery,
permission, and budget validation; admission does not require a bundled
topology classification. Unsupported nested parallel executor classes and graph
semantics still fail with structural diagnostics before a session is persisted.

Style-selected blocking plugin pipelines are now live for `action.proposed`
boundaries. The runtime activates only external plugins named by the immutable
compiled style, preserves compiled interceptor order, and routes typed
continue/replace/reject decisions back through mandatory policy and the normal
effect path. Style-allowed observer plugins receive matching committed events
asynchronously after durability. Activation and blocking invocation results are
canonical and replay-inspectable. Ordered, immutable
`before_model_request` plugin context transforms are separately live through
the context lifecycle described above. Plugin-provided memory retrieval and
compaction are also live when the immutable style selects their exact plugin,
component version, declaration hash, configuration reference, handler, schemas,
timeout, and bounds. Runtime logic separately authorizes invocation and
application, seals a terminal receipt before accepting output, validates the
bounded typed projection, and reduces an exact receipt after restart without
loading or redispatching the worker. Missing, corrupt, substituted, invalid,
timed-out, or ambiguous results fail closed.

An immutable style may also select an automatic plugin memory writer. Its
one-shot identity binds the exact plugin declaration and implementation,
handler, configuration reference, scope, typed value, and semantic request hash.
Durable Ask approval resumes that same operation; a sealed terminal receipt can
complete recovery without relaunch, while missing or invalid post-dispatch
evidence becomes ambiguous and fails closed.

Windows and Ubuntu/WSL2 process matrices cover ordered context replacement,
turn-start/context-node/before-model plugin memory retrieval, plugin compaction,
automatic plugin memory approval/restart/ambiguity/unavailability, invalid and
timeout rejection, duplicate suppression, and restart recovery. The remaining
context-product gaps are additional interceptor/transform lifecycle boundaries,
effectful context transforms, broader semantic DLP, and macOS process evidence—not
plugin memory retrieval, compaction, or automatic plugin/native memory-write
approval availability.

Harness selection is also part of the immutable binding. The runtime harness
registry follows the same service -> logic -> data -> dependency layering as
the style registry. Its injected dependency registry owns adapter descriptors
and routes approved execution by an explicit harness ID; logic owns
availability and capability-set validation. Session creation resolves either
the style's harness declaration or an explicit client override, verifies every
required capability, and persists the exact adapter version and capability-set
hash. Model proposals, action digests, grants, and canonical request events
retain that ID. Restart reports incompatibility instead of silently choosing
the native adapter.

The production composition root currently registers `native` and `fixture`.
Both use independently supervised process adapters; the fixture uses the
credential-free deterministic harness executable unless
`AGENTMOD_FIXTURE_HARNESS_PROGRAM` selects another fixture binary. It
intentionally omits image support so negative negotiation is deterministic.
This is the adapter seam for future harnesses, not a claim that third-party
Pi, OpenCode, Claude Code, or Codex adapters are complete.

Session inspection includes a logic-owned `style_introspection` projection
derived only from the immutable binding and replay state. It presents the
compiled graph, current control/active node, prior outcomes and transitions,
known next transitions, conditional candidates, loop/retry counts, remaining
canonical step/token/iteration budgets, pipeline activity, memory provenance,
compaction boundaries, child/join/reviewer state, and termination. Inspection
never invokes external components. When replay contains the canonical typed
variable projection, inspection evaluates the retained compiled condition AST
and reports each edge as eligible, ineligible, missing input, or invalid
expression. It also exposes bounded declaration metadata, versions, value
hashes, and security classifications without exposing values or secret
references. Legacy snapshots without that projection continue to show
conditional edges only as candidates. Cost and elapsed-time remainder remain
explicitly unknown until canonical accounting records those dimensions.

Enabled child-agent policies are complete execution contracts rather than only
numeric limits. They select an exact `style-id@semver`, workspace mode,
provider/model inheritance, context/token/cost budgets, tool groups, memory
access, join semantics, cancellation propagation, and reviewer-attempt bound.
The built-in `persistent-chat@1.1.0` and planner-worker v1.1 through current
v1.3 versions retain those fields. A persisted binding is not silently upgraded:
if that exact style version is absent, restart or selection reports explicit
unavailability and requires a deliberate migration or branch-with-style.

See [Session-style format](../reference/session-style-format.md).
