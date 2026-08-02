# Graph State, Variables, and Budgets

Status: implemented in `core/graph-state`; session projection in
`apps/runtime/logic/src/graph_state.rs`.

## Ownership

The runtime owns canonical graph state: typed variables, deterministic
expression inputs, branch-safe merge semantics, and complete execution-budget
accounting. Tool hosts, harnesses, plugins, and frontends never mutate this
state; they read deterministic projections through narrow ports.

## Module layout

```text
core/graph-state/                  pure, deterministic, no I/O
  src/value.rs                     bounded canonical values
  src/declare.rs                   variable declarations
  src/event.rs                     canonical events (only mutation surface)
  src/state.rs                     scoped state, reads/writes/merges
  src/reduce.rs                    replay reducer
  src/budget.rs                    budget ledger + events
  src/expression.rs                condition evaluation
  src/parallel.rs                  machine-validated parallel write safety
  src/port.rs                      narrow read ports for generic dispatch

apps/runtime/logic/src/graph_state.rs   session-bound projection
```

## Typed values

Values are fully owned and replay-safe (`GraphValue`):

- null (only where the declaration is `Optional`);
- boolean;
- signed and unsigned integers within explicit declared bounds;
- fixed-point decimal (`Decimal { unscaled, scale }`, scale <= 12, `i128`
  normalization for exact ordering — no floats);
- string with a declared byte bound;
- enum tag from a declared closed tag set;
- list with declared element type and length bound;
- map with declared value type and length bound;
- session, child-session, task, node, and continuation IDs;
- artifact references (large values become immutable artifacts);
- tool-result and child-result references;
- approval decisions;
- timestamps and durations.

Secret values are represented only by approved secret references
(`SecretReference`); declarations classified `Secret` reject every plaintext
representation. Container ordering is canonical (`BTreeMap`), so value bytes,
hashes, and serialized sizes never depend on incidental map iteration order.

## Declarations

Every variable carries a `VariableDeclaration`:

- stable name and `VariableType`;
- scope (`run`, `branch { branch_id }`, `node { node_id }`);
- producer and consumer node IDs (producer authority is enforced on write;
  empty producers means runtime-owned);
- mutability/versioning policy (`immutable`, `assignable`, `versioned`);
- maximum serialized size;
- security classification (`public`, `session_internal`, `secret`);
- merge policy where parallel writes are possible (`reject_conflict`,
  explicit deterministic `last_writer`, `list_append`, `set_union`,
  `object_field_merge`);
- optional default value, validated against the type at declaration time.

Undeclared reads and writes are rejected before any mutation.

## Events and replay

`GraphStateEvent` is the only mutation surface:

- `variables_initialized` (declarations + hash);
- `variable_assigned` (session, style run, node, prior/new version, value,
  value hash, artifact reference);
- `variable_validation_rejected` (audit, no state change);
- `branch_scope_created` / `branch_scope_closed`;
- `variable_merged` (policy, contributors, version, value, hash).

The reducer (`GraphStateReducer`) applies events in order and validates
declared variables, types, sizes, version continuity, and value hashes; it
fails closed on tampering. Identical event streams reconstruct identical
values without calling external systems (values are embedded, bounded by the
declared size). Property tests cover replay equivalence and prefix
equivalence.

## Parallel semantics

Branches are explicit `BranchScope` instances created with `Isolated` or
`CopyOnWrite` policy. Branch-local variables never leak; immutable run
variables are readable from any branch. Run-scoped writes from branches
become merge obligations resolved at the join through `merge_parallel`, which
sorts contributors by branch identity and applies the declared policy:

- `reject_conflict` — any second contributor rejects;
- `last_writer` — deterministic winner by declared ordering
  (`branch_lexical` or `node_lexical`);
- `list_append` — deterministic contributor order;
- `set_union` — deduplicated by canonical value bytes;
- `object_field_merge` — per-key merge, differing values for one key reject.

`validate_parallel_write_safety` machine-validates branch write plans against
the declaration set before any session mutation; undeclared, immutable, or
policy-less parallel writes fail the report.

## Expression integration

Conditions (`agentmod-expression-engine`) evaluate only against canonical
graph variables and canonical budget counters. The environment is built from
sorted sources; results are stable and independent of assignment order or JSON
object ordering. Outcomes are one of:

- `eligible` — true with all inputs present;
- `ineligible` — false with all inputs present;
- `missing required input` — a referenced declared variable is unassigned;
- `invalid expression/type` — undeclared variable, unknown counter, or type
  error.

## Budget model

`BudgetLedger` accounts for: style steps, model requests, tool calls,
iterations, retries, child sessions, concurrent children (gauge), input/
output/total tokens, provider cost, active provider duration, active tool
duration, and the explicitly selected elapsed wall-clock ceiling.

- Provider-reported values and estimates are tracked separately; a value that
  is neither is explicitly unknown (`mark_unknown`), never zero.
- Cost calculations require a `PricingBinding` (model, provider, pricing-record
  version, recorded timestamp).
- Pre-dispatch `check` gates the next consequential action; `commit` records
  exact evidence after a completed action. A completed action may consume the
  final budget; the following check is blocked.
- Child usage rolls up only per an explicit style policy (`full`, `bounded`,
  `none`); branches inherit/reset budgets only per declared policy.
- `reconstruct` rebuilds a ledger from its initialization event and subsequent
  events, reproducing exact remaining amounts after restart.
- Recorded time values used for decisions are canonical inputs from the
  ledger's counters projection.

## Dispatch port

Generic dispatch consumes graph state through `GraphStateReadPort` +
`BudgetReadPort` (composed as `ExecutionGraphState` in core and
`SessionGraphState` in runtime logic). No external SDK or frontend type
crosses the boundary.

## Runtime integration

`apps/runtime/logic/src/graph_state.rs` binds the core state and ledger to a
session and maps immutable `SessionStyleBudgets` onto canonical `BudgetLimits`
(`max_steps` -> style steps, `max_tokens` -> total tokens, `max_cost_micros`
-> provider cost, `max_duration_ms` -> explicitly selected wall clock).
Wiring into the turn executor (check-before-dispatch and
commit-after-completion) belongs to the generic dispatch workstream.
