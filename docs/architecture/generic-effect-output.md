# Generic effect-output projection

The runtime-logic effect-output projector is a pure boundary between an
already completed runtime effect and canonical graph-variable coordination.
It neither performs effects nor appends events. Its inputs are the exact
compiled node ID, that node's `write_variables`, the compiled variable
declarations, and a bounded `EffectResultSlots` value.

Projection is declaration-driven. Ordinary Boolean, integer, decimal, string,
enum, list, and map values are keyed by exact declared variable name.
Runtime-owned values occupy typed slots:

| Declared type | Runtime-owned slot | Native source |
|---|---|---|
| `NodeResultReference` | node-result receipt reference | Any effectful native or plugin node |
| `ToolResultReference` | tool-result receipt reference | Tool execution |
| `ApprovalResult` | canonical approval disposition | User approval |
| `ArtifactReference` | immutable artifact reference | Artifact persistence |
| `ChildId` | one child session ID | Child spawn |
| `List<ChildId>` | canonically ordered child session IDs | Child spawn |
| `Timestamp` | runtime-recorded timestamp | Any effectful node |
| `Duration` | runtime-recorded duration | Any effectful node |

Model, review, and plugin executors may provide ordinary declared fields.
They do not construct runtime-owned receipt, child, approval, artifact, or
time slots. Delay, schedule, and event emission normally project a node-result
reference and any declared runtime-recorded timestamp or duration. Tool,
approval, artifact, and child nodes may project their dedicated slot plus a
node-result reference or recorded time when declared.

The graph compiler rejects statically known incompatible native slot writes,
multiple consumers of one slot, singular/plural child ambiguity, secret or
external-handle outputs, and collections containing runtime-owned slots.
Graphs with no declared writes do not enter this validation and retain their
legacy schema-free effect behavior.

## Turn integration contract

The effect receipt remains canonical whether or not the graph declares an
output variable. `TurnLogic` and parallel effect ports must populate only the
slots consumed by that node's compiled write declarations. They must not pass
every value available in the receipt: an available-but-undeclared result is
intentionally omitted from `EffectResultSlots`, otherwise strict projection
returns `ExtraSlot`. For a legacy or explicit no-write node, Turn skips
canonical output assignment (or equivalently projects empty slots to an empty
object); it does not discard or reinterpret the effect receipt.

After a terminal canonical receipt exists, the owner:

1. Builds ordinary fields only from exact, validated native/plugin proposal
   fields and builds runtime-owned slots from canonical runtime state.
2. Calls `project_effect_output`, or `project_branch_effect_output` with the
   exact stable branch context.
3. Gives `transition_variables` to the canonical-variable coordinator rather
   than mutating the environment directly.
4. Commits timestamp and duration assignments using
   `recorded_runtime_values`; restart reuses those exact values and never
   queries a clock to reconstruct them.
5. Rejects projection before node completion or transition on any missing,
   extra, ambiguous, misowned, mistyped, secret, or oversized value.

Parallel projection does not merge. Branch-scoped assignments retain the
branch context. Complete shared run/session contributions continue to the
existing serialization or merge coordinator, which applies the compiled merge
policy and base-version checks.

This module is a tested integration contract. Live root-Turn and parallel
effect-port wiring is tracked separately and must not be inferred from the
existence of the projector.
