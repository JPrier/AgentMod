# AgentMod problem statement and evaluation rubric

Status: Approved by Josh.

Repository path: `docs/design/problem-statement.md`.

Decision ticket: [Problem statement: pain inventory, root causes, and evaluation rubric](https://github.com/JPrier/AgentMod/issues/14). Parent: [AgentMod realignment map](https://github.com/JPrier/AgentMod/issues/1).

## Problem

Josh needs to experiment freely with agent behavior: different memory systems, context-management strategies, workflows, tools, and combinations of these. The harnesses used so far constrain those experiments by owning context assembly, exposing incomplete history, restricting intervention to completed turns, or requiring disruptive changes to enable new behavior.

The required system gives extensions and operators complete control over context at every execution boundary, preserves an auditable history of that context, permits branching and restoration, and accepts live changes without restarting sessions. These capabilities must remain usable as the workload grows. The current use case is approximately 200 active sessions; hundreds of thousands are a future design consideration, not a prescribed ceiling.

This statement defines outcomes and evaluation criteria. It does not select context-merging algorithms, scheduling mechanics, storage formats, or plugin protocols.

## Terms

- **Context:** the current state available for an agent’s continuation, including the contents selected for a model request. The exact input supplied to each model invocation must be inspectable; a provider’s hidden internals are not implied to be accessible.
- **History:** the auditable record of what happened to context and execution. Context changes, including restoration, are recorded rather than erasing earlier events.
- **Execution boundary:** a point between execution steps, including boundaries of requests, responses, tool invocations, and other extension-driven steps. A complete conversational turn is not the minimum intervention unit.
- **Restoration:** changing current context to the state at an earlier recorded point. The restoration itself becomes history, so it can also be reversed.
- **Branch:** an independent continuation from a selected context state, optionally with changed inputs. Its results may later contribute to another context or remain in a separate session.
- **Active session:** a session with unfinished runnable or waiting work. It need not be consuming compute at every instant.
- **Inactive session:** a saved session with no work currently being executed or requested. Its existence alone must not cause continuing execution overhead.

## Pain inventory and causes

The following distinguishes reported limitations from requirements for future experiments. Causes are stated at the capability boundary; this ticket does not diagnose another implementation’s internals.

| Problem | Evidence or intended use | Limiting condition | Required outcome |
| --- | --- | --- | --- |
| Experiments cannot replace memory or context behavior freely | Central user-reported limitation of prior harnesses | The relevant behavior is owned by the harness rather than exposed through sufficient extension interfaces | Replace and combine these behaviors through supported extensions |
| Context cannot be fully inspected or manipulated during execution | Josh needs arbitrary additions, removals, replacements, and intervention before a full turn completes | Available controls expose only part of the context or too few intervention points | Full context control at every execution boundary |
| Incomplete history limits reconstruction and experimentation | Prior-stack inventory identifies withheld tool outputs and information loss across the integration boundary, including ACP | The external manager does not receive or retain the complete information needed to reconstruct context | Preserve complete context history and tool inputs/outputs; verify specific integration limitations during research rather than assuming an ACP-wide cause |
| Branching and restoration are constrained | Parallel agents may start from similar context with different prompts or inputs; prior states must be recoverable | Context and history cannot be reused as controllable continuation inputs | Restore, branch, and contribute results using user-defined context strategies |
| Changes disrupt running work | Josh needs rapid changes to configuration, tools, memory, and context | Changes require restart or completion of an entire turn | Apply changes at the next applicable step while sessions continue |
| Unsafe work cannot be stopped decisively | User or automation may need to stop an unexpected dangerous tool | A cooperative request to stop is insufficient if execution continues | Terminate controllable running work, cancel requests, and prevent further dispatch |
| Saved sessions burden execution | Josh reports a prior harness becoming unusable around 200 saved sessions, and unable to support the desired 200 active sessions | Session accumulation creates unacceptable overhead; the internal cause is unverified | Inactive history does not impose ongoing execution costs; equivalent active work adds proportional demand |
| Workflows are constrained by the harness | Experiments may require arbitrary per-step context transformations and custom agent flows | Supported extension points do not express the desired behavior | Implement those workflows through ordinary extensions without core rework |

No measured complexity class or language-specific root cause is asserted. Rust is Josh’s chosen language for a new implementation, but another system is not disqualified solely because of its language. The original issue’s references to Python/TypeScript performance and hidden memory bugs do not establish a separate diagnosed defect.

## Required capabilities and acceptance scenarios

All capability rows are mandatory. These are observable scenarios, not instructions for implementing the runtime.

| ID | Requirement | Acceptance scenario |
| --- | --- | --- |
| C1 | Complete context inspection and manipulation | Inspect the exact context used for a model request. Add, remove, and replace content, then show that the next request uses the resulting context. No harness-owned portion needed for the experiment is inaccessible to the extension. Provider protocol validity still applies. |
| C2 | Intervention at every execution boundary | Install an extension that observes and transforms context before/after requests and individual tools, including between tool executions within a turn. Verify that no full-turn completion is required and that the intervention is recorded. Apply the same principle to other execution steps. |
| C3 | Complete, durable, reconstructable history | Record context changes, execution inputs/outputs, and complete tool results. Reconstruct exact context at selected recorded steps after reload. Configure indefinite retention; history reduction must not be forced by compaction or hidden truncation. |
| C4 | Auditable restoration | Restore context to step X. Show a new history entry identifying the restoration and its target. Restore the pre-revert state again without having lost either state or the intervening history. |
| C5 | Branching and reusable context | Start parallel continuations from a selected context with differing prompts or inputs. Preserve their histories and allow extension-defined results to contribute to an existing context or remain separate. No built-in merge strategy is required. |
| C6 | Live experimentation | Change configuration, tools, context management, or memory behavior during a session. Subsequent applicable steps use the change without a core/session restart, forced full-turn wait, or user-visible pause imposed solely for reconfiguration. Unrelated sessions continue. |
| C7 | Full interruption | Trigger a stop from a user and from automation while work is running. Prevent further affected work from starting, terminate controllable tool processes, and cancel outstanding requests. Record the interruption and partial results that are available. Completed external side effects cannot be retroactively undone; remote cancellation limits must be disclosed. |
| C8 | Sufficient supported extension interfaces | Implement the preceding experiments through the working plugin/extension system, including context history, persistence, and manipulation, without a fork, core patches, or major rework. A bundled, fixed memory feature alone does not satisfy this. |

“At any point” requires intervention at every execution boundary and an explicit full-stop capability during active work. It does not require mutating an already submitted model request in place. Normal context edits take effect at the next applicable boundary; a completed conversational turn must not be required. Additional interrupt-and-edit strategies must not be architecturally precluded.

Indefinite retention is an available policy, not an obligation to retain all data under every configuration. Context compaction and history retention are separate concerns: reducing current context must not silently destroy history.

## Scalability and efficiency requirements

These requirements inform design from the start. Detailed performance evaluation follows capability qualification.

| ID | Requirement | Later evaluation |
| --- | --- | --- |
| S1 | No arbitrary session ceiling | Establish that the design supports growth beyond the current 200-active-session use case, including a path toward hundreds of thousands given sufficient resources. Identify actual limits rather than claiming literal infinite capacity. |
| S2 | Proportional work and stable per-session overhead | Compare equivalent workloads at increasing concurrency with sufficient resources. Adding a session should add approximately its own work rather than increasing the execution cost of existing sessions merely through session count. |
| S3 | Inactive sessions do not burden active execution | Increase saved/inactive session count independently of active work. Look for ongoing compute or active-path overhead caused simply by their existence. Storing data and explicitly accessing it still have costs. |
| S4 | Insufficient compute increases waiting time rather than losing work | Submit more runnable work than can execute immediately. Verify preserved progress, eventual continuation, and no arbitrary rejection based solely on session count. Finite storage/resource exhaustion remains a physical limit. |
| S5 | Efficient realization of required features | Evaluate harness CPU, memory, throughput, and latency costs after capability qualification. Exclude optimization of LLM reasoning and model calls from this ticket’s scope. |

There is no fixed 10 ms acceptance threshold in this statement. The goal is efficient implementation of the required features with proportional scaling. Scheduling, backpressure, distribution, hardware sizing, tolerances, and benchmark workloads are later design/evaluation decisions.

## Capability-first evaluation rubric

Evaluate existing systems and AgentMod against the same capability requirements.

1. **Establish capability feasibility first.** Research C1–C8 using supported interfaces and evidence. Do not begin comparative performance analysis merely because a harness is popular or fast.
2. **Use a per-requirement verdict:** Pass, Fail, or Unverified. Attach documentation, interface references, or a focused demonstration. Pass means the full requirement is supported through ordinary extensions; partial support must be described and cannot be counted as a pass. Missing evidence is Unverified, not proof of impossibility.
3. **Apply hard gates rather than an average score.** One failed capability disqualifies the candidate. Unverified mandatory capabilities prevent qualification until resolved. A capability coverage count may help summarize progress but cannot compensate for a failed gate.
4. **Reject fork-dependent solutions.** If the requirements need a fork, core changes, or major rework, Josh prefers building a new system. Do not treat such a candidate as a qualifying foundation or start planning a fork.
5. **Evaluate performance only for qualifying candidates.** Then investigate S1–S5. Language is evidence to consider when examining implementation choices, not a substitute for measurements or a standalone disqualification.
6. **Record the outcome honestly.** If no researched candidate qualifies, report the scope searched and remaining unknowns. Do not generalize that result into proof that no qualifying system exists anywhere.

Suggested comparison columns: Candidate; Version/date; C1–C8 verdicts and evidence; Extension work required; Qualification verdict; S1–S5 findings only after qualification.

## Design boundaries

Deferred decisions include branch consolidation behavior, context-selection algorithms, memory strategies, persistence representation, retention implementation, process supervision, scheduling/fairness, and distributed execution. Their required expressibility belongs here; their mechanisms do not.

The runtime must not need built-in knowledge of each memory or context-management strategy. The objective is to allow experiments through extensions, not to choose the winning strategy now.

## Decision context

This approved statement supersedes the earlier approximately 10 ms / 100-session performance bar. The North Star still requires reconciliation to proportional scaling, a current use case of 200 active sessions, and capability-first evaluation. That separate edit is pending authorization. Framework research and later performance analysis remain subsequent work.
