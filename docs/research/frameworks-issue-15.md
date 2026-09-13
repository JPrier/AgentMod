# Orchestration frameworks against the AgentMod problem rubric

Research date: 2026-09-13. Decision ticket: [Research: orchestration frameworks scored against the problem rubric](https://github.com/JPrier/AgentMod/issues/15).

## Scope and method

This survey covers LangGraph/Platform, Amazon Strands, Temporal, Restate, Mastra, Inngest, and OpenClaw-as-platform against the [approved requirements](https://github.com/JPrier/AgentMod/blob/main/docs/design/problem-statement.md). Luna workers investigated candidate groups; the parent reviewed and corrected the synthesis.

This is documentation research, not an executed compatibility test or exhaustive source audit. Documentation is rolling/current as accessed; installed versions were not pinned. Results must not be represented as certified compatibility for any release. Some worker searches yielded only snippets or landing pages; those do not establish complete capability guarantees.

Pass means the documented supported interface establishes the requirement. Unverified means the complete requirement was not established, even where useful building blocks exist. Absence of a built-in agent-context object does not establish failure: application-defined nodes, tools, hooks, storage adapters, and plugins can qualify without a core fork. A fork or major core rework remains disallowed.

## Capability matrix

| Candidate | C1 Context control | C2 Every boundary | C3 Full history | C4 Audited restore | C5 Branching | C6 Live changes | C7 Full stop | C8 Complete extension feasibility |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| LangGraph / Platform | Unverified | Unverified | Unverified | Unverified | Pass | Unverified | Unverified | Unverified |
| Amazon Strands | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified |
| Temporal | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified |
| Restate | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified |
| Mastra | Pass | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified |
| Inngest | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified* | Unverified |
| OpenClaw | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified |

*Inngest's stock cancellation fails the active-step termination component of C7. Whether an ordinary extension supplying independent process supervision can satisfy the complete requirement remains unverified; the matrix evaluates extension feasibility rather than only stock behavior.

No candidate is established as a complete match. This is not proof that none can meet the requirements. In particular, the many Unverified cells must not be converted into architectural Fail verdicts.

## Candidate evidence and remaining gaps

### LangGraph and Platform

LangGraph explicitly provides checkpoint-based state branching: `get_state_history` selects an earlier state, `update_state` creates a new checkpoint with modified values, and `invoke` continues it while preserving original history. Subgraph checkpoint configuration affects the available granularity. This establishes C5 for graph-owned context and user-defined continuation/results; it does not establish a universal log of every provider request. [Time travel](https://docs.langchain.com/oss/python/langgraph/use-time-travel).

Checkpointers persist graph state. Exact requests and tool outputs must be captured in that state or an extension-owned record; persistence alone does not prove C3. [Persistence](https://docs.langchain.com/oss/python/langgraph/persistence).

User-authored nodes and explicit interrupts provide control points, but node boundaries do not automatically expose every tool/model boundary inside a node. C1/C2 need inspection of the chosen model/tool adapter and graph composition. [Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts), [Graph API](https://docs.langchain.com/oss/python/langgraph/graph-api).

Platform run cancellation offers interrupt and rollback actions. A rollback option that removes run data is unsuitable for the required auditable restore; its existence does not prevent an extension implementing append-only restoration. Cancellation documentation does not establish arbitrary subprocess termination. [Cancel a run](https://docs.langchain.com/langsmith/cancel-run).

C4 remains unverified for explicit restoration provenance and revert-of-revert. C6 remains unverified for all live configuration/memory/tool changes, and C7 for full process supervision. None of these findings demonstrates that a fork is required.

### Amazon Strands

The investigated surfaces include agent lifecycle hooks, tools, session/conversation management, and multi-agent composition. These are plausible supported extension points, not evidence that context ownership must be changed in the core. [Documentation](https://strandsagents.com/latest/documentation/docs/), [Hooks](https://strandsagents.com/latest/documentation/docs/user-guide/agents/hooks/), [Tools](https://strandsagents.com/latest/documentation/docs/user-guide/concepts/tools/).

Access to detailed hook documentation was incomplete in this review. We did not establish which complete request fields are mutable at each boundary, the durability ordering of exact request/tool records, or a documented no-fork implementation of restore, fork, and hard-stop semantics. Accordingly all complete gates remain Unverified. This candidate received weaker evidence coverage than LangGraph and Mastra; the table is not a ranking.

The specific remaining investigation is the hook event payloads and session-manager contract, including what an adapter can replace and persist, rather than whether a built-in memory option exists.

### Temporal

Workflow history and activity boundaries are useful foundations, and application code can own model input. This is not a C1 failure merely because context is represented by application data. Signals/Updates are relevant to state changes, but do not alone demonstrate arbitrary live plugin replacement. [Workflow model](https://docs.temporal.io/workflows), [Message passing](https://docs.temporal.io/encyclopedia/workflow-message-passing).

The Python documentation distinguishes cancellation from termination. Cancellation is cooperative; regular activities need heartbeats to receive cancellation. Workflow termination records an event and stops workflow execution, which does not establish OS-level termination of arbitrary activity subprocesses. Reset creates a new execution from a selected history point and records a reason; exact context restoration and reversal across retained histories still need validation. [Cancellation, termination, and reset](https://docs.temporal.io/develop/python/workflows/cancellation).

C3 requires more than replay: exact context/tool records, indefinite retention configuration, and reload reconstruction must be demonstrated. Child workflows are not sufficient evidence for C5 arbitrary-context branching. A custom activity supervisor and audit adapter may be possible without a fork, but the complete supported composition and its scope were not established.

### Restate

Restate documents durable execution, stateful services, and workflows. These supply journaled application operations and a place for application-owned context; they do not imply that arbitrary context manipulation requires modifying Restate. [Documentation](https://docs.restate.dev/), [Virtual objects](https://docs.restate.dev/fundamentals/virtual-objects), [Workflows](https://docs.restate.dev/fundamentals/workflows).

The review did not establish an exact-request audit layer, arbitrary historical context restoration with restoration provenance, parallel continuations from that state, or hard termination of running external tool processes through a supported extension composition. Durable replay must not be scored as all those features.

Evidence coverage was limited to documented durable-execution building blocks. All complete gates remain Unverified; neither feasibility nor necessity of major rework is demonstrated.

### Mastra

Mastra has concrete supported context-editing interfaces. `processInput` can replace messages and system messages. `processInputStep` runs on loop steps, including tool continuations, and can override model, tools, and messages. `processLLMRequest` rewrites the final provider-facing prompt immediately before invocation. This supports C1. Its edits are transient rather than automatically written back to conversation memory, so C3 requires explicit capture. `processLLMResponse` supplies corresponding response information. [Processors](https://mastra.ai/docs/agents/processors).

These interfaces also provide meaningful partial C6 support. They do not, by themselves, establish every individual tool boundary, arbitrary live replacement of all memory/configuration, audited restore, or complete branching. A worker's claim of a missing hook based on an unlocated issue was excluded; C2 is Unverified, not Fail.

An abort signal is not proof of terminating a noncooperative child process. C7 and complete extension feasibility remain unverified. [Generate reference](https://mastra.ai/reference/agents/generate).

### Inngest

Middleware offers lifecycle transformations around function/step execution. Application-owned context can pass through these interfaces; no built-in LLM context is required by the rubric. Exact tool/model boundary coverage and full audit semantics still need demonstration. [Python middleware lifecycle](https://www.inngest.com/docs/reference/python/middleware/lifecycle).

There is a concrete stock limitation: cancellation does not stop an actively executing step, which continues to completion. Canceling runs also does not prevent new runs being enqueued. These semantics do not satisfy the required full stop on their own. An independent extension supervisor may address the gap, but that composition was not established. [Cancellation](https://www.inngest.com/docs/features/inngest-functions/cancellation).

Replay is not automatically arbitrary context restoration or branch consolidation. History retention and exact payload capture need separate verification. [Replay](https://www.inngest.com/docs/platform/replay).

### OpenClaw-as-platform

OpenClaw supplies a persistent gateway, external application interfaces, session operations, and plugins. Those are relevant platform surfaces but do not establish all context/history contracts. [Repository](https://github.com/openclaw/openclaw), [External applications](https://docs.openclaw.ai/gateway/external-apps).

The configuration documentation distinguishes hot-applied settings from changes needing restart, so C6 needs a setting/plugin-specific assessment rather than a blanket pass or failure. [Configuration](https://docs.openclaw.ai/gateway/configuration).

Gateway event delivery and session persistence are different: a non-replayed event stream is not proof that persisted history is missing. Nor is an observable tool event proof of a mutable context hook. [Gateway](https://docs.openclaw.ai/gateway).

The complete exact-input editing, append-only reconstruction, auditable restore/branch, and process-stop contracts were not established. All gates remain Unverified; no claim that a fork is necessary is supported.

## Additional issue criteria

| Criterion from the research ticket | Finding |
| --- | --- |
| Language-agnostic components and fault isolation | SDK language choices or remote calls do not prove a generic plugin protocol or containment of a hung tool. Complete requirements were not established for any candidate. |
| Always-on multi-session runtime | Deployment services, durable workflow engines, and OpenClaw's gateway offer relevant hosting models. None was tested against the approved session-scale requirements. |
| Harness-grade interactive frontend | Workflow dashboards and development studios are not automatically daily-driver agent frontends. Complete frontend parity was not established. |
| Config-driven workflow composition | User-authored graph/function code does not automatically provide config-driven composition; a supported configuration interpreter may be possible. Its necessity is separate from proving a core fork is required. |
| Complete boundary observability | Checkpoints, traces, and event streams each cover different information. None alone proves C3. |

## Outcome and limits

The seven-candidate desk survey is complete; compatibility certification is not. Mastra demonstrates useful final-prompt control, LangGraph demonstrates history-preserving state forks, and Inngest documents a stock hard-stop limitation. The remaining gaps are explicit rather than silently scored as failures.

No comparative performance analysis was performed, as requested. No candidate has all capability gates established. The survey does not justify choosing to build solely by elimination, and it does not recommend a fork.

If further build-versus-buy certainty is needed, the highest-value empirical checks are exact-request capture plus per-tool edits in Mastra, and an audited restore/live-update/process-stop extension in LangGraph. These are proposed follow-up checks, not agreed designs or claims that those candidates will pass. No new HITL decisions were made by the research agents.
