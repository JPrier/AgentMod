# Framework research against AgentMod problem rubric

Issue #15 | Research date: 2026-09-13

## Method

This is a capability-first desk review of first-party documentation and public source repositories. A verdict is **Pass** only where ordinary supported extension interfaces demonstrate the complete requirement. **Fail** means the documented architecture contradicts the requirement. **Unverified** means the evidence is insufficient; it is not an impossibility claim. No performance scoring was performed because no candidate qualified all mandatory capability gates.

## Results

| Candidate | C1 | C2 | C3 | C4 | C5 | C6 | C7 | C8 | Qualification |
|---|---|---|---|---|---|---|---|---|---|
| LangGraph / Platform | Fail | Pass | Pass | Pass | Pass | Fail | Unverified | Fail | Does not qualify |
| Amazon Strands | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Does not qualify |
| Temporal | Fail | Unverified | Pass | Unverified | Pass | Pass | Pass | Fail | Does not qualify |
| Restate | Fail | Unverified | Pass | Unverified | Pass | Pass | Pass | Fail | Does not qualify |
| Mastra | Unverified | Unverified | Pass | Unverified | Unverified | Unverified | Unverified | Unverified | Does not qualify |
| Inngest | Fail | Unverified | Pass | Unverified | Pass | Pass | Pass | Fail | Does not qualify |
| OpenClaw as platform | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Unverified | Does not qualify |

The Fail verdicts reflect documented abstraction boundaries: workflow engines persist workflow state and inputs/outputs but do not expose arbitrary model context as a first-class, editable state at every model/tool boundary. Where the documentation did not settle a requirement, the verdict remains Unverified.

## Evidence by candidate

### LangGraph and LangGraph Platform

Official docs describe graph state, checkpoints, persistence, time travel, interrupts, and subgraphs. These support reconstructable state, replay/branch-like workflows, and boundary interrupts: [Graph API](https://docs.langchain.com/oss/python/langgraph/graph-api), [Persistence](https://docs.langchain.com/oss/python/langgraph/persistence), [Time travel](https://docs.langchain.com/oss/python/langgraph/use-time-travel), [Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts), [LangGraph Platform](https://docs.langchain.com/langgraph-platform/).

C1 is Fail for the rubric’s stronger requirement: graph state is controllable, but the exact provider request context and all harness-owned context are not exposed as an arbitrary editable state through the normal graph extension API. C2–C5 have substantial support through nodes, checkpoints, interrupts, and replay/time-travel. C6 is Fail as documented: graph/application deployment changes are deployment operations, while changing model/tools/context policy live for an existing running session without restart is not a supported general capability. C7 is Unverified for guaranteed process-level kill of arbitrary tool subprocesses and provider requests. C8 is Fail because meeting C1/C6 would require changing core execution/context ownership, beyond a plugin.

### Amazon Strands

Strands documents model-driven agents, tools, multi-agent patterns, hooks, streaming events, session/conversation management, and model/provider integrations: [Strands Agents docs](https://strandsagents.com/latest/documentation/docs/), [Agent hooks](https://strandsagents.com/latest/documentation/docs/user-guide/agents/hooks/), [Tools](https://strandsagents.com/latest/documentation/docs/user-guide/concepts/tools/), [Multi-agent systems](https://strandsagents.com/latest/documentation/docs/user-guide/concepts/multi-agent-systems/).

The sources establish useful extension points but do not establish C1–C8 at the required strength: arbitrary replacement of the complete provider context at every boundary, immutable full event history with exact request reconstruction, reversible restoration, live hot swap of running sessions, or a platform-level kill guarantee. These therefore remain Unverified pending a focused prototype/source audit. With mandatory capabilities unverified, it does not qualify and receives no performance evaluation.

### Temporal

Temporal documents durable, replayable workflows, Signals, Updates, cancellation, child workflows, and worker versioning: [Temporal docs](https://docs.temporal.io/), [Workflows](https://docs.temporal.io/workflows), [Signals](https://docs.temporal.io/workflows#signal), [Updates](https://docs.temporal.io/encyclopedia/workflow-message-passing), [Cancellation](https://docs.temporal.io/workflows#cancellation), [Worker Versioning](https://docs.temporal.io/production-deployment/worker-versioning).

Temporal’s event history and workflow state support C3, while child workflows support C5 and Signals/Updates/versioning support controlled workflow changes. C1 is Fail: Temporal is a durable workflow engine; arbitrary LLM prompt/context assembly is application code inside workflow/activity boundaries, not a universally inspectable and mutable runtime context. C7 is Pass for workflow cancellation semantics, but this does not guarantee undo of external effects. C8 is Fail for the complete rubric because the missing context ownership would require a separate agent harness or core adaptation. C2/C4 remain Unverified at the exact model/tool boundary granularity.

### Restate

Restate documents durable virtual objects, workflows, promises, journaling, awake/sleep behavior, and deployment/versioning: [Restate docs](https://docs.restate.dev/), [Virtual Objects](https://docs.restate.dev/fundamentals/virtual-objects), [Workflows](https://docs.restate.dev/fundamentals/workflows), [Durable promises](https://docs.restate.dev/fundamentals/durable-promises), [Deployments](https://docs.restate.dev/deploy/deployments).

Restate’s journal gives durable execution history and its object/workflow model supports independent continuations and message-driven changes. C1 is Fail for the same reason as Temporal: the system persists invocation/journal state, not an arbitrary editable LLM context window exposed at every model/tool boundary. C7 is Pass for cancellation of durable invocations subject to external side effects. C2/C4 are Unverified for exact context boundary semantics; C8 is Fail for the complete requirement set.

### Mastra

Mastra documents agents, workflows, memory, tools, processors, observability, and storage: [Mastra docs](https://mastra.ai/docs), [Agents](https://mastra.ai/docs/agents/overview), [Workflows](https://mastra.ai/docs/workflows/overview), [Memory](https://mastra.ai/docs/memory/overview), [Processors](https://mastra.ai/docs/agents/agent-memory#processors), [Observability](https://mastra.ai/docs/observability/overview).

Mastra clearly supplies configurable agent/workflow building blocks and persistent memory/storage. The reviewed first-party material does not establish the stronger guarantees for C1/C2 (complete arbitrary context replacement at every request/tool boundary), C4 (auditable reversible restoration), C5 (history-preserving branch continuations), C6 (live per-session reconfiguration without restart), or C7 (kill of arbitrary running tool processes). These remain Unverified; C8 therefore remains Unverified and the candidate cannot qualify.

### Inngest

Inngest documents durable functions, steps, retries, event history, cancellation, concurrency, throttling, and function versioning: [Inngest docs](https://www.inngest.com/docs), [Functions](https://www.inngest.com/docs/functions), [Steps](https://www.inngest.com/docs/features/inngest-functions/steps-workflows), [Cancellation](https://www.inngest.com/docs/features/inngest-functions/cancellation), [Concurrency](https://www.inngest.com/docs/learn/inngest-steps).

These support durable workflow execution, independent events, concurrency controls, and cancellation. C1 is Fail: Inngest steps do not provide an agent-specific editable context window; context assembly remains application/model integration code. C5/C6 are Pass at the workflow/event deployment level (parallel functions and evolving deployed code), but this is not proof of mutating an already-running session’s agent context and configuration at the next boundary. C7 is Pass for function cancellation within documented semantics, with external side-effect limits. C8 is Fail for the complete rubric because the central context ownership requirement is outside the extension model.

### OpenClaw as platform

OpenClaw’s public repository and documentation describe a gateway, channels, agents, tools, sessions, memory, and skills: [OpenClaw repository](https://github.com/openclaw/openclaw), [OpenClaw docs](https://docs.openclaw.ai/).

The available first-party material establishes an extensible agent product, but does not provide sufficient stable platform contracts to verify C1–C8 at the AgentMod strength—especially exact per-request context replacement, append-only reconstructable history, reversible restoration, branch semantics, live hot swap of running sessions, and hard process interruption. All mandatory rows remain Unverified. It therefore does not qualify; performance analysis is deferred.

## Build-vs-buy conclusion

No candidate in this survey qualifies through an ordinary plugin/extension system. The strongest partial foundations are LangGraph (checkpoint/time-travel/interrupt concepts), Temporal (durable history, signals, cancellation, versioning), Restate (journaling and durable objects), and Inngest (durable event workflows), but each leaves arbitrary model-context ownership outside the platform contract. Combining those systems would still require selecting and designing a context/history/runtime architecture, rather than simply installing a plugin. A new AgentMod runtime is therefore supported by this research, subject to deeper source audits or prototypes before treating any individual Fail as final.

Performance was intentionally not compared because the capability gates were not passed.