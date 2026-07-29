# AgentMod Session-Style Refocus Context

Created: 2026-07-28T23:50:53Z
Repository: `C:\Users\jkpri\AgentMod`
Starting commit: `e99e9e1 fix(mcp): send negotiated protocol version`
Starting branch: `main`

## Task statement

Refocus AgentMod on its original modular orchestration design by connecting the
existing event, artifact, replay, continuation, policy, host, SDK, protocol, and
runtime foundations into explicitly selected, persisted, inspectable, recoverable
session styles.

## Desired outcome

Deliver the user-specified eight implementation phases and twelve process-level
acceptance scenarios. The first vertical slice is a live N-tier runtime style
registry plus persisted session binding, including protocol, CLI, TUI, restart,
inspection, and compatibility behavior.

## Known facts and evidence

- The worktree is clean on local `main`, tracking `origin/main`.
- `STATUS.md` reports 88 workspace packages and substantial live runtime,
  harness, tool-host, plugin-host, SDK, graph, pipeline, memory, compaction,
  replay, branching, recovery, CLI, TUI, and ACP foundations.
- `.omx/plans/prd-agentmod.md` and
  `.omx/plans/test-spec-agentmod.md` exist, satisfying the Ralph planning gate.
- The existing plan places styles, memory, compaction, scheduling, and child
  agents in M7, and plugins in M8, but the new task reprioritizes live style
  composition ahead of unrelated unfinished work.
- The user-level Ralph path is absent, but the installed OMX package contains
  `skills/ralph/SKILL.md`; its requirements are active.

## Constraints

- Preserve all listed low-level kernels and recovery guarantees.
- Maintain `service -> logic -> data -> dependency` and layer-owned types.
- Do not let runtime, harness, frontends, protocols, or external SDK types cross
  prohibited boundaries.
- Reuse the session-style SDK, graph compiler, event-pipeline compiler, canonical
  journal, proposal/policy path, continuations, receipts, and host protocols.
- No paid APIs, public network, external credentials, or external service
  dependencies in default tests.
- Verify before completion; do not reduce scope or claim mocks/manifests as live
  product behavior.

## Unknowns and open questions

- Exact current runtime session record and protocol DTO shapes.
- How much of style identity is already represented by existing branch metadata.
- Whether the SDK compiled representation covers every required runtime node and
  pipeline boundary.
- Current CLI/TUI extensibility for session creation and inspection.
- Existing plugin host seams usable for live session-style composition.
- Baseline suite duration and any stale documentation or platform-specific
  failures.

## Likely codebase touchpoints

- `sdk/session-style-sdk`
- `sdk/plugin-sdk`
- `core/graph-engine`
- `core/event-pipeline`
- `core/conversation-projection`
- `apps/runtime/{service,logic,data,dependency}`
- `protocols/runtime`
- `apps/cli/{service,logic,data,dependency}`
- `apps/tui/{service,logic,data,dependency}`
- session-style manifests, tests/E2E fixtures, architecture checks, benchmarks
- `STATUS.md`, architecture/reference docs, README, requirements traceability

## Initial execution intent

1. Verify current code and tests against documentation.
2. Produce a concise implementation map without stopping.
3. Implement Phase 1 as a complete vertical slice.
4. Verify Phase 1, then continue through the remaining phases and acceptance
   scenarios under the Ralph loop with independent architect verification.
