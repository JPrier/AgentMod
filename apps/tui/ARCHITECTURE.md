# Terminal frontend

The TUI is a separate process and communicates exclusively through the
versioned runtime protocol. It cannot import runtime, harness, or tool-host
internals. Its enforced call direction is `service → logic → data → dependency`.

| Layer | Responsibility and owned types |
|---|---|
| service | Ratatui rendering, Crossterm input, terminal lifecycle/restoration, permission modal, and key-to-logic mapping |
| logic | Session selection, canonical-history projection, streaming transcript state, editor/history behavior, slash commands, approval and cancellation use cases |
| data | Runtime-health, session, event-page, turn-stream, approval, and cancellation datasets with explicit normalization |
| dependency | Authenticated local socket/named-pipe transport, protocol negotiation, bounded framing, credit-window acknowledgement, request identity and sequence validation |
| bin | Environment bootstrap and concrete dependency/data/logic/service assembly |

The frontend loads dormant session summaries without loading all histories,
then pages the selected session's verified canonical events. Live provider
events are rendered only after the runtime binds them to committed sequence
numbers. Tool approval and cancellation call ordinary runtime endpoints; the
TUI has no bypass around interception or mandatory policy.

`ratatui::run` owns raw-mode setup and restoration. Crossterm polling and reads
remain on the terminal thread. Runtime streaming uses a bounded worker channel
so terminal input remains responsive and runtime credit is returned only after
each bounded frame is accepted.

`agentmod-tui --smoke` performs the same authenticated bootstrap and session
listing without entering raw-terminal mode. `--smoke-turn <prompt>` additionally
proves committed streaming and credit-window handling. Both exist for
installation and CI transport diagnostics.

Current limitations:

- Schedule, plugin, MCP, process, artifact, child-agent, and LSP management
  panels are not implemented yet.
- Rich image attachment and path completion are not implemented.
- Approval continuation completion is reconstructed from canonical history
  after the resolution RPC, but an approved tool's subsequent live provider
  continuation is not streamed on that same frontend request yet.
