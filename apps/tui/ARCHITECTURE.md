# Terminal frontend

The TUI is a separate process and communicates exclusively through the
versioned runtime protocol. It cannot import runtime, harness, or tool-host
internals. Its enforced call direction is `service → logic → data → dependency`.

| Layer | Responsibility and owned types |
|---|---|
| service | Ratatui rendering, Crossterm input, terminal lifecycle/restoration, permission modal, attachment help/status, smoke diagnostics, and key-to-logic mapping |
| logic | Session/style/harness selection, deliberate branch-with-style use case, canonical-history and replay-derived style/resource projections, explicit MCP OAuth management, bounded pending-attachment metadata, ACP-compatible rich-envelope construction, streaming transcript state, editor/history behavior, slash commands, approval and cancellation use cases |
| data | Runtime-health, style/harness/session, atomic branch, session-inspection/resource, confined-attachment mapping, event-page and continuous-subscription, plugin-lifecycle, MCP-OAuth, durable-schedule, turn-stream, approval, and cancellation datasets with explicit normalization |
| dependency | Workspace-confined attachment file loading/base64 encoding, authenticated local socket/named-pipe transport, runtime style/harness/session/branch/plugin-lifecycle/MCP-OAuth/schedule requests, protocol negotiation, bounded canonical resource projection, credit-window acknowledgement, request identity and sequence validation |
| bin | Environment bootstrap and concrete dependency/data/logic/service assembly |

The frontend loads dormant session summaries without loading all histories,
then pages the selected session's verified canonical events and starts one
bounded cursor-based subscription worker. The worker reconnects through the
authenticated runtime protocol, applies only unseen sequences, backpressures
through a fixed channel, and stops when the selected session or frontend is
dropped. Live provider events are rendered only after the runtime binds them to
committed sequence numbers. Tool approval, cancellation, exact plugin
disable/enable/quarantine/unquarantine actions, and durable schedule
create/list/remove operations call ordinary runtime endpoints; the TUI has no
bypass around interception or mandatory policy.

The MCP view invokes only the dedicated runtime OAuth begin/status/cancel
management requests. Every request binds the selected session, exact configured
server, fresh cancellation lineage, and optional exact transaction. The
dependency boundary validates response/request identity, bounded status,
transaction, scopes, expiry, hashes, and the transient HTTPS or loopback
authorization URL. Authorization URLs remain frontend-memory-only and are not
added to canonical history by the TUI.

The Runtime view is read-only. It maps the runtime's canonical session
inspection into bounded layer-owned artifact-persistence, child-execution, and
process-reconciliation rows. It never opens runtime storage or invokes a tool
host. LSP management is not inferred from generic tool payloads because the
runtime does not expose a stable canonical LSP management projection.

The Graph view reads the selected session's runtime-produced
`style_introspection` projection. It displays style/harness identity,
active/control node, known next transitions, loop/retry progress, remaining
canonical budgets, pipeline, memory/compaction, child/join/reviewer state, and
termination. Refreshes occur at selection and material turn lifecycle
boundaries; the frontend never interprets the compiled graph or opens runtime
storage itself.

The Styles view also reads the runtime component catalog and exposes
`/memory <id|style-default>` and `/compaction <id|style-default>` selectors.
`/budget <style-default|iterations steps tokens cost-micros duration-ms>` selects
SDK-validated hard limits for the next session; the same five values may follow
the component arguments in `/new` for one-shot creation.
Selecting `/style <id[@version]>` also performs a runtime style-inspection
request through all four frontend layers. The Styles view renders exact source,
compiled availability, harness/memory/compaction selections, and structured
validation diagnostics; it never opens style files directly.
`/new` sends those selections with style and harness through layer-owned
requests; the runtime performs SDK compilation and compatibility checks. The
frontend does not synthesize component profiles.

`ratatui::run` owns raw-mode setup and restoration. Crossterm polling and reads
remain on the terminal thread. Runtime streaming uses a bounded worker channel
so terminal input remains responsive and runtime credit is returned only after
each bounded frame is accepted.

`/attach <path>` loads one file from the selected session workspace through the
dependency layer. `/attachments`, `/attachment-remove <index>`, and
`/attachments-clear` inspect or change the pending set. Canonical containment
and every path component are opened relative to a capability directory with
no-follow semantics; type and size are checked on the same bounded-read handle.
Traversal and paths outside
the workspace, symbolic links, directories/devices, empty or over-512-KiB
files, secret-like names/content, duplicates, more than eight attachments, and
an aggregate over 512 KiB fail closed. Supported signature-checked types are
PNG, JPEG, GIF, WebP, WAV, MP3, Ogg, and `.bin` as
`application/octet-stream`. The dependency returns bounded base64 plus metadata;
logic keeps content private and clears it on submission or every actual selected
session-ID change, including refresh fallback after external session removal.
Attachment turns use the same version-1 rich-prompt JSON envelope as ACP while
text-only turns retain the existing prompt string exactly.

`agentmod-tui --smoke` performs the same authenticated bootstrap and session
listing without entering raw-terminal mode. `--smoke-turn <prompt>` additionally
proves committed streaming and credit-window handling.
`--smoke-command "/branch <sequence> [style]"` traverses the same command
palette and atomic branch path. It also covers component-selected `/new`
commands in process E2Es. `--smoke-watch <milliseconds>` runs the same
continuous subscription used by the fullscreen frontend and reports the exact
number of externally committed events observed after bootstrap. These modes
exist for installation and CI transport diagnostics.
`--smoke-attachment-turn "<prompt>" <path>...` traverses the real attachment
commands and ordinary turn stream without raw-terminal mode.
`tests/e2e/runtime_tui_rich_attachments.ps1` and the matching `.sh` script prove
the exact typed image/blob envelope, provider completion, traversal rejection,
same-process post-submit transient-state clearing, restart, and byte-stable pure
replay. The Linux proof
also rejects a real symbolic link. Both Windows and WSL/Linux commands passed
on 2026-07-31.

Current limitations:

- MCP OAuth management is implemented. Artifact, child-agent, and process
  reconciliation state have read-only canonical views; mutation remains
  runtime-only. LSP management remains unavailable until the runtime exposes a
  stable authority-owned endpoint/projection.
- Interactive path completion is not implemented.
- Approval continuation completion is reconstructed from canonical history
  after the resolution RPC, but an approved tool's subsequent live provider
  continuation is not streamed on that same frontend request yet.
