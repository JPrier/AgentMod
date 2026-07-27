# ADR 0007: MCP integration

Status: Accepted

MCP is a first-class capability hosted out of process, but not AgentMod's internal tool
model. Stdio and streamable HTTP operations are normalized into the tool protocol and
pass through ordinary proposals, permissions, events, cancellation, progress,
artifact limits, and style restrictions.

MCP SDK types remain in the MCP dependency layer. Catalog integrations are opt-in;
untrusted downloads or execution always require approval.
