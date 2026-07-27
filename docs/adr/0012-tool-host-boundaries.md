# ADR 0012: Capability tool-host boundaries

Status: Accepted

Filesystem, process, web, browser, Git, LSP, and MCP are separate long-running
capability hosts. A host may expose related operations and is shared where safe; there
is no process per tool call.

Hosts validate authorization grants and local hard constraints, execute only approved
digests, and return structured results/artifacts. They never append canonical runtime
events or mutate session state.
