# ADR 0008: LSP supervision

Status: Accepted

The LSP host discovers, starts, shares where safe, monitors, and restarts language
servers. Protocol objects and process handles remain in its dependency layer; stable
records cross upward.

LSP is optional and degrades explicitly. Basic filesystem and process tools never
depend on it. Project-root discovery and language-server selection are data/logic
concerns over dependency-provided facts.
