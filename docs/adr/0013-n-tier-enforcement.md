# ADR 0013: Crate-enforced N-tier architecture

Status: Accepted

Every deployable system uses separate service, logic, data, and dependency crates with
the sole business direction `service -> logic -> data -> dependency`. Composition
roots assemble all four but execute no use case.

Cargo metadata and source checks reject skipped/upward edges, cross-process internals,
protocol/SDK leakage, shared business DTOs, re-exports/aliases, and upward callbacks.
Intentional invalid fixtures prove diagnostics. The crate count is accepted because
the boundaries protect process and security domains.
