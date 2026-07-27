# ADR 0014: Layer-local types and mappings

Status: Accepted

Each layer owns requests, commands, records, results, identifier wrappers, state, and
errors. The calling layer maps requests downward and returned values upward. Aliases
or re-exports that pass one business DTO through multiple layers are forbidden.

Transport DTOs stop in service; SDK/protocol adapter types stop in dependency. Mapping
boilerplate is intentional isolation and is covered by layer tests. Stable primitives
remain limited to behavior-free opaque IDs, hashes, versions, sequences, timestamps,
and byte counts.
