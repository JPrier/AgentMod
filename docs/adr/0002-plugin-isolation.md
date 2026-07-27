# ADR 0002: Plugin isolation

Status: Accepted

Trusted first-party Rust extensions may run in process. Third-party extensions run
through a versioned out-of-process plugin protocol or an approved WASI component
sandbox; stable Rust dynamic-library ABI is not used.

Manifests declare capability, scope, authority, ordering, timeout, failure, and state
migration. Observers never receive a canonical-write interface. Isolation adds IPC
cost but prevents plugin ABI churn and limits crashes and authority.
