# ADR 0001: Canonical persistence

Status: Accepted

Canonical session history is checksummed append-only JSONL plus immutable,
content-addressed artifacts. Each record includes its preceding checksum to form a
hash chain. Versioned validated snapshots accelerate pure replay. SQLite is a
rebuildable derived index and is never the only copy of history.

Only an invalid final record may be truncated after quarantine. Interior corruption
places the session in read-only quarantine. Replay does not dispatch side effects.
This favors inspection and recovery over the compactness of a database-only event
store.
