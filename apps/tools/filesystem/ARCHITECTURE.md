# Filesystem Tool Host

The filesystem capability host is an isolated process with the enforced chain:

```text
tool protocol → service → logic → data → dependency → operating system
```

- `service` owns tool-protocol parsing and maps the complete wire authorization
  envelope into a logic-owned envelope.
- `logic` validates business arguments and maps the envelope and operation into
  data-owned types.
- `data` normalizes results and maps the envelope and operation into
  dependency-owned types.
- `dependency` alone imports filesystem libraries, the system clock, and the
  shared keyed-authorization verifier. It recomputes the canonical operation
  digest, verifies every grant, atomically consumes the nonce, then performs the
  operation.
- `bin` is the composition root. It refuses to start without the authorization
  key, authenticated owner, and runtime session configuration.

No upper layer imports filesystem APIs or the shared authorization
implementation. No layer skips the adjacent layer.

## Authorization

Every read, list, glob, grep, write, edit, and patch operation requires an
`agentmod_protocol_support::authorization` grant. The dependency binds:

- authenticated owner;
- runtime session;
- protocol call ID;
- exact `filesystem.*` action;
- recomputed canonical argument digest;
- bounded issue and expiry timestamps;
- a single-use nonce.

Nonce consumption is protected by shared locked state and occurs before any
filesystem access. Clones of the native dependency share the same replay set.
Missing keys, raw unwrapped operations, forged or modified tokens, expired
tokens, wrong owners, action changes, argument changes, and nonce replay are
rejected before filesystem access. Authorization failures are redacted.

Canonical encoding covers all operation fields. Large write content and patch
text are represented by both their BLAKE3 hash and byte length. Ordered arrays
remain ordered and patch base hashes use a stable sorted map.

The composition root requires:

```text
AGENTMOD_FILESYSTEM_AUTH_KEY_HEX   64 hexadecimal characters
AGENTMOD_FILESYSTEM_AUTH_OWNER     authenticated local owner
AGENTMOD_FILESYSTEM_AUTH_SESSION   selected runtime session
```

Health is non-consequential and does not touch workspace content. It reports
whether authorization is configured. Discovery remains lazy.

## Filesystem safety

Authorization complements, but does not replace, the existing path and mutation
controls: canonical approved roots, traversal and symlink-escape rejection,
sensitive/device-file policy, content bounds, expected hashes, atomic writes,
prevalidated edits, and rollback-aware multi-file patches.

The service emits `Started` only after dependency authorization and execution
succeed. A denied request returns only a failure event.

## Tests

Dependency security tests verify that forged, tampered, expired, replayed,
wrong-owner, wrong-digest, missing-key, and raw requests fail closed. Rejected
write requests leave their target absent; rejected reads return no content.
Existing filesystem behavior tests execute with valid short-lived grants.

## Current limitations

- Consumed nonces are shared atomically by all clones in one host process, but
  are not persisted across process restarts. Grant lifetimes therefore remain
  the outer replay bound after a restart.
- The canonical digest format is currently versioned only by this host's Rust
  API and documentation; the tool protocol does not yet negotiate a canonical
  encoding version. Runtime grant issuers must use the matching host algorithm.
- One host process is bound to one configured owner/session pair. A supervisor
  serving several sessions must launch separately bound hosts.
- The composition root currently accepts the verification key through a
  protected environment variable. Operating-system keychain/secret-reference
  resolution remains a future dependency adapter.
