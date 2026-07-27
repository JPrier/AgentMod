# Git Host Architecture and Authorization

The Git host follows `bin → service → logic → data → dependency`; every boundary owns and maps its own authorization and operation types. Only dependency code invokes `git` or writes Git-host artifacts.

Every operation carries owner, session, call ID, action, supplied digest, signed grant, and canonical operation bytes. The service canonicalizes normalized JSON arguments. The dependency recomputes the content digest and verifies the shared short-lived keyed grant, exact request binding, expiry, and atomic single-use nonce before repository discovery. It registers the verified grant against the discovered canonical repository root. Every read or mutation then revalidates the signed claims, permitted action, expiry, consumed nonce, and registered root before invoking Git. A copied capability cannot be redirected to another repository or reused for a different action.

The host refuses startup without an authorization key, owner, or session. Repository and worktree paths are canonicalized beneath the configured workspace. Worktree creation remains detached; cleanup never uses force. Checkpoints remain commit-free immutable patch and untracked-file artifacts with integrity validation and clean-base restoration checks. The implementation contains no commit, push, reset, discard, force-push, or branch-deletion operation.

## Residual limitations

- Authentication identity is local host configuration because the current tool wire contract has no owner/session fields.
- The canonical JSON operation format is currently host-specific; callers must use the same documented tuple of action and recursively key-sorted arguments when computing the digest.
- Host artifacts use local filesystem paths rather than runtime artifact IDs because artifact registration is outside this host’s present protocol surface.
