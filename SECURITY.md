# Security Policy

AgentMod is pre-release software and must not yet be treated as a hardened security
boundary. Security-sensitive implementation status is reported in `STATUS.md`.

Please report vulnerabilities privately through GitHub's security advisory feature
for this repository. Do not include secrets, private source files, full prompts, or
tool output in public reports.

The intended security invariants are:

- every consequential action is intercepted before execution;
- mandatory runtime policy runs last and cannot be bypassed by a plugin or style;
- secrets are referenced, redacted, and excluded from canonical events and logs;
- capability hosts and third-party plugins receive least authority;
- filesystem, process, and network operations enforce platform-aware boundaries;
- recovery never silently repeats an externally uncertain side effect.

Live provider secrets follow the reference-only rule: API keys are resolved from
explicit environment references or `file:` references at harness startup, never
from inline request options. Passing a plaintext `api_key` option is rejected.
TLS peer verification defaults to enabled; disabling it requires an explicit
configuration override. Custom endpoints and proxies require explicit
configuration, response bodies and SSE streams are bounded, and ambiguous
provider disconnects fail closed rather than being redispatched.

These are release requirements, not claims that all controls are implemented today.
