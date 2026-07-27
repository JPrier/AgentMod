# Plugin SDK Reference

No `plugin-sdk` crate or loadable plugin host exists yet. This page documents
only the implemented wire vocabulary so authors do not mistake it for a usable
SDK.

`PluginManifest` currently contains:

- `id`, `version`, and `runtime_api`;
- `category`, `scope`, and `class`;
- required and provided capabilities;
- read and proposed-write authority;
- `before` and `after` handler constraints;
- `timeout_ms`.

Wire commands cover load, intercept, observe, cancel, disable, and health.
Responses cover loaded, continue, replace, reject, observation acceptance,
disabled, and structured failure.

Missing before third-party use: manifest file format, entrypoint declaration,
configuration schemas, tool/network permissions, failure/retry policy,
isolation mode, migration version, observer-write validation, plugin host,
capability grants, signing/trust UX, examples, and compatibility tooling.
