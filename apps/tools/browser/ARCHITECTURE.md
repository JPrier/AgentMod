# Browser capability host

The browser host is a separate tool-protocol process with enforced
`service → logic → data → dependency` calls. Runtime supervision creates one
lazy host per active AgentMod session; dormant sessions have no host process.

| Layer | Owned responsibility and types |
|---|---|
| service | `ToolHostCommand` endpoint parsing, strict argument DTOs, lazy browser schemas, and logic-result projection |
| logic | Browser lifecycle, selector/text/output bounds, commands, results, and business errors |
| data | Browser datasets, dependency selection, records, and dependency-error normalization |
| dependency | WebDriver HTTP, destination policy, session state, cancellation, screenshots/download artifacts, and keyed grant verification |
| bin | Environment bootstrap, concrete assembly, bounded concurrent JSONL transport, and shutdown |

The composition root is the only browser crate that imports all four concrete
layers. Service imports only logic; logic imports only data; data imports only
dependency. The host shares only the versioned tool protocol with runtime and
does not import runtime, harness, frontend, or another tool host.

The concrete adapter supports W3C WebDriver lifecycle, navigation with final-URL
revalidation, rendered source/title/URL inspection, screenshots, CSS-selected
click/type/form submission, credential-preserving page-context downloads,
health, cancellation, and shutdown. Screenshots and downloads are immutable,
hash-described, private session artifacts rather than large inline values.

The WebDriver control endpoint must be TLS or loopback HTTP. Destinations are
HTTPS or an explicitly enabled loopback HTTP origin, may be domain-allowlisted,
reject embedded credentials/fragments and raw private/link-local destinations,
and are checked both before navigation and after redirects. Runtime grants bind
the exact expanded operation, cancellation ID, owner, session, call, expiry, and
nonce; nonce consumption is persisted before WebDriver access.

`tests/e2e/runtime_browser_loop.ps1` compiles a deterministic external WebDriver
server and proves nine operations through CLI → runtime → harness → policy →
browser host → WebDriver → artifacts → canonical results → provider
continuation. The matching Unix script uses the same fixture.

Current limitations:

- A compatible WebDriver server/browser must be installed and explicitly
  configured; AgentMod does not silently download a browser.
- Interactive visible authentication handoff and download-directory event
  watching are not yet exposed. Persistent cookies inside the managed session
  are used by page-context downloads.
- DNS resolution and browser sandbox strength remain properties of the chosen
  WebDriver deployment; production deployments should use an OS sandbox.
