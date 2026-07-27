# Web capability host

The Web host is an independently deployable, JSONL tool-protocol process. It exposes
`http.request`, `web.fetch`, and `web.search`. The runtime remains responsible for
proposal interception, user policy, committed events, and canonical artifacts. The
host performs a second mandatory authorization and network-policy check immediately
before an external operation.

## Layer map

```text
tool-protocol JSONL
       |
       v
service  ->  logic  ->  data  ->  dependency  -> HTTP/DNS/files/secrets
```

Each arrow is both the only permitted business dependency and an explicit mapping:

| Layer | Responsibilities | Owned public types | Interface to caller |
|---|---|---|---|
| `service` | Tool discovery, protocol parsing, bounded argument decoding, canonical JSON construction, endpoint error mapping, result projection | `WebHostServiceConfig`, `WebServiceError`, private request DTOs | `WebHostService::handle` |
| `logic` | HTTP method semantics, URL-shape validation, search and resource limits, use-case coordination | `HttpRequestCommand`, `FetchCommand`, `SearchCommand`, `WebAuthorization`, result types, `WebLogicError` | `WebLogicPort` |
| `data` | Business dataset routing, adapter selection boundary, provider-independent normalization, dependency error translation | `HttpDataRequest`, `FetchDataRequest`, `SearchDataRequest`, data records, `WebDataError` | `WebDataPort` |
| `dependency` | Signed-grant verification, nonce consumption, DNS and network policy, TLS/HTTP, manual redirect validation, secret resolution, HTML parsing, search adapters, cancellation, response artifacts | dependency request/response types, `NetworkPolicy`, `SearchProvider`, `WebDependencyError` | `WebDependencyPort`, `SecretDependencyPort` |
| `bin` | Environment configuration, concrete assembly, bounded concurrent JSONL transport, shutdown on EOF | no business types | process entry point |

No transport DTO passes into logic. No logic type passes unchanged into data. No
external type from reqwest, scraper, URL, Tokio, or an operating-system secret source
escapes dependency.

## Authorization

The service expands validated defaults and canonicalizes the tuple `(tool,
cancellation_id, normalized_arguments)`. JSON object keys are recursively sorted and
array order is preserved. The dependency independently reconstructs the same bytes
solely from its own HTTP, fetch, or search request fields. It hashes those bytes with
BLAKE3 and requires exact agreement among:

- the recomputed digest;
- the protocol `normalized_digest`;
- signed grant claims;
- authenticated owner, session, call ID, action, expiry, and nonce.

The dependency binds identity to the composition-root owner and session and commits
the signed nonce to a checksummed, generation-based durable replay record before
networking or search execution. Replay remains denied after host restart. Runtime grant creation uses
`agentmod_protocol_support::authorization::seal_authorization`; callers must use the
same canonicalization contract.

## Network security

The mandatory dependency policy:

- permits only `http` and `https`, with plaintext HTTP disabled by default;
- rejects URL user information;
- evaluates deny patterns before allow patterns;
- supports exact hosts and `*.example.com` subdomain patterns;
- resolves every hop before execution;
- rejects loopback, private, link-local, multicast, unspecified, documentation,
  carrier-grade NAT, benchmark, and reserved addresses unless explicitly enabled;
- pins direct reqwest connections to the validated DNS results;
- disables reqwest automatic redirects and revalidates every `Location`;
- rejects methods and response, inline, redirect, and timeout limits above host caps;
- uses rustls and normal certificate verification, with no insecure bypass;
- disables ambient system proxy discovery and accepts only an explicit configured
  proxy;
- redacts authentication, cookie, API-key, and subscription-token response headers.

Redirects retain the original method and body. This is deliberately stricter than
browser-style POST rewriting.

Secret-bearing request headers use a value shaped as `{"secret_ref":
"env:VARIABLE_NAME"}`. The environment adapter accepts only uppercase environment
names, returns only the resolved value to dependency-local request construction, and
does not include values in results or errors. Literal headers remain available for
non-secret values.

## Bounded content and artifacts

The adapter reads response chunks with cancellation checks and enforces the hard
response bound before extending its buffer. Content above the inline bound is written
with create-new, flush, sync, and atomic rename to
`.agentmod/web-artifacts/<artifact-id>.bin`; the tool response contains only the
bounded projection and artifact ID. Text-like MIME types are UTF-8 projected. Other
content is base64 projected. `web.fetch` detects PDFs, extracts title, description,
main/article/body text, and up to 200 absolute links, and reports likely
JavaScript-required pages.

The host artifact is immutable by convention. Runtime ingestion and retention events
remain the runtime's responsibility.

## Search providers

The stable search result contains title, URL, snippet, optional publication date, and
provider provenance. Implementations are selected in the composition root:

- `SearchProvider::Mock` performs deterministic, stable-order offline search over
  fixed documents and supports result-domain filtering.
- `SearchProvider::Brave` calls Brave's official Web Search endpoint, sends the
  `X-Subscription-Token` from a secret reference, and maps its provider response into
  the stable record.

The implementation follows the official references:

- Reqwest client, redirect, proxy, DNS, and streaming APIs:
  <https://docs.rs/reqwest/latest/reqwest/>
- URL parsing and relative URL resolution:
  <https://docs.rs/url/latest/url/>
- Scraper document parsing and selectors:
  <https://docs.rs/scraper/latest/scraper/>
- Brave Web Search API:
  <https://api-dashboard.search.brave.com/app/documentation/web-search/get-started>

Network-free tests use the deterministic mock provider and a local Tokio TCP fixture.

## Composition configuration

Required:

- `AGENTMOD_WEB_OWNER`
- `AGENTMOD_WEB_SESSION`
- `AGENTMOD_WEB_AUTH_KEY` (64 hexadecimal characters)

Optional:

- `AGENTMOD_WEB_ALLOWED_DOMAINS` — comma-separated exact or wildcard hosts.
- `AGENTMOD_WEB_DENIED_DOMAINS` — comma-separated denied hosts.
- `AGENTMOD_WEB_ALLOW_PRIVATE` — `true` only for explicitly trusted local targets.
- `AGENTMOD_WEB_ALLOW_HTTP` — `true` to permit plaintext HTTP.
- `AGENTMOD_WEB_PROXY` — explicit HTTP(S) proxy URL.
- `AGENTMOD_BRAVE_API_KEY_REF` — for example `env:BRAVE_SEARCH_API_KEY`; absent
  selects the offline mock provider.

An empty outbound allowlist denies all network requests. Brave mode therefore also
requires `api.search.brave.com` in `AGENTMOD_WEB_ALLOWED_DOMAINS`.

## Testing

Service tests mock logic and verify parsing, lazy discovery, and mapping. Logic tests
mock data and verify bounds and business normalization. Data tests mock dependency
and verify operation/result mappings and error isolation. Dependency tests exercise
signed single-use authorization (including restart replay denial), full-layer
canonical-operation agreement, private-address and redirect denial, header
redaction, bounded output, atomic artifact overflow, and deterministic search without
public network access.

## Current limitations

- Fetch extraction is deterministic HTML text extraction, not browser rendering.
- PDFs are detected and retained as artifacts but text extraction belongs to the
  future rich-content processor.
- Fetch cache is bounded and process-local; it is not persistent and has no HTTP
  revalidation yet.
- The explicit proxy is trusted to route only to the prevalidated target; direct
  connections are DNS-pinned, while an HTTP proxy necessarily performs its own
  upstream resolution.
- Brave is the only network search adapter. Mock search is the default without a
  secret reference.
- The host stores overflow files locally; runtime-owned artifact metadata, retention,
  and event commitment are performed when the runtime ingests the returned artifact.
