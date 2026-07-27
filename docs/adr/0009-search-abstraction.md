# ADR 0009: Search abstraction

Status: Accepted

Web search is a provider-neutral tool contract with query, count, freshness, locale,
language, domain policy, dated snippets, and citation provenance. Provider HTTP types
remain in the web-host dependency layer.

The release ships a deterministic fixture and at least one documented usable adapter;
Brave and self-hosted SearxNG are initial candidates subject to current official API
review. Results are bounded and full captures may become artifacts.
