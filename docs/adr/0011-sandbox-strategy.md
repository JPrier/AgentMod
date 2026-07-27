# ADR 0011: Sandbox capability tiers

Status: Accepted

Sandbox controls are capability-tiered by platform: process isolation, filesystem
roots, environment/secret filtering, network rules, resource limits, and optional
native sandbox adapters. Configuration exposes the effective tier.

When policy requires a control that a platform cannot enforce, execution denies by
default. Documentation must not call advisory command filtering a secure sandbox.
WASI is preferred for compatible untrusted components.
