# ADR 0006: Process supervision

Status: Accepted

The runtime lazily supervises harnesses, shared capability hosts, plugin isolation
groups, and optional scheduler workers. Dormant sessions own none of these resources.
Requests have durable IDs and explicit `pending`, `committed`, `failed`, or `in_doubt`
recovery state.

Platform dependencies use Unix process groups and Windows job/process controls where
available. A crash is isolated, recorded, and reconciled; arbitrary external effects
are never falsely claimed to be exactly once.
