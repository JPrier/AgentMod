# Security Architecture

Implemented logic includes typed consequential actions, action-proposal digests,
path/tool/process/domain/provider matchers, ordered permission rules, and
user-policy plus mandatory-policy evaluation. Mandatory deny wins, and mandatory
ask can strengthen user allow. Journal and artifact dependencies validate
identifiers, bounds, checksums, and transaction state.

Architecture checks prevent SDK use above dependency and prohibited layer
edges. Normal core tests use deterministic local fixtures.

Not implemented: host-side enforcement before real effects, symlink/junction
escape defense, process sandboxing and resource limits, DNS/private-network
controls, redirect revalidation, secret stores/keychains, local RPC ownership
authentication, plugin isolation, capability grants, and protected debug
capture. Current policy structures must not be described as a complete security
boundary until they control every effect path.
