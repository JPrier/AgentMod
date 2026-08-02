//! Canonical typed graph variables and execution-budget accounting.
//!
//! This pure core crate provides the deterministic graph-state substrate for
//! arbitrary execution:
//!
//! - [`value`] — bounded canonical values (optional, boolean, bounded
//!   integers, fixed-point decimals, strings, enum tags, lists, maps, session/
//!   child/task/node/continuation IDs, artifact and result references,
//!   approval decisions, timestamps, durations).
//! - [`declare`] — variable declarations with stable names, types, scopes,
//!   producers, consumers, mutability, size, classification, merge policy, and
//!   defaults. Undeclared reads and writes are rejected.
//! - [`state`] — scoped canonical state with deterministic reads, writes,
//!   branch-local scopes, and policy-driven parallel merges.
//! - [`event`] — canonical events that are the only mutation surface.
//! - [`reduce`] — deterministic replay reducer; identical events reconstruct
//!   identical values without external calls.
//! - [`budget`] — canonical accounting for every budget dimension with
//!   explicit known/estimated/unknown semantics, pricing provenance, child
//!   rollup, and exact restart reconstruction.
//! - [`expression`] — deterministic condition evaluation from canonical
//!   variables and counters with stable eligible/ineligible/missing/invalid
//!   outcomes.
//! - [`parallel`] — machine-validated parallel write safety.
//! - [`port`] — narrow read ports consumed by generic dispatch.
//!
//! The crate contains no external SDK, I/O, clock, or frontend types.

pub mod budget;
pub mod declare;
pub mod event;
pub mod expression;
pub mod parallel;
pub mod port;
pub mod reduce;
pub mod state;
pub mod value;
