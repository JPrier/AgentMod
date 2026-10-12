//! # agentmod-core
//!
//! The deterministic kernel of the AgentMod High-Level Design prototype.
//!
//! * [`shape`] / [`manifest`] — typed shapes; plugin manifests and session
//!   definitions (compiler.md §3–§4).
//! * [`compiler`] — manifests plus a definition in, one compiled graph out:
//!   the routing table and the session's revision (§5–§6), checked against
//!   the versioned [`core_manifest`].
//! * [`kernel`] — per-session serial dispatch over two FIFO lanes, publish
//!   classification (pipeline output vs cited deferred publish vs
//!   `start-session`), dispatcher commands, live config apply, recovery.
//! * [`record`] / [`projection`] — the append-only log and its derived
//!   session → events → invocations read model.
//! * [`context`] — attributed context fold with free checkpoints.
//! * [`stream`] — live provider streams outside the log: normalized frames,
//!   coalescing, recovery state, and a byte-bounded client broker.
//! * [`metrics`] — model-efficiency and control-plane metrics from a log.
//!
//! The crate performs no I/O, reads no clock, and has no LLM client: hosts feed
//! it timestamps and plugin results and execute the [`kernel::Effect`]s it
//! returns. The same crate runs natively and compiled to WebAssembly.

pub mod compiler;
pub mod context;
pub mod core_manifest;
mod embed;
pub mod kernel;
pub mod manifest;
pub mod metrics;
pub mod projection;
pub mod record;
pub mod shape;
pub mod stream;
pub mod types;

pub use compiler::{Compilation, compile};
pub use kernel::{Effect, Kernel};
