//! # agentmod-core
//!
//! The deterministic kernel of the AgentMod High-Level Design prototype.
//!
//! * [`compiler`] — config-compiled event bus: manifests' demand/supply
//!   declarations are validated into per-definition pipelines.
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
pub mod kernel;
pub mod manifest;
pub mod metrics;
pub mod projection;
pub mod record;
pub mod stream;
pub mod types;

pub use compiler::{Compilation, compile};
pub use kernel::{Effect, Kernel};
