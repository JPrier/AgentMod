//! The core manifest: the events the runtime itself emits, versioned with the
//! runtime (compiler.md §2). The compiler treats it exactly like a plugin's
//! manifest; the runtime is the graph's boundary node.

use crate::manifest::{EmitDecl, Manifest};
use crate::shape::{KeyDecl, Keys, Shape};
use crate::types::Lane;

/// Name of the boundary node: the runtime's own node in every compiled graph.
pub const BOUNDARY: &str = "core";

/// The runtime version this build implements (definitions name it in `runtime`).
pub const RUNTIME_VERSION: &str = "0.1";

/// Runtime versions whose core manifest this build carries.
pub const SUPPORTED_RUNTIMES: &[&str] = &[RUNTIME_VERSION];

pub const SESSION_STARTED: &str = "session-started";
pub const CONFIG_APPLIED: &str = "config-applied";
/// An `exactly_one_handler` family member had no handler, or its handler
/// failed without answering.
pub const DISPATCH_FAILED: &str = "dispatch-failed";
/// A `start-session` whose definition failed to compile; delivered to the
/// calling invocation's session with the diagnostics.
pub const SESSION_START_FAILED: &str = "session-start-failed";

fn key(shape: &str) -> KeyDecl {
    KeyDecl::parse(shape, false).unwrap_or_else(|_| KeyDecl::required(Shape::Null))
}
fn opt(shape: &str) -> KeyDecl {
    KeyDecl::parse(shape, true).unwrap_or_else(|_| KeyDecl::optional(Shape::Null))
}

fn keys(pairs: Vec<(&str, KeyDecl)>) -> Keys {
    pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

/// The core manifest for a runtime version, if this build supports it.
#[must_use]
pub fn core_manifest(version: &str) -> Option<Manifest> {
    if !SUPPORTED_RUNTIMES.contains(&version) {
        return None;
    }
    Some(Manifest {
        name: BOUNDARY.into(),
        version: version.into(),
        description: "The runtime's own events (boundary node).".into(),
        emit: vec![
            EmitDecl {
                event: SESSION_STARTED.into(),
                lane: Lane::Normal,
                deferred: false,
                exactly_one_handler: false,
                keys: keys(vec![
                    ("definition", key("string")),
                    ("revision", key("string")),
                    ("fork_of", opt("object")),
                    ("parent_session", opt("string")),
                ]),
            },
            EmitDecl {
                event: CONFIG_APPLIED.into(),
                lane: Lane::Interrupt,
                deferred: false,
                exactly_one_handler: false,
                keys: keys(vec![
                    ("revision", key("string")),
                    ("previous", key("string")),
                ]),
            },
            EmitDecl {
                event: DISPATCH_FAILED.into(),
                lane: Lane::Normal,
                deferred: false,
                exactly_one_handler: false,
                keys: keys(vec![
                    ("reason", key("\"no-handler\" | \"handler-failed\"")),
                    ("event_id", key("string")),
                    ("event_name", key("string")),
                    ("family", key("string")),
                    ("member", key("string")),
                    ("payload", key("object")),
                    ("plugin", opt("string")),
                    ("error", opt("string")),
                    ("handled", opt("array<string>")),
                ]),
            },
            EmitDecl {
                event: SESSION_START_FAILED.into(),
                lane: Lane::Normal,
                deferred: false,
                exactly_one_handler: false,
                keys: keys(vec![
                    ("definition", key("string")),
                    ("error", key("string")),
                    ("diagnostics", key("array<object>")),
                ]),
            },
        ],
        ..Default::default()
    })
}
