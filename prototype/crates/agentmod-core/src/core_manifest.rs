//! The core manifest: the events the runtime itself emits, versioned with the
//! runtime (compiler.md §2). The compiler treats it exactly like a plugin's
//! manifest; the runtime is the graph's boundary node.

use crate::manifest::{EmitDecl, Manifest, normal_only};
use crate::shape::{KeyDecl, Keys, Shape};
use crate::types::Lane;

/// Name of the boundary node: the runtime's own node in every compiled graph.
pub const BOUNDARY: &str = "core";

/// The runtime version this build implements (definitions name it in `runtime`).
pub const RUNTIME_VERSION: &str = "0.1";

/// Runtime versions whose core manifest this build carries.
pub const SUPPORTED_RUNTIMES: &[&str] = &[RUNTIME_VERSION];

/// The first event of every session, emitted only by the core when a
/// `start-session` call succeeds; it carries the caller's input (§9).
pub const SESSION_STARTED: &str = "session-started";
pub const CONFIG_APPLIED: &str = "config-applied";
/// A plugin invocation failed (it reported an error, contradicted a
/// declaration, or gave up after retries): the failed event, the plugin, and
/// the error, so plugins can react (compiler.md §3.4 step 7, §7).
pub const INVOCATION_FAILED: &str = "invocation-failed";
/// A `start-session` whose definition failed to compile; delivered to the
/// calling invocation's session with the diagnostics (the call's error carries
/// them too). No session is created.
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

/// The fixed shape of a `start-session` input, as `session-started` carries
/// it: the text a session starts from (a user's first message, a cron job's
/// prompt, a delegated task), any attachments, and caller-defined structured
/// data (a delegation record, a parent link). Every key is optional.
#[must_use]
pub fn input_shape() -> KeyDecl {
    KeyDecl::optional(Shape::Object(Some(keys(vec![
        ("text", opt("string")),
        ("attachments", opt("array<object>")),
        ("data", opt("object")),
    ]))))
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
                lanes: normal_only(),
                deferred: false,
                keys: keys(vec![
                    ("definition", key("string")),
                    ("revision", key("string")),
                    ("fork_of", opt("object")),
                    ("parent_session", opt("string")),
                    ("input", input_shape()),
                ]),
            },
            EmitDecl {
                event: CONFIG_APPLIED.into(),
                lanes: vec![Lane::Interrupt],
                deferred: false,
                keys: keys(vec![
                    ("revision", key("string")),
                    ("previous", key("string")),
                ]),
            },
            EmitDecl {
                event: INVOCATION_FAILED.into(),
                lanes: normal_only(),
                deferred: false,
                keys: keys(vec![
                    ("event_id", key("string")),
                    ("event_name", key("string")),
                    ("payload", key("object")),
                    ("plugin", key("string")),
                    ("invocation_id", key("string")),
                    ("mode", key("\"blocking\" | \"async\"")),
                    ("error", key("string")),
                ]),
            },
            EmitDecl {
                event: SESSION_START_FAILED.into(),
                lanes: normal_only(),
                deferred: false,
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
