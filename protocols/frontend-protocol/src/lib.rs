//! Presentation-neutral frontend capability contracts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Capabilities a connected frontend can render or answer.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrontendCapabilities {
    /// Explicit presentation features.
    pub supported: BTreeSet<FrontendCapability>,
}

/// One frontend presentation feature.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendCapability {
    /// Interactive approval prompts.
    Approvals,
    /// Inline image display or attachment.
    Images,
    /// Unified diff rendering.
    Diffs,
    /// Terminal/PTY attachment.
    Terminal,
    /// Structured task display.
    Tasks,
    /// Child-agent panels.
    ChildAgents,
}

/// Frontend lifecycle notification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", content = "value", rename_all = "snake_case")]
pub enum FrontendLifecycle {
    /// Frontend connected with declared presentation capabilities.
    Connected {
        /// Stable connection-local frontend ID.
        frontend_id: String,
        /// Presentation features supported by the peer.
        capabilities: FrontendCapabilities,
    },
    /// Frontend disconnected; sessions continue in runtime.
    Disconnected {
        /// Stable connection-local frontend ID.
        frontend_id: String,
    },
}
