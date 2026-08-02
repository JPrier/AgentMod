//! Deterministic replay reducer for canonical graph state.
//!
//! Applying the same event sequence always reconstructs an identical state;
//! values are embedded in events, so reconstruction never calls external
//! systems. The reducer validates version, hash, type, and scope consistency
//! and fails closed on tampering.

use agentmod_primitives::SessionId;
use thiserror::Error;

use crate::{declare::DeclarationSet, event::GraphStateEvent, state::GraphState};

/// Replay failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ReducerError {
    /// No initialization event preceded the sequence.
    #[error("graph-state replay requires a variables-initialized event first")]
    MissingInitialization,
    /// An initialization event repeated after state was established.
    #[error("graph-state replay received a second initialization event")]
    RepeatedInitialization,
    /// The initialization session does not match the reducer session.
    #[error("graph-state replay session mismatch")]
    SessionMismatch,
    /// The event cannot be applied exactly to the current state.
    #[error("graph-state replay event is inconsistent: {detail}")]
    Inconsistent {
        /// Deterministic diagnostic.
        detail: String,
    },
}

/// Applies canonical graph-state events in order.
#[derive(Clone, Debug)]
pub struct GraphStateReducer {
    state: GraphState,
    initialized: bool,
}

impl GraphStateReducer {
    /// Creates a reducer with the given session identity.
    #[must_use]
    pub fn new(session_id: SessionId) -> Self {
        Self {
            state: GraphState::empty(session_id),
            initialized: false,
        }
    }

    /// Applies one canonical event.
    ///
    /// # Errors
    ///
    /// Returns [`ReducerError`] when the event cannot be applied exactly.
    pub fn apply(&mut self, event: &GraphStateEvent) -> Result<(), ReducerError> {
        if let GraphStateEvent::VariablesInitialized {
            session_id,
            declarations,
            ..
        } = event
        {
            if self.initialized {
                return Err(ReducerError::RepeatedInitialization);
            }
            if *session_id != self.state.session_id() {
                return Err(ReducerError::SessionMismatch);
            }
            let mut set = DeclarationSet::new();
            for declaration in declarations {
                set.insert(declaration.clone())
                    .map_err(|error| ReducerError::Inconsistent {
                        detail: error.to_string(),
                    })?;
            }
            self.state = GraphState::new(self.state.session_id(), set)
                .map_err(|error| ReducerError::Inconsistent {
                    detail: error.to_string(),
                })?
                .0;
            self.initialized = true;
            return Ok(());
        }
        if !self.initialized {
            return Err(ReducerError::MissingInitialization);
        }
        self.state
            .apply_event(event)
            .map_err(|error| ReducerError::Inconsistent {
                detail: error.to_string(),
            })
    }

    /// Returns the reconstructed state.
    #[must_use]
    pub const fn state(&self) -> &GraphState {
        &self.state
    }

    /// Returns whether initialization has been applied.
    #[must_use]
    pub const fn initialized(&self) -> bool {
        self.initialized
    }
}
