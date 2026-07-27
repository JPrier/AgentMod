//! Persistence-independent continuation state and exactly-once local transitions.

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use agentmod_primitives::{ContinuationId, TimestampMillis};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const PENDING: u8 = 0;
const RESUMED: u8 = 1;
const CANCELLED: u8 = 2;
const EXPIRED: u8 = 3;

/// Condition that makes a deferred continuation eligible for runtime evaluation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum WakeCondition {
    /// Explicit user or supervisor decision.
    Manual,
    /// Wall-clock threshold, interpreted by an injected scheduler clock.
    At(TimestampMillis),
    /// A committed runtime event matching a stable type and optional selector.
    RuntimeEvent {
        /// Stable event type.
        event_type: String,
        /// Constrained expression evaluated by runtime logic.
        selector: Option<String>,
    },
    /// A supervised process emits matching bounded output.
    ProcessOutput {
        /// Runtime process identifier.
        process_id: String,
        /// Literal or constrained regular expression selected by runtime policy.
        pattern: String,
    },
}

/// Serializable continuation snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContinuationSnapshot {
    /// Opaque continuation ID.
    pub id: ContinuationId,
    /// Current terminal/pending state.
    pub state: ContinuationState,
    /// Wake eligibility representation.
    pub wake_condition: WakeCondition,
    /// Optional expiry interpreted by runtime logic.
    pub expires_at: Option<TimestampMillis>,
}

/// Persistence-independent continuation state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationState {
    /// May transition exactly once.
    Pending,
    /// Successfully claimed for resumption.
    Resumed,
    /// Explicitly cancelled.
    Cancelled,
    /// Expired before resumption.
    Expired,
}

impl ContinuationState {
    fn as_raw(self) -> u8 {
        match self {
            Self::Pending => PENDING,
            Self::Resumed => RESUMED,
            Self::Cancelled => CANCELLED,
            Self::Expired => EXPIRED,
        }
    }

    fn from_raw(value: u8) -> Self {
        match value {
            RESUMED => Self::Resumed,
            CANCELLED => Self::Cancelled,
            EXPIRED => Self::Expired,
            _ => Self::Pending,
        }
    }
}

/// Shared atomic resume-once gate.
#[derive(Clone, Debug)]
pub struct ResumeOnce {
    state: Arc<AtomicU8>,
}

impl ResumeOnce {
    /// Creates a gate restored from durable state.
    #[must_use]
    pub fn from_state(state: ContinuationState) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(state.as_raw())),
        }
    }

    /// Creates a pending gate.
    #[must_use]
    pub fn pending() -> Self {
        Self::from_state(ContinuationState::Pending)
    }

    /// Reads the current state.
    #[must_use]
    pub fn state(&self) -> ContinuationState {
        ContinuationState::from_raw(self.state.load(Ordering::Acquire))
    }

    /// Claims the pending continuation for resumption.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationTransitionError::AlreadyTerminal`] if another terminal
    /// transition already won.
    pub fn try_resume(&self) -> Result<(), ContinuationTransitionError> {
        self.transition(ContinuationState::Resumed)
    }

    /// Cancels a still-pending continuation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationTransitionError::AlreadyTerminal`] if the continuation
    /// is no longer pending.
    pub fn try_cancel(&self) -> Result<(), ContinuationTransitionError> {
        self.transition(ContinuationState::Cancelled)
    }

    /// Expires a still-pending continuation.
    ///
    /// # Errors
    ///
    /// Returns [`ContinuationTransitionError::AlreadyTerminal`] if the continuation
    /// is no longer pending.
    pub fn try_expire(&self) -> Result<(), ContinuationTransitionError> {
        self.transition(ContinuationState::Expired)
    }

    fn transition(&self, target: ContinuationState) -> Result<(), ContinuationTransitionError> {
        self.state
            .compare_exchange(
                PENDING,
                target.as_raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|observed| ContinuationTransitionError::AlreadyTerminal {
                current: ContinuationState::from_raw(observed),
                requested: target,
            })
    }
}

impl Default for ResumeOnce {
    fn default() -> Self {
        Self::pending()
    }
}

/// Invalid continuation state transition.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ContinuationTransitionError {
    /// A terminal continuation cannot transition again.
    #[error("continuation is already {current:?}; cannot transition to {requested:?}")]
    AlreadyTerminal {
        /// Existing state.
        current: ContinuationState,
        /// Requested state.
        requested: ContinuationState,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

    use super::*;

    #[test]
    fn resumed_continuation_cannot_resume_again() {
        let gate = ResumeOnce::pending();
        gate.try_resume().expect("first resume succeeds");
        assert_eq!(gate.state(), ContinuationState::Resumed);
        assert_eq!(
            gate.try_resume(),
            Err(ContinuationTransitionError::AlreadyTerminal {
                current: ContinuationState::Resumed,
                requested: ContinuationState::Resumed,
            })
        );
    }

    #[test]
    fn only_one_concurrent_resume_wins() {
        let gate = ResumeOnce::pending();
        let winners = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..32)
            .map(|_| {
                let gate = gate.clone();
                let winners = Arc::clone(&winners);
                std::thread::spawn(move || {
                    if gate.try_resume().is_ok() {
                        winners.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("thread joins");
        }
        assert_eq!(winners.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(gate.state(), ContinuationState::Resumed);
    }

    #[test]
    fn cancel_and_expire_are_terminal() {
        let cancelled = ResumeOnce::pending();
        cancelled.try_cancel().expect("cancel");
        assert_eq!(cancelled.state(), ContinuationState::Cancelled);
        assert!(cancelled.try_expire().is_err());

        let expired = ResumeOnce::pending();
        expired.try_expire().expect("expire");
        assert_eq!(expired.state(), ContinuationState::Expired);
        assert!(expired.try_cancel().is_err());
    }
}
