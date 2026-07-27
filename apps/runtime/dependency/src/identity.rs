//! External clock and identifier allocation for canonical runtime events.

use std::time::{SystemTime, UNIX_EPOCH};

use agentmod_primitives::{CausationId, CorrelationId, EventId, TimestampMillis};
use thiserror::Error;
use uuid::Uuid;

/// Dependency-owned request for one event identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyAllocateEventIdentityRequest;

/// Dependency-owned clock/random result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyEventIdentity {
    /// New event identifier.
    pub event_id: EventId,
    /// New correlation identifier.
    pub correlation_id: CorrelationId,
    /// New default causation identifier.
    pub causation_id: CausationId,
    /// Current dependency clock value.
    pub timestamp: TimestampMillis,
}

/// Narrow external identity dependency.
pub trait EventIdentityDependencyPort {
    /// Obtains new identifiers and a portable wall-clock timestamp.
    ///
    /// # Errors
    ///
    /// Returns a classified dependency error when the clock cannot be represented.
    fn allocate_event_identity(
        &self,
        request: DependencyAllocateEventIdentityRequest,
    ) -> Result<DependencyEventIdentity, EventIdentityDependencyError>;
}

impl EventIdentityDependencyPort for crate::LocalRuntimeDependencies {
    fn allocate_event_identity(
        &self,
        _request: DependencyAllocateEventIdentityRequest,
    ) -> Result<DependencyEventIdentity, EventIdentityDependencyError> {
        allocate()
    }
}

/// Allocates using the operating-system clock and random source.
///
/// # Errors
///
/// Returns [`EventIdentityDependencyError`] when the clock is outside the
/// portable timestamp representation.
pub fn allocate() -> Result<DependencyEventIdentity, EventIdentityDependencyError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| EventIdentityDependencyError::ClockBeforeEpoch)?;
    let timestamp = i64::try_from(elapsed.as_millis())
        .map_err(|_| EventIdentityDependencyError::ClockOverflow)?;
    Ok(DependencyEventIdentity {
        event_id: EventId::from_uuid(Uuid::now_v7()),
        correlation_id: CorrelationId::from_uuid(Uuid::now_v7()),
        causation_id: CausationId::from_uuid(Uuid::now_v7()),
        timestamp: TimestampMillis::new(timestamp),
    })
}

/// External identity allocation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EventIdentityDependencyError {
    /// The platform clock predates the portable epoch.
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
    /// The platform clock exceeds the portable representation.
    #[error("system clock cannot be represented")]
    ClockOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_identity_has_distinct_nonzero_values() {
        let first = allocate().expect("identity");
        let second = allocate().expect("identity");
        assert_ne!(first.event_id, second.event_id);
        assert!(first.timestamp.get() > 0);
    }
}
