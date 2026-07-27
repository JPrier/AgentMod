//! Business-facing canonical event identity data.

use agentmod_primitives::{CausationId, CorrelationId, EventId, TimestampMillis};
use agentmod_runtime_dependency::identity::{
    DependencyAllocateEventIdentityRequest, EventIdentityDependencyPort,
};
use thiserror::Error;

/// Data-owned allocation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AllocateEventIdentityDataRequest;

/// Stable business dataset for sealing one canonical event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventIdentityDataRecord {
    /// New canonical event identifier.
    pub event_id: EventId,
    /// New correlation identifier.
    pub correlation_id: CorrelationId,
    /// Default causation identifier.
    pub causation_id: CausationId,
    /// Portable timestamp.
    pub timestamp: TimestampMillis,
}

/// Data interface consumed by runtime logic.
pub trait EventIdentityDataPort {
    /// Allocates a fresh event identity.
    ///
    /// # Errors
    ///
    /// Returns a data-owned error when the external identity source is unavailable.
    fn allocate_event_identity(
        &self,
        request: AllocateEventIdentityDataRequest,
    ) -> Result<EventIdentityDataRecord, EventIdentityDataError>;
}

impl<D: EventIdentityDependencyPort> EventIdentityDataPort for super::RuntimeData<D> {
    fn allocate_event_identity(
        &self,
        _request: AllocateEventIdentityDataRequest,
    ) -> Result<EventIdentityDataRecord, EventIdentityDataError> {
        self.dependency
            .allocate_event_identity(DependencyAllocateEventIdentityRequest)
            .map(|value| EventIdentityDataRecord {
                event_id: value.event_id,
                correlation_id: value.correlation_id,
                causation_id: value.causation_id,
                timestamp: value.timestamp,
            })
            .map_err(|_| EventIdentityDataError::Unavailable)
    }
}

/// Event identity data failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EventIdentityDataError {
    /// External clock or ID source was unavailable.
    #[error("event identity data is unavailable")]
    Unavailable,
}
