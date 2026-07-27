//! Generic canonical event structures.
//!
//! Storage, runtime workflows, provider semantics, and tool behavior deliberately live
//! outside this crate.

use agentmod_primitives::{
    ArtifactId, CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId,
    TimestampMillis, Version,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

/// Scope addressed by a canonical event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum EventScope {
    /// Runtime-global state.
    Runtime,
    /// A single persistent session.
    Session(SessionId),
}

/// Authority classification of an event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventClassification {
    /// Proposed action that has not executed.
    Proposal,
    /// Interceptor or policy decision over a proposal.
    Decision,
    /// Immutable result that changes canonical projections.
    Committed,
    /// Non-authoritative observation, such as a streaming delta.
    Observation,
}

/// Origin of an event without importing subsystem implementation types.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventOrigin {
    /// Stable subsystem name.
    pub subsystem: String,
    /// Optional plugin identifier.
    pub plugin: Option<String>,
}

/// Reference to immutable content stored outside the journal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactReference {
    /// Opaque artifact identifier.
    pub id: ArtifactId,
    /// Hash of exact stored bytes.
    pub content_hash: ContentHash,
}

/// Metadata required for every canonical event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventMetadata {
    /// Globally opaque event ID.
    pub event_id: EventId,
    /// Runtime or session scope.
    pub scope: EventScope,
    /// Strictly monotonic sequence within scope.
    pub sequence: Sequence,
    /// Timestamp supplied by the runtime clock dependency.
    pub timestamp: TimestampMillis,
    /// Stable semantic event name.
    pub event_type: String,
    /// Version of the typed event payload.
    pub event_version: Version,
    /// Correlates a complete user-visible operation.
    pub correlation_id: CorrelationId,
    /// Identifies the direct cause.
    pub causation_id: CausationId,
    /// Optional declarative graph node.
    pub parent_graph_node_id: Option<String>,
    /// Producing subsystem/plugin.
    pub origin: EventOrigin,
    /// Envelope schema version.
    pub schema_version: Version,
    /// Large or binary content referenced by this event.
    pub artifacts: Vec<ArtifactReference>,
    /// Proposal/decision/committed/observation authority.
    pub classification: EventClassification,
}

/// A checksummed event envelope with a typed payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventEnvelope<T> {
    /// Common canonical metadata.
    pub metadata: EventMetadata,
    /// Event-specific typed content.
    pub payload: T,
    /// BLAKE3 checksum of canonical JSON for metadata and payload.
    pub integrity_checksum: ContentHash,
}

#[derive(Serialize)]
struct ChecksumInput<'a, T> {
    metadata: &'a EventMetadata,
    payload: &'a T,
}

impl<T> EventEnvelope<T>
where
    T: Serialize,
{
    /// Creates a sealed envelope after hashing metadata and payload.
    ///
    /// # Errors
    ///
    /// Returns [`EventModelError::Serialization`] if the typed content cannot be
    /// represented as canonical JSON.
    pub fn seal(metadata: EventMetadata, payload: T) -> Result<Self, EventModelError> {
        let integrity_checksum = checksum(&metadata, &payload)?;
        Ok(Self {
            metadata,
            payload,
            integrity_checksum,
        })
    }

    /// Verifies the checksum against the current metadata and payload.
    ///
    /// # Errors
    ///
    /// Returns [`EventModelError::Serialization`] if content cannot be encoded or
    /// [`EventModelError::ChecksumMismatch`] when integrity validation fails.
    pub fn verify(&self) -> Result<(), EventModelError> {
        let actual = checksum(&self.metadata, &self.payload)?;
        if actual == self.integrity_checksum {
            Ok(())
        } else {
            Err(EventModelError::ChecksumMismatch {
                expected: self.integrity_checksum,
                actual,
            })
        }
    }
}

impl<T> EventEnvelope<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Decodes JSON and verifies integrity before returning the event.
    ///
    /// # Errors
    ///
    /// Returns [`EventModelError`] when decoding or checksum validation fails.
    pub fn from_verified_json(bytes: &[u8]) -> Result<Self, EventModelError> {
        let envelope: Self = serde_json::from_slice(bytes)?;
        envelope.verify()?;
        Ok(envelope)
    }
}

fn checksum<T: Serialize>(
    metadata: &EventMetadata,
    payload: &T,
) -> Result<ContentHash, EventModelError> {
    // Hash the value representation so struct field declaration order cannot change
    // integrity across an explicit typed-to-layer-owned JSON mapping.
    let canonical = serde_json::to_value(ChecksumInput { metadata, payload })?;
    let bytes = serde_json::to_vec(&canonical)?;
    Ok(ContentHash::digest(&bytes))
}

/// Marks typed content as a proposal without giving it execution authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Proposal<T>(pub T);

/// Marks typed content as a blocking decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct DecisionRecord<T>(pub T);

/// Marks typed content as a committed canonical result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Committed<T>(pub T);

/// Marks typed content as a non-authoritative observation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Observation<T>(pub T);

/// Event construction or integrity failure.
#[derive(Debug, Error)]
pub enum EventModelError {
    /// JSON serialization or decoding failed.
    #[error("event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// Stored and recomputed checksums differ.
    #[error("event checksum mismatch: expected {expected}, computed {actual}")]
    ChecksumMismatch {
        /// Checksum persisted with the record.
        expected: ContentHash,
        /// Checksum computed from current content.
        actual: ContentHash,
    },
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use uuid::Uuid;

    fn id<T>(value: &str, wrap: fn(Uuid) -> T) -> T {
        wrap(Uuid::from_str(value).expect("fixture UUID"))
    }

    fn metadata() -> EventMetadata {
        EventMetadata {
            event_id: id("018f6f83-7b80-7000-8000-000000000001", EventId::from_uuid),
            scope: EventScope::Session(id(
                "018f6f83-7b80-7000-8000-000000000002",
                SessionId::from_uuid,
            )),
            sequence: Sequence::FIRST,
            timestamp: TimestampMillis::new(1_700_000_000_000),
            event_type: "session.created".into(),
            event_version: Version::new(1, 0),
            correlation_id: id(
                "018f6f83-7b80-7000-8000-000000000003",
                CorrelationId::from_uuid,
            ),
            causation_id: id(
                "018f6f83-7b80-7000-8000-000000000004",
                CausationId::from_uuid,
            ),
            parent_graph_node_id: None,
            origin: EventOrigin {
                subsystem: "runtime".into(),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts: Vec::new(),
            classification: EventClassification::Committed,
        }
    }

    #[test]
    fn checksum_detects_payload_tampering() {
        let mut envelope = EventEnvelope::seal(metadata(), serde_json::json!({"workspace": "a"}))
            .expect("event seals");
        envelope.payload = serde_json::json!({"workspace": "b"});
        assert!(matches!(
            envelope.verify(),
            Err(EventModelError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn serialized_event_verifies_on_decode() {
        let envelope =
            EventEnvelope::seal(metadata(), Proposal(String::from("safe"))).expect("event seals");
        let json = serde_json::to_vec(&envelope).expect("event serializes");
        assert_eq!(
            EventEnvelope::<Proposal<String>>::from_verified_json(&json).expect("event verifies"),
            envelope
        );
    }
}
