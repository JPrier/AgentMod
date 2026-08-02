//! Generic canonical event structures.
//!
//! Storage, runtime workflows, provider semantics, and tool behavior deliberately live
//! outside this crate.

use std::{fmt, str::FromStr};

use agentmod_primitives::{
    ArtifactId, CausationId, ContentHash, CorrelationId, EventId, Sequence, SessionId,
    TimestampMillis, Version,
};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use thiserror::Error;

const BLAKE3_ARTIFACT_PREFIX: &str = "blake3:";
const BLAKE3_HEX_LENGTH: usize = 64;
const MAX_ARTIFACT_IDENTIFIER_BYTES: usize = BLAKE3_ARTIFACT_PREFIX.len() + BLAKE3_HEX_LENGTH;

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
    /// Validated portable artifact identifier.
    pub id: ArtifactIdentifier,
    /// Hash of exact stored bytes.
    pub content_hash: ContentHash,
}

/// Validated artifact identifier carried by canonical event envelopes.
///
/// UUID identifiers remain compatible with events written before artifacts
/// became content-addressed. New artifact storage uses the exact
/// `blake3:<64 lowercase hexadecimal characters>` identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ArtifactIdentifier(String);

impl ArtifactIdentifier {
    /// Parses a portable event artifact identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactIdentifierError`] for empty, oversized, malformed, or
    /// path-like identifiers.
    pub fn parse(value: impl Into<String>) -> Result<Self, ArtifactIdentifierError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_ARTIFACT_IDENTIFIER_BYTES {
            return Err(ArtifactIdentifierError);
        }
        if let Ok(uuid) = ArtifactId::from_str(&value) {
            return Ok(Self(uuid.to_string()));
        }
        let Some(hash) = value.strip_prefix(BLAKE3_ARTIFACT_PREFIX) else {
            return Err(ArtifactIdentifierError);
        };
        if hash.len() != BLAKE3_HEX_LENGTH
            || !hash
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(ArtifactIdentifierError);
        }
        Ok(Self(value))
    }

    /// Returns the exact portable identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<ArtifactId> for ArtifactIdentifier {
    fn from(value: ArtifactId) -> Self {
        Self(value.to_string())
    }
}

impl FromStr for ArtifactIdentifier {
    type Err = ArtifactIdentifierError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for ArtifactIdentifier {
    type Error = ArtifactIdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl fmt::Display for ArtifactIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'de> Deserialize<'de> for ArtifactIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// Invalid canonical event artifact identifier.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("artifact identifier must be a UUID or blake3:<64 lowercase hex characters>")]
pub struct ArtifactIdentifierError;

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

    #[test]
    fn uuid_artifact_identifier_preserves_event_json_compatibility() {
        let uuid = id(
            "018f6f83-7b80-7000-8000-000000000005",
            ArtifactId::from_uuid,
        );
        let identifier = ArtifactIdentifier::from(uuid);

        assert_eq!(
            serde_json::to_string(&identifier).expect("identifier serializes"),
            "\"018f6f83-7b80-7000-8000-000000000005\""
        );
        assert_eq!(
            serde_json::from_str::<ArtifactIdentifier>("\"018F6F83-7B80-7000-8000-000000000005\"")
                .expect("legacy UUID parses"),
            identifier
        );
    }

    #[test]
    fn content_addressed_artifact_identifier_round_trips_exactly() {
        let expected = format!("blake3:{}", "a5".repeat(32));
        let identifier = ArtifactIdentifier::parse(expected.clone()).expect("valid BLAKE3 ID");
        let json = serde_json::to_vec(&identifier).expect("identifier serializes");
        let decoded =
            serde_json::from_slice::<ArtifactIdentifier>(&json).expect("identifier deserializes");

        assert_eq!(identifier.as_str(), expected);
        assert_eq!(decoded, identifier);
        assert_eq!(decoded.to_string(), expected);
    }

    #[test]
    fn invalid_artifact_identifiers_fail_closed() {
        let invalid = [
            String::new(),
            String::from("../artifact"),
            String::from("folder/artifact"),
            String::from(r"folder\artifact"),
            String::from("blake3:"),
            format!("blake3:{}", "a".repeat(63)),
            format!("blake3:{}", "a".repeat(65)),
            format!("blake3:{}", "A".repeat(64)),
            format!("sha256:{}", "a".repeat(64)),
        ];

        for value in invalid {
            assert!(
                ArtifactIdentifier::parse(value.clone()).is_err(),
                "{value:?} must be rejected"
            );
            let json = serde_json::to_string(&value).expect("test string serializes");
            assert!(
                serde_json::from_str::<ArtifactIdentifier>(&json).is_err(),
                "{value:?} must fail during event decoding"
            );
        }
    }
}
