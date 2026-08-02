//! Business-facing durable child-message receipt dataset.
//!
//! A child message is a committed canonical event in the existing worker
//! journal. This module deliberately does not create a mailbox, conversation
//! projection, or alternate idempotency registry.

use std::path::PathBuf;

use agentmod_event_model::{EventClassification, EventEnvelope, EventScope};
use agentmod_primitives::{ByteCount, ContentHash, EventId, Sequence, SessionId};
use agentmod_runtime_dependency::registry::{
    ChildMessageDependencyError, DependencyAppendChildMessageRequest, DependencyChildJournalHead,
    DependencyChildMessageReceipt, DependencyChildParentLink, SessionCatalogDependencyPort,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const CHILD_MESSAGE_EVENT_TYPE: &str = "child_agent.message_received";
const MAX_EVENT_BYTES: usize = 256 * 1024;
const MAX_ARTIFACT_REFERENCES: usize = 64;

/// Immutable parent link that authorizes a worker-message delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildMessageParentLinkData {
    /// Parent session that owns the target worker.
    pub parent_session_id: SessionId,
    /// Parent action that created the worker.
    pub parent_action_sequence: Sequence,
    /// Parent graph node that owns the worker.
    pub parent_graph_node_id: String,
    /// Exact runtime-owned child task.
    pub task_id: String,
}

/// Verified child journal head observed before message dispatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildMessageJournalHeadData {
    /// Last committed child-journal sequence.
    pub sequence: Sequence,
    /// Full journal-frame checksum at that sequence.
    pub checksum: ContentHash,
}

/// Data-owned append-or-replay request for one child message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendChildMessageDataRequest {
    /// Root containing canonical session directories.
    pub sessions_root: PathBuf,
    /// Immutable owning parent/worker link.
    pub parent_link: ChildMessageParentLinkData,
    /// Target child session.
    pub child_session_id: SessionId,
    /// Stable message identity. It is intentionally also the canonical event ID.
    pub message_id: EventId,
    /// Canonical child-journal sequence assigned to the receipt.
    pub message_sequence: Sequence,
    /// Exact child tail expected before the append.
    pub expected_head: ChildMessageJournalHeadData,
    /// Sealed committed event. Its payload remains typed by runtime logic.
    pub event: EventEnvelope<Value>,
    /// Runtime-computed hash of the bounded typed message payload.
    pub payload_hash: ContentHash,
    /// Runtime-computed hash of ordered canonical artifact references.
    pub artifact_references_hash: ContentHash,
}

/// Result of a fresh append or exact duplicate replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendedChildMessageDataRecord {
    /// Whether this call replayed an already durable exact receipt.
    pub replayed: bool,
    /// Canonical receipt sequence.
    pub sequence: Sequence,
    /// Stable canonical message identity.
    pub message_id: EventId,
    /// Verified event-envelope checksum.
    pub envelope_checksum: ContentHash,
    /// Verified complete journal-frame checksum.
    pub journal_checksum: ContentHash,
    /// Previous journal-frame checksum.
    pub previous_journal_checksum: Option<ContentHash>,
    /// Receipt frame byte offset.
    pub offset: ByteCount,
    /// Verified journal byte length after the receipt.
    pub journal_bytes: ByteCount,
}

/// Narrow child-message storage interface consumed by runtime logic.
pub trait ChildMessageDataPort {
    /// Atomically appends a canonical child-message receipt or replays an exact
    /// prior receipt for the same message identity.
    ///
    /// # Errors
    ///
    /// Returns [`ChildMessageDataError`] for invalid canonical bindings, or for
    /// a mapped dependency refusal.
    fn append_child_message(
        &self,
        request: AppendChildMessageDataRequest,
    ) -> Result<AppendedChildMessageDataRecord, ChildMessageDataError>;
}

impl<D> ChildMessageDataPort for super::RuntimeData<D>
where
    D: SessionCatalogDependencyPort,
{
    fn append_child_message(
        &self,
        request: AppendChildMessageDataRequest,
    ) -> Result<AppendedChildMessageDataRecord, ChildMessageDataError> {
        append_child_message(&self.dependency, request)
    }
}

fn append_child_message<D: SessionCatalogDependencyPort>(
    dependency: &D,
    request: AppendChildMessageDataRequest,
) -> Result<AppendedChildMessageDataRecord, ChildMessageDataError> {
    validate_request(&request)?;
    let event_json = serde_json::to_vec(&request.event).map_err(|error| {
        ChildMessageDataError::EventSerialization {
            message: error.to_string(),
        }
    })?;
    if event_json.len() > MAX_EVENT_BYTES {
        return Err(ChildMessageDataError::EventTooLarge);
    }
    let response = dependency
        .append_child_message(DependencyAppendChildMessageRequest {
            sessions_root: request.sessions_root,
            parent_link: DependencyChildParentLink {
                parent_session_id: request.parent_link.parent_session_id.to_string(),
                parent_action_sequence: request.parent_link.parent_action_sequence.get(),
                parent_graph_node_id: request.parent_link.parent_graph_node_id,
                task_id: request.parent_link.task_id,
            },
            child_session_id: request.child_session_id.to_string(),
            message_id: request.message_id.to_string(),
            message_sequence: request.message_sequence.get(),
            expected_head: DependencyChildJournalHead {
                sequence: request.expected_head.sequence.get(),
                checksum: request.expected_head.checksum.to_string(),
            },
            canonical_event_json: event_json,
            canonical_event_checksum: request.event.integrity_checksum.to_string(),
            payload_hash: request.payload_hash.to_string(),
            artifact_references_hash: request.artifact_references_hash.to_string(),
        })
        .map_err(|error| map_dependency_error(&error))?;
    map_response(response)
}

fn validate_request(request: &AppendChildMessageDataRequest) -> Result<(), ChildMessageDataError> {
    if request.sessions_root.as_os_str().is_empty()
        || request.parent_link.parent_graph_node_id.trim().is_empty()
        || request.parent_link.task_id.trim().is_empty()
        || request.event.metadata.event_id != request.message_id
        || request.event.metadata.sequence != request.message_sequence
        || request.event.metadata.scope != EventScope::Session(request.child_session_id)
        || request.event.metadata.event_type != CHILD_MESSAGE_EVENT_TYPE
        || request.event.metadata.classification != EventClassification::Committed
        || request.event.metadata.artifacts.len() > MAX_ARTIFACT_REFERENCES
    {
        return Err(ChildMessageDataError::InvalidRequest);
    }
    let expected_sequence = request
        .expected_head
        .sequence
        .checked_next()
        .map_err(|_| ChildMessageDataError::SequenceOverflow)?;
    if request.message_sequence != expected_sequence {
        return Err(ChildMessageDataError::SequenceMismatch {
            expected: expected_sequence,
            actual: request.message_sequence,
        });
    }
    request
        .event
        .verify()
        .map_err(|error| ChildMessageDataError::EventIntegrity {
            message: error.to_string(),
        })?;
    let payload = serde_json::to_vec(&request.event.payload).map_err(|error| {
        ChildMessageDataError::EventSerialization {
            message: error.to_string(),
        }
    })?;
    let artifacts = serde_json::to_vec(&request.event.metadata.artifacts).map_err(|error| {
        ChildMessageDataError::EventSerialization {
            message: error.to_string(),
        }
    })?;
    if ContentHash::digest(&payload) != request.payload_hash
        || ContentHash::digest(&artifacts) != request.artifact_references_hash
    {
        return Err(ChildMessageDataError::ProjectionHashMismatch);
    }
    Ok(())
}

fn map_response(
    response: DependencyChildMessageReceipt,
) -> Result<AppendedChildMessageDataRecord, ChildMessageDataError> {
    let sequence =
        Sequence::new(response.sequence).map_err(|_| ChildMessageDataError::InvalidReceipt)?;
    let message_id = response
        .message_id
        .parse()
        .map_err(|_| ChildMessageDataError::InvalidReceipt)?;
    let envelope_checksum = response
        .canonical_event_checksum
        .parse()
        .map_err(|_| ChildMessageDataError::InvalidReceipt)?;
    let journal_checksum = response
        .journal_checksum
        .parse()
        .map_err(|_| ChildMessageDataError::InvalidReceipt)?;
    let previous_journal_checksum = response
        .previous_journal_checksum
        .map(|checksum| checksum.parse())
        .transpose()
        .map_err(|_| ChildMessageDataError::InvalidReceipt)?;
    Ok(AppendedChildMessageDataRecord {
        replayed: response.replayed,
        sequence,
        message_id,
        envelope_checksum,
        journal_checksum,
        previous_journal_checksum,
        offset: ByteCount::new(response.offset),
        journal_bytes: ByteCount::new(response.journal_bytes),
    })
}

fn map_dependency_error(error: &ChildMessageDependencyError) -> ChildMessageDataError {
    let category = match error {
        ChildMessageDependencyError::InvalidRequest
        | ChildMessageDependencyError::InvalidCanonicalEvent => {
            ChildMessageDependencyFailure::InvalidRequest
        }
        ChildMessageDependencyError::SessionUnavailable
        | ChildMessageDependencyError::InvalidSessionMetadata
        | ChildMessageDependencyError::ParentLinkMismatch => {
            ChildMessageDependencyFailure::Identity
        }
        ChildMessageDependencyError::ChildNotWritable => ChildMessageDependencyFailure::Lifecycle,
        ChildMessageDependencyError::MissingChildHead
        | ChildMessageDependencyError::StaleChildHead { .. }
        | ChildMessageDependencyError::ConcurrentJournalAdvance => {
            ChildMessageDependencyFailure::StaleHead
        }
        ChildMessageDependencyError::MessageSequenceMismatch { .. }
        | ChildMessageDependencyError::SequenceOverflow => ChildMessageDependencyFailure::Sequence,
        ChildMessageDependencyError::ConflictingDuplicate => {
            ChildMessageDependencyFailure::ConflictingDuplicate
        }
        ChildMessageDependencyError::Journal(_) => ChildMessageDependencyFailure::Journal,
        ChildMessageDependencyError::Io(_) => ChildMessageDependencyFailure::Access,
    };
    ChildMessageDataError::Dependency {
        category,
        message: error.to_string(),
    }
}

/// Stable dependency-failure category exposed to runtime logic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildMessageDependencyFailure {
    /// Request or canonical envelope was invalid.
    InvalidRequest,
    /// Parent or child identity was not the requested immutable link.
    Identity,
    /// Child session is not writable.
    Lifecycle,
    /// Child journal tail no longer matches dispatch.
    StaleHead,
    /// Receipt sequence was invalid.
    Sequence,
    /// Same message identity had different canonical content.
    ConflictingDuplicate,
    /// Existing canonical journal rejected the operation.
    Journal,
    /// Filesystem or lock access failed.
    Access,
}

/// Data-owned child-message storage failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ChildMessageDataError {
    /// Request fields or canonical event bindings were invalid.
    #[error("child message data request is invalid")]
    InvalidRequest,
    /// The requested sequence did not follow the supplied journal head.
    #[error("child message sequence mismatch (expected {expected:?}, actual {actual:?})")]
    SequenceMismatch {
        /// Required receipt sequence.
        expected: Sequence,
        /// Supplied receipt sequence.
        actual: Sequence,
    },
    /// Child sequence arithmetic overflowed.
    #[error("child message sequence overflow")]
    SequenceOverflow,
    /// Sealed canonical event integrity was invalid.
    #[error("child message event integrity failed: {message}")]
    EventIntegrity {
        /// Redacted event-model diagnostic.
        message: String,
    },
    /// The data boundary could not serialize a canonical value.
    #[error("child message event serialization failed: {message}")]
    EventSerialization {
        /// Redacted serialization diagnostic.
        message: String,
    },
    /// Canonical event bytes exceeded the fixed receipt bound.
    #[error("child message event exceeds its fixed bound")]
    EventTooLarge,
    /// Payload or artifact projection did not match its requested hash.
    #[error("child message projection hash mismatch")]
    ProjectionHashMismatch,
    /// Dependency returned a receipt outside the canonical primitive formats.
    #[error("child message dependency returned an invalid receipt")]
    InvalidReceipt,
    /// Dependency failure normalized to a stable category.
    #[error("child message dependency failed ({category:?}): {message}")]
    Dependency {
        /// Stable category independent of the dependency implementation.
        category: ChildMessageDependencyFailure,
        /// Redacted dependency diagnostic.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Mutex};

    use agentmod_event_model::{EventMetadata, EventOrigin};
    use agentmod_primitives::{CausationId, CorrelationId, TimestampMillis, Version};
    use agentmod_runtime_dependency::registry::{
        DependencyCreateBranchRequest, DependencyCreateChildSessionRequest,
        DependencyCreateSessionRequest, DependencyCreatedSession, DependencyListSessionsRequest,
        DependencyPrepareSessionRequest, DependencyPreparedSession, DependencySessionMetadata,
        SessionCatalogDependencyError,
    };

    use super::*;

    #[derive(Default)]
    struct MockCatalog {
        request: Mutex<Option<DependencyAppendChildMessageRequest>>,
    }

    impl SessionCatalogDependencyPort for MockCatalog {
        fn prepare_session(
            &self,
            _request: DependencyPrepareSessionRequest,
        ) -> Result<DependencyPreparedSession, SessionCatalogDependencyError> {
            unreachable!("not used by child-message mapping")
        }

        fn create_session(
            &self,
            _request: DependencyCreateSessionRequest,
        ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
            unreachable!("not used by child-message mapping")
        }

        fn create_branch(
            &self,
            _request: DependencyCreateBranchRequest,
        ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
            unreachable!("not used by child-message mapping")
        }

        fn create_child_session(
            &self,
            _request: DependencyCreateChildSessionRequest,
        ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
            unreachable!("not used by child-message mapping")
        }

        fn append_child_message(
            &self,
            request: DependencyAppendChildMessageRequest,
        ) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
            *self.request.lock().expect("request lock") = Some(request.clone());
            Ok(DependencyChildMessageReceipt {
                replayed: false,
                sequence: request.message_sequence,
                message_id: request.message_id,
                canonical_event_checksum: request.canonical_event_checksum,
                journal_checksum: ContentHash::digest(b"journal").to_string(),
                previous_journal_checksum: Some(request.expected_head.checksum),
                offset: 100,
                journal_bytes: 200,
            })
        }

        fn list_sessions(
            &self,
            _request: DependencyListSessionsRequest,
        ) -> Result<Vec<DependencySessionMetadata>, SessionCatalogDependencyError> {
            unreachable!("not used by child-message mapping")
        }
    }

    #[test]
    fn data_maps_a_sealed_child_message_to_the_exact_dependency_contract() {
        let parent: SessionId = "00000000-0000-0000-0000-000000000001"
            .parse()
            .expect("parent");
        let child: SessionId = "00000000-0000-0000-0000-000000000002"
            .parse()
            .expect("child");
        let message_id: EventId = "00000000-0000-0000-0000-000000000003"
            .parse()
            .expect("message");
        let payload = serde_json::json!({"kind":"instruction","body":"continue"});
        let event = EventEnvelope::seal(
            EventMetadata {
                event_id: message_id,
                scope: EventScope::Session(child),
                sequence: Sequence::new(3).expect("sequence"),
                timestamp: TimestampMillis::new(100),
                event_type: CHILD_MESSAGE_EVENT_TYPE.into(),
                event_version: Version::new(1, 0),
                correlation_id: "00000000-0000-0000-0000-000000000004"
                    .parse::<CorrelationId>()
                    .expect("correlation"),
                causation_id: "00000000-0000-0000-0000-000000000005"
                    .parse::<CausationId>()
                    .expect("causation"),
                parent_graph_node_id: Some(String::from("send-message")),
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: vec![],
                classification: EventClassification::Committed,
            },
            payload.clone(),
        )
        .expect("seal");
        let data = super::super::RuntimeData::new(MockCatalog::default());
        let appended = data
            .append_child_message(AppendChildMessageDataRequest {
                sessions_root: PathBuf::from("sessions"),
                parent_link: ChildMessageParentLinkData {
                    parent_session_id: parent,
                    parent_action_sequence: Sequence::new(17).expect("sequence"),
                    parent_graph_node_id: String::from("send-message"),
                    task_id: String::from("task-1"),
                },
                child_session_id: child,
                message_id,
                message_sequence: Sequence::new(3).expect("sequence"),
                expected_head: ChildMessageJournalHeadData {
                    sequence: Sequence::new(2).expect("sequence"),
                    checksum: ContentHash::digest(b"head"),
                },
                payload_hash: ContentHash::digest(&serde_json::to_vec(&payload).expect("payload")),
                artifact_references_hash: ContentHash::digest(b"[]"),
                event,
            })
            .expect("append");
        assert!(!appended.replayed);
        assert_eq!(appended.message_id, message_id);
        let request = data
            .dependency
            .request
            .lock()
            .expect("request lock")
            .clone()
            .expect("mapped request");
        assert_eq!(request.parent_link.parent_session_id, parent.to_string());
        assert_eq!(request.child_session_id, child.to_string());
        assert_eq!(request.message_id, message_id.to_string());
        assert_eq!(request.message_sequence, 3);
        assert_eq!(request.expected_head.sequence, 2);
    }
}
