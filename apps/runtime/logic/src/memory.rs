//! Memory write and retrieval business semantics.

use agentmod_primitives::{ByteCount, EventId, TimestampMillis};
use agentmod_runtime_data::memory::{
    MemoryDataError, MemoryDataPort, MemoryScopeRecord, RetrieveMemoryDataRequest,
    WriteMemoryDataRequest,
};
use thiserror::Error;

const MAX_MEMORY_CONTENT: usize = 1024 * 1024;
const MAX_QUERY_LENGTH: usize = 1024 * 1024;
const MAX_RETRIEVAL_ITEMS: usize = 100;

/// Logic-owned memory scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemoryScope {
    /// One session.
    Session(String),
    /// One project.
    Project(String),
    /// One user.
    User(String),
    /// Runtime-wide.
    Runtime,
}

/// Proof that the runtime proposal/policy pipeline approved a memory write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryWriteAuthorization {
    /// Write passed all mandatory interception and policy phases.
    Approved,
    /// Write has not passed the mandatory phases.
    Unapproved,
}

/// Logic-owned write command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteMemoryCommand {
    /// Style-selected provider.
    pub provider: String,
    /// Scope.
    pub scope: MemoryScope,
    /// Provenance.
    pub source: String,
    /// Content.
    pub content: String,
    /// Approved creation time from a data/dependency clock.
    pub created_at: TimestampMillis,
    /// Mandatory policy outcome.
    pub authorization: MemoryWriteAuthorization,
}

/// Logic-owned write result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteMemoryResult {
    /// Provider.
    pub provider: String,
    /// Provider-local reference.
    pub reference: String,
    /// Whether the provider retained it.
    pub retained: bool,
}

/// Logic-owned retrieval command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrieveMemoryCommand {
    /// Style-selected provider.
    pub provider: String,
    /// Scope.
    pub scope: MemoryScope,
    /// Query.
    pub query: String,
    /// Maximum items.
    pub limit: usize,
    /// Canonical event recording this injection.
    pub injection_event: EventId,
}

/// Logic-owned injected item with complete provenance.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievedMemoryItem {
    /// Provider.
    pub provider: String,
    /// Query that selected this item.
    pub query: String,
    /// Stable scope key.
    pub scope: String,
    /// Original source.
    pub source: String,
    /// Provider-local reference.
    pub reference: String,
    /// Content.
    pub content: String,
    /// Provider relevance.
    pub score: Option<f64>,
    /// Creation time.
    pub created_at: TimestampMillis,
    /// Canonical context injection event.
    pub injection_event: EventId,
    /// Provider-visible size contribution.
    pub size: ByteCount,
}

/// Narrow memory business interface.
pub trait MemoryLogicPort {
    /// Persists an already-approved write.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryLogicError`] for missing authorization, invalid business
    /// input, or translated data failure.
    fn write_memory(
        &self,
        command: WriteMemoryCommand,
    ) -> Result<WriteMemoryResult, MemoryLogicError>;

    /// Retrieves bounded memory with injection provenance.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryLogicError`] for invalid input, oversized provider data,
    /// or translated data failure.
    fn retrieve_memory(
        &self,
        command: RetrieveMemoryCommand,
    ) -> Result<Vec<RetrievedMemoryItem>, MemoryLogicError>;
}

/// Memory business coordinator over data only.
#[derive(Clone, Debug)]
pub struct MemoryLogic<D> {
    data: D,
}

impl<D> MemoryLogic<D> {
    /// Creates a memory coordinator.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D> MemoryLogicPort for MemoryLogic<D>
where
    D: MemoryDataPort,
{
    fn write_memory(
        &self,
        command: WriteMemoryCommand,
    ) -> Result<WriteMemoryResult, MemoryLogicError> {
        if command.authorization != MemoryWriteAuthorization::Approved {
            return Err(MemoryLogicError::WriteNotApproved);
        }
        validate_text(&command.source, &command.content)?;
        let record = self
            .data
            .write_memory(WriteMemoryDataRequest {
                provider: command.provider,
                scope: to_data_scope(command.scope)?,
                source: command.source,
                content: command.content,
                created_at_millis: command.created_at.get(),
            })
            .map_err(MemoryLogicError::Data)?;
        Ok(WriteMemoryResult {
            provider: record.provider,
            reference: record.reference,
            retained: record.retained,
        })
    }

    fn retrieve_memory(
        &self,
        command: RetrieveMemoryCommand,
    ) -> Result<Vec<RetrievedMemoryItem>, MemoryLogicError> {
        if command.query.trim().is_empty() || command.query.len() > MAX_QUERY_LENGTH {
            return Err(MemoryLogicError::InvalidQuery);
        }
        if command.limit == 0 || command.limit > MAX_RETRIEVAL_ITEMS {
            return Err(MemoryLogicError::InvalidLimit);
        }
        let query = command.query;
        let limit = command.limit;
        self.data
            .retrieve_memory(RetrieveMemoryDataRequest {
                provider: command.provider,
                scope: to_data_scope(command.scope)?,
                query: query.clone(),
                limit,
            })
            .map_err(MemoryLogicError::Data)?
            .into_iter()
            // A dependency is outside the logic trust boundary. Enforce the
            // style-selected item cap again even when a provider ignores the
            // requested limit.
            .take(limit)
            .map(|record| {
                let size = u64::try_from(record.content.len())
                    .map_err(|_| MemoryLogicError::SizeOverflow)?;
                if size > MAX_MEMORY_CONTENT as u64 {
                    return Err(MemoryLogicError::ProviderContentTooLarge);
                }
                Ok(RetrievedMemoryItem {
                    provider: record.provider,
                    query: query.clone(),
                    scope: record.scope,
                    source: record.source,
                    reference: record.reference,
                    content: record.content,
                    score: record.score,
                    created_at: TimestampMillis::new(record.created_at_millis),
                    injection_event: command.injection_event,
                    size: ByteCount::new(size),
                })
            })
            .collect()
    }
}

fn validate_text(source: &str, content: &str) -> Result<(), MemoryLogicError> {
    if source.trim().is_empty() || content.trim().is_empty() {
        return Err(MemoryLogicError::InvalidWrite);
    }
    if content.len() > MAX_MEMORY_CONTENT {
        return Err(MemoryLogicError::ProviderContentTooLarge);
    }
    Ok(())
}

fn to_data_scope(scope: MemoryScope) -> Result<MemoryScopeRecord, MemoryLogicError> {
    let (kind, identity) = match scope {
        MemoryScope::Session(value) => ("session", value),
        MemoryScope::Project(value) => ("project", value),
        MemoryScope::User(value) => ("user", value),
        MemoryScope::Runtime => ("runtime", String::new()),
    };
    if kind != "runtime" && identity.trim().is_empty() {
        return Err(MemoryLogicError::InvalidScope);
    }
    Ok(MemoryScopeRecord {
        kind: kind.into(),
        identity,
    })
}

/// Memory business failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MemoryLogicError {
    /// Writes cannot bypass the proposal/policy pipeline.
    #[error("memory write was not approved")]
    WriteNotApproved,
    /// Scope identity is empty.
    #[error("memory scope is invalid")]
    InvalidScope,
    /// Source/content is invalid.
    #[error("memory write is invalid")]
    InvalidWrite,
    /// Query is empty or too long.
    #[error("memory query is invalid")]
    InvalidQuery,
    /// Limit is outside the fixed bound.
    #[error("memory retrieval limit is invalid")]
    InvalidLimit,
    /// Provider content is too large for injection.
    #[error("memory content exceeds the injection bound")]
    ProviderContentTooLarge,
    /// Content byte size cannot be represented.
    #[error("memory content size overflow")]
    SizeOverflow,
    /// Data operation failed.
    #[error("memory data failed: {0}")]
    Data(MemoryDataError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_data::memory::{RetrievedMemoryDataRecord, WriteMemoryDataRecord};
    use uuid::Uuid;

    use super::*;

    struct MockData {
        writes: RefCell<Vec<WriteMemoryDataRequest>>,
    }

    impl MemoryDataPort for MockData {
        fn write_memory(
            &self,
            request: WriteMemoryDataRequest,
        ) -> Result<WriteMemoryDataRecord, MemoryDataError> {
            self.writes.borrow_mut().push(request);
            Ok(WriteMemoryDataRecord {
                provider: String::from("mock"),
                reference: String::from("m1"),
                retained: true,
            })
        }

        fn retrieve_memory(
            &self,
            _request: RetrieveMemoryDataRequest,
        ) -> Result<Vec<RetrievedMemoryDataRecord>, MemoryDataError> {
            Ok(vec![RetrievedMemoryDataRecord {
                provider: String::from("mock"),
                reference: String::from("m1"),
                scope: String::from("project:p1"),
                source: String::from("fixture"),
                content: String::from("remember this"),
                score: Some(0.5),
                created_at_millis: 1,
            }])
        }
    }

    #[test]
    fn unapproved_write_never_reaches_data() {
        let logic = MemoryLogic::new(MockData {
            writes: RefCell::new(vec![]),
        });
        assert_eq!(
            logic.write_memory(WriteMemoryCommand {
                provider: String::from("mock"),
                scope: MemoryScope::Runtime,
                source: String::from("fixture"),
                content: String::from("blocked"),
                created_at: TimestampMillis::new(1),
                authorization: MemoryWriteAuthorization::Unapproved,
            }),
            Err(MemoryLogicError::WriteNotApproved)
        );
        assert!(logic.data.writes.borrow().is_empty());
    }

    #[test]
    fn retrieval_records_full_injection_provenance() {
        let logic = MemoryLogic::new(MockData {
            writes: RefCell::new(vec![]),
        });
        let injection = EventId::from_uuid(Uuid::from_u128(8));
        let items = logic
            .retrieve_memory(RetrieveMemoryCommand {
                provider: String::from("mock"),
                scope: MemoryScope::Project(String::from("p1")),
                query: String::from("remember"),
                limit: 5,
                injection_event: injection,
            })
            .expect("retrieve");
        assert_eq!(items[0].provider, "mock");
        assert_eq!(items[0].query, "remember");
        assert_eq!(items[0].scope, "project:p1");
        assert_eq!(items[0].injection_event, injection);
        assert_eq!(items[0].size, ByteCount::new(13));
    }

    struct OverReturningData;

    impl MemoryDataPort for OverReturningData {
        fn write_memory(
            &self,
            _request: WriteMemoryDataRequest,
        ) -> Result<WriteMemoryDataRecord, MemoryDataError> {
            unreachable!("fixture does not write memory")
        }

        fn retrieve_memory(
            &self,
            _request: RetrieveMemoryDataRequest,
        ) -> Result<Vec<RetrievedMemoryDataRecord>, MemoryDataError> {
            Ok((0..5)
                .map(|index| RetrievedMemoryDataRecord {
                    provider: String::from("mock"),
                    reference: format!("m{index}"),
                    scope: String::from("runtime"),
                    source: String::from("fixture"),
                    content: format!("record {index}"),
                    score: None,
                    created_at_millis: i64::from(index),
                })
                .collect())
        }
    }

    #[test]
    fn retrieval_reenforces_item_limit_against_over_returning_provider() {
        let items = MemoryLogic::new(OverReturningData)
            .retrieve_memory(RetrieveMemoryCommand {
                provider: String::from("mock"),
                scope: MemoryScope::Runtime,
                query: String::from("bounded"),
                limit: 2,
                injection_event: EventId::from_uuid(Uuid::from_u128(9)),
            })
            .expect("retrieve");
        assert_eq!(
            items
                .iter()
                .map(|item| item.reference.as_str())
                .collect::<Vec<_>>(),
            ["m0", "m1"]
        );
    }
}
