//! Business-facing replaceable memory datasets.

use agentmod_runtime_dependency::memory::{
    DependencyMemoryQueryRequest, DependencyMemoryWriteRequest, FileMemoryDependency,
    MemoryDependencyError, MemoryDependencyPort, NoMemoryDependency, SqliteFtsMemoryDependency,
};
use std::path::Path;
use thiserror::Error;

/// Data-owned memory scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryScopeRecord {
    /// Stable scope kind (`session`, `project`, `user`, or `runtime`).
    pub kind: String,
    /// Scope-specific identity, empty only for runtime scope.
    pub identity: String,
}

impl MemoryScopeRecord {
    fn external_key(&self) -> Result<String, MemoryDataError> {
        if !matches!(
            self.kind.as_str(),
            "session" | "project" | "user" | "runtime"
        ) || (self.kind != "runtime" && self.identity.is_empty())
        {
            return Err(MemoryDataError::InvalidScope);
        }
        Ok(if self.kind == "runtime" {
            String::from("runtime")
        } else {
            format!("{}:{}", self.kind, self.identity)
        })
    }
}

/// Data-owned write request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteMemoryDataRequest {
    /// Selected provider ID.
    pub provider: String,
    /// Memory scope.
    pub scope: MemoryScopeRecord,
    /// Provenance.
    pub source: String,
    /// Approved content.
    pub content: String,
    /// External creation time.
    pub created_at_millis: i64,
    /// Canonical cross-restart duplicate key, when automatic writes are active.
    pub deduplication_key: Option<String>,
}

/// Data-owned write result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteMemoryDataRecord {
    /// Provider name.
    pub provider: String,
    /// Provider-local reference.
    pub reference: String,
    /// Whether the selected provider retained it.
    pub retained: bool,
    /// Whether an identical canonical write was already retained.
    pub deduplicated: bool,
}

/// Data-owned retrieval request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrieveMemoryDataRequest {
    /// Selected provider ID.
    pub provider: String,
    /// Scope.
    pub scope: MemoryScopeRecord,
    /// Query.
    pub query: String,
    /// Strict result bound.
    pub limit: usize,
}

/// Data-owned retrieved item.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievedMemoryDataRecord {
    /// Provider name.
    pub provider: String,
    /// Provider-local reference.
    pub reference: String,
    /// Scope key.
    pub scope: String,
    /// Original source.
    pub source: String,
    /// Content.
    pub content: String,
    /// Relevance score.
    pub score: Option<f64>,
    /// Creation time.
    pub created_at_millis: i64,
}

/// Narrow memory data interface.
pub trait MemoryDataPort {
    /// Writes through the selected memory dependency.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryDataError`] for invalid normalized scope or dependency failure.
    fn write_memory(
        &self,
        request: WriteMemoryDataRequest,
    ) -> Result<WriteMemoryDataRecord, MemoryDataError>;

    /// Retrieves a provider-independent memory dataset.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryDataError`] for invalid normalized scope or dependency failure.
    fn retrieve_memory(
        &self,
        request: RetrieveMemoryDataRequest,
    ) -> Result<Vec<RetrievedMemoryDataRecord>, MemoryDataError>;
}

/// Data router over one selected provider.
#[derive(Clone, Debug)]
pub struct MemoryData<D> {
    dependency: D,
}

impl<D> MemoryData<D> {
    /// Creates a memory data router.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D> MemoryDataPort for MemoryData<D>
where
    D: MemoryDependencyPort,
{
    fn write_memory(
        &self,
        request: WriteMemoryDataRequest,
    ) -> Result<WriteMemoryDataRecord, MemoryDataError> {
        if request.provider != self.dependency.provider_name() {
            return Err(MemoryDataError::InvalidProvider);
        }
        let response = self
            .dependency
            .write(DependencyMemoryWriteRequest {
                scope: request.scope.external_key()?,
                source: request.source,
                content: request.content,
                created_at_millis: request.created_at_millis,
                deduplication_key: request.deduplication_key,
            })
            .map_err(MemoryDataError::Dependency)?;
        Ok(WriteMemoryDataRecord {
            provider: self.dependency.provider_name().into(),
            reference: response.id,
            retained: response.retained,
            deduplicated: response.deduplicated,
        })
    }

    fn retrieve_memory(
        &self,
        request: RetrieveMemoryDataRequest,
    ) -> Result<Vec<RetrievedMemoryDataRecord>, MemoryDataError> {
        if request.provider != self.dependency.provider_name() {
            return Err(MemoryDataError::InvalidProvider);
        }
        let scope = request.scope.external_key()?;
        let provider = self.dependency.provider_name();
        self.dependency
            .query(DependencyMemoryQueryRequest {
                scope,
                query: request.query,
                limit: request.limit,
            })
            .map_err(MemoryDataError::Dependency)?
            .into_iter()
            .map(|item| {
                if item.content.is_empty() || item.id.is_empty() {
                    return Err(MemoryDataError::InvalidDependencyRecord);
                }
                Ok(RetrievedMemoryDataRecord {
                    provider: provider.into(),
                    reference: item.id,
                    scope: item.scope,
                    source: item.source,
                    content: item.content,
                    score: item.score,
                    created_at_millis: item.created_at_millis,
                })
            })
            .collect()
    }
}

/// Data-layer router over the first-party replaceable memory dependencies.
#[derive(Clone, Debug)]
pub struct RuntimeMemoryData {
    none: MemoryData<NoMemoryDependency>,
    file: MemoryData<FileMemoryDependency>,
    sqlite_fts: MemoryData<SqliteFtsMemoryDependency>,
}

impl RuntimeMemoryData {
    /// Creates the first-party provider set below an explicit runtime-owned root.
    #[must_use]
    pub fn first_party(root: &Path) -> Self {
        Self {
            none: MemoryData::new(NoMemoryDependency),
            file: MemoryData::new(FileMemoryDependency::new(root.join("file.jsonl"))),
            sqlite_fts: MemoryData::new(SqliteFtsMemoryDependency::new(
                root.join("sqlite-fts.sqlite3"),
            )),
        }
    }

    fn selected(&self, provider: &str) -> Result<&dyn MemoryDataPort, MemoryDataError> {
        match provider {
            "none" => Ok(&self.none),
            "file" => Ok(&self.file),
            "sqlite-fts" | "sqlite-fts5" => Ok(&self.sqlite_fts),
            _ => Err(MemoryDataError::InvalidProvider),
        }
    }
}

impl MemoryDataPort for RuntimeMemoryData {
    fn write_memory(
        &self,
        request: WriteMemoryDataRequest,
    ) -> Result<WriteMemoryDataRecord, MemoryDataError> {
        let provider = request.provider.clone();
        self.selected(&provider)?.write_memory(request)
    }

    fn retrieve_memory(
        &self,
        request: RetrieveMemoryDataRequest,
    ) -> Result<Vec<RetrievedMemoryDataRecord>, MemoryDataError> {
        let provider = request.provider.clone();
        self.selected(&provider)?.retrieve_memory(request)
    }
}

/// Memory data-layer failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MemoryDataError {
    /// Provider ID is not part of the configured data router.
    #[error("memory provider is invalid or unavailable")]
    InvalidProvider,
    /// Scope is not a normalized supported scope.
    #[error("memory scope is invalid")]
    InvalidScope,
    /// Provider returned a malformed record.
    #[error("memory provider returned an invalid record")]
    InvalidDependencyRecord,
    /// Provider operation failed.
    #[error("memory dependency failed: {0}")]
    Dependency(MemoryDependencyError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_dependency::memory::{
        DependencyMemoryItem, DependencyMemoryWriteResponse,
    };

    use super::*;

    struct MockMemory {
        writes: RefCell<Vec<DependencyMemoryWriteRequest>>,
    }

    impl MemoryDependencyPort for MockMemory {
        fn write(
            &self,
            request: DependencyMemoryWriteRequest,
        ) -> Result<DependencyMemoryWriteResponse, MemoryDependencyError> {
            self.writes.borrow_mut().push(request);
            Ok(DependencyMemoryWriteResponse {
                id: String::from("m1"),
                retained: true,
                deduplicated: false,
            })
        }

        fn query(
            &self,
            request: DependencyMemoryQueryRequest,
        ) -> Result<Vec<DependencyMemoryItem>, MemoryDependencyError> {
            Ok(vec![DependencyMemoryItem {
                id: String::from("m1"),
                scope: request.scope,
                source: String::from("fixture"),
                content: String::from("event sourcing"),
                score: Some(0.8),
                created_at_millis: 1,
            }])
        }

        fn provider_name(&self) -> &'static str {
            "mock"
        }
    }

    #[test]
    fn maps_scope_and_normalizes_provider_records() {
        let data = MemoryData::new(MockMemory {
            writes: RefCell::new(vec![]),
        });
        let written = data
            .write_memory(WriteMemoryDataRequest {
                provider: String::from("mock"),
                scope: MemoryScopeRecord {
                    kind: String::from("project"),
                    identity: String::from("p1"),
                },
                source: String::from("user"),
                content: String::from("remember"),
                created_at_millis: 1,
                deduplication_key: None,
            })
            .expect("write");
        assert_eq!(written.provider, "mock");
        assert_eq!(data.dependency.writes.borrow()[0].scope, "project:p1");
        let retrieved = data
            .retrieve_memory(RetrieveMemoryDataRequest {
                provider: String::from("mock"),
                scope: MemoryScopeRecord {
                    kind: String::from("project"),
                    identity: String::from("p1"),
                },
                query: String::from("event"),
                limit: 5,
            })
            .expect("retrieve");
        assert_eq!(retrieved[0].content, "event sourcing");
        assert_eq!(retrieved[0].provider, "mock");
    }

    #[test]
    fn first_party_router_keeps_file_sqlite_and_none_distinct() {
        let root = tempfile::tempdir().expect("root");
        let data = RuntimeMemoryData::first_party(root.path());
        for provider in ["file", "sqlite-fts"] {
            data.write_memory(WriteMemoryDataRequest {
                provider: provider.into(),
                scope: MemoryScopeRecord {
                    kind: String::from("session"),
                    identity: String::from("s1"),
                },
                source: String::from("fixture"),
                content: format!("orchid retained by {provider}"),
                created_at_millis: 1,
                deduplication_key: None,
            })
            .expect("write");
            let retrieved = data
                .retrieve_memory(RetrieveMemoryDataRequest {
                    provider: provider.into(),
                    scope: MemoryScopeRecord {
                        kind: String::from("session"),
                        identity: String::from("s1"),
                    },
                    query: String::from("orchid"),
                    limit: 4,
                })
                .expect("retrieve");
            assert_eq!(retrieved.len(), 1);
            assert_eq!(retrieved[0].provider, provider);
        }
        assert!(
            data.retrieve_memory(RetrieveMemoryDataRequest {
                provider: String::from("none"),
                scope: MemoryScopeRecord {
                    kind: String::from("session"),
                    identity: String::from("s1"),
                },
                query: String::from("orchid"),
                limit: 4,
            })
            .expect("none")
            .is_empty()
        );
    }
}
