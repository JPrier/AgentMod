//! Replaceable external memory storage adapters.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use fs2::FileExt;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

const RECORD_LIMIT: usize = 1024 * 1024;
const MAX_RESULTS: usize = 100;
const MAX_QUERY_BYTES: usize = 1024 * 1024;

/// Dependency-owned memory write request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyMemoryWriteRequest {
    /// Stable scope label.
    pub scope: String,
    /// Provenance label.
    pub source: String,
    /// Content to retain.
    pub content: String,
    /// External clock value.
    pub created_at_millis: i64,
    /// Canonical cross-restart duplicate key, when automatic writes are active.
    pub deduplication_key: Option<String>,
}

/// Dependency-owned written reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyMemoryWriteResponse {
    /// Provider-local immutable identifier.
    pub id: String,
    /// Whether this provider retained the item.
    pub retained: bool,
    /// Whether an identical canonical write was already retained.
    pub deduplicated: bool,
}

/// Dependency-owned memory query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyMemoryQueryRequest {
    /// Scope to search.
    pub scope: String,
    /// Natural-language query.
    pub query: String,
    /// Strict result bound.
    pub limit: usize,
}

/// Dependency-owned result.
#[derive(Clone, Debug, PartialEq)]
pub struct DependencyMemoryItem {
    /// Provider-local identifier.
    pub id: String,
    /// Scope.
    pub scope: String,
    /// Original provenance.
    pub source: String,
    /// Stored content.
    pub content: String,
    /// Relevance score where supported.
    pub score: Option<f64>,
    /// Creation time.
    pub created_at_millis: i64,
}

/// Narrow memory dependency contract.
pub trait MemoryDependencyPort {
    /// Writes one approved memory item.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryDependencyError`] for invalid input or storage failure.
    fn write(
        &self,
        request: DependencyMemoryWriteRequest,
    ) -> Result<DependencyMemoryWriteResponse, MemoryDependencyError>;

    /// Queries bounded memory results.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryDependencyError`] for invalid input or storage failure.
    fn query(
        &self,
        request: DependencyMemoryQueryRequest,
    ) -> Result<Vec<DependencyMemoryItem>, MemoryDependencyError>;

    /// Stable provider name.
    fn provider_name(&self) -> &'static str;
}

/// Provider that intentionally retains and retrieves nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoMemoryDependency;

impl MemoryDependencyPort for NoMemoryDependency {
    fn write(
        &self,
        request: DependencyMemoryWriteRequest,
    ) -> Result<DependencyMemoryWriteResponse, MemoryDependencyError> {
        validate_write(&request)?;
        Ok(DependencyMemoryWriteResponse {
            id: Uuid::now_v7().to_string(),
            retained: false,
            deduplicated: false,
        })
    }

    fn query(
        &self,
        request: DependencyMemoryQueryRequest,
    ) -> Result<Vec<DependencyMemoryItem>, MemoryDependencyError> {
        validate_query(&request)?;
        Ok(vec![])
    }

    fn provider_name(&self) -> &'static str {
        "none"
    }
}

/// Checksum-protected append-only JSONL memory adapter.
#[derive(Clone, Debug)]
pub struct FileMemoryDependency {
    path: PathBuf,
}

impl FileMemoryDependency {
    /// Creates a file-backed adapter. The parent is created lazily.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl MemoryDependencyPort for FileMemoryDependency {
    fn write(
        &self,
        request: DependencyMemoryWriteRequest,
    ) -> Result<DependencyMemoryWriteResponse, MemoryDependencyError> {
        validate_write(&request)?;
        ensure_parent(&self.path)?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .map_err(redacted_io)?;
        file.lock_exclusive().map_err(redacted_io)?;
        if let Some(key) = request.deduplication_key.as_deref() {
            if let Some(existing) = find_dedup_record(&file, request.scope.as_str(), key)? {
                FileExt::unlock(&file).map_err(redacted_io)?;
                return Ok(DependencyMemoryWriteResponse {
                    id: existing.id,
                    retained: true,
                    deduplicated: true,
                });
            }
        }
        let record = StoredFileRecord {
            schema_version: 2,
            id: Uuid::now_v7().to_string(),
            scope: request.scope,
            source: request.source,
            content: request.content,
            created_at_millis: request.created_at_millis,
            deduplication_key: request.deduplication_key,
            checksum: String::new(),
        }
        .seal()?;
        let mut bytes =
            serde_json::to_vec(&record).map_err(|_| MemoryDependencyError::Serialization)?;
        if bytes.len() > RECORD_LIMIT {
            return Err(MemoryDependencyError::ContentTooLarge);
        }
        bytes.push(b'\n');
        file.write_all(&bytes).map_err(redacted_io)?;
        file.sync_data().map_err(redacted_io)?;
        FileExt::unlock(&file).map_err(redacted_io)?;
        Ok(DependencyMemoryWriteResponse {
            id: record.id,
            retained: true,
            deduplicated: false,
        })
    }

    fn query(
        &self,
        request: DependencyMemoryQueryRequest,
    ) -> Result<Vec<DependencyMemoryItem>, MemoryDependencyError> {
        validate_query(&request)?;
        if !self.path.exists() {
            return Ok(vec![]);
        }
        let file = File::open(&self.path).map_err(redacted_io)?;
        file.lock_shared().map_err(redacted_io)?;
        let terms = query_terms(&request.query);
        let mut matches = Vec::new();
        for line in BufReader::new(&file).lines() {
            let line = line.map_err(redacted_io)?;
            if line.len() > RECORD_LIMIT {
                return Err(MemoryDependencyError::CorruptRecord);
            }
            let record: StoredFileRecord =
                serde_json::from_str(&line).map_err(|_| MemoryDependencyError::CorruptRecord)?;
            record.verify()?;
            if record.scope != request.scope {
                continue;
            }
            let lowercase = record.content.to_lowercase();
            let hits = terms
                .iter()
                .filter(|term| lowercase.contains(term.as_str()))
                .count();
            if hits == 0 {
                continue;
            }
            let hits = u32::try_from(hits).map_err(|_| MemoryDependencyError::InvalidInput)?;
            let term_count =
                u32::try_from(terms.len()).map_err(|_| MemoryDependencyError::InvalidInput)?;
            matches.push(DependencyMemoryItem {
                id: record.id,
                scope: record.scope,
                source: record.source,
                content: record.content,
                score: Some(f64::from(hits) / f64::from(term_count)),
                created_at_millis: record.created_at_millis,
            });
        }
        FileExt::unlock(&file).map_err(redacted_io)?;
        matches.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| right.created_at_millis.cmp(&left.created_at_millis))
                .then_with(|| left.id.cmp(&right.id))
        });
        matches.truncate(request.limit);
        Ok(matches)
    }

    fn provider_name(&self) -> &'static str {
        "file"
    }
}

/// `SQLite` FTS5 memory adapter.
#[derive(Clone, Debug)]
pub struct SqliteFtsMemoryDependency {
    path: PathBuf,
}

impl SqliteFtsMemoryDependency {
    /// Creates an adapter. Schema initialization occurs on first operation.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn connection(&self) -> Result<Connection, MemoryDependencyError> {
        ensure_parent(&self.path)?;
        let connection =
            Connection::open(&self.path).map_err(|_| MemoryDependencyError::Database)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
                    id UNINDEXED, scope UNINDEXED, source UNINDEXED, content,
                    created_at_millis UNINDEXED, tokenize='unicode61'
                 );
                 CREATE TABLE IF NOT EXISTS memory_dedup (
                    dedup_key TEXT PRIMARY KEY,
                    memory_id TEXT NOT NULL
                 );",
            )
            .map_err(|_| MemoryDependencyError::Database)?;
        Ok(connection)
    }
}

impl MemoryDependencyPort for SqliteFtsMemoryDependency {
    fn write(
        &self,
        request: DependencyMemoryWriteRequest,
    ) -> Result<DependencyMemoryWriteResponse, MemoryDependencyError> {
        validate_write(&request)?;
        let id = Uuid::now_v7().to_string();
        let connection = self.connection()?;
        if let Some(key) = request.deduplication_key.as_deref() {
            let mut existing = connection
                .prepare("SELECT memory_id FROM memory_dedup WHERE dedup_key = ?1")
                .map_err(|_| MemoryDependencyError::Database)?;
            let rows = existing
                .query_map(params![key], |row| row.get::<_, String>(0))
                .map_err(|_| MemoryDependencyError::Database)?;
            let rows = rows.collect::<Result<Vec<_>, _>>()
                .map_err(|_| MemoryDependencyError::Database)?;
            if let Some(existing_id) = rows.into_iter().next() {
                return Ok(DependencyMemoryWriteResponse {
                    id: existing_id,
                    retained: true,
                    deduplicated: true,
                });
            }
        }
        connection
            .execute(
                "INSERT INTO memory_fts
                 (id, scope, source, content, created_at_millis)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    id,
                    request.scope,
                    request.source,
                    request.content,
                    request.created_at_millis
                ],
            )
            .map_err(|_| MemoryDependencyError::Database)?;
        if let Some(key) = request.deduplication_key.as_deref() {
            connection
                .execute(
                    "INSERT INTO memory_dedup (dedup_key, memory_id) VALUES (?1, ?2)",
                    params![key, id],
                )
                .map_err(|_| MemoryDependencyError::Database)?;
        }
        Ok(DependencyMemoryWriteResponse {
            id,
            retained: true,
            deduplicated: false,
        })
    }

    fn query(
        &self,
        request: DependencyMemoryQueryRequest,
    ) -> Result<Vec<DependencyMemoryItem>, MemoryDependencyError> {
        validate_query(&request)?;
        let expression = fts_expression(&request.query)?;
        let limit =
            i64::try_from(request.limit).map_err(|_| MemoryDependencyError::InvalidLimit)?;
        let connection = self.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id, scope, source, content, created_at_millis, rank
                 FROM memory_fts
                 WHERE memory_fts MATCH ?1 AND scope = ?2
                 ORDER BY rank, created_at_millis DESC, id
                 LIMIT ?3",
            )
            .map_err(|_| MemoryDependencyError::Database)?;
        let rows = statement
            .query_map(params![expression, request.scope, limit], |row| {
                Ok(DependencyMemoryItem {
                    id: row.get(0)?,
                    scope: row.get(1)?,
                    source: row.get(2)?,
                    content: row.get(3)?,
                    created_at_millis: row.get(4)?,
                    score: row.get::<_, Option<f64>>(5)?.map(|rank| -rank),
                })
            })
            .map_err(|_| MemoryDependencyError::Database)?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|_| MemoryDependencyError::Database)
    }

    fn provider_name(&self) -> &'static str {
        "sqlite-fts"
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredFileRecord {
    schema_version: u32,
    id: String,
    scope: String,
    source: String,
    content: String,
    created_at_millis: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    deduplication_key: Option<String>,
    checksum: String,
}

impl StoredFileRecord {
    fn seal(mut self) -> Result<Self, MemoryDependencyError> {
        self.checksum = self.expected_checksum()?;
        Ok(self)
    }

    fn verify(&self) -> Result<(), MemoryDependencyError> {
        if !matches!(self.schema_version, 1 | 2)
            || self.id.parse::<Uuid>().is_err()
            || self.checksum != self.expected_checksum()?
        {
            return Err(MemoryDependencyError::CorruptRecord);
        }
        Ok(())
    }

    fn expected_checksum(&self) -> Result<String, MemoryDependencyError> {
        let tuple = if self.schema_version == 1 {
            serde_json::to_value((
                self.schema_version,
                &self.id,
                &self.scope,
                &self.source,
                &self.content,
                self.created_at_millis,
            ))
        } else {
            serde_json::to_value((
                self.schema_version,
                &self.id,
                &self.scope,
                &self.source,
                &self.content,
                self.created_at_millis,
                self.deduplication_key.as_deref().unwrap_or(""),
            ))
        }
        .map_err(|_| MemoryDependencyError::Serialization)?;
        let bytes = serde_json::to_vec(&tuple)
            .map_err(|_| MemoryDependencyError::Serialization)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }
}

fn find_dedup_record(
    file: &File,
    scope: &str,
    key: &str,
) -> Result<Option<StoredFileRecord>, MemoryDependencyError> {
    let mut found = None;
    for line in BufReader::new(file).lines() {
        let line = line.map_err(redacted_io)?;
        if line.len() > RECORD_LIMIT {
            return Err(MemoryDependencyError::CorruptRecord);
        }
        let record: StoredFileRecord =
            serde_json::from_str(&line).map_err(|_| MemoryDependencyError::CorruptRecord)?;
        record.verify()?;
        if record.scope == scope && record.deduplication_key.as_deref() == Some(key) {
            found = Some(record);
        }
    }
    Ok(found)
}

fn validate_write(request: &DependencyMemoryWriteRequest) -> Result<(), MemoryDependencyError> {
    if request.scope.is_empty() || request.source.is_empty() || request.content.trim().is_empty() {
        return Err(MemoryDependencyError::InvalidInput);
    }
    if request.content.len() > RECORD_LIMIT {
        return Err(MemoryDependencyError::ContentTooLarge);
    }
    Ok(())
}

fn validate_query(request: &DependencyMemoryQueryRequest) -> Result<(), MemoryDependencyError> {
    if request.scope.is_empty()
        || request.query.trim().is_empty()
        || request.query.len() > MAX_QUERY_BYTES
    {
        return Err(MemoryDependencyError::InvalidInput);
    }
    if request.limit == 0 || request.limit > MAX_RESULTS {
        return Err(MemoryDependencyError::InvalidLimit);
    }
    Ok(())
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn fts_expression(query: &str) -> Result<String, MemoryDependencyError> {
    let terms = query_terms(query);
    if terms.is_empty() {
        return Err(MemoryDependencyError::InvalidInput);
    }
    Ok(terms
        .iter()
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND "))
}

fn ensure_parent(path: &Path) -> Result<(), MemoryDependencyError> {
    let parent = path.parent().ok_or(MemoryDependencyError::InvalidPath)?;
    fs::create_dir_all(parent).map_err(redacted_io)
}

#[allow(clippy::needless_pass_by_value)]
fn redacted_io(_error: std::io::Error) -> MemoryDependencyError {
    MemoryDependencyError::Io
}

/// External memory adapter failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MemoryDependencyError {
    /// Request fields are empty or malformed.
    #[error("memory request is invalid")]
    InvalidInput,
    /// Result limit is outside the supported range.
    #[error("memory query limit is invalid")]
    InvalidLimit,
    /// Content exceeds the fixed bound.
    #[error("memory content exceeds the size limit")]
    ContentTooLarge,
    /// Configured path has no usable parent.
    #[error("memory path is invalid")]
    InvalidPath,
    /// Filesystem operation failed.
    #[error("memory filesystem operation failed")]
    Io,
    /// JSON encoding failed.
    #[error("memory serialization failed")]
    Serialization,
    /// A checksum/schema record is invalid.
    #[error("memory record is corrupt")]
    CorruptRecord,
    /// SQLite/FTS operation failed.
    #[error("memory database operation failed")]
    Database,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(provider: &impl MemoryDependencyPort, content: &str, at: i64) {
        provider
            .write(DependencyMemoryWriteRequest {
                scope: String::from("project:p1"),
                source: String::from("fixture"),
                content: content.into(),
                created_at_millis: at,
                deduplication_key: None,
            })
            .expect("write");
    }

    fn query(provider: &impl MemoryDependencyPort, text: &str) -> Vec<DependencyMemoryItem> {
        provider
            .query(DependencyMemoryQueryRequest {
                scope: String::from("project:p1"),
                query: text.into(),
                limit: 10,
            })
            .expect("query")
    }

    #[test]
    fn no_memory_validates_but_never_retains() {
        let response = NoMemoryDependency
            .write(DependencyMemoryWriteRequest {
                scope: String::from("session:s1"),
                source: String::from("fixture"),
                content: String::from("ignored"),
                created_at_millis: 1,
                deduplication_key: None,
            })
            .expect("write");
        assert!(!response.retained);
        assert!(query(&NoMemoryDependency, "ignored").is_empty());
    }

    #[test]
    fn file_memory_is_checksum_protected_and_ranked() {
        let root = tempfile::tempdir().expect("root");
        let provider = FileMemoryDependency::new(root.path().join("memory.jsonl"));
        write(&provider, "rust event platform", 1);
        write(&provider, "rust rust provider", 2);
        let matches = query(&provider, "rust provider");
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].content, "rust rust provider");
        let mut bytes = fs::read(root.path().join("memory.jsonl")).expect("read");
        bytes[20] ^= 1;
        fs::write(root.path().join("memory.jsonl"), bytes).expect("tamper");
        assert_eq!(
            provider.query(DependencyMemoryQueryRequest {
                scope: String::from("project:p1"),
                query: String::from("rust"),
                limit: 10,
            }),
            Err(MemoryDependencyError::CorruptRecord)
        );
    }

    #[test]
    fn sqlite_fts_memory_filters_scope_and_ranks() {
        let root = tempfile::tempdir().expect("root");
        let provider = SqliteFtsMemoryDependency::new(root.path().join("memory.sqlite3"));
        write(&provider, "canonical session journal", 1);
        write(&provider, "canonical session context", 2);
        let matches = query(&provider, "canonical context");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].content, "canonical session context");
        assert!(matches[0].score.is_some());
    }

    #[test]
    fn file_memory_deduplication_key_survives_adapter_recreation() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("memory.jsonl");
        let key = "canonical-write:abc123";
        let write = |provider: &dyn MemoryDependencyPort| {
            provider
                .write(DependencyMemoryWriteRequest {
                    scope: String::from("session:s1"),
                    source: String::from("auto"),
                    content: String::from("durable memory fact"),
                    created_at_millis: 10,
                    deduplication_key: Some(key.to_owned()),
                })
                .expect("write")
        };
        let first = write(&FileMemoryDependency::new(path.clone()));
        assert!(!first.deduplicated);
        // A new adapter over the same store simulates a restart.
        let second = write(&FileMemoryDependency::new(path));
        assert!(second.deduplicated);
        assert_eq!(first.id, second.id);
        let matches = FileMemoryDependency::new(root.path().join("memory.jsonl"))
            .query(DependencyMemoryQueryRequest {
                scope: String::from("session:s1"),
                query: String::from("durable memory fact"),
                limit: 10,
            })
            .expect("query");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].id, first.id);
    }

    #[test]
    fn sqlite_deduplication_key_is_scoped_and_idempotent_across_recreation() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("memory.sqlite3");
        let key = "canonical-write:xyz";
        let write = |provider: &dyn MemoryDependencyPort| {
            provider
                .write(DependencyMemoryWriteRequest {
                    scope: String::from("project:p2"),
                    source: String::from("auto"),
                    content: String::from("sqlite durable fact"),
                    created_at_millis: 11,
                    deduplication_key: Some(key.to_owned()),
                })
                .expect("write")
        };
        let first = write(&SqliteFtsMemoryDependency::new(path.clone()));
        assert!(!first.deduplicated);
        let second = write(&SqliteFtsMemoryDependency::new(path));
        assert!(second.deduplicated);
        assert_eq!(first.id, second.id);
    }

    #[test]
    fn legacy_schema_one_records_still_verify_after_dedup_field_lands() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("memory.jsonl");
        // A schema-v1 record must remain readable by the v2 reader.
        let legacy = StoredFileRecord {
            schema_version: 1,
            id: Uuid::now_v7().to_string(),
            scope: String::from("session:s1"),
            source: String::from("legacy"),
            content: String::from("old memory fact"),
            created_at_millis: 1,
            deduplication_key: None,
            checksum: String::new(),
        }
        .seal()
        .expect("seal");
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&legacy).expect("json")),
        )
        .expect("write");
        let matches = FileMemoryDependency::new(path)
            .query(DependencyMemoryQueryRequest {
                scope: String::from("session:s1"),
                query: String::from("old memory fact"),
                limit: 10,
            })
            .expect("query");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].source, "legacy");
    }
}
