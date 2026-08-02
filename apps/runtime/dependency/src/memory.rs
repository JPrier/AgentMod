//! Replaceable external memory storage adapters.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
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
}

/// Dependency-owned written reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyMemoryWriteResponse {
    /// Provider-local immutable identifier.
    pub id: String,
    /// Whether this provider retained the item.
    pub retained: bool,
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
            id: memory_record_id(&request)?,
            retained: false,
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
    post_persist_delay: Duration,
}

impl FileMemoryDependency {
    /// Creates a file-backed adapter. The parent is created lazily.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            post_persist_delay: Duration::ZERO,
        }
    }

    /// Delays a newly persisted terminal response after the exact record is
    /// durable. This supports deterministic process crash-cut validation.
    #[must_use]
    pub fn with_post_persist_delay(mut self, delay: Duration) -> Self {
        self.post_persist_delay = delay;
        self
    }

    /// Returns the configured deterministic post-persist delay.
    #[must_use]
    pub const fn post_persist_delay(&self) -> Duration {
        self.post_persist_delay
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
        repair_file_tail(&self.path, &mut file)?;
        let id = memory_record_id(&request)?;
        file.seek(SeekFrom::Start(0)).map_err(redacted_io)?;
        for line in BufReader::new(&file).lines() {
            let line = line.map_err(redacted_io)?;
            if line.len() > RECORD_LIMIT {
                return Err(MemoryDependencyError::CorruptRecord);
            }
            let existing: StoredFileRecord =
                serde_json::from_str(&line).map_err(|_| MemoryDependencyError::CorruptRecord)?;
            existing.verify()?;
            if existing.id == id {
                if existing.scope != request.scope
                    || existing.source != request.source
                    || existing.content != request.content
                    || existing.created_at_millis != request.created_at_millis
                {
                    return Err(MemoryDependencyError::CorruptRecord);
                }
                FileExt::unlock(&file).map_err(redacted_io)?;
                return Ok(DependencyMemoryWriteResponse { id, retained: true });
            }
        }
        let record = StoredFileRecord {
            schema_version: 1,
            id,
            scope: request.scope,
            source: request.source,
            content: request.content,
            created_at_millis: request.created_at_millis,
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
        if !self.post_persist_delay.is_zero() {
            std::thread::sleep(self.post_persist_delay);
        }
        Ok(DependencyMemoryWriteResponse {
            id: record.id,
            retained: true,
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
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(redacted_io)?;
        file.lock_exclusive().map_err(redacted_io)?;
        repair_file_tail(&self.path, &mut file)?;
        file.seek(SeekFrom::Start(0)).map_err(redacted_io)?;
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
    post_commit_delay: Duration,
}

impl SqliteFtsMemoryDependency {
    /// Creates an adapter. Schema initialization occurs on first operation.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            post_commit_delay: Duration::ZERO,
        }
    }

    /// Delays a terminal response after the exact transaction commit is
    /// durable. This supports deterministic process crash-cut validation.
    #[must_use]
    pub fn with_post_commit_delay(mut self, delay: Duration) -> Self {
        self.post_commit_delay = delay;
        self
    }

    /// Returns the configured deterministic post-commit delay.
    #[must_use]
    pub const fn post_commit_delay(&self) -> Duration {
        self.post_commit_delay
    }

    fn retained_after_commit(&self, id: String) -> DependencyMemoryWriteResponse {
        if !self.post_commit_delay.is_zero() {
            std::thread::sleep(self.post_commit_delay);
        }
        DependencyMemoryWriteResponse { id, retained: true }
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
        let id = memory_record_id(&request)?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| MemoryDependencyError::Database)?;
        let existing = transaction
            .query_row(
                "SELECT scope, source, content, created_at_millis
                 FROM memory_fts WHERE id = ?1 LIMIT 1",
                params![&id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| MemoryDependencyError::Database)?;
        if let Some((scope, source, content, created_at_millis)) = existing {
            if scope != request.scope
                || source != request.source
                || content != request.content
                || created_at_millis != request.created_at_millis
            {
                return Err(MemoryDependencyError::CorruptRecord);
            }
            transaction
                .commit()
                .map_err(|_| MemoryDependencyError::Database)?;
            return Ok(self.retained_after_commit(id));
        }
        transaction
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
        transaction
            .commit()
            .map_err(|_| MemoryDependencyError::Database)?;
        Ok(self.retained_after_commit(id))
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
    checksum: String,
}

impl StoredFileRecord {
    fn seal(mut self) -> Result<Self, MemoryDependencyError> {
        self.checksum = self.expected_checksum()?;
        Ok(self)
    }

    fn verify(&self) -> Result<(), MemoryDependencyError> {
        if self.schema_version != 1
            || self.id.parse::<Uuid>().is_err()
            || self.checksum != self.expected_checksum()?
        {
            return Err(MemoryDependencyError::CorruptRecord);
        }
        Ok(())
    }

    fn expected_checksum(&self) -> Result<String, MemoryDependencyError> {
        let bytes = serde_json::to_vec(&(
            self.schema_version,
            &self.id,
            &self.scope,
            &self.source,
            &self.content,
            self.created_at_millis,
        ))
        .map_err(|_| MemoryDependencyError::Serialization)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }
}

enum FileTailRepair {
    None,
    AppendTerminator,
    Truncate { valid_length: u64, invalid: Vec<u8> },
}

fn repair_file_tail(path: &Path, file: &mut File) -> Result<(), MemoryDependencyError> {
    file.seek(SeekFrom::Start(0)).map_err(redacted_io)?;
    let length = file.metadata().map_err(redacted_io)?.len();
    let repair = {
        let mut reader = BufReader::new(&mut *file);
        let mut valid_length = 0_u64;
        let mut line = Vec::new();
        let mut repair = FileTailRepair::None;
        loop {
            line.clear();
            let count = reader.read_until(b'\n', &mut line).map_err(redacted_io)?;
            if count == 0 {
                break;
            }
            let count = u64::try_from(count).map_err(|_| MemoryDependencyError::CorruptRecord)?;
            let line_end = valid_length
                .checked_add(count)
                .ok_or(MemoryDependencyError::CorruptRecord)?;
            let terminated = line.last() == Some(&b'\n');
            let mut payload_end = line.len() - usize::from(terminated);
            if payload_end > 0 && line[payload_end - 1] == b'\r' {
                payload_end -= 1;
            }
            let valid = payload_end <= RECORD_LIMIT
                && serde_json::from_slice::<StoredFileRecord>(&line[..payload_end])
                    .is_ok_and(|record| record.verify().is_ok());
            if valid {
                if terminated {
                    valid_length = line_end;
                    continue;
                }
                if line_end != length {
                    return Err(MemoryDependencyError::CorruptRecord);
                }
                repair = FileTailRepair::AppendTerminator;
                break;
            }
            if terminated || line_end != length {
                return Err(MemoryDependencyError::CorruptRecord);
            }
            repair = FileTailRepair::Truncate {
                valid_length,
                invalid: line.clone(),
            };
            break;
        }
        repair
    };

    match repair {
        FileTailRepair::None => {}
        FileTailRepair::AppendTerminator => {
            file.seek(SeekFrom::End(0)).map_err(redacted_io)?;
            file.write_all(b"\n").map_err(redacted_io)?;
            file.sync_data().map_err(redacted_io)?;
        }
        FileTailRepair::Truncate {
            valid_length,
            invalid,
        } => {
            quarantine_memory_tail(path, &invalid)?;
            file.set_len(valid_length).map_err(redacted_io)?;
            file.sync_data().map_err(redacted_io)?;
        }
    }
    file.seek(SeekFrom::Start(0)).map_err(redacted_io)?;
    Ok(())
}

fn quarantine_memory_tail(path: &Path, invalid: &[u8]) -> Result<(), MemoryDependencyError> {
    let parent = path.parent().ok_or(MemoryDependencyError::InvalidPath)?;
    let directory = parent.join("quarantine");
    fs::create_dir_all(&directory).map_err(redacted_io)?;
    let digest = blake3::hash(invalid).to_hex();
    let quarantine = directory.join(format!("memory-tail-{digest}.bin"));
    match OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&quarantine)
    {
        Ok(mut file) => {
            file.write_all(invalid).map_err(redacted_io)?;
            file.sync_all().map_err(redacted_io)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(quarantine).map_err(redacted_io)? == invalid {
                Ok(())
            } else {
                Err(MemoryDependencyError::CorruptRecord)
            }
        }
        Err(error) => Err(redacted_io(error)),
    }
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

fn memory_record_id(
    request: &DependencyMemoryWriteRequest,
) -> Result<String, MemoryDependencyError> {
    let encoded = serde_json::to_vec(&(
        "agentmod.memory-record.v1",
        &request.scope,
        &request.source,
        &request.content,
        request.created_at_millis,
    ))
    .map_err(|_| MemoryDependencyError::Serialization)?;
    let hash = blake3::hash(&encoded);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    Ok(Uuid::from_bytes(bytes).hyphenated().to_string())
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
    fn exact_memory_writes_are_idempotent_for_file_and_sqlite() {
        let root = tempfile::tempdir().expect("root");
        let file_path = root.path().join("memory.jsonl");
        let file = FileMemoryDependency::new(file_path.clone());
        let sqlite = SqliteFtsMemoryDependency::new(root.path().join("memory.sqlite3"));
        let request = DependencyMemoryWriteRequest {
            scope: String::from("session:s1"),
            source: String::from("runtime.turn_completion"),
            content: String::from("bounded canonical memory"),
            created_at_millis: 7,
        };
        let first_file = file.write(request.clone()).expect("first file write");
        let second_file = file.write(request.clone()).expect("replayed file write");
        assert_eq!(first_file, second_file);
        assert_eq!(
            BufReader::new(File::open(file_path).expect("file"))
                .lines()
                .count(),
            1
        );

        let first_sqlite = sqlite.write(request.clone()).expect("first sqlite write");
        let second_sqlite = sqlite.write(request).expect("replayed sqlite write");
        assert_eq!(first_sqlite, second_sqlite);
        assert_eq!(
            sqlite
                .query(DependencyMemoryQueryRequest {
                    scope: String::from("session:s1"),
                    query: String::from("bounded canonical"),
                    limit: 10,
                })
                .expect("query")
                .len(),
            1
        );
    }

    #[test]
    fn durable_memory_delay_configuration_is_explicit_for_file_and_sqlite() {
        let root = tempfile::tempdir().expect("root");
        let delay = Duration::from_millis(37);
        let file = FileMemoryDependency::new(root.path().join("memory.jsonl"))
            .with_post_persist_delay(delay);
        let sqlite = SqliteFtsMemoryDependency::new(root.path().join("memory.sqlite3"))
            .with_post_commit_delay(delay);

        assert_eq!(file.post_persist_delay(), delay);
        assert_eq!(sqlite.post_commit_delay(), delay);
    }

    #[test]
    fn file_memory_quarantines_only_an_invalid_partial_final_record() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("memory.jsonl");
        let provider = FileMemoryDependency::new(path.clone());
        write(&provider, "first durable record", 1);
        let valid = fs::read(&path).expect("valid bytes");
        let invalid = br#"{"schema_version":1,"id":"partial"#;
        let mut file = OpenOptions::new().append(true).open(&path).expect("append");
        file.write_all(invalid).expect("partial write");
        file.sync_all().expect("partial durable");

        assert_eq!(query(&provider, "first durable").len(), 1);
        assert_eq!(fs::read(&path).expect("repaired bytes"), valid);
        let quarantine = root.path().join("quarantine").join(format!(
            "memory-tail-{}.bin",
            blake3::hash(invalid).to_hex()
        ));
        assert_eq!(fs::read(quarantine).expect("quarantined tail"), invalid);
        write(&provider, "second durable record", 2);
        assert_eq!(query(&provider, "durable record").len(), 2);
    }

    #[test]
    fn file_memory_never_repairs_a_corrupt_complete_or_interior_record() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("memory.jsonl");
        let provider = FileMemoryDependency::new(path.clone());
        write(&provider, "first durable record", 1);
        write(&provider, "second durable record", 2);
        let mut corrupted = fs::read(&path).expect("bytes");
        corrupted[20] ^= 1;
        fs::write(&path, &corrupted).expect("corrupt");

        assert_eq!(
            provider.query(DependencyMemoryQueryRequest {
                scope: String::from("project:p1"),
                query: String::from("durable"),
                limit: 10,
            }),
            Err(MemoryDependencyError::CorruptRecord)
        );
        assert_eq!(fs::read(&path).expect("unchanged"), corrupted);
        assert!(!root.path().join("quarantine").exists());
    }

    #[test]
    fn file_memory_completes_a_valid_unterminated_final_record() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("memory.jsonl");
        let provider = FileMemoryDependency::new(path.clone());
        write(&provider, "fully encoded record", 1);
        let mut bytes = fs::read(&path).expect("bytes");
        assert_eq!(bytes.pop(), Some(b'\n'));
        fs::write(&path, &bytes).expect("remove terminator");

        assert_eq!(query(&provider, "fully encoded").len(), 1);
        let repaired = fs::read(&path).expect("repaired");
        assert_eq!(repaired.last(), Some(&b'\n'));
        assert!(!repaired[..repaired.len() - 1].contains(&b'\n'));
    }
}
