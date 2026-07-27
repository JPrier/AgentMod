//! Secure operating-system adapters for the filesystem capability host.
#![allow(clippy::needless_pass_by_value)]

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, Metadata};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationKey, ExpectedAuthorization, verify_authorization,
};
use diffy::{Patch, PatchFormatter, apply, create_patch};
use encoding_rs::{UTF_16BE, UTF_16LE, WINDOWS_1252};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::WalkBuilder;
use regex::RegexBuilder;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use thiserror::Error;

/// Maximum defaults chosen to keep every dependency operation bounded.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Approved roots and hard dependency bounds.
#[derive(Clone, Debug)]
pub struct FilesystemDependencyConfig {
    roots: Vec<PathBuf>,
    /// Maximum file size read into memory for a single operation.
    pub max_file_bytes: u64,
    authorization: Option<FilesystemAuthorizationConfig>,
}

/// Dependency-owned trust configuration for local runtime grants.
#[derive(Clone, Debug)]
pub struct FilesystemAuthorizationConfig {
    /// Authenticated local connection owner.
    pub owner: String,
    /// Runtime session allowed to invoke this host.
    pub session: String,
    /// Shared keyed-grant verification secret.
    pub key: Arc<AuthorizationKey>,
}

impl FilesystemDependencyConfig {
    /// Canonicalizes approved workspace and additional roots.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] if no roots are supplied, a root is missing,
    /// is not a directory, or cannot be canonicalized.
    pub fn new(
        workspace_roots: Vec<PathBuf>,
        additional_roots: Vec<PathBuf>,
        max_file_bytes: u64,
    ) -> Result<Self, DependencyError> {
        if max_file_bytes == 0 {
            return Err(DependencyError::InvalidLimit("max_file_bytes"));
        }
        let mut roots = Vec::new();
        for root in workspace_roots.into_iter().chain(additional_roots) {
            let canonical = fs::canonicalize(&root).map_err(|error| DependencyError::Io {
                operation: "canonicalize root",
                path: root.display().to_string(),
                detail: error.to_string(),
            })?;
            if !canonical.is_dir() {
                return Err(DependencyError::RootNotDirectory(
                    canonical.display().to_string(),
                ));
            }
            if !roots.contains(&canonical) {
                roots.push(canonical);
            }
        }
        roots.sort();
        if roots.is_empty() {
            return Err(DependencyError::NoApprovedRoots);
        }
        Ok(Self {
            roots,
            max_file_bytes,
            authorization: None,
        })
    }

    /// Returns canonical approved roots.
    #[must_use]
    pub fn roots(&self) -> &[PathBuf] {
        &self.roots
    }

    /// Installs the mandatory runtime authorization trust root.
    #[must_use]
    pub fn with_authorization(mut self, authorization: FilesystemAuthorizationConfig) -> Self {
        self.authorization = Some(authorization);
        self
    }

    /// Returns whether execution authorization is configured.
    #[must_use]
    pub const fn authorization_ready(&self) -> bool {
        self.authorization.is_some()
    }
}

/// Dependency-owned copy of the execution authorization envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAuthorization {
    /// Runtime call ID.
    pub call_id: String,
    /// Declared action ID.
    pub action: String,
    /// Runtime-supplied normalized digest.
    pub normalized_digest: String,
    /// Opaque shared keyed grant.
    pub grant: String,
}

/// Dependency-owned filesystem request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyRequest {
    /// Report adapter health.
    Health,
    /// Authorized operation. Raw operations are rejected fail-closed.
    Authorized {
        /// Layer-owned authorization envelope.
        authorization: DependencyAuthorization,
        /// Exact operation to execute after verification.
        operation: Box<DependencyRequest>,
    },
    /// Read a regular file.
    Read(ReadRequest),
    /// List a directory tree.
    List(ListRequest),
    /// Match workspace paths.
    Glob(GlobRequest),
    /// Search text files.
    Grep(GrepRequest),
    /// Atomically create or replace a file.
    Write(WriteRequest),
    /// Atomically apply exact replacements.
    Edit(EditRequest),
    /// Apply a prevalidated unified multi-file patch.
    ApplyPatch(PatchRequest),
}

/// Dependency-owned filesystem result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyResponse {
    /// Health details.
    Health(HealthRecord),
    /// File read.
    Read(ReadRecord),
    /// Directory or glob results.
    Entries(EntriesRecord),
    /// Grep results.
    Grep(GrepRecord),
    /// One mutation.
    Mutation(MutationRecord),
    /// Multi-file mutation.
    Patch(PatchRecord),
}

/// Read selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadRange {
    /// Beginning of the complete file, bounded by projection bytes.
    All,
    /// Inclusive one-based line range.
    Lines {
        /// First line.
        start: usize,
        /// Last line.
        end: usize,
    },
    /// Byte range.
    Bytes {
        /// Zero-based byte offset.
        offset: u64,
        /// Requested bytes.
        length: usize,
    },
}

/// File read request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    /// Approved-root-relative or approved absolute path.
    pub path: String,
    /// Requested range.
    pub range: ReadRange,
    /// Maximum inline projection bytes.
    pub max_projection_bytes: usize,
}

/// Directory listing request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListRequest {
    /// Directory path.
    pub path: String,
    /// Recursive depth, where zero lists only the root.
    pub max_depth: usize,
    /// Include dot-prefixed entries.
    pub include_hidden: bool,
    /// Honor Git and ignore files.
    pub honor_ignore: bool,
    /// Additional ignore globs.
    pub ignore_patterns: Vec<String>,
    /// Maximum returned entries.
    pub max_results: usize,
}

/// Glob request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlobRequest {
    /// Directory to search.
    pub path: String,
    /// One or more glob patterns.
    pub patterns: Vec<String>,
    /// Include hidden entries.
    pub include_hidden: bool,
    /// Honor ignore files.
    pub honor_ignore: bool,
    /// Maximum returned entries.
    pub max_results: usize,
}

/// Grep request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrepRequest {
    /// Directory or file to search.
    pub path: String,
    /// Literal or regular-expression pattern.
    pub pattern: String,
    /// Interpret pattern as a regular expression.
    pub regex: bool,
    /// Case-insensitive matching.
    pub case_insensitive: bool,
    /// Optional file globs.
    pub file_patterns: Vec<String>,
    /// Lines before each match.
    pub before_context: usize,
    /// Lines after each match.
    pub after_context: usize,
    /// Maximum structured matches.
    pub max_matches: usize,
}

/// Atomic write mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteMode {
    /// Destination normally must not exist.
    Create,
    /// Destination must already exist.
    Replace,
}

/// Atomic write request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRequest {
    /// Destination.
    pub path: String,
    /// Complete new bytes.
    pub content: Vec<u8>,
    /// Create or replace semantics.
    pub mode: WriteMode,
    /// Required prior hash when supplied.
    pub expected_hash: Option<String>,
    /// Explicitly allow replacing an existing file.
    pub overwrite: bool,
    /// Allow creation of missing parent directories under an approved root.
    pub create_parents: bool,
}

/// One exact replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactReplacement {
    /// Exact old text.
    pub old: String,
    /// Replacement text.
    pub new: String,
    /// Exact expected match count.
    pub expected_occurrences: usize,
}

/// Exact edit request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EditRequest {
    /// UTF-8 file.
    pub path: String,
    /// Ordered replacements, all prevalidated before commit.
    pub replacements: Vec<ExactReplacement>,
    /// Required prior content hash when supplied.
    pub expected_hash: Option<String>,
}

/// Unified patch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatchRequest {
    /// Concatenated unified file patches.
    pub patch: String,
    /// Required BLAKE3 base hash by normalized patch path.
    pub base_hashes: BTreeMap<String, String>,
    /// Allow missing parent directories for created files.
    pub create_parents: bool,
}

/// Adapter health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthRecord {
    /// Canonical approved roots.
    pub approved_roots: Vec<String>,
    /// Configured file bound.
    pub max_file_bytes: u64,
    /// Whether the dependency has a grant-verification trust root.
    pub authorization_ready: bool,
}

/// Portable file metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMetadataRecord {
    /// Byte size.
    pub size: u64,
    /// Read-only attribute.
    pub readonly: bool,
    /// Modified Unix milliseconds, if representable.
    pub modified_ms: Option<u64>,
    /// Entry kind.
    pub kind: EntryKind,
}

/// Entry kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryKind {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Symbolic link or junction.
    Symlink,
    /// Other special file.
    Other,
}

/// One numbered text line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumberedLine {
    /// One-based line number.
    pub number: usize,
    /// Line content without newline.
    pub text: String,
}

/// Bounded file read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRecord {
    /// Canonical path.
    pub path: String,
    /// Complete-file BLAKE3.
    pub content_hash: String,
    /// Metadata.
    pub metadata: FileMetadataRecord,
    /// Detected encoding.
    pub encoding: String,
    /// Binary classification.
    pub binary: bool,
    /// Text lines for text projections.
    pub lines: Vec<NumberedLine>,
    /// Hex bytes for binary/byte projections.
    pub bytes_hex: Option<String>,
    /// Whether projection was bounded.
    pub truncated: bool,
}

/// One listed path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryRecord {
    /// Canonical path.
    pub path: String,
    /// Depth below requested root.
    pub depth: usize,
    /// Metadata.
    pub metadata: FileMetadataRecord,
}

/// Bounded path results.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntriesRecord {
    /// Stable sorted entries.
    pub entries: Vec<EntryRecord>,
    /// Whether more results existed.
    pub truncated: bool,
}

/// Structured grep match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrepMatchRecord {
    /// Canonical file.
    pub path: String,
    /// One-based line.
    pub line: usize,
    /// One-based UTF-8 byte column.
    pub column: usize,
    /// Matching line.
    pub text: String,
    /// Bounded context before.
    pub before: Vec<NumberedLine>,
    /// Bounded context after.
    pub after: Vec<NumberedLine>,
}

/// Bounded grep result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GrepRecord {
    /// Stable file/line/column ordered matches.
    pub matches: Vec<GrepMatchRecord>,
    /// Whether more matches existed.
    pub truncated: bool,
    /// Binary files excluded.
    pub binary_files_skipped: usize,
}

/// Mutation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationRecord {
    /// Canonical path.
    pub path: String,
    /// Previous hash.
    pub old_hash: Option<String>,
    /// New hash.
    pub new_hash: String,
    /// Unified diff or binary summary.
    pub diff: String,
    /// Bytes written.
    pub bytes_written: u64,
}

/// Multi-file patch result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatchRecord {
    /// Per-file results in stable path order.
    pub files: Vec<MutationRecord>,
    /// Rollback guarantee used by the dependency.
    pub atomicity: String,
}

/// Narrow dependency interface consumed only by filesystem data.
pub trait FilesystemDependencyPort {
    /// Executes an operation using dependency-owned types.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for security, validation, concurrency, parse,
    /// bounded-resource, or operating-system failures.
    fn execute(&self, request: DependencyRequest) -> Result<DependencyResponse, DependencyError>;
}

/// Native bounded filesystem implementation.
#[derive(Clone, Debug)]
pub struct NativeFilesystem {
    config: FilesystemDependencyConfig,
    consumed_nonces: Arc<Mutex<BTreeSet<String>>>,
}

impl NativeFilesystem {
    /// Creates a native adapter from validated configuration.
    #[must_use]
    pub fn new(config: FilesystemDependencyConfig) -> Self {
        Self {
            config,
            consumed_nonces: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    fn health(&self) -> HealthRecord {
        HealthRecord {
            approved_roots: self
                .config
                .roots
                .iter()
                .map(|path| path.display().to_string())
                .collect(),
            max_file_bytes: self.config.max_file_bytes,
            authorization_ready: self.config.authorization_ready(),
        }
    }

    fn authorize(
        &self,
        envelope: &DependencyAuthorization,
        operation: &DependencyRequest,
    ) -> Result<(), DependencyError> {
        let configured = self
            .config
            .authorization
            .as_ref()
            .ok_or(DependencyError::AuthorizationRequired)?;
        let action =
            operation_action(operation).ok_or(DependencyError::InvalidAuthorizationEnvelope)?;
        if envelope.action != action {
            return Err(DependencyError::AuthorizationDenied);
        }
        let recomputed = canonical_operation_digest(operation)?;
        let supplied = ContentHash::from_str(&envelope.normalized_digest)
            .map_err(|_| DependencyError::InvalidAuthorizationEnvelope)?;
        if supplied != recomputed {
            return Err(DependencyError::AuthorizationDenied);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DependencyError::Clock)?
            .as_millis();
        let now = i64::try_from(now).map_err(|_| DependencyError::Clock)?;
        let claims = verify_authorization(
            &envelope.grant,
            configured.key.as_ref(),
            ExpectedAuthorization {
                owner: &configured.owner,
                session: &configured.session,
                call_id: &envelope.call_id,
                action,
                normalized_digest: recomputed,
            },
            TimestampMillis::new(now),
        )
        .map_err(|_| DependencyError::AuthorizationDenied)?;
        let replay_key = format!("{}\0{}", claims.session, claims.nonce);
        let mut consumed = self
            .consumed_nonces
            .lock()
            .map_err(|_| DependencyError::AuthorizationState)?;
        if !consumed.insert(replay_key) {
            return Err(DependencyError::AuthorizationReplay);
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn read(&self, request: ReadRequest) -> Result<ReadRecord, DependencyError> {
        if request.max_projection_bytes == 0 {
            return Err(DependencyError::InvalidLimit("max_projection_bytes"));
        }
        let path = self.resolve_existing(&request.path)?;
        Self::ensure_regular_file(&path)?;
        let bytes = self.read_bounded(&path)?;
        let hash = hash_bytes(&bytes);
        let metadata = metadata_record(
            &fs::symlink_metadata(&path).map_err(|error| io_error("metadata", &path, error))?,
        );
        let (binary, encoding, decoded) = decode_bytes(&bytes);

        match request.range {
            ReadRange::Lines { start, end } if start == 0 || end < start => {
                Err(DependencyError::InvalidLineRange { start, end })
            }
            ReadRange::Lines { .. } if binary => Err(DependencyError::BinaryTextOperation(
                path.display().to_string(),
            )),
            ReadRange::Lines { start, end } => {
                let text = decoded.expect("non-binary decode has text");
                let mut truncated = false;
                let mut used = 0;
                let mut lines = Vec::new();
                for (index, line) in text.lines().enumerate() {
                    let number = index + 1;
                    if number < start || number > end {
                        continue;
                    }
                    if used + line.len() > request.max_projection_bytes {
                        truncated = true;
                        break;
                    }
                    used += line.len();
                    lines.push(NumberedLine {
                        number,
                        text: line.to_owned(),
                    });
                }
                Ok(ReadRecord {
                    path: path.display().to_string(),
                    content_hash: hash,
                    metadata,
                    encoding,
                    binary,
                    lines,
                    bytes_hex: None,
                    truncated,
                })
            }
            ReadRange::Bytes { offset, length } => {
                let start = usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(bytes.len());
                let requested_end = start.saturating_add(length).min(bytes.len());
                let projected_end = start
                    .saturating_add(request.max_projection_bytes)
                    .min(requested_end);
                Ok(ReadRecord {
                    path: path.display().to_string(),
                    content_hash: hash,
                    metadata,
                    encoding,
                    binary,
                    lines: Vec::new(),
                    bytes_hex: Some(hex(&bytes[start..projected_end])),
                    truncated: projected_end < requested_end,
                })
            }
            ReadRange::All if binary => {
                let end = bytes.len().min(request.max_projection_bytes);
                Ok(ReadRecord {
                    path: path.display().to_string(),
                    content_hash: hash,
                    metadata,
                    encoding,
                    binary,
                    lines: Vec::new(),
                    bytes_hex: Some(hex(&bytes[..end])),
                    truncated: end < bytes.len(),
                })
            }
            ReadRange::All => {
                let text = decoded.expect("non-binary decode has text");
                let mut used = 0;
                let mut lines = Vec::new();
                let mut truncated = false;
                for (index, line) in text.lines().enumerate() {
                    if used + line.len() > request.max_projection_bytes {
                        truncated = true;
                        break;
                    }
                    used += line.len();
                    lines.push(NumberedLine {
                        number: index + 1,
                        text: line.to_owned(),
                    });
                }
                Ok(ReadRecord {
                    path: path.display().to_string(),
                    content_hash: hash,
                    metadata,
                    encoding,
                    binary,
                    lines,
                    bytes_hex: None,
                    truncated,
                })
            }
        }
    }

    fn list(&self, request: ListRequest) -> Result<EntriesRecord, DependencyError> {
        if request.max_results == 0 {
            return Err(DependencyError::InvalidLimit("max_results"));
        }
        let root = self.resolve_existing(&request.path)?;
        if !root.is_dir() {
            return Err(DependencyError::NotDirectory(root.display().to_string()));
        }
        let excludes = build_globs(&request.ignore_patterns)?;
        let mut builder = WalkBuilder::new(&root);
        builder
            .max_depth(Some(request.max_depth.saturating_add(1)))
            .hidden(!request.include_hidden)
            .git_ignore(request.honor_ignore)
            .git_exclude(request.honor_ignore)
            .parents(request.honor_ignore)
            .follow_links(false)
            .sort_by_file_name(std::cmp::Ord::cmp);
        let mut entries = Vec::new();
        let mut truncated = false;
        for item in builder.build() {
            let item = item.map_err(|error| DependencyError::Walk(error.to_string()))?;
            if item.path() == root {
                continue;
            }
            let relative = item.path().strip_prefix(&root).unwrap_or(item.path());
            if excludes.is_match(relative) {
                continue;
            }
            let canonical = self.resolve_walk_entry(item.path())?;
            let depth = relative.components().count();
            let metadata = fs::symlink_metadata(item.path())
                .map_err(|error| io_error("metadata", item.path(), error))?;
            if entries.len() == request.max_results {
                truncated = true;
                break;
            }
            entries.push(EntryRecord {
                path: canonical.display().to_string(),
                depth,
                metadata: metadata_record(&metadata),
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(EntriesRecord { entries, truncated })
    }

    fn glob(&self, request: GlobRequest) -> Result<EntriesRecord, DependencyError> {
        if request.patterns.is_empty() {
            return Err(DependencyError::EmptyPatterns);
        }
        if request.max_results == 0 {
            return Err(DependencyError::InvalidLimit("max_results"));
        }
        let matches = build_globs(&request.patterns)?;
        let root = self.resolve_existing(&request.path)?;
        if !root.is_dir() {
            return Err(DependencyError::NotDirectory(root.display().to_string()));
        }
        let mut builder = WalkBuilder::new(&root);
        builder
            .hidden(!request.include_hidden)
            .git_ignore(request.honor_ignore)
            .git_exclude(request.honor_ignore)
            .parents(request.honor_ignore)
            .follow_links(false)
            .sort_by_file_name(std::cmp::Ord::cmp);
        let mut entries = Vec::new();
        let mut truncated = false;
        for item in builder.build() {
            let item = item.map_err(|error| DependencyError::Walk(error.to_string()))?;
            if item.path() == root {
                continue;
            }
            let relative = item.path().strip_prefix(&root).unwrap_or(item.path());
            if matches.is_match(relative) {
                if entries.len() == request.max_results {
                    truncated = true;
                    break;
                }
                let canonical = self.resolve_walk_entry(item.path())?;
                let metadata = fs::symlink_metadata(item.path())
                    .map_err(|error| io_error("metadata", item.path(), error))?;
                entries.push(EntryRecord {
                    path: canonical.display().to_string(),
                    depth: relative.components().count(),
                    metadata: metadata_record(&metadata),
                });
            }
        }
        Ok(EntriesRecord { entries, truncated })
    }

    fn grep(&self, request: GrepRequest) -> Result<GrepRecord, DependencyError> {
        if request.max_matches == 0 {
            return Err(DependencyError::InvalidLimit("max_matches"));
        }
        if request.pattern.is_empty() {
            return Err(DependencyError::EmptySearchPattern);
        }
        let escaped;
        let pattern = if request.regex {
            request.pattern.as_str()
        } else {
            escaped = regex::escape(&request.pattern);
            &escaped
        };
        let expression = RegexBuilder::new(pattern)
            .case_insensitive(request.case_insensitive)
            .build()
            .map_err(|error| DependencyError::InvalidRegex(error.to_string()))?;
        let filter = build_globs(&request.file_patterns)?;
        let root = self.resolve_existing(&request.path)?;
        let files = self.grep_files(&root)?;
        let mut matches = Vec::new();
        let mut binary_files_skipped = 0;
        let mut truncated = false;
        'files: for file in files {
            let relative = if root.is_dir() {
                file.strip_prefix(&root).unwrap_or(&file)
            } else {
                file.file_name().map_or(file.as_path(), Path::new)
            };
            if !request.file_patterns.is_empty() && !filter.is_match(relative) {
                continue;
            }
            let bytes = self.read_bounded(&file)?;
            let (binary, _, decoded) = decode_bytes(&bytes);
            if binary {
                binary_files_skipped += 1;
                continue;
            }
            let text = decoded.expect("text decode");
            let lines: Vec<_> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                for found in expression.find_iter(line) {
                    if matches.len() == request.max_matches {
                        truncated = true;
                        break 'files;
                    }
                    let before_start = index.saturating_sub(request.before_context);
                    let after_end = index
                        .saturating_add(request.after_context + 1)
                        .min(lines.len());
                    matches.push(GrepMatchRecord {
                        path: file.display().to_string(),
                        line: index + 1,
                        column: found.start() + 1,
                        text: (*line).to_owned(),
                        before: lines[before_start..index]
                            .iter()
                            .enumerate()
                            .map(|(offset, text)| NumberedLine {
                                number: before_start + offset + 1,
                                text: (*text).to_owned(),
                            })
                            .collect(),
                        after: lines[index + 1..after_end]
                            .iter()
                            .enumerate()
                            .map(|(offset, text)| NumberedLine {
                                number: index + offset + 2,
                                text: (*text).to_owned(),
                            })
                            .collect(),
                    });
                }
            }
        }
        Ok(GrepRecord {
            matches,
            truncated,
            binary_files_skipped,
        })
    }

    fn write(&self, request: WriteRequest) -> Result<MutationRecord, DependencyError> {
        if request.content.len() as u64 > self.config.max_file_bytes {
            return Err(DependencyError::FileTooLarge {
                path: request.path,
                size: request.content.len() as u64,
                maximum: self.config.max_file_bytes,
            });
        }
        let (path, existed) = self.resolve_for_write(&request.path, request.create_parents)?;
        match request.mode {
            WriteMode::Create if existed && !request.overwrite => {
                return Err(DependencyError::AlreadyExists(path.display().to_string()));
            }
            WriteMode::Replace if !existed => {
                return Err(DependencyError::NotFound(path.display().to_string()));
            }
            WriteMode::Replace if !request.overwrite => {
                return Err(DependencyError::OverwriteNotApproved(
                    path.display().to_string(),
                ));
            }
            _ => {}
        }
        let old = if existed {
            Self::ensure_regular_file(&path)?;
            Some(self.read_bounded(&path)?)
        } else {
            None
        };
        Self::check_expected_hash(&path, old.as_deref(), request.expected_hash.as_deref())?;
        if existed {
            self.recheck_hash(&path, old.as_deref().expect("existing bytes"))?;
        }
        atomic_write(&path, &request.content, !existed)?;
        Ok(mutation_record(&path, old.as_deref(), &request.content))
    }

    fn edit(&self, request: EditRequest) -> Result<MutationRecord, DependencyError> {
        if request.replacements.is_empty() {
            return Err(DependencyError::EmptyReplacements);
        }
        let path = self.resolve_existing(&request.path)?;
        Self::ensure_regular_file(&path)?;
        let old = self.read_bounded(&path)?;
        Self::check_expected_hash(&path, Some(&old), request.expected_hash.as_deref())?;
        let mut text = String::from_utf8(old.clone())
            .map_err(|_| DependencyError::BinaryTextOperation(path.display().to_string()))?;
        for (index, replacement) in request.replacements.iter().enumerate() {
            if replacement.old.is_empty() || replacement.expected_occurrences == 0 {
                return Err(DependencyError::InvalidReplacement { index });
            }
            let actual = text.matches(&replacement.old).count();
            if actual != replacement.expected_occurrences {
                return Err(DependencyError::ReplacementMismatch {
                    index,
                    expected: replacement.expected_occurrences,
                    actual,
                });
            }
            text = text.replace(&replacement.old, &replacement.new);
        }
        let new = text.into_bytes();
        if new.len() as u64 > self.config.max_file_bytes {
            return Err(DependencyError::FileTooLarge {
                path: path.display().to_string(),
                size: new.len() as u64,
                maximum: self.config.max_file_bytes,
            });
        }
        self.recheck_hash(&path, &old)?;
        atomic_write(&path, &new, false)?;
        Ok(mutation_record(&path, Some(&old), &new))
    }

    fn apply_patch(&self, request: PatchRequest) -> Result<PatchRecord, DependencyError> {
        let sections = split_patch(&request.patch)?;
        let mut plans = Vec::new();
        let mut seen = BTreeSet::new();
        for section in sections {
            if !seen.insert(section.path.clone()) {
                return Err(DependencyError::DuplicatePatchPath(section.path));
            }
            let (path, existed) = self.resolve_for_write(&section.path, request.create_parents)?;
            let old = if existed {
                Self::ensure_regular_file(&path)?;
                self.read_bounded(&path)?
            } else {
                Vec::new()
            };
            let expected = request
                .base_hashes
                .get(&section.path)
                .ok_or_else(|| DependencyError::MissingBaseHash(section.path.clone()))?;
            let actual = hash_bytes(&old);
            if &actual != expected {
                return Err(DependencyError::HashMismatch {
                    path: path.display().to_string(),
                    expected: expected.clone(),
                    actual,
                });
            }
            let parsed_patch = Patch::from_str(&section.text)
                .map_err(|error| DependencyError::InvalidPatch(error.to_string()))?;
            let old_text = String::from_utf8(old.clone())
                .map_err(|_| DependencyError::BinaryTextOperation(path.display().to_string()))?;
            let new_text = apply(&old_text, &parsed_patch)
                .map_err(|error| DependencyError::PatchDoesNotApply(error.to_string()))?;
            if new_text.len() as u64 > self.config.max_file_bytes {
                return Err(DependencyError::FileTooLarge {
                    path: path.display().to_string(),
                    size: new_text.len() as u64,
                    maximum: self.config.max_file_bytes,
                });
            }
            plans.push(PatchPlan {
                path,
                existed,
                old,
                new: new_text.into_bytes(),
            });
        }
        plans.sort_by(|left, right| left.path.cmp(&right.path));
        for plan in &plans {
            if plan.existed {
                self.recheck_hash(&plan.path, &plan.old)?;
            } else if plan.path.exists() {
                return Err(DependencyError::ConcurrentModification(
                    plan.path.display().to_string(),
                ));
            }
        }

        let mut committed = Vec::new();
        for (index, plan) in plans.iter().enumerate() {
            if let Err(commit_error) = atomic_write(&plan.path, &plan.new, !plan.existed) {
                let mut rollback_errors = Vec::new();
                for committed_index in committed.into_iter().rev() {
                    let prior: &PatchPlan = &plans[committed_index];
                    let result = if prior.existed {
                        atomic_write(&prior.path, &prior.old, false)
                    } else {
                        fs::remove_file(&prior.path)
                            .map_err(|error| io_error("rollback remove", &prior.path, error))
                    };
                    if let Err(error) = result {
                        rollback_errors.push(error.to_string());
                    }
                }
                return Err(DependencyError::PatchCommitFailed {
                    failed_index: index,
                    detail: commit_error.to_string(),
                    rollback_errors,
                });
            }
            committed.push(index);
        }
        Ok(PatchRecord {
            files: plans
                .iter()
                .map(|plan| {
                    mutation_record(&plan.path, plan.existed.then_some(&plan.old), &plan.new)
                })
                .collect(),
            atomicity: "prevalidated staged replacement with explicit rollback".into(),
        })
    }

    fn resolve_existing(&self, requested: &str) -> Result<PathBuf, DependencyError> {
        let candidate = self.candidate(requested)?;
        let canonical = fs::canonicalize(&candidate)
            .map_err(|error| io_error("canonicalize", &candidate, error))?;
        self.ensure_allowed(&candidate, &canonical)?;
        Self::reject_sensitive(&canonical)?;
        Self::reject_special(&canonical)?;
        Ok(canonical)
    }

    fn resolve_walk_entry(&self, path: &Path) -> Result<PathBuf, DependencyError> {
        let canonical =
            fs::canonicalize(path).map_err(|error| io_error("canonicalize", path, error))?;
        self.ensure_allowed(path, &canonical)?;
        Self::reject_sensitive(&canonical)?;
        Self::reject_special(&canonical)?;
        Ok(canonical)
    }

    fn resolve_for_write(
        &self,
        requested: &str,
        create_parents: bool,
    ) -> Result<(PathBuf, bool), DependencyError> {
        let candidate = self.candidate(requested)?;
        Self::reject_sensitive(&candidate)?;
        if candidate.exists() {
            return self.resolve_existing(requested).map(|path| (path, true));
        }
        let parent = candidate
            .parent()
            .ok_or_else(|| DependencyError::InvalidPath(requested.to_owned()))?;
        if !parent.exists() {
            if !create_parents {
                return Err(DependencyError::ParentMissing(parent.display().to_string()));
            }
            let existing = nearest_existing(parent)?;
            let canonical_existing = fs::canonicalize(existing)
                .map_err(|error| io_error("canonicalize parent", existing, error))?;
            self.ensure_allowed(&candidate, &canonical_existing)?;
            fs::create_dir_all(parent)
                .map_err(|error| io_error("create parents", parent, error))?;
        }
        let canonical_parent = fs::canonicalize(parent)
            .map_err(|error| io_error("canonicalize parent", parent, error))?;
        self.ensure_allowed(&candidate, &canonical_parent)?;
        let name = candidate
            .file_name()
            .ok_or_else(|| DependencyError::InvalidPath(requested.to_owned()))?;
        Ok((canonical_parent.join(name), false))
    }

    fn candidate(&self, requested: &str) -> Result<PathBuf, DependencyError> {
        if requested.trim().is_empty() {
            return Err(DependencyError::InvalidPath(requested.to_owned()));
        }
        let path = Path::new(requested);
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(DependencyError::TraversalRejected(requested.to_owned()));
        }
        if has_windows_device_name(path) {
            return Err(DependencyError::DeviceRejected(requested.to_owned()));
        }
        Ok(if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.config.roots[0].join(path)
        })
    }

    fn ensure_allowed(&self, original: &Path, canonical: &Path) -> Result<(), DependencyError> {
        if self
            .config
            .roots
            .iter()
            .any(|root| canonical.starts_with(root))
        {
            Ok(())
        } else {
            Err(DependencyError::SymlinkEscape {
                requested: original.display().to_string(),
                resolved: canonical.display().to_string(),
            })
        }
    }

    fn reject_sensitive(path: &Path) -> Result<(), DependencyError> {
        if path.components().any(|component| {
            let value = component.as_os_str().to_string_lossy().to_ascii_lowercase();
            value == ".ssh"
                || value == ".aws"
                || value == ".gnupg"
                || value == ".env"
                || value.starts_with(".env.")
                || matches!(
                    value.as_str(),
                    "id_rsa" | "id_ed25519" | "credentials" | "secrets"
                )
                || Path::new(&value).extension().is_some_and(|extension| {
                    matches!(extension.to_str(), Some("pem" | "key" | "p12"))
                })
        }) {
            Err(DependencyError::SensitivePathRejected(
                path.display().to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn reject_special(path: &Path) -> Result<(), DependencyError> {
        let metadata = fs::metadata(path).map_err(|error| io_error("metadata", path, error))?;
        if metadata.is_file() || metadata.is_dir() {
            Ok(())
        } else {
            Err(DependencyError::DeviceRejected(path.display().to_string()))
        }
    }

    fn ensure_regular_file(path: &Path) -> Result<(), DependencyError> {
        let metadata = fs::metadata(path).map_err(|error| io_error("metadata", path, error))?;
        if metadata.is_file() {
            Ok(())
        } else {
            Err(DependencyError::NotRegularFile(path.display().to_string()))
        }
    }

    fn read_bounded(&self, path: &Path) -> Result<Vec<u8>, DependencyError> {
        let size = fs::metadata(path)
            .map_err(|error| io_error("metadata", path, error))?
            .len();
        if size > self.config.max_file_bytes {
            return Err(DependencyError::FileTooLarge {
                path: path.display().to_string(),
                size,
                maximum: self.config.max_file_bytes,
            });
        }
        let file = File::open(path).map_err(|error| io_error("open", path, error))?;
        let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        file.take(self.config.max_file_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| io_error("read", path, error))?;
        if bytes.len() as u64 > self.config.max_file_bytes {
            return Err(DependencyError::FileTooLarge {
                path: path.display().to_string(),
                size: bytes.len() as u64,
                maximum: self.config.max_file_bytes,
            });
        }
        Ok(bytes)
    }

    fn grep_files(&self, root: &Path) -> Result<Vec<PathBuf>, DependencyError> {
        if root.is_file() {
            return Ok(vec![root.to_path_buf()]);
        }
        let mut files = Vec::new();
        let mut builder = WalkBuilder::new(root);
        builder
            .hidden(false)
            .git_ignore(true)
            .git_exclude(true)
            .parents(true)
            .follow_links(false)
            .sort_by_file_name(std::cmp::Ord::cmp);
        for item in builder.build() {
            let item = item.map_err(|error| DependencyError::Walk(error.to_string()))?;
            if item.file_type().is_some_and(|kind| kind.is_file()) {
                files.push(self.resolve_walk_entry(item.path())?);
            }
        }
        files.sort();
        Ok(files)
    }

    fn check_expected_hash(
        path: &Path,
        bytes: Option<&[u8]>,
        expected: Option<&str>,
    ) -> Result<(), DependencyError> {
        if let Some(expected) = expected {
            let actual = bytes.map_or_else(|| hash_bytes(&[]), hash_bytes);
            if actual != expected {
                return Err(DependencyError::HashMismatch {
                    path: path.display().to_string(),
                    expected: expected.to_owned(),
                    actual,
                });
            }
        }
        Ok(())
    }

    fn recheck_hash(&self, path: &Path, expected_bytes: &[u8]) -> Result<(), DependencyError> {
        let current = self.read_bounded(path)?;
        if hash_bytes(&current) == hash_bytes(expected_bytes) {
            Ok(())
        } else {
            Err(DependencyError::ConcurrentModification(
                path.display().to_string(),
            ))
        }
    }
}

impl FilesystemDependencyPort for NativeFilesystem {
    fn execute(&self, request: DependencyRequest) -> Result<DependencyResponse, DependencyError> {
        match request {
            DependencyRequest::Health => Ok(DependencyResponse::Health(self.health())),
            DependencyRequest::Authorized {
                authorization,
                operation,
            } => {
                self.authorize(&authorization, &operation)?;
                self.execute_authorized(*operation)
            }
            DependencyRequest::Read(_)
            | DependencyRequest::List(_)
            | DependencyRequest::Glob(_)
            | DependencyRequest::Grep(_)
            | DependencyRequest::Write(_)
            | DependencyRequest::Edit(_)
            | DependencyRequest::ApplyPatch(_) => Err(DependencyError::AuthorizationRequired),
        }
    }
}

impl NativeFilesystem {
    fn execute_authorized(
        &self,
        request: DependencyRequest,
    ) -> Result<DependencyResponse, DependencyError> {
        match request {
            DependencyRequest::Read(request) => self.read(request).map(DependencyResponse::Read),
            DependencyRequest::List(request) => self.list(request).map(DependencyResponse::Entries),
            DependencyRequest::Glob(request) => self.glob(request).map(DependencyResponse::Entries),
            DependencyRequest::Grep(request) => self.grep(request).map(DependencyResponse::Grep),
            DependencyRequest::Write(request) => {
                self.write(request).map(DependencyResponse::Mutation)
            }
            DependencyRequest::Edit(request) => {
                self.edit(request).map(DependencyResponse::Mutation)
            }
            DependencyRequest::ApplyPatch(request) => {
                self.apply_patch(request).map(DependencyResponse::Patch)
            }
            DependencyRequest::Health | DependencyRequest::Authorized { .. } => {
                Err(DependencyError::InvalidAuthorizationEnvelope)
            }
        }
    }
}

fn operation_action(request: &DependencyRequest) -> Option<&'static str> {
    match request {
        DependencyRequest::Read(_) => Some("filesystem.read"),
        DependencyRequest::List(_) => Some("filesystem.list"),
        DependencyRequest::Glob(_) => Some("filesystem.glob"),
        DependencyRequest::Grep(_) => Some("filesystem.grep"),
        DependencyRequest::Write(_) => Some("filesystem.write"),
        DependencyRequest::Edit(_) => Some("filesystem.edit"),
        DependencyRequest::ApplyPatch(_) => Some("filesystem.apply_patch"),
        DependencyRequest::Health | DependencyRequest::Authorized { .. } => None,
    }
}

/// Computes the stable canonical digest verified immediately before execution.
///
/// Every operation field is represented. Potentially large content is included
/// through its BLAKE3 content hash and byte length.
///
/// # Errors
///
/// Returns [`DependencyError::InvalidAuthorizationEnvelope`] for a health or
/// nested authorization request, or [`DependencyError::CanonicalEncoding`] if
/// deterministic JSON encoding fails.
pub fn canonical_operation_digest(
    request: &DependencyRequest,
) -> Result<ContentHash, DependencyError> {
    let value = match request {
        DependencyRequest::Read(request) => json!({
            "action":"filesystem.read",
            "path":request.path,
            "range": match request.range {
                ReadRange::All => json!({"kind":"all"}),
                ReadRange::Lines { start, end } => json!({"kind":"lines","start":start,"end":end}),
                ReadRange::Bytes { offset, length } => json!({"kind":"bytes","offset":offset,"length":length}),
            },
            "max_projection_bytes":request.max_projection_bytes,
        }),
        DependencyRequest::List(request) => json!({
            "action":"filesystem.list","path":request.path,"max_depth":request.max_depth,
            "include_hidden":request.include_hidden,"honor_ignore":request.honor_ignore,
            "ignore_patterns":request.ignore_patterns,"max_results":request.max_results,
        }),
        DependencyRequest::Glob(request) => json!({
            "action":"filesystem.glob","path":request.path,"patterns":request.patterns,
            "include_hidden":request.include_hidden,"honor_ignore":request.honor_ignore,
            "max_results":request.max_results,
        }),
        DependencyRequest::Grep(request) => json!({
            "action":"filesystem.grep","path":request.path,"pattern":request.pattern,
            "regex":request.regex,"case_insensitive":request.case_insensitive,
            "file_patterns":request.file_patterns,"before_context":request.before_context,
            "after_context":request.after_context,"max_matches":request.max_matches,
        }),
        DependencyRequest::Write(request) => json!({
            "action":"filesystem.write","path":request.path,
            "content_hash":ContentHash::digest(&request.content).to_hex(),
            "content_bytes":request.content.len(),
            "mode":match request.mode { WriteMode::Create => "create", WriteMode::Replace => "replace" },
            "expected_hash":request.expected_hash,"overwrite":request.overwrite,
            "create_parents":request.create_parents,
        }),
        DependencyRequest::Edit(request) => json!({
            "action":"filesystem.edit","path":request.path,
            "replacements":request.replacements.iter().map(|item| json!({
                "old":item.old,"new":item.new,"expected_occurrences":item.expected_occurrences
            })).collect::<Vec<Value>>(),
            "expected_hash":request.expected_hash,
        }),
        DependencyRequest::ApplyPatch(request) => json!({
            "action":"filesystem.apply_patch",
            "patch_hash":ContentHash::digest(request.patch.as_bytes()).to_hex(),
            "patch_bytes":request.patch.len(),
            "base_hashes":request.base_hashes,
            "create_parents":request.create_parents,
        }),
        DependencyRequest::Health | DependencyRequest::Authorized { .. } => {
            return Err(DependencyError::InvalidAuthorizationEnvelope);
        }
    };
    serde_json::to_vec(&value)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| DependencyError::CanonicalEncoding)
}

fn metadata_record(metadata: &Metadata) -> FileMetadataRecord {
    FileMetadataRecord {
        size: metadata.len(),
        readonly: metadata.permissions().readonly(),
        modified_ms: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .and_then(|duration| u64::try_from(duration.as_millis()).ok()),
        kind: if metadata.file_type().is_symlink() {
            EntryKind::Symlink
        } else if metadata.is_file() {
            EntryKind::File
        } else if metadata.is_dir() {
            EntryKind::Directory
        } else {
            EntryKind::Other
        },
    }
}

fn decode_bytes(bytes: &[u8]) -> (bool, String, Option<Cow<'_, str>>) {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let (text, _, malformed) = UTF_16LE.decode(&bytes[2..]);
        return (malformed, "utf-16le".into(), (!malformed).then_some(text));
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let (text, _, malformed) = UTF_16BE.decode(&bytes[2..]);
        return (malformed, "utf-16be".into(), (!malformed).then_some(text));
    }
    if let Ok(text) = std::str::from_utf8(bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes))
    {
        let binary = is_binary(bytes);
        return (
            binary,
            "utf-8".into(),
            (!binary).then_some(Cow::Borrowed(text)),
        );
    }
    if is_binary(bytes) {
        return (true, "binary".into(), None);
    }
    let (text, _, _) = WINDOWS_1252.decode(bytes);
    (false, "windows-1252".into(), Some(text))
}

fn is_binary(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return false;
    }
    if bytes.contains(&0) {
        return true;
    }
    let controls = bytes
        .iter()
        .filter(|byte| **byte < 0x09 || (0x0E..0x20).contains(&**byte))
        .count();
    controls.saturating_mul(20) > bytes.len()
}

fn hash_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0F) as usize] as char);
    }
    output
}

fn build_globs(patterns: &[String]) -> Result<GlobSet, DependencyError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(
            Glob::new(pattern).map_err(|error| DependencyError::InvalidGlob(error.to_string()))?,
        );
    }
    builder
        .build()
        .map_err(|error| DependencyError::InvalidGlob(error.to_string()))
}

fn nearest_existing(path: &Path) -> Result<&Path, DependencyError> {
    let mut current = path;
    loop {
        if current.exists() {
            return Ok(current);
        }
        current = current
            .parent()
            .ok_or_else(|| DependencyError::ParentMissing(path.display().to_string()))?;
    }
}

fn atomic_write(path: &Path, bytes: &[u8], no_clobber: bool) -> Result<(), DependencyError> {
    let parent = path
        .parent()
        .ok_or_else(|| DependencyError::InvalidPath(path.display().to_string()))?;
    let mut temporary =
        NamedTempFile::new_in(parent).map_err(|error| io_error("stage write", path, error))?;
    temporary
        .write_all(bytes)
        .map_err(|error| io_error("write staging file", path, error))?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| io_error("sync staging file", path, error))?;
    if no_clobber {
        temporary
            .persist_noclobber(path)
            .map_err(|error| io_error("commit create", path, error.error))?;
    } else {
        temporary
            .persist(path)
            .map_err(|error| io_error("commit replace", path, error.error))?;
    }
    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn mutation_record(path: &Path, old: Option<&[u8]>, new: &[u8]) -> MutationRecord {
    let diff = match (
        old.and_then(|bytes| std::str::from_utf8(bytes).ok()),
        std::str::from_utf8(new),
    ) {
        (Some(old), Ok(new)) => {
            let generated_diff = create_patch(old, new);
            format!("{}", PatchFormatter::new().fmt_patch(&generated_diff))
        }
        (None, Ok(new)) => {
            let generated_diff = create_patch("", new);
            format!("{}", PatchFormatter::new().fmt_patch(&generated_diff))
        }
        _ => format!(
            "binary change: {} -> {} bytes",
            old.map_or(0, <[u8]>::len),
            new.len()
        ),
    };
    MutationRecord {
        path: path.display().to_string(),
        old_hash: old.map(hash_bytes),
        new_hash: hash_bytes(new),
        diff,
        bytes_written: new.len() as u64,
    }
}

struct PatchSection {
    path: String,
    text: String,
}

struct PatchPlan {
    path: PathBuf,
    existed: bool,
    old: Vec<u8>,
    new: Vec<u8>,
}

fn split_patch(source: &str) -> Result<Vec<PatchSection>, DependencyError> {
    if source.len() > 8 * 1024 * 1024 {
        return Err(DependencyError::PatchTooLarge);
    }
    let lines: Vec<_> = source.split_inclusive('\n').collect();
    let mut starts = Vec::new();
    for index in 0..lines.len().saturating_sub(1) {
        if lines[index].starts_with("--- ") && lines[index + 1].starts_with("+++ ") {
            starts.push(index);
        }
    }
    if starts.is_empty() {
        return Err(DependencyError::InvalidPatch(
            "no unified file header".into(),
        ));
    }
    let mut sections = Vec::new();
    for (position, start) in starts.iter().copied().enumerate() {
        let end = starts.get(position + 1).copied().unwrap_or(lines.len());
        let old_name = patch_header_path(lines[start].trim_end(), "--- ")?;
        let new_name = patch_header_path(lines[start + 1].trim_end(), "+++ ")?;
        if old_name == "/dev/null" || new_name == "/dev/null" {
            return Err(DependencyError::UnsupportedPatchOperation(
                "file creation and deletion markers are not supported".into(),
            ));
        }
        let normalized_old = strip_patch_prefix(&old_name);
        let normalized_new = strip_patch_prefix(&new_name);
        if normalized_old != normalized_new {
            return Err(DependencyError::UnsupportedPatchOperation(
                "renames are not supported".into(),
            ));
        }
        sections.push(PatchSection {
            path: normalized_new,
            text: lines[start..end].concat(),
        });
    }
    Ok(sections)
}

fn patch_header_path(line: &str, prefix: &str) -> Result<String, DependencyError> {
    line.strip_prefix(prefix)
        .and_then(|value| value.split('\t').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| DependencyError::InvalidPatch("invalid file header".into()))
}

fn strip_patch_prefix(path: &str) -> String {
    path.strip_prefix("a/")
        .or_else(|| path.strip_prefix("b/"))
        .unwrap_or(path)
        .replace('\\', "/")
}

fn has_windows_device_name(path: &Path) -> bool {
    path.components().any(|component| {
        let name = component
            .as_os_str()
            .to_string_lossy()
            .trim_end_matches(['.', ' '])
            .split('.')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        matches!(name.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || (name.len() == 4
                && (name.starts_with("COM") || name.starts_with("LPT"))
                && name.as_bytes()[3].is_ascii_digit()
                && name.as_bytes()[3] != b'0')
    })
}

#[allow(clippy::needless_pass_by_value)]
fn io_error(operation: &'static str, path: &Path, error: std::io::Error) -> DependencyError {
    DependencyError::Io {
        operation,
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

/// Dependency-layer failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DependencyError {
    /// Host has no configured authorization trust root.
    #[error("filesystem authorization is not configured")]
    AuthorizationRequired,
    /// Envelope is structurally invalid or wraps a non-operation.
    #[error("filesystem authorization envelope is invalid")]
    InvalidAuthorizationEnvelope,
    /// Grant authentication, time, binding, action, or digest failed.
    #[error("filesystem authorization was denied")]
    AuthorizationDenied,
    /// Single-use nonce was already consumed.
    #[error("filesystem authorization grant was replayed")]
    AuthorizationReplay,
    /// Replay state could not be accessed safely.
    #[error("filesystem authorization state is unavailable")]
    AuthorizationState,
    /// System clock cannot produce a portable authorization timestamp.
    #[error("filesystem authorization clock is unavailable")]
    Clock,
    /// Canonical request encoding failed.
    #[error("filesystem canonical request encoding failed")]
    CanonicalEncoding,
    /// No approved roots were configured.
    #[error("at least one approved root is required")]
    NoApprovedRoots,
    /// Root is not a directory.
    #[error("approved root is not a directory: {0}")]
    RootNotDirectory(String),
    /// Limit is zero or otherwise invalid.
    #[error("invalid limit `{0}`")]
    InvalidLimit(&'static str),
    /// Path is empty or malformed.
    #[error("invalid path `{0}`")]
    InvalidPath(String),
    /// Lexical traversal was rejected.
    #[error("parent traversal is rejected: {0}")]
    TraversalRejected(String),
    /// Canonical target escapes all roots.
    #[error("path `{requested}` resolves outside approved roots to `{resolved}`")]
    SymlinkEscape {
        /// Requested path.
        requested: String,
        /// Canonical target.
        resolved: String,
    },
    /// Device or special file rejected.
    #[error("device or special file rejected: {0}")]
    DeviceRejected(String),
    /// Sensitive path policy rejected the target.
    #[error("sensitive path rejected: {0}")]
    SensitivePathRejected(String),
    /// Target is not a regular file.
    #[error("not a regular file: {0}")]
    NotRegularFile(String),
    /// Target is not a directory.
    #[error("not a directory: {0}")]
    NotDirectory(String),
    /// Target missing.
    #[error("path not found: {0}")]
    NotFound(String),
    /// Parent missing.
    #[error("parent directory is missing: {0}")]
    ParentMissing(String),
    /// Destination exists.
    #[error("destination already exists: {0}")]
    AlreadyExists(String),
    /// Replacement lacks explicit overwrite approval.
    #[error("overwrite was not approved: {0}")]
    OverwriteNotApproved(String),
    /// File exceeds bounded operation size.
    #[error("file `{path}` is {size} bytes; maximum is {maximum}")]
    FileTooLarge {
        /// Path.
        path: String,
        /// Actual bytes.
        size: u64,
        /// Maximum bytes.
        maximum: u64,
    },
    /// Invalid line range.
    #[error("invalid one-based line range {start}..={end}")]
    InvalidLineRange {
        /// Start.
        start: usize,
        /// End.
        end: usize,
    },
    /// Text operation applied to binary content.
    #[error("text operation requires a text file: {0}")]
    BinaryTextOperation(String),
    /// Glob syntax invalid.
    #[error("invalid glob: {0}")]
    InvalidGlob(String),
    /// Pattern list empty.
    #[error("at least one pattern is required")]
    EmptyPatterns,
    /// Search pattern empty.
    #[error("search pattern is empty")]
    EmptySearchPattern,
    /// Regex invalid.
    #[error("invalid regular expression: {0}")]
    InvalidRegex(String),
    /// Ignore walker failure.
    #[error("directory traversal failed: {0}")]
    Walk(String),
    /// Expected hash mismatch.
    #[error("hash mismatch for `{path}`: expected {expected}, actual {actual}")]
    HashMismatch {
        /// Path.
        path: String,
        /// Expected hash.
        expected: String,
        /// Actual hash.
        actual: String,
    },
    /// File changed during operation.
    #[error("file changed during operation: {0}")]
    ConcurrentModification(String),
    /// Edit replacements empty.
    #[error("at least one replacement is required")]
    EmptyReplacements,
    /// Replacement definition invalid.
    #[error("replacement {index} has empty old text or zero expected occurrences")]
    InvalidReplacement {
        /// Replacement index.
        index: usize,
    },
    /// Exact occurrence mismatch.
    #[error("replacement {index} expected {expected} occurrences, found {actual}")]
    ReplacementMismatch {
        /// Replacement index.
        index: usize,
        /// Expected.
        expected: usize,
        /// Actual.
        actual: usize,
    },
    /// Patch source exceeds hard bound.
    #[error("patch exceeds 8 MiB")]
    PatchTooLarge,
    /// Unified patch invalid.
    #[error("invalid unified patch: {0}")]
    InvalidPatch(String),
    /// Patch operation unsupported.
    #[error("unsupported patch operation: {0}")]
    UnsupportedPatchOperation(String),
    /// Duplicate patch target.
    #[error("patch contains duplicate target `{0}`")]
    DuplicatePatchPath(String),
    /// Base hash omitted.
    #[error("patch is missing base hash for `{0}`")]
    MissingBaseHash(String),
    /// Patch hunks do not apply.
    #[error("patch does not apply: {0}")]
    PatchDoesNotApply(String),
    /// Commit failed and rollback outcome is explicit.
    #[error(
        "patch commit failed at file {failed_index}: {detail}; rollback errors: {rollback_errors:?}"
    )]
    PatchCommitFailed {
        /// Failed plan index.
        failed_index: usize,
        /// Commit error.
        detail: String,
        /// Empty only when rollback completed.
        rollback_errors: Vec<String>,
    },
    /// Operating-system failure.
    #[error("{operation} failed for `{path}`: {detail}")]
    Io {
        /// Operation.
        operation: &'static str,
        /// Safe path.
        path: String,
        /// OS detail.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use agentmod_protocol_support::authorization::{AuthorizationClaims, seal_authorization};

    use super::*;

    static NONCE: AtomicU64 = AtomicU64::new(1);

    fn configured(root: &Path, maximum: u64) -> FilesystemDependencyConfig {
        FilesystemDependencyConfig::new(vec![root.to_path_buf()], Vec::new(), maximum)
            .expect("config")
            .with_authorization(FilesystemAuthorizationConfig {
                owner: "owner".into(),
                session: "session".into(),
                key: Arc::new(AuthorizationKey::from_bytes([9; 32])),
            })
    }

    fn fixture() -> (tempfile::TempDir, NativeFilesystem) {
        let root = tempfile::tempdir().expect("temp root");
        let config = configured(root.path(), 1024 * 1024);
        (root, NativeFilesystem::new(config))
    }

    fn execute(filesystem: &NativeFilesystem, request: DependencyRequest) -> DependencyResponse {
        try_execute(filesystem, request).expect("filesystem operation")
    }

    fn try_execute(
        filesystem: &NativeFilesystem,
        request: DependencyRequest,
    ) -> Result<DependencyResponse, DependencyError> {
        let action = operation_action(&request).expect("operation action");
        let digest = canonical_operation_digest(&request).expect("canonical digest");
        let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis();
        let now = i64::try_from(now).expect("timestamp");
        let authorization = filesystem
            .config
            .authorization
            .as_ref()
            .expect("authorization");
        let call_id = format!("call-{nonce}");
        let token = seal_authorization(
            &AuthorizationClaims {
                owner: authorization.owner.clone(),
                session: authorization.session.clone(),
                call_id: call_id.clone(),
                action: action.into(),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(now - 1),
                expires_at: TimestampMillis::new(now + 30_000),
                nonce: format!("nonce-{nonce}"),
            },
            authorization.key.as_ref(),
        )
        .expect("seal");
        filesystem.execute(DependencyRequest::Authorized {
            authorization: DependencyAuthorization {
                call_id,
                action: action.into(),
                normalized_digest: digest.to_hex(),
                grant: token,
            },
            operation: Box::new(request),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn wrap(
        filesystem: &NativeFilesystem,
        request: DependencyRequest,
        owner: &str,
        digest: ContentHash,
        issued_at: i64,
        expires_at: i64,
        nonce: &str,
        signing_key: &AuthorizationKey,
    ) -> DependencyRequest {
        let action = operation_action(&request).expect("action");
        let call_id = format!("security-{nonce}");
        let configured = filesystem
            .config
            .authorization
            .as_ref()
            .expect("authorization");
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: owner.into(),
                session: configured.session.clone(),
                call_id: call_id.clone(),
                action: action.into(),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(issued_at),
                expires_at: TimestampMillis::new(expires_at),
                nonce: nonce.into(),
            },
            signing_key,
        )
        .expect("grant");
        DependencyRequest::Authorized {
            authorization: DependencyAuthorization {
                call_id,
                action: action.into(),
                normalized_digest: digest.to_hex(),
                grant,
            },
            operation: Box::new(request),
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn invalid_grants_never_read_or_mutate_the_filesystem() {
        let (root, filesystem) = fixture();
        fs::write(root.path().join("secret.txt"), "classified").expect("secret");
        let configured = filesystem
            .config
            .authorization
            .as_ref()
            .expect("authorization");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis();
        let now = i64::try_from(now).expect("timestamp");

        let read = DependencyRequest::Read(ReadRequest {
            path: "secret.txt".into(),
            range: ReadRange::All,
            max_projection_bytes: 1024,
        });
        let read_digest = canonical_operation_digest(&read).expect("digest");
        let forged = wrap(
            &filesystem,
            read.clone(),
            "owner",
            read_digest,
            now - 1,
            now + 1_000,
            "forged",
            &AuthorizationKey::from_bytes([3; 32]),
        );
        assert!(matches!(
            filesystem.execute(forged),
            Err(DependencyError::AuthorizationDenied)
        ));

        let wrong_owner = wrap(
            &filesystem,
            read.clone(),
            "other-owner",
            read_digest,
            now - 1,
            now + 1_000,
            "wrong-owner",
            configured.key.as_ref(),
        );
        assert!(matches!(
            filesystem.execute(wrong_owner),
            Err(DependencyError::AuthorizationDenied)
        ));

        let expired = wrap(
            &filesystem,
            read,
            "owner",
            read_digest,
            now - 20_000,
            now - 10_000,
            "expired",
            configured.key.as_ref(),
        );
        assert!(matches!(
            filesystem.execute(expired),
            Err(DependencyError::AuthorizationDenied)
        ));

        for (name, nonce) in [
            ("forged.txt", "forged-write"),
            ("tampered.txt", "tampered-write"),
            ("wrong-digest.txt", "wrong-digest-write"),
            ("wrong-action.txt", "wrong-action-write"),
        ] {
            let write = DependencyRequest::Write(WriteRequest {
                path: name.into(),
                content: b"must-not-exist".to_vec(),
                mode: WriteMode::Create,
                expected_hash: None,
                overwrite: false,
                create_parents: false,
            });
            let actual = canonical_operation_digest(&write).expect("digest");
            let mut request = if name == "forged.txt" {
                wrap(
                    &filesystem,
                    write,
                    "owner",
                    actual,
                    now - 1,
                    now + 1_000,
                    nonce,
                    &AuthorizationKey::from_bytes([4; 32]),
                )
            } else if name == "wrong-digest.txt" {
                wrap(
                    &filesystem,
                    write,
                    "owner",
                    ContentHash::digest(b"different operation"),
                    now - 1,
                    now + 1_000,
                    nonce,
                    configured.key.as_ref(),
                )
            } else {
                wrap(
                    &filesystem,
                    write,
                    "owner",
                    actual,
                    now - 1,
                    now + 1_000,
                    nonce,
                    configured.key.as_ref(),
                )
            };
            if name == "tampered.txt"
                && let DependencyRequest::Authorized { authorization, .. } = &mut request
            {
                authorization.grant.push('0');
            }
            if name == "wrong-action.txt"
                && let DependencyRequest::Authorized { authorization, .. } = &mut request
            {
                authorization.action = "filesystem.read".into();
            }
            assert!(filesystem.execute(request).is_err());
            assert!(!root.path().join(name).exists());
        }

        let replay_write = DependencyRequest::Write(WriteRequest {
            path: "once.txt".into(),
            content: b"once".to_vec(),
            mode: WriteMode::Create,
            expected_hash: None,
            overwrite: false,
            create_parents: false,
        });
        let replay = wrap(
            &filesystem,
            replay_write,
            "owner",
            canonical_operation_digest(&DependencyRequest::Write(WriteRequest {
                path: "once.txt".into(),
                content: b"once".to_vec(),
                mode: WriteMode::Create,
                expected_hash: None,
                overwrite: false,
                create_parents: false,
            }))
            .expect("digest"),
            now - 1,
            now + 1_000,
            "replay",
            configured.key.as_ref(),
        );
        filesystem.execute(replay.clone()).expect("first use");
        assert!(matches!(
            filesystem.execute(replay),
            Err(DependencyError::AuthorizationReplay)
        ));
        assert_eq!(
            fs::read(root.path().join("once.txt")).expect("once"),
            b"once"
        );
    }

    #[test]
    fn missing_key_and_raw_operations_are_fail_closed() {
        let root = tempfile::tempdir().expect("root");
        let filesystem = NativeFilesystem::new(
            FilesystemDependencyConfig::new(vec![root.path().to_path_buf()], Vec::new(), 1024)
                .expect("config"),
        );
        let operation = DependencyRequest::Write(WriteRequest {
            path: "denied.txt".into(),
            content: b"denied".to_vec(),
            mode: WriteMode::Create,
            expected_hash: None,
            overwrite: false,
            create_parents: false,
        });
        assert!(matches!(
            filesystem.execute(operation.clone()),
            Err(DependencyError::AuthorizationRequired)
        ));
        let digest = canonical_operation_digest(&operation).expect("digest");
        assert!(matches!(
            filesystem.execute(DependencyRequest::Authorized {
                authorization: DependencyAuthorization {
                    call_id: "call".into(),
                    action: "filesystem.write".into(),
                    normalized_digest: digest.to_hex(),
                    grant: "not-a-grant".into(),
                },
                operation: Box::new(operation),
            }),
            Err(DependencyError::AuthorizationRequired)
        ));
        assert!(!root.path().join("denied.txt").exists());
    }

    #[test]
    fn read_detects_text_binary_ranges_hash_and_projection_limits() {
        let (root, filesystem) = fixture();
        fs::write(root.path().join("text.txt"), "one\ntwo\nthree\n").expect("text");
        let DependencyResponse::Read(record) = execute(
            &filesystem,
            DependencyRequest::Read(ReadRequest {
                path: "text.txt".into(),
                range: ReadRange::Lines { start: 2, end: 3 },
                max_projection_bytes: 1024,
            }),
        ) else {
            panic!("read response");
        };
        assert_eq!(
            record.lines,
            vec![
                NumberedLine {
                    number: 2,
                    text: "two".into(),
                },
                NumberedLine {
                    number: 3,
                    text: "three".into(),
                },
            ]
        );
        assert_eq!(record.content_hash, hash_bytes(b"one\ntwo\nthree\n"));
        assert_eq!(record.encoding, "utf-8");

        fs::write(root.path().join("binary.bin"), [0, 1, 2, 3]).expect("binary");
        let DependencyResponse::Read(binary) = execute(
            &filesystem,
            DependencyRequest::Read(ReadRequest {
                path: "binary.bin".into(),
                range: ReadRange::All,
                max_projection_bytes: 2,
            }),
        ) else {
            panic!("binary response");
        };
        assert!(binary.binary);
        assert_eq!(binary.bytes_hex.as_deref(), Some("0001"));
        assert!(binary.truncated);
    }

    #[test]
    fn list_glob_and_grep_are_stable_bounded_and_ignore_binary() {
        let (root, filesystem) = fixture();
        fs::create_dir(root.path().join("src")).expect("src");
        fs::write(root.path().join("src/b.rs"), "zero\nneedle two\n").expect("b");
        fs::write(root.path().join("src/a.rs"), "needle one\nnext\n").expect("a");
        fs::write(root.path().join("src/skip.log"), "needle").expect("log");
        fs::write(root.path().join(".hidden"), "hidden").expect("hidden");
        fs::write(root.path().join("src/binary.rs"), [0, 1, 2]).expect("binary");

        let DependencyResponse::Entries(listed) = execute(
            &filesystem,
            DependencyRequest::List(ListRequest {
                path: ".".into(),
                max_depth: 4,
                include_hidden: false,
                honor_ignore: true,
                ignore_patterns: vec!["**/*.log".into()],
                max_results: 2,
            }),
        ) else {
            panic!("list");
        };
        assert_eq!(listed.entries.len(), 2);
        assert!(listed.truncated);
        assert!(
            listed
                .entries
                .windows(2)
                .all(|pair| pair[0].path <= pair[1].path)
        );

        let DependencyResponse::Entries(globbed) = execute(
            &filesystem,
            DependencyRequest::Glob(GlobRequest {
                path: ".".into(),
                patterns: vec!["**/*.rs".into()],
                include_hidden: false,
                honor_ignore: true,
                max_results: 10,
            }),
        ) else {
            panic!("glob");
        };
        assert_eq!(globbed.entries.len(), 3);

        let DependencyResponse::Grep(grep) = execute(
            &filesystem,
            DependencyRequest::Grep(GrepRequest {
                path: ".".into(),
                pattern: "needle".into(),
                regex: false,
                case_insensitive: false,
                file_patterns: vec!["**/*.rs".into()],
                before_context: 0,
                after_context: 1,
                max_matches: 10,
            }),
        ) else {
            panic!("grep");
        };
        assert_eq!(grep.matches.len(), 2);
        assert_eq!(grep.binary_files_skipped, 1);
        assert_eq!(grep.matches[0].line, 1);
        assert_eq!(grep.matches[0].after[0].text, "next");
    }

    #[test]
    fn atomic_write_and_edit_enforce_hashes_and_exact_counts() {
        let (root, filesystem) = fixture();
        let DependencyResponse::Mutation(created) = execute(
            &filesystem,
            DependencyRequest::Write(WriteRequest {
                path: "file.txt".into(),
                content: b"alpha alpha\n".to_vec(),
                mode: WriteMode::Create,
                expected_hash: None,
                overwrite: false,
                create_parents: false,
            }),
        ) else {
            panic!("create");
        };
        assert_eq!(
            fs::read(root.path().join("file.txt")).expect("read"),
            b"alpha alpha\n"
        );

        let error = try_execute(
            &filesystem,
            DependencyRequest::Write(WriteRequest {
                path: "file.txt".into(),
                content: b"destroyed".to_vec(),
                mode: WriteMode::Replace,
                expected_hash: Some(hash_bytes(b"wrong")),
                overwrite: true,
                create_parents: false,
            }),
        );
        assert!(matches!(error, Err(DependencyError::HashMismatch { .. })));
        assert_eq!(
            fs::read(root.path().join("file.txt")).expect("read"),
            b"alpha alpha\n"
        );

        let DependencyResponse::Mutation(edited) = execute(
            &filesystem,
            DependencyRequest::Edit(EditRequest {
                path: "file.txt".into(),
                replacements: vec![
                    ExactReplacement {
                        old: "alpha".into(),
                        new: "beta".into(),
                        expected_occurrences: 2,
                    },
                    ExactReplacement {
                        old: "beta beta".into(),
                        new: "done".into(),
                        expected_occurrences: 1,
                    },
                ],
                expected_hash: Some(created.new_hash),
            }),
        ) else {
            panic!("edit");
        };
        assert_eq!(
            fs::read_to_string(root.path().join("file.txt")).expect("text"),
            "done\n"
        );
        assert!(edited.diff.contains("+done"));

        let mismatch = try_execute(
            &filesystem,
            DependencyRequest::Edit(EditRequest {
                path: "file.txt".into(),
                replacements: vec![ExactReplacement {
                    old: "done".into(),
                    new: "bad".into(),
                    expected_occurrences: 2,
                }],
                expected_hash: Some(edited.new_hash),
            }),
        );
        assert!(matches!(
            mismatch,
            Err(DependencyError::ReplacementMismatch { .. })
        ));
        assert_eq!(
            fs::read_to_string(root.path().join("file.txt")).expect("text"),
            "done\n"
        );
    }

    #[test]
    fn multi_file_patch_prevalidates_every_base_before_any_commit() {
        let (root, filesystem) = fixture();
        fs::write(root.path().join("a.txt"), "one\n").expect("a");
        fs::write(root.path().join("b.txt"), "two\n").expect("b");
        let patch = "\
--- a/a.txt
+++ b/a.txt
@@ -1 +1 @@
-one
+ONE
--- a/b.txt
+++ b/b.txt
@@ -1 +1 @@
-two
+TWO
";
        let mut bad_hashes = BTreeMap::new();
        bad_hashes.insert("a.txt".into(), hash_bytes(b"one\n"));
        bad_hashes.insert("b.txt".into(), hash_bytes(b"wrong\n"));
        let rejected = try_execute(
            &filesystem,
            DependencyRequest::ApplyPatch(PatchRequest {
                patch: patch.into(),
                base_hashes: bad_hashes,
                create_parents: false,
            }),
        );
        assert!(matches!(
            rejected,
            Err(DependencyError::HashMismatch { .. })
        ));
        assert_eq!(
            fs::read_to_string(root.path().join("a.txt")).expect("a"),
            "one\n"
        );

        let mut hashes = BTreeMap::new();
        hashes.insert("a.txt".into(), hash_bytes(b"one\n"));
        hashes.insert("b.txt".into(), hash_bytes(b"two\n"));
        let DependencyResponse::Patch(result) = execute(
            &filesystem,
            DependencyRequest::ApplyPatch(PatchRequest {
                patch: patch.into(),
                base_hashes: hashes,
                create_parents: false,
            }),
        ) else {
            panic!("patch");
        };
        assert_eq!(result.files.len(), 2);
        assert_eq!(
            fs::read_to_string(root.path().join("a.txt")).expect("a"),
            "ONE\n"
        );
        assert_eq!(
            fs::read_to_string(root.path().join("b.txt")).expect("b"),
            "TWO\n"
        );
    }

    #[test]
    fn traversal_sensitive_paths_and_file_limits_are_rejected() {
        let (root, filesystem) = fixture();
        assert!(matches!(
            try_execute(
                &filesystem,
                DependencyRequest::Read(ReadRequest {
                    path: "../outside".into(),
                    range: ReadRange::All,
                    max_projection_bytes: 10,
                })
            ),
            Err(DependencyError::TraversalRejected(_))
        ));
        fs::write(root.path().join(".env"), "SECRET=x").expect("sensitive");
        assert!(matches!(
            try_execute(
                &filesystem,
                DependencyRequest::Read(ReadRequest {
                    path: ".env".into(),
                    range: ReadRange::All,
                    max_projection_bytes: 10,
                })
            ),
            Err(DependencyError::SensitivePathRejected(_))
        ));

        let limited = NativeFilesystem::new(configured(root.path(), 2));
        fs::write(root.path().join("large"), "large").expect("large");
        assert!(matches!(
            try_execute(
                &limited,
                DependencyRequest::Read(ReadRequest {
                    path: "large".into(),
                    range: ReadRange::All,
                    max_projection_bytes: 10,
                })
            ),
            Err(DependencyError::FileTooLarge { .. })
        ));
    }

    #[test]
    fn symlink_escape_is_rejected_when_platform_allows_symlink_creation() {
        let (root, filesystem) = fixture();
        let outside = tempfile::tempdir().expect("outside");
        let target = outside.path().join("secret.txt");
        fs::write(&target, "secret").expect("secret");
        let link = root.path().join("link.txt");
        #[cfg(unix)]
        let created = std::os::unix::fs::symlink(&target, &link);
        #[cfg(windows)]
        let created = std::os::windows::fs::symlink_file(&target, &link);
        if created.is_err() {
            return;
        }
        assert!(matches!(
            try_execute(
                &filesystem,
                DependencyRequest::Read(ReadRequest {
                    path: "link.txt".into(),
                    range: ReadRange::All,
                    max_projection_bytes: 10,
                })
            ),
            Err(DependencyError::SymlinkEscape { .. })
        ));
    }
}
