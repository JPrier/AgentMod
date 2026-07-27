//! Filesystem-host business dataset construction.

use std::collections::BTreeMap;

use agentmod_filesystem_host_dependency as dependency;
use thiserror::Error;

/// Data-owned operation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataRequest {
    /// Health.
    Health,
    /// Authorized operation.
    Authorized {
        /// Data-owned authorization envelope.
        authorization: DataAuthorization,
        /// Exact operation.
        operation: Box<DataRequest>,
    },
    /// Read.
    Read {
        /// Path.
        path: String,
        /// Range.
        range: DataReadRange,
        /// Projection bound.
        max_projection_bytes: usize,
    },
    /// List/tree.
    List {
        /// Path.
        path: String,
        /// Depth.
        max_depth: usize,
        /// Hidden entries.
        include_hidden: bool,
        /// Ignore files.
        honor_ignore: bool,
        /// Extra excludes.
        ignore_patterns: Vec<String>,
        /// Result limit.
        max_results: usize,
    },
    /// Glob.
    Glob {
        /// Path.
        path: String,
        /// Patterns.
        patterns: Vec<String>,
        /// Hidden entries.
        include_hidden: bool,
        /// Ignore files.
        honor_ignore: bool,
        /// Result limit.
        max_results: usize,
    },
    /// Grep.
    Grep {
        /// Path.
        path: String,
        /// Search.
        pattern: String,
        /// Regex mode.
        regex: bool,
        /// Case insensitive.
        case_insensitive: bool,
        /// File patterns.
        file_patterns: Vec<String>,
        /// Context before.
        before_context: usize,
        /// Context after.
        after_context: usize,
        /// Match limit.
        max_matches: usize,
    },
    /// Atomic write.
    Write {
        /// Path.
        path: String,
        /// Bytes.
        content: Vec<u8>,
        /// Mode.
        mode: DataWriteMode,
        /// Expected hash.
        expected_hash: Option<String>,
        /// Explicit overwrite.
        overwrite: bool,
        /// Parent policy.
        create_parents: bool,
    },
    /// Exact edit.
    Edit {
        /// Path.
        path: String,
        /// Replacements.
        replacements: Vec<DataReplacement>,
        /// Expected hash.
        expected_hash: Option<String>,
    },
    /// Unified patch.
    ApplyPatch {
        /// Patch.
        patch: String,
        /// Base hashes.
        base_hashes: BTreeMap<String, String>,
        /// Parent policy.
        create_parents: bool,
    },
}

/// Data-owned authorization envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataAuthorization {
    /// Runtime call ID.
    pub call_id: String,
    /// Stable action ID.
    pub action: String,
    /// Runtime normalized digest.
    pub normalized_digest: String,
    /// Opaque keyed grant.
    pub grant: String,
}

/// Data-owned read range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataReadRange {
    /// Complete bounded projection.
    All,
    /// Inclusive lines.
    Lines {
        /// Start.
        start: usize,
        /// End.
        end: usize,
    },
    /// Byte range.
    Bytes {
        /// Offset.
        offset: u64,
        /// Length.
        length: usize,
    },
}

/// Data-owned write mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataWriteMode {
    /// Create.
    Create,
    /// Replace.
    Replace,
}

/// Data-owned replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataReplacement {
    /// Old text.
    pub old: String,
    /// New text.
    pub new: String,
    /// Expected count.
    pub expected_occurrences: usize,
}

/// Data-owned normalized response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataResponse {
    /// Health.
    Health {
        /// Approved roots.
        roots: Vec<String>,
        /// File bound.
        max_file_bytes: u64,
        /// Dependency authorization state.
        authorization_ready: bool,
    },
    /// Read.
    Read(DataReadRecord),
    /// Entries.
    Entries {
        /// Stable entries.
        entries: Vec<DataEntryRecord>,
        /// Overflow.
        truncated: bool,
    },
    /// Grep.
    Grep {
        /// Matches.
        matches: Vec<DataMatchRecord>,
        /// Overflow.
        truncated: bool,
        /// Binary skips.
        binary_files_skipped: usize,
    },
    /// Mutation.
    Mutation(DataMutationRecord),
    /// Patch.
    Patch {
        /// Per-file mutations.
        files: Vec<DataMutationRecord>,
        /// Atomicity statement.
        atomicity: String,
    },
}

/// Data-owned read record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataReadRecord {
    /// Path.
    pub path: String,
    /// Hash.
    pub content_hash: String,
    /// Size.
    pub size: u64,
    /// Read-only.
    pub readonly: bool,
    /// Encoding.
    pub encoding: String,
    /// Binary.
    pub binary: bool,
    /// Lines.
    pub lines: Vec<DataLineRecord>,
    /// Hex bytes.
    pub bytes_hex: Option<String>,
    /// Overflow.
    pub truncated: bool,
}

/// Data-owned numbered line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataLineRecord {
    /// Line.
    pub number: usize,
    /// Text.
    pub text: String,
}

/// Data-owned entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataEntryRecord {
    /// Path.
    pub path: String,
    /// Depth.
    pub depth: usize,
    /// Size.
    pub size: u64,
    /// Stable kind.
    pub kind: String,
}

/// Data-owned grep match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataMatchRecord {
    /// Path.
    pub path: String,
    /// Line.
    pub line: usize,
    /// Column.
    pub column: usize,
    /// Text.
    pub text: String,
    /// Context before.
    pub before: Vec<DataLineRecord>,
    /// Context after.
    pub after: Vec<DataLineRecord>,
}

/// Data-owned mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataMutationRecord {
    /// Path.
    pub path: String,
    /// Old hash.
    pub old_hash: Option<String>,
    /// New hash.
    pub new_hash: String,
    /// Diff.
    pub diff: String,
    /// Bytes.
    pub bytes_written: u64,
}

/// Narrow data interface consumed only by filesystem logic.
pub trait FilesystemDataPort {
    /// Builds a normalized filesystem dataset.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the selected dependency rejects or cannot
    /// execute the request.
    fn execute(&self, request: DataRequest) -> Result<DataResponse, DataError>;
}

/// Data implementation over an injected dependency.
#[derive(Clone, Debug)]
pub struct FilesystemData<D> {
    dependency: D,
}

impl<D> FilesystemData<D> {
    /// Creates data routing.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D: dependency::FilesystemDependencyPort> FilesystemDataPort for FilesystemData<D> {
    fn execute(&self, request: DataRequest) -> Result<DataResponse, DataError> {
        self.dependency
            .execute(to_dependency_request(request))
            .map(to_data_response)
            .map_err(|error| DataError::Dependency {
                code: dependency_error_code(&error),
                detail: error.to_string(),
            })
    }
}

#[allow(clippy::too_many_lines)]
fn to_dependency_request(request: DataRequest) -> dependency::DependencyRequest {
    match request {
        DataRequest::Health => dependency::DependencyRequest::Health,
        DataRequest::Authorized {
            authorization,
            operation,
        } => dependency::DependencyRequest::Authorized {
            authorization: dependency::DependencyAuthorization {
                call_id: authorization.call_id,
                action: authorization.action,
                normalized_digest: authorization.normalized_digest,
                grant: authorization.grant,
            },
            operation: Box::new(to_dependency_request(*operation)),
        },
        DataRequest::Read {
            path,
            range,
            max_projection_bytes,
        } => dependency::DependencyRequest::Read(dependency::ReadRequest {
            path,
            range: match range {
                DataReadRange::All => dependency::ReadRange::All,
                DataReadRange::Lines { start, end } => dependency::ReadRange::Lines { start, end },
                DataReadRange::Bytes { offset, length } => {
                    dependency::ReadRange::Bytes { offset, length }
                }
            },
            max_projection_bytes,
        }),
        DataRequest::List {
            path,
            max_depth,
            include_hidden,
            honor_ignore,
            ignore_patterns,
            max_results,
        } => dependency::DependencyRequest::List(dependency::ListRequest {
            path,
            max_depth,
            include_hidden,
            honor_ignore,
            ignore_patterns,
            max_results,
        }),
        DataRequest::Glob {
            path,
            patterns,
            include_hidden,
            honor_ignore,
            max_results,
        } => dependency::DependencyRequest::Glob(dependency::GlobRequest {
            path,
            patterns,
            include_hidden,
            honor_ignore,
            max_results,
        }),
        DataRequest::Grep {
            path,
            pattern,
            regex,
            case_insensitive,
            file_patterns,
            before_context,
            after_context,
            max_matches,
        } => dependency::DependencyRequest::Grep(dependency::GrepRequest {
            path,
            pattern,
            regex,
            case_insensitive,
            file_patterns,
            before_context,
            after_context,
            max_matches,
        }),
        DataRequest::Write {
            path,
            content,
            mode,
            expected_hash,
            overwrite,
            create_parents,
        } => dependency::DependencyRequest::Write(dependency::WriteRequest {
            path,
            content,
            mode: match mode {
                DataWriteMode::Create => dependency::WriteMode::Create,
                DataWriteMode::Replace => dependency::WriteMode::Replace,
            },
            expected_hash,
            overwrite,
            create_parents,
        }),
        DataRequest::Edit {
            path,
            replacements,
            expected_hash,
        } => dependency::DependencyRequest::Edit(dependency::EditRequest {
            path,
            replacements: replacements
                .into_iter()
                .map(|item| dependency::ExactReplacement {
                    old: item.old,
                    new: item.new,
                    expected_occurrences: item.expected_occurrences,
                })
                .collect(),
            expected_hash,
        }),
        DataRequest::ApplyPatch {
            patch,
            base_hashes,
            create_parents,
        } => dependency::DependencyRequest::ApplyPatch(dependency::PatchRequest {
            patch,
            base_hashes,
            create_parents,
        }),
    }
}

fn to_data_response(response: dependency::DependencyResponse) -> DataResponse {
    match response {
        dependency::DependencyResponse::Health(record) => DataResponse::Health {
            roots: record.approved_roots,
            max_file_bytes: record.max_file_bytes,
            authorization_ready: record.authorization_ready,
        },
        dependency::DependencyResponse::Read(record) => DataResponse::Read(DataReadRecord {
            path: record.path,
            content_hash: record.content_hash,
            size: record.metadata.size,
            readonly: record.metadata.readonly,
            encoding: record.encoding,
            binary: record.binary,
            lines: record.lines.into_iter().map(to_data_line).collect(),
            bytes_hex: record.bytes_hex,
            truncated: record.truncated,
        }),
        dependency::DependencyResponse::Entries(record) => DataResponse::Entries {
            entries: record
                .entries
                .into_iter()
                .map(|entry| DataEntryRecord {
                    path: entry.path,
                    depth: entry.depth,
                    size: entry.metadata.size,
                    kind: format!("{:?}", entry.metadata.kind).to_ascii_lowercase(),
                })
                .collect(),
            truncated: record.truncated,
        },
        dependency::DependencyResponse::Grep(record) => DataResponse::Grep {
            matches: record
                .matches
                .into_iter()
                .map(|item| DataMatchRecord {
                    path: item.path,
                    line: item.line,
                    column: item.column,
                    text: item.text,
                    before: item.before.into_iter().map(to_data_line).collect(),
                    after: item.after.into_iter().map(to_data_line).collect(),
                })
                .collect(),
            truncated: record.truncated,
            binary_files_skipped: record.binary_files_skipped,
        },
        dependency::DependencyResponse::Mutation(record) => {
            DataResponse::Mutation(to_data_mutation(record))
        }
        dependency::DependencyResponse::Patch(record) => DataResponse::Patch {
            files: record.files.into_iter().map(to_data_mutation).collect(),
            atomicity: record.atomicity,
        },
    }
}

fn to_data_line(line: dependency::NumberedLine) -> DataLineRecord {
    DataLineRecord {
        number: line.number,
        text: line.text,
    }
}

fn to_data_mutation(record: dependency::MutationRecord) -> DataMutationRecord {
    DataMutationRecord {
        path: record.path,
        old_hash: record.old_hash,
        new_hash: record.new_hash,
        diff: record.diff,
        bytes_written: record.bytes_written,
    }
}

fn dependency_error_code(error: &dependency::DependencyError) -> &'static str {
    match error {
        dependency::DependencyError::AuthorizationRequired
        | dependency::DependencyError::InvalidAuthorizationEnvelope
        | dependency::DependencyError::AuthorizationDenied
        | dependency::DependencyError::AuthorizationReplay
        | dependency::DependencyError::AuthorizationState
        | dependency::DependencyError::Clock => "authorization_denied",
        dependency::DependencyError::TraversalRejected(_)
        | dependency::DependencyError::SymlinkEscape { .. }
        | dependency::DependencyError::DeviceRejected(_)
        | dependency::DependencyError::SensitivePathRejected(_) => "security_denied",
        dependency::DependencyError::HashMismatch { .. }
        | dependency::DependencyError::ConcurrentModification(_) => "conflict",
        dependency::DependencyError::FileTooLarge { .. } => "limit_exceeded",
        dependency::DependencyError::Io { .. }
        | dependency::DependencyError::Walk(_)
        | dependency::DependencyError::PatchCommitFailed { .. } => "io_failure",
        _ => "invalid_request",
    }
}

/// Data-layer failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DataError {
    /// Normalized dependency failure.
    #[error("filesystem dependency failed ({code}): {detail}")]
    Dependency {
        /// Stable class.
        code: &'static str,
        /// Sanitized detail.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    struct MockDependency {
        observed: RefCell<Vec<dependency::DependencyRequest>>,
    }

    impl dependency::FilesystemDependencyPort for MockDependency {
        fn execute(
            &self,
            request: dependency::DependencyRequest,
        ) -> Result<dependency::DependencyResponse, dependency::DependencyError> {
            self.observed.borrow_mut().push(request);
            Ok(dependency::DependencyResponse::Health(
                dependency::HealthRecord {
                    approved_roots: vec!["fixture".into()],
                    max_file_bytes: 100,
                    authorization_ready: true,
                },
            ))
        }
    }

    #[test]
    fn maps_health_through_direct_dependency_only() {
        let data = FilesystemData::new(MockDependency {
            observed: RefCell::new(Vec::new()),
        });
        assert_eq!(
            data.execute(DataRequest::Health).expect("health"),
            DataResponse::Health {
                roots: vec!["fixture".into()],
                max_file_bytes: 100,
                authorization_ready: true,
            }
        );
        assert_eq!(
            data.dependency.observed.into_inner(),
            vec![dependency::DependencyRequest::Health]
        );
    }

    #[test]
    fn maps_write_request_explicitly() {
        let request = DataRequest::Authorized {
            authorization: DataAuthorization {
                call_id: "call".into(),
                action: "filesystem.write".into(),
                normalized_digest: "digest".into(),
                grant: "grant".into(),
            },
            operation: Box::new(DataRequest::Write {
                path: "a.txt".into(),
                content: b"x".to_vec(),
                mode: DataWriteMode::Create,
                expected_hash: None,
                overwrite: false,
                create_parents: false,
            }),
        };
        assert_eq!(
            to_dependency_request(request),
            dependency::DependencyRequest::Authorized {
                authorization: dependency::DependencyAuthorization {
                    call_id: "call".into(),
                    action: "filesystem.write".into(),
                    normalized_digest: "digest".into(),
                    grant: "grant".into(),
                },
                operation: Box::new(dependency::DependencyRequest::Write(
                    dependency::WriteRequest {
                        path: "a.txt".into(),
                        content: b"x".to_vec(),
                        mode: dependency::WriteMode::Create,
                        expected_hash: None,
                        overwrite: false,
                        create_parents: false,
                    }
                )),
            }
        );
    }
}
