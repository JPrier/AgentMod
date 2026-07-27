//! Filesystem-host business use cases and policy.

use std::collections::BTreeMap;

use agentmod_filesystem_host_data as data;
use thiserror::Error;

/// Logic-owned filesystem command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilesystemCommand {
    /// Health.
    Health,
    /// Authorized operation.
    Authorized {
        /// Logic-owned authorization envelope.
        authorization: LogicAuthorization,
        /// Exact business operation.
        operation: Box<FilesystemCommand>,
    },
    /// Read.
    Read {
        /// Path.
        path: String,
        /// Range.
        range: LogicReadRange,
        /// Projection bound.
        max_projection_bytes: usize,
    },
    /// List.
    List {
        /// Path.
        path: String,
        /// Depth.
        max_depth: usize,
        /// Hidden.
        include_hidden: bool,
        /// Ignore files.
        honor_ignore: bool,
        /// Extra ignores.
        ignore_patterns: Vec<String>,
        /// Limit.
        max_results: usize,
    },
    /// Glob.
    Glob {
        /// Path.
        path: String,
        /// Patterns.
        patterns: Vec<String>,
        /// Hidden.
        include_hidden: bool,
        /// Ignore files.
        honor_ignore: bool,
        /// Limit.
        max_results: usize,
    },
    /// Grep.
    Grep {
        /// Path.
        path: String,
        /// Pattern.
        pattern: String,
        /// Regex.
        regex: bool,
        /// Case-insensitive.
        case_insensitive: bool,
        /// File filters.
        file_patterns: Vec<String>,
        /// Before.
        before_context: usize,
        /// After.
        after_context: usize,
        /// Limit.
        max_matches: usize,
    },
    /// Write.
    Write {
        /// Path.
        path: String,
        /// Bytes.
        content: Vec<u8>,
        /// Mode.
        mode: LogicWriteMode,
        /// Hash.
        expected_hash: Option<String>,
        /// Overwrite.
        overwrite: bool,
        /// Parents.
        create_parents: bool,
    },
    /// Edit.
    Edit {
        /// Path.
        path: String,
        /// Replacements.
        replacements: Vec<LogicReplacement>,
        /// Hash.
        expected_hash: Option<String>,
    },
    /// Patch.
    ApplyPatch {
        /// Unified patch.
        patch: String,
        /// Base hashes.
        base_hashes: BTreeMap<String, String>,
        /// Parents.
        create_parents: bool,
    },
}

/// Logic-owned authorization envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicAuthorization {
    /// Runtime call ID.
    pub call_id: String,
    /// Stable action ID.
    pub action: String,
    /// Runtime normalized digest.
    pub normalized_digest: String,
    /// Opaque keyed grant.
    pub grant: String,
}

/// Logic-owned read range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicReadRange {
    /// All.
    All,
    /// Lines.
    Lines {
        /// Start.
        start: usize,
        /// End.
        end: usize,
    },
    /// Bytes.
    Bytes {
        /// Offset.
        offset: u64,
        /// Length.
        length: usize,
    },
}

/// Logic-owned write mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicWriteMode {
    /// Create.
    Create,
    /// Replace.
    Replace,
}

/// Logic-owned replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicReplacement {
    /// Old.
    pub old: String,
    /// New.
    pub new: String,
    /// Expected count.
    pub expected_occurrences: usize,
}

/// Logic-owned use-case result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilesystemResult {
    /// Health.
    Health {
        /// Approved roots.
        roots: Vec<String>,
        /// Bound.
        max_file_bytes: u64,
        /// Whether execution grants can be verified.
        authorization_ready: bool,
    },
    /// Read.
    Read(LogicReadRecord),
    /// Entries.
    Entries {
        /// Entries.
        entries: Vec<LogicEntryRecord>,
        /// Overflow.
        truncated: bool,
    },
    /// Grep.
    Grep {
        /// Matches.
        matches: Vec<LogicMatchRecord>,
        /// Overflow.
        truncated: bool,
        /// Binary skips.
        binary_files_skipped: usize,
    },
    /// Mutation.
    Mutation(LogicMutationRecord),
    /// Patch.
    Patch {
        /// Files.
        files: Vec<LogicMutationRecord>,
        /// Atomicity.
        atomicity: String,
    },
}

/// Logic-owned line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicLineRecord {
    /// Number.
    pub number: usize,
    /// Text.
    pub text: String,
}

/// Logic-owned read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicReadRecord {
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
    pub lines: Vec<LogicLineRecord>,
    /// Hex.
    pub bytes_hex: Option<String>,
    /// Overflow.
    pub truncated: bool,
}

/// Logic-owned entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicEntryRecord {
    /// Path.
    pub path: String,
    /// Depth.
    pub depth: usize,
    /// Size.
    pub size: u64,
    /// Kind.
    pub kind: String,
}

/// Logic-owned match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicMatchRecord {
    /// Path.
    pub path: String,
    /// Line.
    pub line: usize,
    /// Column.
    pub column: usize,
    /// Text.
    pub text: String,
    /// Before.
    pub before: Vec<LogicLineRecord>,
    /// After.
    pub after: Vec<LogicLineRecord>,
}

/// Logic-owned mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicMutationRecord {
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

/// Narrow logic interface consumed by filesystem service.
pub trait FilesystemLogicPort {
    /// Executes a validated business command.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid business limits or unavailable data.
    fn execute(&self, command: FilesystemCommand) -> Result<FilesystemResult, LogicError>;
}

/// Filesystem logic implementation.
#[derive(Clone, Debug)]
pub struct FilesystemLogic<D> {
    data: D,
}

impl<D> FilesystemLogic<D> {
    /// Creates logic using only data.
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D: data::FilesystemDataPort> FilesystemLogicPort for FilesystemLogic<D> {
    fn execute(&self, command: FilesystemCommand) -> Result<FilesystemResult, LogicError> {
        validate_command(&command)?;
        self.data
            .execute(to_data_request(command))
            .map(to_logic_result)
            .map_err(|error| LogicError::Data {
                code: match &error {
                    data::DataError::Dependency { code, .. } => code,
                },
                detail: error.to_string(),
            })
    }
}

fn validate_command(command: &FilesystemCommand) -> Result<(), LogicError> {
    let path = match command {
        FilesystemCommand::Health => return Ok(()),
        FilesystemCommand::Authorized {
            authorization,
            operation,
        } => {
            if authorization.call_id.trim().is_empty()
                || authorization.action.trim().is_empty()
                || authorization.normalized_digest.trim().is_empty()
                || authorization.grant.trim().is_empty()
            {
                return Err(LogicError::InvalidCommand(
                    "authorization envelope is incomplete",
                ));
            }
            if matches!(
                operation.as_ref(),
                FilesystemCommand::Health | FilesystemCommand::Authorized { .. }
            ) {
                return Err(LogicError::InvalidCommand(
                    "authorization must wrap exactly one filesystem operation",
                ));
            }
            return validate_command(operation);
        }
        FilesystemCommand::Read { path, .. }
        | FilesystemCommand::List { path, .. }
        | FilesystemCommand::Glob { path, .. }
        | FilesystemCommand::Grep { path, .. }
        | FilesystemCommand::Write { path, .. }
        | FilesystemCommand::Edit { path, .. } => path,
        FilesystemCommand::ApplyPatch { patch, .. } => {
            if patch.trim().is_empty() {
                return Err(LogicError::InvalidCommand("patch is empty"));
            }
            return Ok(());
        }
    };
    if path.trim().is_empty() {
        Err(LogicError::InvalidCommand("path is empty"))
    } else {
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
fn to_data_request(command: FilesystemCommand) -> data::DataRequest {
    match command {
        FilesystemCommand::Health => data::DataRequest::Health,
        FilesystemCommand::Authorized {
            authorization,
            operation,
        } => data::DataRequest::Authorized {
            authorization: data::DataAuthorization {
                call_id: authorization.call_id,
                action: authorization.action,
                normalized_digest: authorization.normalized_digest,
                grant: authorization.grant,
            },
            operation: Box::new(to_data_request(*operation)),
        },
        FilesystemCommand::Read {
            path,
            range,
            max_projection_bytes,
        } => data::DataRequest::Read {
            path,
            range: match range {
                LogicReadRange::All => data::DataReadRange::All,
                LogicReadRange::Lines { start, end } => data::DataReadRange::Lines { start, end },
                LogicReadRange::Bytes { offset, length } => {
                    data::DataReadRange::Bytes { offset, length }
                }
            },
            max_projection_bytes,
        },
        FilesystemCommand::List {
            path,
            max_depth,
            include_hidden,
            honor_ignore,
            ignore_patterns,
            max_results,
        } => data::DataRequest::List {
            path,
            max_depth,
            include_hidden,
            honor_ignore,
            ignore_patterns,
            max_results,
        },
        FilesystemCommand::Glob {
            path,
            patterns,
            include_hidden,
            honor_ignore,
            max_results,
        } => data::DataRequest::Glob {
            path,
            patterns,
            include_hidden,
            honor_ignore,
            max_results,
        },
        FilesystemCommand::Grep {
            path,
            pattern,
            regex,
            case_insensitive,
            file_patterns,
            before_context,
            after_context,
            max_matches,
        } => data::DataRequest::Grep {
            path,
            pattern,
            regex,
            case_insensitive,
            file_patterns,
            before_context,
            after_context,
            max_matches,
        },
        FilesystemCommand::Write {
            path,
            content,
            mode,
            expected_hash,
            overwrite,
            create_parents,
        } => data::DataRequest::Write {
            path,
            content,
            mode: match mode {
                LogicWriteMode::Create => data::DataWriteMode::Create,
                LogicWriteMode::Replace => data::DataWriteMode::Replace,
            },
            expected_hash,
            overwrite,
            create_parents,
        },
        FilesystemCommand::Edit {
            path,
            replacements,
            expected_hash,
        } => data::DataRequest::Edit {
            path,
            replacements: replacements
                .into_iter()
                .map(|item| data::DataReplacement {
                    old: item.old,
                    new: item.new,
                    expected_occurrences: item.expected_occurrences,
                })
                .collect(),
            expected_hash,
        },
        FilesystemCommand::ApplyPatch {
            patch,
            base_hashes,
            create_parents,
        } => data::DataRequest::ApplyPatch {
            patch,
            base_hashes,
            create_parents,
        },
    }
}

fn to_logic_result(response: data::DataResponse) -> FilesystemResult {
    match response {
        data::DataResponse::Health {
            roots,
            max_file_bytes,
            authorization_ready,
        } => FilesystemResult::Health {
            roots,
            max_file_bytes,
            authorization_ready,
        },
        data::DataResponse::Read(record) => FilesystemResult::Read(LogicReadRecord {
            path: record.path,
            content_hash: record.content_hash,
            size: record.size,
            readonly: record.readonly,
            encoding: record.encoding,
            binary: record.binary,
            lines: record.lines.into_iter().map(to_logic_line).collect(),
            bytes_hex: record.bytes_hex,
            truncated: record.truncated,
        }),
        data::DataResponse::Entries { entries, truncated } => FilesystemResult::Entries {
            entries: entries
                .into_iter()
                .map(|entry| LogicEntryRecord {
                    path: entry.path,
                    depth: entry.depth,
                    size: entry.size,
                    kind: entry.kind,
                })
                .collect(),
            truncated,
        },
        data::DataResponse::Grep {
            matches,
            truncated,
            binary_files_skipped,
        } => FilesystemResult::Grep {
            matches: matches
                .into_iter()
                .map(|item| LogicMatchRecord {
                    path: item.path,
                    line: item.line,
                    column: item.column,
                    text: item.text,
                    before: item.before.into_iter().map(to_logic_line).collect(),
                    after: item.after.into_iter().map(to_logic_line).collect(),
                })
                .collect(),
            truncated,
            binary_files_skipped,
        },
        data::DataResponse::Mutation(record) => {
            FilesystemResult::Mutation(to_logic_mutation(record))
        }
        data::DataResponse::Patch { files, atomicity } => FilesystemResult::Patch {
            files: files.into_iter().map(to_logic_mutation).collect(),
            atomicity,
        },
    }
}

fn to_logic_line(line: data::DataLineRecord) -> LogicLineRecord {
    LogicLineRecord {
        number: line.number,
        text: line.text,
    }
}

fn to_logic_mutation(record: data::DataMutationRecord) -> LogicMutationRecord {
    LogicMutationRecord {
        path: record.path,
        old_hash: record.old_hash,
        new_hash: record.new_hash,
        diff: record.diff,
        bytes_written: record.bytes_written,
    }
}

/// Logic-layer failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LogicError {
    /// Business validation.
    #[error("invalid filesystem command: {0}")]
    InvalidCommand(&'static str),
    /// Data failure.
    #[error("filesystem data unavailable ({code}): {detail}")]
    Data {
        /// Stable data failure code.
        code: &'static str,
        /// Sanitized detail.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    struct MockData {
        observed: RefCell<Vec<data::DataRequest>>,
    }

    impl data::FilesystemDataPort for MockData {
        fn execute(
            &self,
            request: data::DataRequest,
        ) -> Result<data::DataResponse, data::DataError> {
            self.observed.borrow_mut().push(request);
            Ok(data::DataResponse::Health {
                roots: vec!["root".into()],
                max_file_bytes: 64,
                authorization_ready: true,
            })
        }
    }

    #[test]
    fn health_maps_only_to_data() {
        let logic = FilesystemLogic::new(MockData {
            observed: RefCell::new(Vec::new()),
        });
        assert_eq!(
            logic.execute(FilesystemCommand::Health).expect("health"),
            FilesystemResult::Health {
                roots: vec!["root".into()],
                max_file_bytes: 64,
                authorization_ready: true,
            }
        );
        assert_eq!(
            logic.data.observed.into_inner(),
            vec![data::DataRequest::Health]
        );
    }

    #[test]
    fn empty_path_is_rejected_before_data() {
        let logic = FilesystemLogic::new(MockData {
            observed: RefCell::new(Vec::new()),
        });
        let result = logic.execute(FilesystemCommand::Read {
            path: " ".into(),
            range: LogicReadRange::All,
            max_projection_bytes: 10,
        });
        assert_eq!(result, Err(LogicError::InvalidCommand("path is empty")));
        assert!(logic.data.observed.into_inner().is_empty());
    }

    #[test]
    fn authorization_envelope_maps_explicitly_to_data() {
        let logic = FilesystemLogic::new(MockData {
            observed: RefCell::new(Vec::new()),
        });
        logic
            .execute(FilesystemCommand::Authorized {
                authorization: LogicAuthorization {
                    call_id: "call".into(),
                    action: "filesystem.read".into(),
                    normalized_digest: "digest".into(),
                    grant: "grant".into(),
                },
                operation: Box::new(FilesystemCommand::Read {
                    path: "src/lib.rs".into(),
                    range: LogicReadRange::All,
                    max_projection_bytes: 128,
                }),
            })
            .expect("execute");
        assert!(matches!(
            logic.data.observed.into_inner().as_slice(),
            [data::DataRequest::Authorized {
                authorization,
                operation,
            }] if authorization.call_id == "call"
                && matches!(operation.as_ref(), data::DataRequest::Read { path, .. } if path == "src/lib.rs")
        ));
    }
}
