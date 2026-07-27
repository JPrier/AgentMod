//! Filesystem tool-protocol endpoint mappings.

use std::collections::BTreeMap;

use agentmod_filesystem_host_logic as logic;
use agentmod_tool_protocol::{ToolDescriptor, ToolHostCommand, ToolHostEvent};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

const GROUP: &str = "filesystem";

/// Service-owned execution request after wire validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceExecutionRequest {
    /// Runtime call ID.
    pub call_id: String,
    /// Service-owned operation.
    pub operation: ServiceOperation,
    /// Normalized proposal digest.
    pub normalized_digest: String,
    /// Opaque authorization grant.
    pub authorization_grant: String,
}

/// Service-owned filesystem operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceOperation {
    /// Read.
    Read(ReadArguments),
    /// List.
    List(ListArguments),
    /// Glob.
    Glob(GlobArguments),
    /// Grep.
    Grep(GrepArguments),
    /// Write.
    Write(WriteArguments),
    /// Edit.
    Edit(EditArguments),
    /// Patch.
    ApplyPatch(PatchArguments),
}

/// Read wire arguments translated into service ownership.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadArguments {
    /// Path.
    pub path: String,
    /// Line start.
    #[serde(default)]
    pub line_start: Option<usize>,
    /// Line end.
    #[serde(default)]
    pub line_end: Option<usize>,
    /// Byte offset.
    #[serde(default)]
    pub byte_offset: Option<u64>,
    /// Byte length.
    #[serde(default)]
    pub byte_length: Option<usize>,
    /// Projection bytes.
    #[serde(default = "default_projection")]
    pub max_projection_bytes: usize,
}

/// List arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ListArguments {
    /// Path.
    pub path: String,
    /// Depth.
    #[serde(default = "default_depth")]
    pub max_depth: usize,
    /// Hidden.
    #[serde(default)]
    pub include_hidden: bool,
    /// Ignore files.
    #[serde(default = "default_true")]
    pub honor_ignore: bool,
    /// Additional ignores.
    #[serde(default)]
    pub ignore_patterns: Vec<String>,
    /// Limit.
    #[serde(default = "default_results")]
    pub max_results: usize,
}

/// Glob arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GlobArguments {
    /// Path.
    pub path: String,
    /// Patterns.
    pub patterns: Vec<String>,
    /// Hidden.
    #[serde(default)]
    pub include_hidden: bool,
    /// Ignore files.
    #[serde(default = "default_true")]
    pub honor_ignore: bool,
    /// Limit.
    #[serde(default = "default_results")]
    pub max_results: usize,
}

/// Grep arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GrepArguments {
    /// Path.
    pub path: String,
    /// Pattern.
    pub pattern: String,
    /// Regex mode.
    #[serde(default)]
    pub regex: bool,
    /// Case-insensitive.
    #[serde(default)]
    pub case_insensitive: bool,
    /// File globs.
    #[serde(default)]
    pub file_patterns: Vec<String>,
    /// Before context.
    #[serde(default)]
    pub before_context: usize,
    /// After context.
    #[serde(default)]
    pub after_context: usize,
    /// Match limit.
    #[serde(default = "default_matches")]
    pub max_matches: usize,
}

/// Write arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WriteArguments {
    /// Path.
    pub path: String,
    /// UTF-8 content.
    pub content: String,
    /// `create` or `replace`.
    pub mode: String,
    /// Expected BLAKE3.
    #[serde(default)]
    pub expected_hash: Option<String>,
    /// Explicit overwrite.
    #[serde(default)]
    pub overwrite: bool,
    /// Parent creation.
    #[serde(default)]
    pub create_parents: bool,
}

/// Edit arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EditArguments {
    /// Path.
    pub path: String,
    /// Replacements.
    pub replacements: Vec<ReplacementArguments>,
    /// Expected hash.
    #[serde(default)]
    pub expected_hash: Option<String>,
}

/// Replacement arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReplacementArguments {
    /// Old.
    pub old: String,
    /// New.
    pub new: String,
    /// Exact count.
    #[serde(default = "default_occurrences")]
    pub expected_occurrences: usize,
}

/// Patch arguments.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PatchArguments {
    /// Unified multi-file patch.
    pub patch: String,
    /// Base hashes by patch path.
    pub base_hashes: BTreeMap<String, String>,
    /// Parent creation.
    #[serde(default)]
    pub create_parents: bool,
}

/// Endpoint-facing filesystem service.
#[derive(Clone, Debug)]
pub struct FilesystemService<L> {
    logic: L,
}

impl<L> FilesystemService<L> {
    /// Creates a service with injected logic.
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L: logic::FilesystemLogicPort> FilesystemService<L> {
    /// Maps one tool-protocol command to ordered endpoint events.
    #[must_use]
    pub fn handle_wire(&self, command: ToolHostCommand) -> Vec<ToolHostEvent> {
        match command {
            ToolHostCommand::DiscoverGroups => vec![ToolHostEvent::Groups {
                groups: vec![GROUP.into()],
            }],
            ToolHostCommand::DiscoverTools { groups } => vec![ToolHostEvent::Tools {
                tools: if groups.iter().any(|group| group == GROUP) {
                    descriptors()
                } else {
                    Vec::new()
                },
            }],
            ToolHostCommand::Health => {
                vec![self.execute_logic("health", logic::FilesystemCommand::Health)]
            }
            ToolHostCommand::Cancel { cancellation_id } => vec![ToolHostEvent::Cancelled {
                call_id: cancellation_id.to_string(),
            }],
            ToolHostCommand::Execute {
                call_id,
                tool,
                arguments,
                normalized_digest,
                authorization_grant,
                ..
            } => {
                let service_request = parse_execution(
                    call_id.clone(),
                    &tool,
                    arguments,
                    normalized_digest,
                    authorization_grant,
                );
                match service_request.and_then(to_logic_command) {
                    Ok(command) => {
                        let result = self.execute_logic(&call_id, command);
                        if matches!(result, ToolHostEvent::Completed { .. }) {
                            vec![
                                ToolHostEvent::Started {
                                    call_id: call_id.clone(),
                                },
                                result,
                            ]
                        } else {
                            vec![result]
                        }
                    }
                    Err(error) => vec![failed(call_id, &error)],
                }
            }
        }
    }

    fn execute_logic(&self, call_id: &str, command: logic::FilesystemCommand) -> ToolHostEvent {
        match self.logic.execute(command) {
            Ok(result) => {
                let (result, truncated) = render_result(result);
                ToolHostEvent::Completed {
                    call_id: call_id.to_owned(),
                    result,
                    artifact: None,
                    truncated,
                }
            }
            Err(
                error @ logic::LogicError::Data {
                    code: "authorization_denied",
                    ..
                },
            ) => failed(
                call_id.to_owned(),
                &ServiceError::Authorization {
                    detail: error.to_string(),
                },
            ),
            Err(error) => failed(
                call_id.to_owned(),
                &ServiceError::Logic {
                    detail: error.to_string(),
                },
            ),
        }
    }
}

fn parse_execution(
    call_id: String,
    tool: &str,
    arguments: Value,
    normalized_digest: String,
    authorization_grant: String,
) -> Result<ServiceExecutionRequest, ServiceError> {
    if call_id.trim().is_empty() {
        return Err(ServiceError::InvalidEnvelope("call_id is empty"));
    }
    if normalized_digest.len() != 64
        || !normalized_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ServiceError::InvalidEnvelope(
            "normalized_digest must be 64 hexadecimal characters",
        ));
    }
    if authorization_grant.trim().len() < 16 {
        return Err(ServiceError::InvalidEnvelope(
            "authorization_grant is missing or too short",
        ));
    }
    let operation = match tool {
        "filesystem.read" => deserialize(arguments).map(ServiceOperation::Read)?,
        "filesystem.list" => deserialize(arguments).map(ServiceOperation::List)?,
        "filesystem.glob" => deserialize(arguments).map(ServiceOperation::Glob)?,
        "filesystem.grep" => deserialize(arguments).map(ServiceOperation::Grep)?,
        "filesystem.write" => deserialize(arguments).map(ServiceOperation::Write)?,
        "filesystem.edit" => deserialize(arguments).map(ServiceOperation::Edit)?,
        "filesystem.apply_patch" => deserialize(arguments).map(ServiceOperation::ApplyPatch)?,
        _ => return Err(ServiceError::UnknownTool(tool.to_owned())),
    };
    Ok(ServiceExecutionRequest {
        call_id,
        operation,
        normalized_digest,
        authorization_grant,
    })
}

fn deserialize<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, ServiceError> {
    serde_json::from_value(value).map_err(|error| ServiceError::Arguments {
        detail: error.to_string(),
    })
}

#[allow(clippy::too_many_lines)]
fn to_logic_command(
    request: ServiceExecutionRequest,
) -> Result<logic::FilesystemCommand, ServiceError> {
    let ServiceExecutionRequest {
        call_id,
        operation,
        normalized_digest,
        authorization_grant,
    } = request;
    let action = match &operation {
        ServiceOperation::Read(_) => "filesystem.read",
        ServiceOperation::List(_) => "filesystem.list",
        ServiceOperation::Glob(_) => "filesystem.glob",
        ServiceOperation::Grep(_) => "filesystem.grep",
        ServiceOperation::Write(_) => "filesystem.write",
        ServiceOperation::Edit(_) => "filesystem.edit",
        ServiceOperation::ApplyPatch(_) => "filesystem.apply_patch",
    };
    let operation = match operation {
        ServiceOperation::Read(arguments) => {
            let range = match (
                arguments.line_start,
                arguments.line_end,
                arguments.byte_offset,
                arguments.byte_length,
            ) {
                (None, None, None, None) => logic::LogicReadRange::All,
                (Some(start), Some(end), None, None) => logic::LogicReadRange::Lines { start, end },
                (None, None, Some(offset), Some(length)) => {
                    logic::LogicReadRange::Bytes { offset, length }
                }
                _ => {
                    return Err(ServiceError::Arguments {
                        detail: "read range must be complete lines, complete bytes, or omitted"
                            .into(),
                    });
                }
            };
            Ok(logic::FilesystemCommand::Read {
                path: arguments.path,
                range,
                max_projection_bytes: arguments.max_projection_bytes,
            })
        }
        ServiceOperation::List(arguments) => Ok(logic::FilesystemCommand::List {
            path: arguments.path,
            max_depth: arguments.max_depth,
            include_hidden: arguments.include_hidden,
            honor_ignore: arguments.honor_ignore,
            ignore_patterns: arguments.ignore_patterns,
            max_results: arguments.max_results,
        }),
        ServiceOperation::Glob(arguments) => Ok(logic::FilesystemCommand::Glob {
            path: arguments.path,
            patterns: arguments.patterns,
            include_hidden: arguments.include_hidden,
            honor_ignore: arguments.honor_ignore,
            max_results: arguments.max_results,
        }),
        ServiceOperation::Grep(arguments) => Ok(logic::FilesystemCommand::Grep {
            path: arguments.path,
            pattern: arguments.pattern,
            regex: arguments.regex,
            case_insensitive: arguments.case_insensitive,
            file_patterns: arguments.file_patterns,
            before_context: arguments.before_context,
            after_context: arguments.after_context,
            max_matches: arguments.max_matches,
        }),
        ServiceOperation::Write(arguments) => Ok(logic::FilesystemCommand::Write {
            path: arguments.path,
            content: arguments.content.into_bytes(),
            mode: match arguments.mode.as_str() {
                "create" => logic::LogicWriteMode::Create,
                "replace" => logic::LogicWriteMode::Replace,
                _ => {
                    return Err(ServiceError::Arguments {
                        detail: "write mode must be `create` or `replace`".into(),
                    });
                }
            },
            expected_hash: arguments.expected_hash,
            overwrite: arguments.overwrite,
            create_parents: arguments.create_parents,
        }),
        ServiceOperation::Edit(arguments) => Ok(logic::FilesystemCommand::Edit {
            path: arguments.path,
            replacements: arguments
                .replacements
                .into_iter()
                .map(|item| logic::LogicReplacement {
                    old: item.old,
                    new: item.new,
                    expected_occurrences: item.expected_occurrences,
                })
                .collect(),
            expected_hash: arguments.expected_hash,
        }),
        ServiceOperation::ApplyPatch(arguments) => Ok(logic::FilesystemCommand::ApplyPatch {
            patch: arguments.patch,
            base_hashes: arguments.base_hashes,
            create_parents: arguments.create_parents,
        }),
    }?;
    Ok(logic::FilesystemCommand::Authorized {
        authorization: logic::LogicAuthorization {
            call_id,
            action: action.into(),
            normalized_digest,
            grant: authorization_grant,
        },
        operation: Box::new(operation),
    })
}

fn render_result(result: logic::FilesystemResult) -> (Value, bool) {
    match result {
        logic::FilesystemResult::Health {
            roots,
            max_file_bytes,
            authorization_ready,
        } => (
            json!({"status": if authorization_ready {"ok"} else {"authorization_required"}, "approved_roots": roots, "max_file_bytes": max_file_bytes, "authorization_ready": authorization_ready}),
            false,
        ),
        logic::FilesystemResult::Read(record) => (
            json!({
                "path": record.path,
                "content_hash": record.content_hash,
                "size": record.size,
                "readonly": record.readonly,
                "encoding": record.encoding,
                "binary": record.binary,
                "lines": record.lines.into_iter().map(|line| line_json(&line)).collect::<Vec<_>>(),
                "bytes_hex": record.bytes_hex,
                "overflow": record.truncated,
            }),
            record.truncated,
        ),
        logic::FilesystemResult::Entries { entries, truncated } => (
            json!({
                "entries": entries.into_iter().map(|entry| json!({
                    "path": entry.path, "depth": entry.depth, "size": entry.size, "kind": entry.kind
                })).collect::<Vec<_>>(),
                "overflow": truncated,
            }),
            truncated,
        ),
        logic::FilesystemResult::Grep {
            matches,
            truncated,
            binary_files_skipped,
        } => (
            json!({
                "matches": matches.into_iter().map(|item| json!({
                    "path": item.path,
                    "line": item.line,
                    "column": item.column,
                    "text": item.text,
                    "before": item.before.into_iter().map(|line| line_json(&line)).collect::<Vec<_>>(),
                    "after": item.after.into_iter().map(|line| line_json(&line)).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
                "binary_files_skipped": binary_files_skipped,
                "overflow": truncated,
            }),
            truncated,
        ),
        logic::FilesystemResult::Mutation(record) => (mutation_json(&record), false),
        logic::FilesystemResult::Patch { files, atomicity } => (
            json!({
                "files": files.into_iter().map(|record| mutation_json(&record)).collect::<Vec<_>>(),
                "atomicity": atomicity,
            }),
            false,
        ),
    }
}

fn line_json(line: &logic::LogicLineRecord) -> Value {
    json!({"number": line.number, "text": line.text})
}

fn mutation_json(record: &logic::LogicMutationRecord) -> Value {
    json!({
        "path": record.path,
        "old_hash": record.old_hash,
        "new_hash": record.new_hash,
        "diff": record.diff,
        "bytes_written": record.bytes_written,
    })
}

fn failed(call_id: String, error: &ServiceError) -> ToolHostEvent {
    ToolHostEvent::Failed {
        call_id,
        code: error.code().into(),
        message: error.to_string(),
        retryable: false,
    }
}

fn descriptors() -> Vec<ToolDescriptor> {
    [
        (
            "filesystem.read",
            "Read bounded text, line, or byte ranges with metadata and content hash",
            json!({"type":"object","required":["path"],"properties":{
                "path":{"type":"string"},"line_start":{"type":"integer","minimum":1},
                "line_end":{"type":"integer","minimum":1},"byte_offset":{"type":"integer","minimum":0},
                "byte_length":{"type":"integer","minimum":0},"max_projection_bytes":{"type":"integer","minimum":1}
            }}),
        ),
        (
            "filesystem.list",
            "List a stable bounded directory tree with ignore and hidden-file policy",
            json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"},"max_depth":{"type":"integer","minimum":0},"include_hidden":{"type":"boolean"},"honor_ignore":{"type":"boolean"},"ignore_patterns":{"type":"array","items":{"type":"string"}},"max_results":{"type":"integer","minimum":1}}}),
        ),
        (
            "filesystem.glob",
            "Match stable bounded workspace paths",
            json!({"type":"object","required":["path","patterns"],"properties":{"path":{"type":"string"},"patterns":{"type":"array","items":{"type":"string"},"minItems":1},"include_hidden":{"type":"boolean"},"honor_ignore":{"type":"boolean"},"max_results":{"type":"integer","minimum":1}}}),
        ),
        (
            "filesystem.grep",
            "Search text files with literal or regex matching and context",
            json!({"type":"object","required":["path","pattern"],"properties":{"path":{"type":"string"},"pattern":{"type":"string"},"regex":{"type":"boolean"},"case_insensitive":{"type":"boolean"},"file_patterns":{"type":"array","items":{"type":"string"}},"before_context":{"type":"integer","minimum":0},"after_context":{"type":"integer","minimum":0},"max_matches":{"type":"integer","minimum":1}}}),
        ),
        (
            "filesystem.write",
            "Atomically create or explicitly replace a file with hash preconditions",
            json!({"type":"object","required":["path","content","mode"],"properties":{"path":{"type":"string"},"content":{"type":"string"},"mode":{"enum":["create","replace"]},"expected_hash":{"type":"string"},"overwrite":{"type":"boolean"},"create_parents":{"type":"boolean"}}}),
        ),
        (
            "filesystem.edit",
            "Atomically apply prevalidated exact text replacements",
            json!({"type":"object","required":["path","replacements"],"properties":{"path":{"type":"string"},"replacements":{"type":"array","items":{"type":"object","required":["old","new"],"properties":{"old":{"type":"string"},"new":{"type":"string"},"expected_occurrences":{"type":"integer","minimum":1}}}},"expected_hash":{"type":"string"}}}),
        ),
        (
            "filesystem.apply_patch",
            "Prevalidate and apply a unified multi-file patch with base hashes and rollback",
            json!({"type":"object","required":["patch","base_hashes"],"properties":{"patch":{"type":"string"},"base_hashes":{"type":"object","additionalProperties":{"type":"string"}},"create_parents":{"type":"boolean"}}}),
        ),
    ]
    .into_iter()
    .map(|(id, description, input_schema)| ToolDescriptor {
        id: id.into(),
        group: GROUP.into(),
        description: description.into(),
        input_schema,
        supported_decisions: vec![
            "continue".into(),
            "replace".into(),
            "reject".into(),
            "require_approval".into(),
            "defer".into(),
            "cancel".into(),
        ],
    })
    .collect()
}

const fn default_projection() -> usize {
    64 * 1024
}
const fn default_depth() -> usize {
    4
}
const fn default_results() -> usize {
    1_000
}
const fn default_matches() -> usize {
    1_000
}
const fn default_occurrences() -> usize {
    1
}
const fn default_true() -> bool {
    true
}

/// Service boundary failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ServiceError {
    /// Invalid tool envelope.
    #[error("invalid tool envelope: {0}")]
    InvalidEnvelope(&'static str),
    /// Unknown namespaced tool.
    #[error("unknown filesystem tool `{0}`")]
    UnknownTool(String),
    /// Argument decoding or cross-field validation.
    #[error("invalid tool arguments: {detail}")]
    Arguments {
        /// Detail.
        detail: String,
    },
    /// Business execution failure.
    #[error("filesystem operation failed: {detail}")]
    Logic {
        /// Detail.
        detail: String,
    },
    /// Mandatory dependency authorization rejected the request.
    #[error("filesystem authorization failed: {detail}")]
    Authorization {
        /// Redacted detail.
        detail: String,
    },
}

impl ServiceError {
    const fn code(&self) -> &'static str {
        match self {
            Self::InvalidEnvelope(_) | Self::Authorization { .. } => "invalid_authorization",
            Self::UnknownTool(_) => "unknown_tool",
            Self::Arguments { .. } => "invalid_arguments",
            Self::Logic { .. } => "filesystem_error",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    struct MockLogic {
        observed: RefCell<Vec<logic::FilesystemCommand>>,
    }

    struct DenyLogic;

    impl logic::FilesystemLogicPort for MockLogic {
        fn execute(
            &self,
            command: logic::FilesystemCommand,
        ) -> Result<logic::FilesystemResult, logic::LogicError> {
            self.observed.borrow_mut().push(command);
            Ok(logic::FilesystemResult::Health {
                roots: vec!["root".into()],
                max_file_bytes: 10,
                authorization_ready: true,
            })
        }
    }

    impl logic::FilesystemLogicPort for DenyLogic {
        fn execute(
            &self,
            _command: logic::FilesystemCommand,
        ) -> Result<logic::FilesystemResult, logic::LogicError> {
            Err(logic::LogicError::Data {
                code: "authorization_denied",
                detail: "denied".into(),
            })
        }
    }

    #[test]
    fn discovery_advertises_complete_native_surface() {
        let service = FilesystemService::new(MockLogic {
            observed: RefCell::new(Vec::new()),
        });
        let events = service.handle_wire(ToolHostCommand::DiscoverTools {
            groups: vec!["filesystem".into()],
        });
        let [ToolHostEvent::Tools { tools }] = events.as_slice() else {
            panic!("tools response");
        };
        assert_eq!(tools.len(), 7);
        assert!(tools.iter().any(|tool| tool.id == "filesystem.apply_patch"));
    }

    #[test]
    fn health_maps_to_logic_and_completed_event() {
        let service = FilesystemService::new(MockLogic {
            observed: RefCell::new(Vec::new()),
        });
        let events = service.handle_wire(ToolHostCommand::Health);
        assert!(matches!(
            events.as_slice(),
            [ToolHostEvent::Completed { .. }]
        ));
        assert_eq!(
            service.logic.observed.into_inner(),
            vec![logic::FilesystemCommand::Health]
        );
    }

    #[test]
    fn execute_validates_wire_envelope_and_maps_read_arguments() {
        let service = FilesystemService::new(MockLogic {
            observed: RefCell::new(Vec::new()),
        });
        let events = service.handle_wire(ToolHostCommand::Execute {
            call_id: "call-1".into(),
            tool: "filesystem.read".into(),
            arguments: json!({
                "path": "src/lib.rs",
                "line_start": 2,
                "line_end": 4,
                "max_projection_bytes": 128
            }),
            normalized_digest: "a".repeat(64),
            authorization_grant: "approved-grant-01".into(),
            cancellation_id: "018f3d44-7b41-7cc8-9c1f-97cd5c6688a0"
                .parse()
                .expect("cancellation id"),
        });
        assert!(matches!(
            events.as_slice(),
            [
                ToolHostEvent::Started { .. },
                ToolHostEvent::Completed { .. }
            ]
        ));
        assert_eq!(
            service.logic.observed.into_inner(),
            vec![logic::FilesystemCommand::Authorized {
                authorization: logic::LogicAuthorization {
                    call_id: "call-1".into(),
                    action: "filesystem.read".into(),
                    normalized_digest: "a".repeat(64),
                    grant: "approved-grant-01".into(),
                },
                operation: Box::new(logic::FilesystemCommand::Read {
                    path: "src/lib.rs".into(),
                    range: logic::LogicReadRange::Lines { start: 2, end: 4 },
                    max_projection_bytes: 128,
                }),
            }]
        );
    }

    #[test]
    fn authorization_failure_never_emits_started() {
        let service = FilesystemService::new(DenyLogic);
        let events = service.handle_wire(ToolHostCommand::Execute {
            call_id: "denied".into(),
            tool: "filesystem.read".into(),
            arguments: json!({"path":"src/lib.rs"}),
            normalized_digest: "a".repeat(64),
            authorization_grant: "syntactically-present-grant".into(),
            cancellation_id: "018f3d44-7b41-7cc8-9c1f-97cd5c6688a0"
                .parse()
                .expect("cancellation id"),
        });
        assert!(matches!(
            events.as_slice(),
            [ToolHostEvent::Failed { code, .. }] if code == "invalid_authorization"
        ));
    }
}
