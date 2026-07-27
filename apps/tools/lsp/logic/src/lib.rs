//! LSP capability negotiation and safe edit-proposal business policy.
#![allow(missing_docs)]

use std::collections::BTreeSet;

use agentmod_lsp_host_data as data;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicPosition {
    pub line: u32,
    pub character: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicRange {
    pub start: LogicPosition,
    pub end: LogicPosition,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LogicOperation {
    ProjectRoot {
        path: String,
    },
    Diagnostics {
        document: String,
    },
    DocumentSymbols {
        document: String,
    },
    WorkspaceSymbols {
        query: String,
    },
    Definition {
        document: String,
        position: LogicPosition,
    },
    References {
        document: String,
        position: LogicPosition,
        include_declaration: bool,
    },
    Hover {
        document: String,
        position: LogicPosition,
    },
    SignatureHelp {
        document: String,
        position: LogicPosition,
    },
    Rename {
        document: String,
        position: LogicPosition,
        new_name: String,
    },
    Formatting {
        document: String,
        tab_size: u32,
        insert_spaces: bool,
    },
    CodeActions {
        document: String,
        range: LogicRange,
        diagnostics: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum LogicCommand {
    DetectProjectRoot {
        path: String,
    },
    Health {
        document: Option<String>,
    },
    Execute {
        cancellation_key: String,
        call_id: String,
        normalized_digest: String,
        authorization_grant: String,
        operation: LogicOperation,
    },
    Cancel {
        cancellation_key: String,
    },
    Shutdown,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LogicResult {
    ProjectRoot(String),
    Health(LogicHealth),
    Unavailable(String),
    Diagnostics(Vec<LogicDiagnostic>),
    Symbols(Vec<LogicSymbol>),
    Locations(Vec<LogicLocation>),
    Hover(Option<LogicHover>),
    Signature(Option<LogicSignature>),
    WorkspaceEdit(LogicWorkspaceEdit),
    TextEdits(Vec<LogicTextEdit>),
    CodeActions(Vec<LogicCodeAction>),
    Cancelled(bool),
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicHealth {
    pub ready: bool,
    pub server: Option<String>,
    pub capabilities: BTreeSet<String>,
    pub restart_count: u8,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicDiagnostic {
    pub path: String,
    pub range: LogicRange,
    pub severity: Option<u32>,
    pub code: Option<String>,
    pub source: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicSymbol {
    pub name: String,
    pub kind: u32,
    pub detail: Option<String>,
    pub location: Option<LogicLocation>,
    pub selection_range: Option<LogicRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicLocation {
    pub path: String,
    pub range: LogicRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicHover {
    pub contents: String,
    pub range: Option<LogicRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicSignature {
    pub signatures: Vec<String>,
    pub active_signature: Option<u32>,
    pub active_parameter: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicTextEdit {
    pub range: LogicRange,
    pub new_text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicFileEdits {
    pub path: String,
    pub edits: Vec<LogicTextEdit>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicWorkspaceEdit {
    pub files: Vec<LogicFileEdits>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicCodeAction {
    pub title: String,
    pub kind: Option<String>,
    pub edit: Option<LogicWorkspaceEdit>,
    pub command: Option<String>,
}

pub trait LspLogicPort {
    /// Executes a validated LSP business command.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] when business validation, safe-edit policy, or
    /// the translated data operation fails.
    fn execute(&self, command: LogicCommand) -> Result<LogicResult, LogicError>;
}

pub struct LspLogic<D> {
    data: D,
}

impl<D> LspLogic<D> {
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

impl<D: data::LspDataPort> LspLogicPort for LspLogic<D> {
    fn execute(&self, command: LogicCommand) -> Result<LogicResult, LogicError> {
        if let LogicCommand::Execute {
            cancellation_key,
            call_id,
            normalized_digest,
            authorization_grant,
            operation,
        } = command
        {
            validate_operation(&operation)?;
            if matches!(operation, LogicOperation::ProjectRoot { .. }) {
                return self
                    .data
                    .execute(data::DataRequest::Execute {
                        cancellation_key,
                        call_id,
                        normalized_digest,
                        authorization_grant,
                        operation: to_data_operation(operation),
                    })
                    .map_err(LogicError::from)
                    .map(from_data_response);
            }
            let result = self
                .data
                .execute(data::DataRequest::Execute {
                    cancellation_key,
                    call_id,
                    normalized_digest,
                    authorization_grant,
                    operation: to_data_operation(operation),
                })
                .map_err(LogicError::from)?;
            let result = from_data_response(result);
            validate_result(&result)?;
            return Ok(result);
        }
        let request = match command {
            LogicCommand::DetectProjectRoot { path } => {
                data::DataRequest::DetectProjectRoot { path }
            }
            LogicCommand::Health { document } => data::DataRequest::Health { document },
            LogicCommand::Cancel { cancellation_key } => {
                data::DataRequest::Cancel { cancellation_key }
            }
            LogicCommand::Shutdown => data::DataRequest::Shutdown,
            LogicCommand::Execute { .. } => unreachable!(),
        };
        self.data
            .execute(request)
            .map_err(LogicError::from)
            .map(from_data_response)
    }
}

fn validate_operation(operation: &LogicOperation) -> Result<(), LogicError> {
    match operation {
        LogicOperation::Rename { new_name, .. } if new_name.trim().is_empty() => Err(
            LogicError::InvalidCommand("rename target must not be empty"),
        ),
        LogicOperation::Formatting { tab_size: 0, .. } => {
            Err(LogicError::InvalidCommand("tab size must be non-zero"))
        }
        LogicOperation::CodeActions { range, .. } if !ordered(range.start, range.end) => Err(
            LogicError::InvalidCommand("range start must precede range end"),
        ),
        _ => Ok(()),
    }
}

fn validate_result(result: &LogicResult) -> Result<(), LogicError> {
    let validate_edits = |edits: &[LogicTextEdit]| {
        let mut ranges: Vec<_> = edits.iter().map(|edit| edit.range).collect();
        if ranges.iter().any(|range| !ordered(range.start, range.end)) {
            return Err(LogicError::UnsafeEdit("server returned an inverted range"));
        }
        ranges.sort_by_key(|range| (range.start.line, range.start.character));
        if ranges
            .windows(2)
            .any(|pair| ordered(pair[1].start, pair[0].end) && pair[1].start != pair[0].end)
        {
            return Err(LogicError::UnsafeEdit("server returned overlapping edits"));
        }
        Ok(())
    };
    match result {
        LogicResult::TextEdits(edits) => validate_edits(edits),
        LogicResult::WorkspaceEdit(edit) => {
            for file in &edit.files {
                validate_edits(&file.edits)?;
            }
            Ok(())
        }
        LogicResult::CodeActions(actions) => {
            for edit in actions.iter().filter_map(|action| action.edit.as_ref()) {
                for file in &edit.files {
                    validate_edits(&file.edits)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

const fn ordered(start: LogicPosition, end: LogicPosition) -> bool {
    start.line < end.line || (start.line == end.line && start.character <= end.character)
}

fn to_data_operation(operation: LogicOperation) -> data::DataOperation {
    match operation {
        LogicOperation::ProjectRoot { path } => data::DataOperation::ProjectRoot { path },
        LogicOperation::Diagnostics { document } => data::DataOperation::Diagnostics { document },
        LogicOperation::DocumentSymbols { document } => {
            data::DataOperation::DocumentSymbols { document }
        }
        LogicOperation::WorkspaceSymbols { query } => {
            data::DataOperation::WorkspaceSymbols { query }
        }
        LogicOperation::Definition { document, position } => data::DataOperation::Definition {
            document,
            position: to_data_position(position),
        },
        LogicOperation::References {
            document,
            position,
            include_declaration,
        } => data::DataOperation::References {
            document,
            position: to_data_position(position),
            include_declaration,
        },
        LogicOperation::Hover { document, position } => data::DataOperation::Hover {
            document,
            position: to_data_position(position),
        },
        LogicOperation::SignatureHelp { document, position } => {
            data::DataOperation::SignatureHelp {
                document,
                position: to_data_position(position),
            }
        }
        LogicOperation::Rename {
            document,
            position,
            new_name,
        } => data::DataOperation::Rename {
            document,
            position: to_data_position(position),
            new_name,
        },
        LogicOperation::Formatting {
            document,
            tab_size,
            insert_spaces,
        } => data::DataOperation::Formatting {
            document,
            tab_size,
            insert_spaces,
        },
        LogicOperation::CodeActions {
            document,
            range,
            diagnostics,
        } => data::DataOperation::CodeActions {
            document,
            range: to_data_range(range),
            diagnostics,
        },
    }
}

const fn to_data_position(value: LogicPosition) -> data::DataPosition {
    data::DataPosition {
        line: value.line,
        character: value.character,
    }
}

const fn to_data_range(value: LogicRange) -> data::DataRange {
    data::DataRange {
        start: to_data_position(value.start),
        end: to_data_position(value.end),
    }
}

fn from_data_response(response: data::DataResponse) -> LogicResult {
    match response {
        data::DataResponse::ProjectRoot(root) => LogicResult::ProjectRoot(root),
        data::DataResponse::Health(v) => LogicResult::Health(LogicHealth {
            ready: v.availability == data::DataAvailability::Ready,
            server: v.server,
            capabilities: v.capabilities,
            restart_count: v.restart_count,
            detail: v.detail,
        }),
        data::DataResponse::Unavailable(reason) => LogicResult::Unavailable(reason),
        data::DataResponse::Diagnostics(values) => LogicResult::Diagnostics(
            values
                .into_iter()
                .map(|v| LogicDiagnostic {
                    path: v.path,
                    range: from_range(v.range),
                    severity: v.severity,
                    code: v.code,
                    source: v.source,
                    message: v.message,
                })
                .collect(),
        ),
        data::DataResponse::Symbols(values) => LogicResult::Symbols(
            values
                .into_iter()
                .map(|v| LogicSymbol {
                    name: v.name,
                    kind: v.kind,
                    detail: v.detail,
                    location: v.location.map(from_location),
                    selection_range: v.selection_range.map(from_range),
                })
                .collect(),
        ),
        data::DataResponse::Locations(values) => {
            LogicResult::Locations(values.into_iter().map(from_location).collect())
        }
        data::DataResponse::Hover(value) => LogicResult::Hover(value.map(|v| LogicHover {
            contents: v.contents,
            range: v.range.map(from_range),
        })),
        data::DataResponse::Signature(value) => {
            LogicResult::Signature(value.map(|v| LogicSignature {
                signatures: v.signatures,
                active_signature: v.active_signature,
                active_parameter: v.active_parameter,
            }))
        }
        data::DataResponse::WorkspaceEdit(value) => {
            LogicResult::WorkspaceEdit(from_workspace_edit(value))
        }
        data::DataResponse::TextEdits(values) => {
            LogicResult::TextEdits(values.into_iter().map(from_edit).collect())
        }
        data::DataResponse::CodeActions(values) => LogicResult::CodeActions(
            values
                .into_iter()
                .map(|v| LogicCodeAction {
                    title: v.title,
                    kind: v.kind,
                    edit: v.edit.map(from_workspace_edit),
                    command: v.command,
                })
                .collect(),
        ),
        data::DataResponse::Cancelled(active) => LogicResult::Cancelled(active),
        data::DataResponse::Shutdown => LogicResult::Shutdown,
    }
}

const fn from_range(value: data::DataRange) -> LogicRange {
    LogicRange {
        start: LogicPosition {
            line: value.start.line,
            character: value.start.character,
        },
        end: LogicPosition {
            line: value.end.line,
            character: value.end.character,
        },
    }
}

fn from_location(value: data::DataLocation) -> LogicLocation {
    LogicLocation {
        path: value.path,
        range: from_range(value.range),
    }
}

fn from_edit(value: data::DataTextEdit) -> LogicTextEdit {
    LogicTextEdit {
        range: from_range(value.range),
        new_text: value.new_text,
    }
}

fn from_workspace_edit(value: data::DataWorkspaceEdit) -> LogicWorkspaceEdit {
    LogicWorkspaceEdit {
        files: value
            .files
            .into_iter()
            .map(|file| LogicFileEdits {
                path: file.path,
                edits: file.edits.into_iter().map(from_edit).collect(),
            })
            .collect(),
    }
}

#[derive(Debug, Error)]
pub enum LogicError {
    #[error("invalid LSP command: {0}")]
    InvalidCommand(&'static str),
    #[error("unsafe LSP edit proposal: {0}")]
    UnsafeEdit(&'static str),
    #[error("LSP data invariant failed: {0}")]
    Invariant(&'static str),
    #[error("LSP data failure ({code}): {detail}")]
    Data { code: &'static str, detail: String },
}

impl From<data::DataError> for LogicError {
    fn from(value: data::DataError) -> Self {
        Self::Data {
            code: value.code,
            detail: value.detail,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockData {
        requests: Mutex<Vec<data::DataRequest>>,
        response: Mutex<Option<data::DataResponse>>,
    }

    impl data::LspDataPort for MockData {
        fn execute(
            &self,
            request: data::DataRequest,
        ) -> Result<data::DataResponse, data::DataError> {
            self.requests.lock().expect("requests").push(request);
            self.response
                .lock()
                .expect("response")
                .take()
                .ok_or(data::DataError {
                    code: "fixture",
                    detail: "missing response".into(),
                })
        }
    }

    #[test]
    fn maps_execution_envelope_without_skipping_data() {
        let logic = LspLogic::new(MockData {
            requests: Mutex::new(Vec::new()),
            response: Mutex::new(Some(data::DataResponse::Hover(None))),
        });
        let result = logic
            .execute(LogicCommand::Execute {
                cancellation_key: "cancel".into(),
                call_id: "call".into(),
                normalized_digest: "digest".into(),
                authorization_grant: "grant".into(),
                operation: LogicOperation::Hover {
                    document: "src/main.rs".into(),
                    position: LogicPosition {
                        line: 0,
                        character: 0,
                    },
                },
            })
            .expect("execute");
        assert_eq!(result, LogicResult::Hover(None));
        assert!(matches!(
            logic.data.requests.lock().expect("requests").first(),
            Some(data::DataRequest::Execute {
                call_id,
                authorization_grant,
                ..
            }) if call_id == "call" && authorization_grant == "grant"
        ));
    }

    #[test]
    fn rejects_invalid_input_and_overlapping_server_edits() {
        let invalid = LspLogic::new(MockData {
            requests: Mutex::new(Vec::new()),
            response: Mutex::new(None),
        });
        assert!(matches!(
            invalid.execute(LogicCommand::Execute {
                cancellation_key: "c".into(),
                call_id: "c".into(),
                normalized_digest: "d".into(),
                authorization_grant: "g".into(),
                operation: LogicOperation::Formatting {
                    document: "src/main.rs".into(),
                    tab_size: 0,
                    insert_spaces: true,
                },
            }),
            Err(LogicError::InvalidCommand(_))
        ));
        assert!(invalid.data.requests.lock().expect("requests").is_empty());

        let range = data::DataRange {
            start: data::DataPosition {
                line: 0,
                character: 0,
            },
            end: data::DataPosition {
                line: 0,
                character: 3,
            },
        };
        let overlap = data::DataRange {
            start: data::DataPosition {
                line: 0,
                character: 2,
            },
            end: data::DataPosition {
                line: 0,
                character: 4,
            },
        };
        let logic = LspLogic::new(MockData {
            requests: Mutex::new(Vec::new()),
            response: Mutex::new(Some(data::DataResponse::TextEdits(vec![
                data::DataTextEdit {
                    range,
                    new_text: "a".into(),
                },
                data::DataTextEdit {
                    range: overlap,
                    new_text: "b".into(),
                },
            ]))),
        });
        assert!(matches!(
            logic.execute(LogicCommand::Execute {
                cancellation_key: "c".into(),
                call_id: "c".into(),
                normalized_digest: "d".into(),
                authorization_grant: "g".into(),
                operation: LogicOperation::Formatting {
                    document: "src/main.rs".into(),
                    tab_size: 4,
                    insert_spaces: true,
                },
            }),
            Err(LogicError::UnsafeEdit(_))
        ));
    }
}
