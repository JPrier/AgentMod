//! Stable business-facing LSP dataset construction.
#![allow(missing_docs)]

use std::collections::BTreeSet;

use agentmod_lsp_host_dependency as dependency;
use thiserror::Error;

/// Data-owned position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataPosition {
    pub line: u32,
    pub character: u32,
}

/// Data-owned range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataRange {
    pub start: DataPosition,
    pub end: DataPosition,
}

/// Data-owned operation request.
#[derive(Clone, Debug, PartialEq)]
pub enum DataOperation {
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
        position: DataPosition,
    },
    References {
        document: String,
        position: DataPosition,
        include_declaration: bool,
    },
    Hover {
        document: String,
        position: DataPosition,
    },
    SignatureHelp {
        document: String,
        position: DataPosition,
    },
    Rename {
        document: String,
        position: DataPosition,
        new_name: String,
    },
    Formatting {
        document: String,
        tab_size: u32,
        insert_spaces: bool,
    },
    CodeActions {
        document: String,
        range: DataRange,
        diagnostics: Vec<String>,
    },
}

/// Data-owned request.
#[derive(Clone, Debug, PartialEq)]
pub enum DataRequest {
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
        operation: DataOperation,
    },
    Cancel {
        cancellation_key: String,
    },
    Shutdown,
}

/// Normalized availability record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataAvailability {
    Ready,
    Unavailable,
}

/// Data-owned response.
#[derive(Clone, Debug, PartialEq)]
pub enum DataResponse {
    ProjectRoot(String),
    Health(DataHealth),
    Unavailable(String),
    Diagnostics(Vec<DataDiagnostic>),
    Symbols(Vec<DataSymbol>),
    Locations(Vec<DataLocation>),
    Hover(Option<DataHover>),
    Signature(Option<DataSignature>),
    WorkspaceEdit(DataWorkspaceEdit),
    TextEdits(Vec<DataTextEdit>),
    CodeActions(Vec<DataCodeAction>),
    Cancelled(bool),
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataHealth {
    pub availability: DataAvailability,
    pub server: Option<String>,
    pub capabilities: BTreeSet<String>,
    pub restart_count: u8,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataDiagnostic {
    pub path: String,
    pub range: DataRange,
    pub severity: Option<u32>,
    pub code: Option<String>,
    pub source: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSymbol {
    pub name: String,
    pub kind: u32,
    pub detail: Option<String>,
    pub location: Option<DataLocation>,
    pub selection_range: Option<DataRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataLocation {
    pub path: String,
    pub range: DataRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataHover {
    pub contents: String,
    pub range: Option<DataRange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSignature {
    pub signatures: Vec<String>,
    pub active_signature: Option<u32>,
    pub active_parameter: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataTextEdit {
    pub range: DataRange,
    pub new_text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataFileEdits {
    pub path: String,
    pub edits: Vec<DataTextEdit>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataWorkspaceEdit {
    pub files: Vec<DataFileEdits>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataCodeAction {
    pub title: String,
    pub kind: Option<String>,
    pub edit: Option<DataWorkspaceEdit>,
    pub command: Option<String>,
}

/// Narrow data interface consumed by logic.
pub trait LspDataPort {
    /// Executes one business-facing data operation.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] after translating a dependency failure.
    fn execute(&self, request: DataRequest) -> Result<DataResponse, DataError>;
}

/// Data implementation routing datasets through one dependency boundary.
pub struct LspData<D> {
    dependency: D,
}

impl<D> LspData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D: dependency::LspDependencyPort> LspDataPort for LspData<D> {
    fn execute(&self, request: DataRequest) -> Result<DataResponse, DataError> {
        self.dependency
            .execute(to_dependency_request(request))
            .map(from_dependency_response)
            .map_err(DataError::from)
    }
}

fn to_dependency_request(request: DataRequest) -> dependency::DependencyRequest {
    match request {
        DataRequest::DetectProjectRoot { path } => {
            dependency::DependencyRequest::DetectProjectRoot { path }
        }
        DataRequest::Health { document } => dependency::DependencyRequest::Health { document },
        DataRequest::Cancel { cancellation_key } => {
            dependency::DependencyRequest::Cancel { cancellation_key }
        }
        DataRequest::Shutdown => dependency::DependencyRequest::Shutdown,
        DataRequest::Execute {
            cancellation_key,
            call_id,
            normalized_digest,
            authorization_grant,
            operation,
        } => dependency::DependencyRequest::Execute {
            cancellation_key,
            call_id,
            normalized_digest,
            authorization_grant,
            operation: to_dependency_operation(operation),
        },
    }
}

fn to_dependency_operation(operation: DataOperation) -> dependency::DependencyOperation {
    match operation {
        DataOperation::ProjectRoot { path } => {
            dependency::DependencyOperation::ProjectRoot { path }
        }
        DataOperation::Diagnostics { document } => {
            dependency::DependencyOperation::Diagnostics { document }
        }
        DataOperation::DocumentSymbols { document } => {
            dependency::DependencyOperation::DocumentSymbols { document }
        }
        DataOperation::WorkspaceSymbols { query } => {
            dependency::DependencyOperation::WorkspaceSymbols { query }
        }
        DataOperation::Definition { document, position } => {
            dependency::DependencyOperation::Definition {
                document,
                position: to_dependency_position(position),
            }
        }
        DataOperation::References {
            document,
            position,
            include_declaration,
        } => dependency::DependencyOperation::References {
            document,
            position: to_dependency_position(position),
            include_declaration,
        },
        DataOperation::Hover { document, position } => dependency::DependencyOperation::Hover {
            document,
            position: to_dependency_position(position),
        },
        DataOperation::SignatureHelp { document, position } => {
            dependency::DependencyOperation::SignatureHelp {
                document,
                position: to_dependency_position(position),
            }
        }
        DataOperation::Rename {
            document,
            position,
            new_name,
        } => dependency::DependencyOperation::Rename {
            document,
            position: to_dependency_position(position),
            new_name,
        },
        DataOperation::Formatting {
            document,
            tab_size,
            insert_spaces,
        } => dependency::DependencyOperation::Formatting {
            document,
            tab_size,
            insert_spaces,
        },
        DataOperation::CodeActions {
            document,
            range,
            diagnostics,
        } => dependency::DependencyOperation::CodeActions {
            document,
            range: to_dependency_range(range),
            diagnostics,
        },
    }
}

const fn to_dependency_position(value: DataPosition) -> dependency::DependencyPosition {
    dependency::DependencyPosition {
        line: value.line,
        character: value.character,
    }
}

const fn to_dependency_range(value: DataRange) -> dependency::DependencyRange {
    dependency::DependencyRange {
        start: to_dependency_position(value.start),
        end: to_dependency_position(value.end),
    }
}

fn from_dependency_response(response: dependency::DependencyResponse) -> DataResponse {
    match response {
        dependency::DependencyResponse::ProjectRoot { root } => DataResponse::ProjectRoot(root),
        dependency::DependencyResponse::Health {
            availability,
            server,
            capabilities,
            restart_count,
            detail,
        } => DataResponse::Health(DataHealth {
            availability: match availability {
                dependency::DependencyAvailability::Ready => DataAvailability::Ready,
                dependency::DependencyAvailability::Unavailable => DataAvailability::Unavailable,
            },
            server,
            capabilities,
            restart_count,
            detail,
        }),
        dependency::DependencyResponse::Unavailable { reason } => DataResponse::Unavailable(reason),
        dependency::DependencyResponse::Diagnostics(values) => DataResponse::Diagnostics(
            values
                .into_iter()
                .map(|v| DataDiagnostic {
                    path: v.path,
                    range: from_range(v.range),
                    severity: v.severity,
                    code: v.code,
                    source: v.source,
                    message: v.message,
                })
                .collect(),
        ),
        dependency::DependencyResponse::Symbols(values) => DataResponse::Symbols(
            values
                .into_iter()
                .map(|v| DataSymbol {
                    name: v.name,
                    kind: v.kind,
                    detail: v.detail,
                    location: v.location.map(from_location),
                    selection_range: v.selection_range.map(from_range),
                })
                .collect(),
        ),
        dependency::DependencyResponse::Locations(values) => {
            DataResponse::Locations(values.into_iter().map(from_location).collect())
        }
        dependency::DependencyResponse::Hover(value) => {
            DataResponse::Hover(value.map(|v| DataHover {
                contents: v.contents,
                range: v.range.map(from_range),
            }))
        }
        dependency::DependencyResponse::Signature(value) => {
            DataResponse::Signature(value.map(|v| DataSignature {
                signatures: v.signatures,
                active_signature: v.active_signature,
                active_parameter: v.active_parameter,
            }))
        }
        dependency::DependencyResponse::WorkspaceEdit(value) => {
            DataResponse::WorkspaceEdit(from_workspace_edit(value))
        }
        dependency::DependencyResponse::TextEdits(values) => {
            DataResponse::TextEdits(values.into_iter().map(from_text_edit).collect())
        }
        dependency::DependencyResponse::CodeActions(values) => DataResponse::CodeActions(
            values
                .into_iter()
                .map(|v| DataCodeAction {
                    title: v.title,
                    kind: v.kind,
                    edit: v.edit.map(from_workspace_edit),
                    command: v.command,
                })
                .collect(),
        ),
        dependency::DependencyResponse::Cancelled { active } => DataResponse::Cancelled(active),
        dependency::DependencyResponse::Shutdown => DataResponse::Shutdown,
    }
}

const fn from_range(value: dependency::DependencyRange) -> DataRange {
    DataRange {
        start: DataPosition {
            line: value.start.line,
            character: value.start.character,
        },
        end: DataPosition {
            line: value.end.line,
            character: value.end.character,
        },
    }
}

fn from_location(value: dependency::DependencyLocation) -> DataLocation {
    DataLocation {
        path: value.path,
        range: from_range(value.range),
    }
}

fn from_text_edit(value: dependency::DependencyTextEdit) -> DataTextEdit {
    DataTextEdit {
        range: from_range(value.range),
        new_text: value.new_text,
    }
}

fn from_workspace_edit(value: dependency::DependencyWorkspaceEdit) -> DataWorkspaceEdit {
    DataWorkspaceEdit {
        files: value
            .files
            .into_iter()
            .map(|file| DataFileEdits {
                path: file.path,
                edits: file.edits.into_iter().map(from_text_edit).collect(),
            })
            .collect(),
    }
}

/// Stable data error. Raw process/IO/RPC error types never escape dependency.
#[derive(Debug, Error)]
#[error("{code}: {detail}")]
pub struct DataError {
    pub code: &'static str,
    pub detail: String,
}

impl From<dependency::DependencyError> for DataError {
    fn from(value: dependency::DependencyError) -> Self {
        let code = match value {
            dependency::DependencyError::ServerUnavailable(_) => "unavailable",
            dependency::DependencyError::Timeout => "timeout",
            dependency::DependencyError::Cancelled => "cancelled",
            dependency::DependencyError::WorkspaceEscape => "workspace_escape",
            dependency::DependencyError::ConnectionClosed => "connection_closed",
            dependency::DependencyError::AuthorizationDenied(_) => "authorization_denied",
            _ => "dependency_failure",
        };
        Self {
            code,
            detail: value.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockDependency {
        requests: Mutex<Vec<dependency::DependencyRequest>>,
    }

    impl dependency::LspDependencyPort for MockDependency {
        fn execute(
            &self,
            request: dependency::DependencyRequest,
        ) -> Result<dependency::DependencyResponse, dependency::DependencyError> {
            self.requests.lock().expect("requests").push(request);
            Ok(dependency::DependencyResponse::Hover(Some(
                dependency::DependencyHover {
                    contents: "hover".into(),
                    range: None,
                },
            )))
        }
    }

    #[test]
    fn maps_owned_requests_and_records_across_both_boundaries() {
        let data = LspData::new(MockDependency {
            requests: Mutex::new(Vec::new()),
        });
        let result = data
            .execute(DataRequest::Execute {
                cancellation_key: "cancel".into(),
                call_id: "call".into(),
                normalized_digest: "digest".into(),
                authorization_grant: "grant".into(),
                operation: DataOperation::Hover {
                    document: "src/main.rs".into(),
                    position: DataPosition {
                        line: 3,
                        character: 2,
                    },
                },
            })
            .expect("mapped");
        assert_eq!(
            result,
            DataResponse::Hover(Some(DataHover {
                contents: "hover".into(),
                range: None
            }))
        );
        let request = data.dependency.requests.lock().expect("requests").pop();
        assert!(matches!(
            request,
            Some(dependency::DependencyRequest::Execute {
                call_id,
                normalized_digest,
                authorization_grant,
                ..
            }) if call_id == "call"
                && normalized_digest == "digest"
                && authorization_grant == "grant"
        ));
    }
}
