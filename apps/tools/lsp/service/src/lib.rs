//! LSP tool-protocol endpoint mappings.
//!
//! `Started` is emitted only after the lower dependency boundary has verified
//! the exact digest and one-time authorization grant. Rename, formatting, and
//! code actions return proposals; this service has no endpoint that applies
//! edits or executes server-returned commands.
#![allow(missing_docs)]

use std::io::{BufRead, Write};

use agentmod_lsp_host_logic as logic;
use agentmod_tool_protocol::{ToolDescriptor, ToolHostCommand, ToolHostEvent};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

const GROUP: &str = "lsp";
const MAX_COMMAND_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PositionArguments {
    pub line: u32,
    pub character: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RangeArguments {
    pub start: PositionArguments,
    pub end: PositionArguments,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct DocumentArguments {
    document: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct PositionedArguments {
    document: String,
    position: PositionArguments,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct WorkspaceSymbolArguments {
    #[serde(default)]
    query: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ReferenceArguments {
    document: String,
    position: PositionArguments,
    #[serde(default)]
    include_declaration: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct RenameArguments {
    document: String,
    position: PositionArguments,
    new_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct FormattingArguments {
    document: String,
    #[serde(default = "default_tab_size")]
    tab_size: u32,
    #[serde(default = "default_true")]
    insert_spaces: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct CodeActionArguments {
    document: String,
    range: RangeArguments,
    #[serde(default)]
    diagnostics: Vec<String>,
}

fn default_tab_size() -> u32 {
    4
}

fn default_true() -> bool {
    true
}

/// Endpoint implementation over a mocked or concrete logic interface.
pub struct LspService<L> {
    logic: L,
}

impl<L> LspService<L> {
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L: logic::LspLogicPort> LspService<L> {
    /// Maps one versioned tool-host command into service-owned types.
    #[must_use]
    pub fn handle_wire(&self, command: ToolHostCommand) -> Vec<ToolHostEvent> {
        match command {
            ToolHostCommand::DiscoverGroups => vec![ToolHostEvent::Groups {
                groups: vec![GROUP.into()],
            }],
            ToolHostCommand::DiscoverTools { groups } => {
                let tools = if groups.iter().any(|group| group == GROUP) {
                    descriptors()
                } else {
                    Vec::new()
                };
                vec![ToolHostEvent::Tools { tools }]
            }
            ToolHostCommand::Health => {
                let result = self
                    .logic
                    .execute(logic::LogicCommand::Health { document: None });
                vec![result_event("health", result)]
            }
            ToolHostCommand::Cancel { cancellation_id } => {
                let key = cancellation_id.to_string();
                let result = self.logic.execute(logic::LogicCommand::Cancel {
                    cancellation_key: key.clone(),
                });
                match result {
                    Ok(logic::LogicResult::Cancelled(_)) => {
                        vec![ToolHostEvent::Cancelled { call_id: key }]
                    }
                    Ok(other) => vec![failed(
                        "cancel",
                        "mapping",
                        &format!("unexpected result: {other:?}"),
                        false,
                    )],
                    Err(error) => vec![failed("cancel", "logic", &error.to_string(), false)],
                }
            }
            ToolHostCommand::Execute {
                call_id,
                tool,
                arguments,
                normalized_digest,
                authorization_grant,
                cancellation_id,
            } => {
                let operation = match parse_operation(&tool, arguments) {
                    Ok(operation) => operation,
                    Err(error) => {
                        return vec![failed(
                            &call_id,
                            "invalid_arguments",
                            &error.to_string(),
                            false,
                        )];
                    }
                };
                let result = self.logic.execute(logic::LogicCommand::Execute {
                    cancellation_key: cancellation_id.to_string(),
                    call_id: call_id.clone(),
                    normalized_digest,
                    authorization_grant,
                    operation,
                });
                match result {
                    Ok(value) => vec![
                        ToolHostEvent::Started {
                            call_id: call_id.clone(),
                        },
                        completed(call_id, value),
                    ],
                    Err(error) => {
                        let code = match &error {
                            logic::LogicError::Data { code, .. } => *code,
                            logic::LogicError::InvalidCommand(_) => "invalid_arguments",
                            logic::LogicError::UnsafeEdit(_) => "unsafe_edit",
                            logic::LogicError::Invariant(_) => "internal",
                        };
                        vec![failed(
                            &call_id,
                            code,
                            &error.to_string(),
                            code == "timeout",
                        )]
                    }
                }
            }
        }
    }

    /// Runs the bounded newline-delimited JSON stdio endpoint.
    ///
    /// # Errors
    ///
    /// Returns a service transport error for oversized, malformed, or failed
    /// input/output. Individual business failures are returned as protocol
    /// events and do not terminate the stream.
    pub fn run_jsonl<R: BufRead, W: Write>(
        &self,
        mut reader: R,
        mut writer: W,
    ) -> Result<(), ServiceError> {
        loop {
            let mut line = String::new();
            let bytes = reader.read_line(&mut line).map_err(ServiceError::Read)?;
            if bytes == 0 {
                return Ok(());
            }
            if bytes > MAX_COMMAND_BYTES {
                return Err(ServiceError::CommandTooLarge);
            }
            let command = serde_json::from_str::<ToolHostCommand>(&line)
                .map_err(|error| ServiceError::Malformed(error.to_string()))?;
            for event in self.handle_wire(command) {
                serde_json::to_writer(&mut writer, &event)
                    .map_err(|error| ServiceError::Serialize(error.to_string()))?;
                writer.write_all(b"\n").map_err(ServiceError::Write)?;
                writer.flush().map_err(ServiceError::Write)?;
            }
        }
    }
}

fn parse_operation(tool: &str, arguments: Value) -> Result<logic::LogicOperation, ServiceError> {
    match tool {
        "lsp.project_root" => {
            let value: DocumentArguments = parse(arguments)?;
            Ok(logic::LogicOperation::ProjectRoot {
                path: value.document,
            })
        }
        "lsp.diagnostics" => {
            parse::<DocumentArguments>(arguments).map(|v| logic::LogicOperation::Diagnostics {
                document: v.document,
            })
        }
        "lsp.document_symbols" => {
            parse::<DocumentArguments>(arguments).map(|v| logic::LogicOperation::DocumentSymbols {
                document: v.document,
            })
        }
        "lsp.workspace_symbols" => parse::<WorkspaceSymbolArguments>(arguments)
            .map(|v| logic::LogicOperation::WorkspaceSymbols { query: v.query }),
        "lsp.definition" => {
            parse::<PositionedArguments>(arguments).map(|v| logic::LogicOperation::Definition {
                document: v.document,
                position: position(v.position),
            })
        }
        "lsp.references" => {
            parse::<ReferenceArguments>(arguments).map(|v| logic::LogicOperation::References {
                document: v.document,
                position: position(v.position),
                include_declaration: v.include_declaration,
            })
        }
        "lsp.hover" => {
            parse::<PositionedArguments>(arguments).map(|v| logic::LogicOperation::Hover {
                document: v.document,
                position: position(v.position),
            })
        }
        "lsp.signature_help" => {
            parse::<PositionedArguments>(arguments).map(|v| logic::LogicOperation::SignatureHelp {
                document: v.document,
                position: position(v.position),
            })
        }
        "lsp.rename" => {
            parse::<RenameArguments>(arguments).map(|v| logic::LogicOperation::Rename {
                document: v.document,
                position: position(v.position),
                new_name: v.new_name,
            })
        }
        "lsp.formatting" => {
            parse::<FormattingArguments>(arguments).map(|v| logic::LogicOperation::Formatting {
                document: v.document,
                tab_size: v.tab_size,
                insert_spaces: v.insert_spaces,
            })
        }
        "lsp.code_actions" => {
            parse::<CodeActionArguments>(arguments).map(|v| logic::LogicOperation::CodeActions {
                document: v.document,
                range: range(v.range),
                diagnostics: v.diagnostics,
            })
        }
        _ => Err(ServiceError::UnknownTool(tool.to_owned())),
    }
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, ServiceError> {
    serde_json::from_value(value).map_err(|error| ServiceError::Malformed(error.to_string()))
}

const fn position(value: PositionArguments) -> logic::LogicPosition {
    logic::LogicPosition {
        line: value.line,
        character: value.character,
    }
}

const fn range(value: RangeArguments) -> logic::LogicRange {
    logic::LogicRange {
        start: position(value.start),
        end: position(value.end),
    }
}

fn result_event(
    call_id: &str,
    result: Result<logic::LogicResult, logic::LogicError>,
) -> ToolHostEvent {
    match result {
        Ok(result) => completed(call_id.into(), result),
        Err(error) => failed(call_id, "logic", &error.to_string(), false),
    }
}

fn completed(call_id: String, result: logic::LogicResult) -> ToolHostEvent {
    ToolHostEvent::Completed {
        call_id,
        result: result_json(result),
        artifact: None,
        truncated: false,
    }
}

fn result_json(result: logic::LogicResult) -> Value {
    match result {
        logic::LogicResult::ProjectRoot(root) => json!({"root":root}),
        logic::LogicResult::Health(v) => json!({"ready":v.ready,"server":v.server,"capabilities":v.capabilities,"restart_count":v.restart_count,"detail":v.detail}),
        logic::LogicResult::Unavailable(reason) => json!({"available":false,"reason":reason}),
        logic::LogicResult::Diagnostics(values) => Value::Array(values.into_iter().map(|v| json!({"path":v.path,"range":range_json(v.range),"severity":v.severity,"code":v.code,"source":v.source,"message":v.message})).collect()),
        logic::LogicResult::Symbols(values) => Value::Array(values.into_iter().map(|v| json!({"name":v.name,"kind":v.kind,"detail":v.detail,"location":v.location.map(location_json),"selection_range":v.selection_range.map(range_json)})).collect()),
        logic::LogicResult::Locations(values) => Value::Array(values.into_iter().map(location_json).collect()),
        logic::LogicResult::Hover(value) => value.map_or(Value::Null, |v| json!({"contents":v.contents,"range":v.range.map(range_json)})),
        logic::LogicResult::Signature(value) => value.map_or(Value::Null, |v| json!({"signatures":v.signatures,"active_signature":v.active_signature,"active_parameter":v.active_parameter})),
        logic::LogicResult::WorkspaceEdit(value) => workspace_edit_json(value),
        logic::LogicResult::TextEdits(values) => Value::Array(values.into_iter().map(edit_json).collect()),
        logic::LogicResult::CodeActions(values) => Value::Array(values.into_iter().map(|v| json!({"title":v.title,"kind":v.kind,"edit":v.edit.map(workspace_edit_json),"command":v.command})).collect()),
        logic::LogicResult::Cancelled(active) => json!({"cancelled":active}),
        logic::LogicResult::Shutdown => json!({"shutdown":true}),
    }
}

fn range_json(value: logic::LogicRange) -> Value {
    json!({"start":{"line":value.start.line,"character":value.start.character},"end":{"line":value.end.line,"character":value.end.character}})
}

#[allow(clippy::needless_pass_by_value)]
fn location_json(value: logic::LogicLocation) -> Value {
    json!({"path":value.path,"range":range_json(value.range)})
}

#[allow(clippy::needless_pass_by_value)]
fn edit_json(value: logic::LogicTextEdit) -> Value {
    json!({"range":range_json(value.range),"new_text":value.new_text})
}

fn workspace_edit_json(value: logic::LogicWorkspaceEdit) -> Value {
    Value::Array(
        value
            .files
            .into_iter()
            .map(|file| {
                json!({
                    "path":file.path,
                    "edits":file.edits.into_iter().map(edit_json).collect::<Vec<_>>()
                })
            })
            .collect(),
    )
}

fn failed(call_id: &str, code: &str, message: &str, retryable: bool) -> ToolHostEvent {
    ToolHostEvent::Failed {
        call_id: call_id.into(),
        code: code.into(),
        message: message.chars().take(512).collect(),
        retryable,
    }
}

fn descriptors() -> Vec<ToolDescriptor> {
    vec![
        descriptor(
            "project_root",
            "Detect the workspace-contained project root.",
            document_schema(),
        ),
        descriptor(
            "diagnostics",
            "Collect published diagnostics for a document.",
            document_schema(),
        ),
        descriptor(
            "document_symbols",
            "List symbols in one document.",
            document_schema(),
        ),
        descriptor(
            "workspace_symbols",
            "Search symbols in the workspace.",
            json!({"type":"object","properties":{"query":{"type":"string"}},"additionalProperties":false}),
        ),
        descriptor("definition", "Find definitions.", positioned_schema()),
        descriptor(
            "references",
            "Find references.",
            json!({"type":"object","required":["document","position"],"properties":{"document":{"type":"string"},"position":position_schema(),"include_declaration":{"type":"boolean"}},"additionalProperties":false}),
        ),
        descriptor("hover", "Get hover information.", positioned_schema()),
        descriptor("signature_help", "Get signature help.", positioned_schema()),
        descriptor(
            "rename",
            "Propose, but do not apply, a workspace rename edit.",
            json!({"type":"object","required":["document","position","new_name"],"properties":{"document":{"type":"string"},"position":position_schema(),"new_name":{"type":"string","minLength":1}},"additionalProperties":false}),
        ),
        descriptor(
            "formatting",
            "Propose, but do not apply, document formatting edits.",
            json!({"type":"object","required":["document"],"properties":{"document":{"type":"string"},"tab_size":{"type":"integer","minimum":1},"insert_spaces":{"type":"boolean"}},"additionalProperties":false}),
        ),
        descriptor(
            "code_actions",
            "List code-action and edit proposals without executing them.",
            json!({"type":"object","required":["document","range"],"properties":{"document":{"type":"string"},"range":{"type":"object","required":["start","end"],"properties":{"start":position_schema(),"end":position_schema()}},"diagnostics":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}),
        ),
    ]
}

fn descriptor(name: &str, description: &str, input_schema: Value) -> ToolDescriptor {
    ToolDescriptor {
        id: format!("lsp.{name}"),
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
    }
}

fn document_schema() -> Value {
    json!({"type":"object","required":["document"],"properties":{"document":{"type":"string"}},"additionalProperties":false})
}

fn position_schema() -> Value {
    json!({"type":"object","required":["line","character"],"properties":{"line":{"type":"integer","minimum":0},"character":{"type":"integer","minimum":0}},"additionalProperties":false})
}

fn positioned_schema() -> Value {
    json!({"type":"object","required":["document","position"],"properties":{"document":{"type":"string"},"position":position_schema()},"additionalProperties":false})
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("unknown LSP tool: {0}")]
    UnknownTool(String),
    #[error("malformed service request: {0}")]
    Malformed(String),
    #[error("tool command exceeds the endpoint bound")]
    CommandTooLarge,
    #[error("service input failed: {0}")]
    Read(std::io::Error),
    #[error("service output failed: {0}")]
    Write(std::io::Error),
    #[error("service serialization failed: {0}")]
    Serialize(String),
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Mutex;

    use super::*;

    struct MockLogic {
        commands: Mutex<Vec<logic::LogicCommand>>,
    }

    impl logic::LspLogicPort for MockLogic {
        fn execute(
            &self,
            command: logic::LogicCommand,
        ) -> Result<logic::LogicResult, logic::LogicError> {
            self.commands.lock().expect("commands").push(command);
            Ok(logic::LogicResult::Hover(None))
        }
    }

    fn execute_command() -> ToolHostCommand {
        serde_json::from_value(json!({
            "command":"execute",
            "value":{
                "call_id":"call",
                "tool":"lsp.hover",
                "arguments":{"document":"src/main.rs","position":{"line":1,"character":2}},
                "normalized_digest":"digest",
                "authorization_grant":"grant",
                "cancellation_id":"00000000-0000-0000-0000-000000000001"
            }
        }))
        .expect("wire command")
    }

    #[test]
    fn discovery_is_lazy_and_execute_maps_the_complete_envelope() {
        let service = LspService::new(MockLogic {
            commands: Mutex::new(Vec::new()),
        });
        assert!(matches!(
            service.handle_wire(ToolHostCommand::DiscoverGroups).as_slice(),
            [ToolHostEvent::Groups { groups }] if groups == &vec!["lsp".to_owned()]
        ));
        let events = service.handle_wire(execute_command());
        assert!(matches!(
            events.as_slice(),
            [
                ToolHostEvent::Started { .. },
                ToolHostEvent::Completed { .. }
            ]
        ));
        assert!(matches!(
            service.logic.commands.lock().expect("commands").first(),
            Some(logic::LogicCommand::Execute {
                call_id,
                normalized_digest,
                authorization_grant,
                operation: logic::LogicOperation::Hover { position, .. },
                ..
            }) if call_id == "call"
                && normalized_digest == "digest"
                && authorization_grant == "grant"
                && *position == logic::LogicPosition { line: 1, character: 2 }
        ));
    }

    #[test]
    fn jsonl_endpoint_rejects_unknown_fields_and_streams_events() {
        let service = LspService::new(MockLogic {
            commands: Mutex::new(Vec::new()),
        });
        let input = serde_json::to_string(&ToolHostCommand::DiscoverGroups).expect("json") + "\n";
        let mut output = Vec::new();
        service
            .run_jsonl(Cursor::new(input), &mut output)
            .expect("endpoint");
        let event: ToolHostEvent =
            serde_json::from_slice(output.strip_suffix(b"\n").expect("newline")).expect("event");
        assert!(matches!(event, ToolHostEvent::Groups { .. }));

        let events = service.handle_wire(
            serde_json::from_value(json!({
                "command":"execute",
                "value":{
                    "call_id":"bad",
                    "tool":"lsp.hover",
                    "arguments":{"document":"src/main.rs","position":{"line":0,"character":0},"unexpected":true},
                    "normalized_digest":"digest",
                    "authorization_grant":"grant",
                    "cancellation_id":"00000000-0000-0000-0000-000000000001"
                }
            }))
            .expect("wire"),
        );
        assert!(
            matches!(events.as_slice(), [ToolHostEvent::Failed { code, .. }] if code == "invalid_arguments")
        );
    }
}
