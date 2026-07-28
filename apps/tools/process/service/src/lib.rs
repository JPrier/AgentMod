//! Authenticated tool-protocol service for process operations.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use agentmod_process_host_logic::{
    CancelProcessCommand, CleanupPolicy, ExecutionMode, InputProcessCommand, OutputRange,
    OutputStream as LogicOutputStream, ProcessAuthorization, ProcessControlCommand, ProcessId,
    ProcessIdentity, ProcessLogicError, ProcessLogicPort, ProcessRecoveryStatus, ProcessResult,
    ProcessStatus, ReadOutputQuery, ResizeTerminalCommand, StartProcessCommand, TerminalSize,
};
use agentmod_tool_protocol::{OutputStream, ToolDescriptor, ToolHostCommand, ToolHostEvent};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

const PROCESS_GROUP: &str = "process";

/// Mandatory local caller configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessHostServiceConfig {
    /// Authenticated local owner.
    pub owner_id: String,
    /// Runtime session.
    pub session_id: String,
}

/// Redacted endpoint error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProcessServiceError {
    /// Host lacks identity configuration.
    #[error("process host authorization configuration is unavailable")]
    MissingConfiguration,
    /// Invalid envelope.
    #[error("process authorization envelope is invalid")]
    InvalidAuthorizationEnvelope,
    /// Unknown tool.
    #[error("unknown process tool")]
    UnknownTool,
    /// Invalid args.
    #[error("invalid process arguments")]
    InvalidArguments,
    /// Authorization or ownership denied by business policy.
    #[error("process authorization denied")]
    Authorization,
    /// Business failure.
    #[error("process operation failed")]
    Logic,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StartRequest {
    executable: String,
    #[serde(default)]
    arguments: Vec<String>,
    working_directory: Option<PathBuf>,
    #[serde(default)]
    environment: BTreeMap<String, String>,
    timeout_ms: Option<u64>,
    output_limit_bytes: u64,
    #[serde(default)]
    cleanup: ServiceCleanup,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal: Option<ServiceTerminalSize>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceTerminalSize {
    columns: u16,
    rows: u16,
    #[serde(default)]
    pixel_width: u16,
    #[serde(default)]
    pixel_height: u16,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ServiceCleanup {
    #[default]
    Retain,
    RemoveLogsOnSuccess,
    RemoveLogsAlways,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProcessRequest {
    process_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest {
    process_id: String,
    stream: ServiceStream,
    offset: u64,
    length: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ServiceStream {
    Stdout,
    Stderr,
    Terminal,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InputRequest {
    process_id: String,
    content: String,
    #[serde(default)]
    close: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResizeRequest {
    process_id: String,
    columns: u16,
    rows: u16,
    #[serde(default)]
    pixel_width: u16,
    #[serde(default)]
    pixel_height: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

/// Process service.
#[derive(Clone)]
pub struct ProcessHostService<L> {
    logic: L,
    config: ProcessHostServiceConfig,
}

impl<L> ProcessHostService<L> {
    /// Constructs a service only with explicit owner/session identity.
    ///
    /// # Errors
    ///
    /// Returns an error when either identity component is absent.
    pub fn new(logic: L, config: ProcessHostServiceConfig) -> Result<Self, ProcessServiceError> {
        if config.owner_id.trim().is_empty() || config.session_id.trim().is_empty() {
            return Err(ProcessServiceError::MissingConfiguration);
        }
        Ok(Self { logic, config })
    }
}

impl<L: ProcessLogicPort> ProcessHostService<L> {
    /// Handles a request without terminating the host on failure.
    ///
    /// # Errors
    ///
    /// Returns a redacted endpoint error for malformed or rejected requests.
    pub async fn handle(
        &self,
        command: ToolHostCommand,
    ) -> Result<Vec<ToolHostEvent>, ProcessServiceError> {
        match command {
            ToolHostCommand::DiscoverGroups => Ok(vec![ToolHostEvent::Groups {
                groups: vec![PROCESS_GROUP.to_owned()],
            }]),
            ToolHostCommand::DiscoverTools { groups } => Ok(vec![ToolHostEvent::Tools {
                tools: groups
                    .iter()
                    .any(|group| group == PROCESS_GROUP)
                    .then(tool_descriptors)
                    .unwrap_or_default(),
            }]),
            ToolHostCommand::Health => Ok(vec![ToolHostEvent::Progress {
                call_id: "health".to_owned(),
                message: "process host ready".to_owned(),
                completed: Some(1),
                total: Some(1),
            }]),
            ToolHostCommand::Cancel { cancellation_id } => {
                let id = cancellation_id.to_string();
                let call_id = self
                    .logic
                    .cancel(CancelProcessCommand {
                        identity: self.identity(),
                        cancellation_id: id,
                    })
                    .await
                    .map_err(map_logic_error)?;
                Ok(vec![ToolHostEvent::Cancelled { call_id }])
            }
            ToolHostCommand::Execute {
                call_id,
                tool,
                arguments,
                normalized_digest,
                authorization_grant,
                cancellation_id,
            } => {
                if call_id.trim().is_empty()
                    || tool.trim().is_empty()
                    || normalized_digest.len() != 64
                    || authorization_grant.trim().is_empty()
                {
                    return Err(ProcessServiceError::InvalidAuthorizationEnvelope);
                }
                let cancellation_id = cancellation_id.to_string();
                let canonical_operation = canonical_operation(&tool, &arguments, &cancellation_id)?;
                let authorization = ProcessAuthorization {
                    identity: self.identity(),
                    call_id: call_id.clone(),
                    tool: tool.clone(),
                    normalized_digest,
                    grant: authorization_grant,
                    cancellation_id,
                    canonical_operation,
                };
                self.execute(call_id, tool, arguments, authorization).await
            }
        }
    }

    fn identity(&self) -> ProcessIdentity {
        ProcessIdentity {
            owner_id: self.config.owner_id.clone(),
            session_id: self.config.session_id.clone(),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "explicit endpoint-to-logic mappings remain visible at the service boundary"
    )]
    async fn execute(
        &self,
        call_id: String,
        tool: String,
        arguments: Value,
        authorization: ProcessAuthorization,
    ) -> Result<Vec<ToolHostEvent>, ProcessServiceError> {
        let mut events = Vec::new();
        match tool.as_str() {
            "process.run" | "process.start" | "process.run_pty" | "process.start_pty" => {
                let request: StartRequest = parse(arguments)?;
                let is_terminal = matches!(tool.as_str(), "process.run_pty" | "process.start_pty");
                if is_terminal != request.terminal.is_some() {
                    return Err(ProcessServiceError::InvalidArguments);
                }
                let result = self
                    .logic
                    .start(map_start(
                        request,
                        authorization,
                        if matches!(tool.as_str(), "process.run" | "process.run_pty") {
                            ExecutionMode::Foreground
                        } else {
                            ExecutionMode::LongRunning
                        },
                    ))
                    .await
                    .map_err(map_logic_error)?;
                events.push(ToolHostEvent::Started {
                    call_id: call_id.clone(),
                });
                append_process_result(&mut events, &call_id, &result);
            }
            "process.read" => {
                let request: ReadRequest = parse(arguments)?;
                let stream = match request.stream {
                    ServiceStream::Stdout => LogicOutputStream::Stdout,
                    ServiceStream::Stderr => LogicOutputStream::Stderr,
                    ServiceStream::Terminal => LogicOutputStream::Terminal,
                };
                let result = self
                    .logic
                    .read_output(ReadOutputQuery {
                        authorization,
                        process_id: parse_id(request.process_id)?,
                        stream,
                        offset: request.offset,
                        length: request.length,
                    })
                    .await
                    .map_err(map_logic_error)?;
                events.push(ToolHostEvent::Started {
                    call_id: call_id.clone(),
                });
                append_output(&mut events, &call_id, stream, &result);
            }
            "process.input" => {
                let request: InputRequest = parse(arguments)?;
                self.logic
                    .input(InputProcessCommand {
                        authorization,
                        process_id: parse_id(request.process_id)?,
                        bytes: request.content.into_bytes(),
                        close: request.close,
                    })
                    .await
                    .map_err(map_logic_error)?;
                events.extend(simple_success(&call_id));
            }
            "process.resize" => {
                let request: ResizeRequest = parse(arguments)?;
                let result = self
                    .logic
                    .resize(ResizeTerminalCommand {
                        authorization,
                        process_id: parse_id(request.process_id)?,
                        size: TerminalSize {
                            columns: request.columns,
                            rows: request.rows,
                            pixel_width: request.pixel_width,
                            pixel_height: request.pixel_height,
                        },
                    })
                    .await
                    .map_err(map_logic_error)?;
                events.push(ToolHostEvent::Started {
                    call_id: call_id.clone(),
                });
                append_process_result(&mut events, &call_id, &result);
            }
            "process.wait" | "process.interrupt" | "process.kill" | "process.detach"
            | "process.reattach" => {
                let request: ProcessRequest = parse(arguments)?;
                let command = ProcessControlCommand {
                    authorization,
                    process_id: parse_id(request.process_id)?,
                };
                match tool.as_str() {
                    "process.wait" => {
                        let result = self.logic.wait(command).await.map_err(map_logic_error)?;
                        events.push(ToolHostEvent::Started {
                            call_id: call_id.clone(),
                        });
                        append_process_result(&mut events, &call_id, &result);
                    }
                    "process.interrupt" => {
                        self.logic
                            .interrupt(command)
                            .await
                            .map_err(map_logic_error)?;
                        events.extend(simple_success(&call_id));
                    }
                    "process.kill" => {
                        self.logic.kill(command).await.map_err(map_logic_error)?;
                        events.extend(simple_success(&call_id));
                    }
                    "process.detach" => {
                        let result = self.logic.detach(command).await.map_err(map_logic_error)?;
                        events.push(ToolHostEvent::Started {
                            call_id: call_id.clone(),
                        });
                        append_process_result(&mut events, &call_id, &result);
                    }
                    _ => {
                        let result = self
                            .logic
                            .reattach(command)
                            .await
                            .map_err(map_logic_error)?;
                        events.push(ToolHostEvent::Started {
                            call_id: call_id.clone(),
                        });
                        append_process_result(&mut events, &call_id, &result);
                    }
                }
            }
            "process.list" => {
                let _: EmptyRequest = parse(arguments)?;
                let records = self
                    .logic
                    .list(authorization)
                    .await
                    .map_err(map_logic_error)?;
                events.push(ToolHostEvent::Started {
                    call_id: call_id.clone(),
                });
                events.push(completed(
                    &call_id,
                    json!({"processes": records.iter().map(process_json).collect::<Vec<_>>() }),
                    false,
                ));
            }
            _ => return Err(ProcessServiceError::UnknownTool),
        }
        Ok(events)
    }
}

fn canonical_operation(
    tool: &str,
    arguments: &Value,
    cancellation_id: &str,
) -> Result<Vec<u8>, ProcessServiceError> {
    let normalized = match tool {
        "process.run" | "process.start" | "process.run_pty" | "process.start_pty" => {
            serde_json::to_value(parse::<StartRequest>(arguments.clone())?)
        }
        "process.read" => serde_json::to_value(parse::<ReadRequest>(arguments.clone())?),
        "process.input" => serde_json::to_value(parse::<InputRequest>(arguments.clone())?),
        "process.resize" => serde_json::to_value(parse::<ResizeRequest>(arguments.clone())?),
        "process.wait" | "process.interrupt" | "process.kill" | "process.detach"
        | "process.reattach" => serde_json::to_value(parse::<ProcessRequest>(arguments.clone())?),
        "process.list" => serde_json::to_value(parse::<EmptyRequest>(arguments.clone())?),
        _ => return Err(ProcessServiceError::UnknownTool),
    }
    .map_err(|_| ProcessServiceError::InvalidArguments)?;
    let normalized = normalize_json(&normalized);
    serde_json::to_vec(&(tool, cancellation_id, normalized))
        .map_err(|_| ProcessServiceError::InvalidArguments)
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).unwrap_or(Value::Null)
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}

fn map_start(
    request: StartRequest,
    authorization: ProcessAuthorization,
    mode: ExecutionMode,
) -> StartProcessCommand {
    StartProcessCommand {
        authorization,
        executable: request.executable,
        arguments: request.arguments,
        working_directory: request.working_directory,
        environment: request.environment,
        timeout: request.timeout_ms.map(Duration::from_millis),
        output_limit_bytes: request.output_limit_bytes,
        cleanup: match request.cleanup {
            ServiceCleanup::Retain => CleanupPolicy::Retain,
            ServiceCleanup::RemoveLogsOnSuccess => CleanupPolicy::RemoveLogsOnSuccess,
            ServiceCleanup::RemoveLogsAlways => CleanupPolicy::RemoveLogsAlways,
        },
        mode,
        terminal_size: request.terminal.map(|size| TerminalSize {
            columns: size.columns,
            rows: size.rows,
            pixel_width: size.pixel_width,
            pixel_height: size.pixel_height,
        }),
    }
}

fn append_process_result(events: &mut Vec<ToolHostEvent>, call_id: &str, result: &ProcessResult) {
    if !result.stdout.is_empty() {
        events.push(ToolHostEvent::Output {
            call_id: call_id.to_owned(),
            stream: OutputStream::Standard,
            content: String::from_utf8_lossy(&result.stdout).into_owned(),
        });
    }
    if !result.stderr.is_empty() {
        events.push(ToolHostEvent::Output {
            call_id: call_id.to_owned(),
            stream: OutputStream::Error,
            content: String::from_utf8_lossy(&result.stderr).into_owned(),
        });
    }
    events.push(completed(
        call_id,
        process_json(result),
        result.stdout_truncated || result.stderr_truncated,
    ));
}

fn append_output(
    events: &mut Vec<ToolHostEvent>,
    call_id: &str,
    stream: LogicOutputStream,
    result: &OutputRange,
) {
    if !result.bytes.is_empty() {
        events.push(ToolHostEvent::Output {
            call_id: call_id.to_owned(),
            stream: match stream {
                LogicOutputStream::Stdout | LogicOutputStream::Terminal => OutputStream::Standard,
                LogicOutputStream::Stderr => OutputStream::Error,
            },
            content: String::from_utf8_lossy(&result.bytes).into_owned(),
        });
    }
    events.push(completed(
        call_id,
        json!({"next_offset":result.next_offset,"retained_bytes":result.retained_bytes}),
        result.truncated,
    ));
}

fn process_json(result: &ProcessResult) -> Value {
    json!({
        "process_id":result.process_id.as_str(),
        "owner_id":result.owner_id,
        "session_id":result.session_id,
        "executable":result.executable,
        "working_directory":result.working_directory,
        "status":match result.status { ProcessStatus::Running=>"running", ProcessStatus::Exited=>"exited" },
        "exit":result.exit.as_ref().map(|exit| json!({"code":exit.code,"success":exit.success,"timed_out":exit.timed_out})),
        "detached":result.detached,
        "stdout_truncated":result.stdout_truncated,
        "stderr_truncated":result.stderr_truncated,
        "logs_removed":result.logs_removed,
        "cleanup_failed":result.cleanup_failed,
        "terminal":result.terminal,
        "terminal_size":result.terminal_size.map(|size| json!({
            "columns":size.columns,
            "rows":size.rows,
            "pixel_width":size.pixel_width,
            "pixel_height":size.pixel_height,
        })),
        "os_process_id":result.os_process_id,
        "os_start_time":result.os_start_time,
        "recovery_status":match result.recovery_status {
            ProcessRecoveryStatus::Live=>"live",
            ProcessRecoveryStatus::RecoveredRunningUnattached=>"recovered_running_unattached",
            ProcessRecoveryStatus::RecoveredExited=>"recovered_exited",
            ProcessRecoveryStatus::DispatchUncertain=>"dispatch_uncertain",
        },
    })
}

fn simple_success(call_id: &str) -> Vec<ToolHostEvent> {
    vec![
        ToolHostEvent::Started {
            call_id: call_id.to_owned(),
        },
        completed(call_id, json!({"accepted":true}), false),
    ]
}

fn completed(call_id: &str, result: Value, truncated: bool) -> ToolHostEvent {
    ToolHostEvent::Completed {
        call_id: call_id.to_owned(),
        result,
        artifact: None,
        truncated,
    }
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, ProcessServiceError> {
    serde_json::from_value(value).map_err(|_| ProcessServiceError::InvalidArguments)
}

fn parse_id(value: String) -> Result<ProcessId, ProcessServiceError> {
    ProcessId::parse(value).map_err(map_logic_error)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err supplies an owned layer error which is reduced to a service class"
)]
fn map_logic_error(error: ProcessLogicError) -> ProcessServiceError {
    match error {
        ProcessLogicError::Authorization | ProcessLogicError::Ownership => {
            ProcessServiceError::Authorization
        }
        ProcessLogicError::InvalidExecutable
        | ProcessLogicError::InvalidArgument
        | ProcessLogicError::InvalidProcessId
        | ProcessLogicError::WorkingDirectoryEscape
        | ProcessLogicError::InvalidEnvironment
        | ProcessLogicError::EnvironmentDenied
        | ProcessLogicError::InvalidTimeout
        | ProcessLogicError::InvalidOutputLimit
        | ProcessLogicError::InvalidOutputRange => ProcessServiceError::InvalidArguments,
        ProcessLogicError::Operation => ProcessServiceError::Logic,
    }
}

fn tool_descriptors() -> Vec<ToolDescriptor> {
    [
        "process.run",
        "process.start",
        "process.run_pty",
        "process.start_pty",
        "process.read",
        "process.input",
        "process.resize",
        "process.wait",
        "process.interrupt",
        "process.kill",
        "process.detach",
        "process.reattach",
        "process.list",
    ]
    .into_iter()
    .map(|id| ToolDescriptor {
        id: id.to_owned(),
        group: PROCESS_GROUP.to_owned(),
        description: format!("Authenticated {id} operation."),
        input_schema: json!({"type":"object"}),
        supported_decisions: vec![
            "continue".to_owned(),
            "reject".to_owned(),
            "require_approval".to_owned(),
            "cancel".to_owned(),
        ],
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockLogic;

    #[async_trait::async_trait]
    impl ProcessLogicPort for MockLogic {
        async fn start(
            &self,
            _command: StartProcessCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            unreachable!()
        }
        async fn read_output(
            &self,
            _query: ReadOutputQuery,
        ) -> Result<OutputRange, ProcessLogicError> {
            unreachable!()
        }
        async fn input(&self, _command: InputProcessCommand) -> Result<(), ProcessLogicError> {
            unreachable!()
        }
        async fn resize(
            &self,
            _command: ResizeTerminalCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            unreachable!()
        }
        async fn wait(
            &self,
            _command: ProcessControlCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            unreachable!()
        }
        async fn interrupt(
            &self,
            _command: ProcessControlCommand,
        ) -> Result<(), ProcessLogicError> {
            unreachable!()
        }
        async fn kill(&self, _command: ProcessControlCommand) -> Result<(), ProcessLogicError> {
            unreachable!()
        }
        async fn detach(
            &self,
            _command: ProcessControlCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            unreachable!()
        }
        async fn reattach(
            &self,
            _command: ProcessControlCommand,
        ) -> Result<ProcessResult, ProcessLogicError> {
            unreachable!()
        }
        async fn list(
            &self,
            _authorization: ProcessAuthorization,
        ) -> Result<Vec<ProcessResult>, ProcessLogicError> {
            Ok(Vec::new())
        }
        async fn cancel(
            &self,
            _command: CancelProcessCommand,
        ) -> Result<String, ProcessLogicError> {
            Ok("call".to_owned())
        }
    }

    #[test]
    fn refuses_missing_identity_configuration() {
        assert!(matches!(
            ProcessHostService::new(
                MockLogic,
                ProcessHostServiceConfig {
                    owner_id: String::new(),
                    session_id: "session".to_owned(),
                }
            ),
            Err(ProcessServiceError::MissingConfiguration)
        ));
    }
}
