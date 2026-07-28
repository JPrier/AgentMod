//! Process business policy, ownership, and authorization propagation.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use agentmod_process_host_data::{
    ProcessCancelDataRequest, ProcessControlDataRequest, ProcessDataAuthorization,
    ProcessDataCleanup, ProcessDataError, ProcessDataExit, ProcessDataId, ProcessDataIdentity,
    ProcessDataPort, ProcessDataRecord, ProcessDataRecoveryState, ProcessDataState,
    ProcessDataStream, ProcessDataTerminalSize, ProcessInputDataRequest, ProcessOutputDataRecord,
    ReadProcessOutputDataRequest, ResizeProcessTerminalDataRequest, StartProcessDataRequest,
};
use async_trait::async_trait;
use thiserror::Error;

/// Logic-owned identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessIdentity {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
}

/// Logic-owned authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessAuthorization {
    /// Identity.
    pub identity: ProcessIdentity,
    /// Call.
    pub call_id: String,
    /// Tool.
    pub tool: String,
    /// Digest.
    pub normalized_digest: String,
    /// Grant.
    pub grant: String,
    /// Cancellation.
    pub cancellation_id: String,
    /// Canonical operation.
    pub canonical_operation: Vec<u8>,
}

/// Process ID.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProcessId(String);

impl ProcessId {
    /// Parses ID.
    ///
    /// # Errors
    ///
    /// Returns an error for empty identifier text.
    pub fn parse(value: String) -> Result<Self, ProcessLogicError> {
        if value.trim().is_empty() {
            Err(ProcessLogicError::InvalidProcessId)
        } else {
            Ok(Self(value))
        }
    }

    /// ID text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Execution mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionMode {
    /// Wait.
    Foreground,
    /// Return running.
    LongRunning,
}

/// Cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupPolicy {
    /// Retain.
    Retain,
    /// Success.
    RemoveLogsOnSuccess,
    /// Always.
    RemoveLogsAlways,
}

/// Logic-owned terminal dimensions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TerminalSize {
    /// Columns.
    pub columns: u16,
    /// Rows.
    pub rows: u16,
    /// Cell width in pixels.
    pub pixel_width: u16,
    /// Cell height in pixels.
    pub pixel_height: u16,
}

/// Start command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartProcessCommand {
    /// Authorization.
    pub authorization: ProcessAuthorization,
    /// Executable.
    pub executable: String,
    /// Args.
    pub arguments: Vec<String>,
    /// Cwd.
    pub working_directory: Option<PathBuf>,
    /// Environment overrides.
    pub environment: BTreeMap<String, String>,
    /// Timeout.
    pub timeout: Option<Duration>,
    /// Output bound.
    pub output_limit_bytes: u64,
    /// Cleanup.
    pub cleanup: CleanupPolicy,
    /// Mode.
    pub mode: ExecutionMode,
    /// PTY dimensions.
    pub terminal_size: Option<TerminalSize>,
}

/// State.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStatus {
    /// Running.
    Running,
    /// Exited.
    Exited,
}

/// Logic-owned restart-reconciliation classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessRecoveryStatus {
    /// Live in this host.
    Live,
    /// Exact child still exists but inherited handles cannot be reconstructed.
    RecoveredRunningUnattached,
    /// Recovered as exited.
    RecoveredExited,
    /// Dispatch outcome was uncertain and execution was not repeated.
    DispatchUncertain,
}

/// Exit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExitStatus {
    /// Code.
    pub code: Option<i32>,
    /// Success.
    pub success: bool,
    /// Timeout.
    pub timed_out: bool,
}

/// Result.
#[allow(
    clippy::struct_excessive_bools,
    reason = "explicit independent state flags"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessResult {
    /// ID.
    pub process_id: ProcessId,
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Executable.
    pub executable: String,
    /// Cwd.
    pub working_directory: PathBuf,
    /// Status.
    pub status: ProcessStatus,
    /// Exit.
    pub exit: Option<ExitStatus>,
    /// Detached.
    pub detached: bool,
    /// stdout.
    pub stdout: Vec<u8>,
    /// stderr.
    pub stderr: Vec<u8>,
    /// stdout truncation.
    pub stdout_truncated: bool,
    /// stderr truncation.
    pub stderr_truncated: bool,
    /// Logs removed.
    pub logs_removed: bool,
    /// Cleanup failure.
    pub cleanup_failed: bool,
    /// PTY marker.
    pub terminal: bool,
    /// Current terminal dimensions.
    pub terminal_size: Option<TerminalSize>,
    /// OS process ID.
    pub os_process_id: Option<u32>,
    /// OS start time.
    pub os_start_time: Option<u64>,
    /// Reconciliation classification.
    pub recovery_status: ProcessRecoveryStatus,
}

/// Stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputStream {
    /// stdout.
    Stdout,
    /// stderr.
    Stderr,
    /// Combined PTY stream.
    Terminal,
}

/// Control command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessControlCommand {
    /// Authorization.
    pub authorization: ProcessAuthorization,
    /// ID.
    pub process_id: ProcessId,
}

/// Output query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadOutputQuery {
    /// Authorization.
    pub authorization: ProcessAuthorization,
    /// ID.
    pub process_id: ProcessId,
    /// Stream.
    pub stream: OutputStream,
    /// Offset.
    pub offset: u64,
    /// Length.
    pub length: u64,
}

/// Output range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRange {
    /// Bytes.
    pub bytes: Vec<u8>,
    /// Next.
    pub next_offset: u64,
    /// Retained.
    pub retained_bytes: u64,
    /// Truncated.
    pub truncated: bool,
}

/// Input command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputProcessCommand {
    /// Authorization.
    pub authorization: ProcessAuthorization,
    /// ID.
    pub process_id: ProcessId,
    /// Bytes.
    pub bytes: Vec<u8>,
    /// Close.
    pub close: bool,
}

/// Resize terminal command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResizeTerminalCommand {
    /// Authorization.
    pub authorization: ProcessAuthorization,
    /// ID.
    pub process_id: ProcessId,
    /// Dimensions.
    pub size: TerminalSize,
}

/// Cancel command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelProcessCommand {
    /// Identity.
    pub identity: ProcessIdentity,
    /// Cancellation token.
    pub cancellation_id: String,
}

/// Logic policy.
#[derive(Clone, Debug)]
pub struct ProcessLogicConfig {
    /// Workspace.
    pub workspace_root: PathBuf,
    /// Allowed override keys.
    pub environment_allowlist: BTreeSet<String>,
    /// Denied override keys.
    pub environment_denylist: BTreeSet<String>,
    /// Max timeout.
    pub max_timeout: Duration,
    /// Max output.
    pub max_output_bytes: u64,
    /// Max projection.
    pub max_projection_bytes: u64,
}

/// Redacted logic error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProcessLogicError {
    /// Executable.
    #[error("invalid executable")]
    InvalidExecutable,
    /// Argument.
    #[error("invalid argument")]
    InvalidArgument,
    /// ID.
    #[error("invalid process ID")]
    InvalidProcessId,
    /// Escape.
    #[error("working directory escape")]
    WorkingDirectoryEscape,
    /// Environment.
    #[error("invalid environment")]
    InvalidEnvironment,
    /// Denied environment.
    #[error("environment override denied")]
    EnvironmentDenied,
    /// Timeout.
    #[error("invalid timeout")]
    InvalidTimeout,
    /// Output.
    #[error("invalid output limit")]
    InvalidOutputLimit,
    /// Range.
    #[error("invalid output range")]
    InvalidOutputRange,
    /// Authorization.
    #[error("process authorization denied")]
    Authorization,
    /// Ownership.
    #[error("process ownership denied")]
    Ownership,
    /// Lifecycle.
    #[error("process operation failed")]
    Operation,
}

/// Logic interface.
#[async_trait]
pub trait ProcessLogicPort: Send + Sync {
    /// Start.
    async fn start(&self, command: StartProcessCommand)
    -> Result<ProcessResult, ProcessLogicError>;
    /// Read.
    async fn read_output(&self, query: ReadOutputQuery) -> Result<OutputRange, ProcessLogicError>;
    /// Input.
    async fn input(&self, command: InputProcessCommand) -> Result<(), ProcessLogicError>;
    /// Resize PTY.
    async fn resize(
        &self,
        command: ResizeTerminalCommand,
    ) -> Result<ProcessResult, ProcessLogicError>;
    /// Wait.
    async fn wait(
        &self,
        command: ProcessControlCommand,
    ) -> Result<ProcessResult, ProcessLogicError>;
    /// Interrupt.
    async fn interrupt(&self, command: ProcessControlCommand) -> Result<(), ProcessLogicError>;
    /// Kill.
    async fn kill(&self, command: ProcessControlCommand) -> Result<(), ProcessLogicError>;
    /// Detach.
    async fn detach(
        &self,
        command: ProcessControlCommand,
    ) -> Result<ProcessResult, ProcessLogicError>;
    /// Reattach.
    async fn reattach(
        &self,
        command: ProcessControlCommand,
    ) -> Result<ProcessResult, ProcessLogicError>;
    /// List.
    async fn list(
        &self,
        authorization: ProcessAuthorization,
    ) -> Result<Vec<ProcessResult>, ProcessLogicError>;
    /// Cancel.
    async fn cancel(&self, command: CancelProcessCommand) -> Result<String, ProcessLogicError>;
}

/// Logic implementation.
#[derive(Clone)]
pub struct ProcessLogic<D> {
    data: D,
    config: ProcessLogicConfig,
}

impl<D> ProcessLogic<D> {
    /// Injects data.
    #[must_use]
    pub const fn new(data: D, config: ProcessLogicConfig) -> Self {
        Self { data, config }
    }
}

#[async_trait]
impl<D: ProcessDataPort> ProcessLogicPort for ProcessLogic<D> {
    async fn start(
        &self,
        command: StartProcessCommand,
    ) -> Result<ProcessResult, ProcessLogicError> {
        validate_start(&command, &self.config)?;
        let requested_working_directory = command.working_directory.clone();
        let cwd = resolve_cwd(&self.config.workspace_root, command.working_directory)?;
        let environment = filter_environment(
            command.environment,
            &self.config.environment_allowlist,
            &self.config.environment_denylist,
        )?;
        map_record(
            self.data
                .start_process(StartProcessDataRequest {
                    authorization: map_authorization(command.authorization),
                    workspace_root: self.config.workspace_root.clone(),
                    working_directory: cwd,
                    requested_working_directory,
                    executable: command.executable,
                    arguments: command.arguments,
                    environment,
                    timeout: command.timeout,
                    output_limit_bytes: command.output_limit_bytes,
                    cleanup: match command.cleanup {
                        CleanupPolicy::Retain => ProcessDataCleanup::Retain,
                        CleanupPolicy::RemoveLogsOnSuccess => {
                            ProcessDataCleanup::RemoveLogsOnSuccess
                        }
                        CleanupPolicy::RemoveLogsAlways => ProcessDataCleanup::RemoveLogsAlways,
                    },
                    foreground: command.mode == ExecutionMode::Foreground,
                    terminal_size: command.terminal_size.map(map_terminal_size),
                })
                .await
                .map_err(map_error)?,
        )
    }

    async fn read_output(&self, query: ReadOutputQuery) -> Result<OutputRange, ProcessLogicError> {
        if query.length == 0 || query.length > self.config.max_projection_bytes {
            return Err(ProcessLogicError::InvalidOutputRange);
        }
        self.data
            .read_process_output(ReadProcessOutputDataRequest {
                authorization: map_authorization(query.authorization),
                process_id: map_id(query.process_id)?,
                stream: match query.stream {
                    OutputStream::Stdout => ProcessDataStream::Stdout,
                    OutputStream::Stderr => ProcessDataStream::Stderr,
                    OutputStream::Terminal => ProcessDataStream::Terminal,
                },
                offset: query.offset,
                length: query.length,
            })
            .await
            .map(map_output)
            .map_err(map_error)
    }

    async fn input(&self, command: InputProcessCommand) -> Result<(), ProcessLogicError> {
        self.data
            .input_process(ProcessInputDataRequest {
                authorization: map_authorization(command.authorization),
                process_id: map_id(command.process_id)?,
                bytes: command.bytes,
                close: command.close,
            })
            .await
            .map_err(map_error)
    }

    async fn resize(
        &self,
        command: ResizeTerminalCommand,
    ) -> Result<ProcessResult, ProcessLogicError> {
        validate_terminal_size(command.size)?;
        map_record(
            self.data
                .resize_process_terminal(ResizeProcessTerminalDataRequest {
                    authorization: map_authorization(command.authorization),
                    process_id: map_id(command.process_id)?,
                    size: map_terminal_size(command.size),
                })
                .await
                .map_err(map_error)?,
        )
    }

    async fn wait(
        &self,
        command: ProcessControlCommand,
    ) -> Result<ProcessResult, ProcessLogicError> {
        control_result(&self.data, command, ControlKind::Wait).await
    }

    async fn interrupt(&self, command: ProcessControlCommand) -> Result<(), ProcessLogicError> {
        self.data
            .interrupt_process(map_control(command)?)
            .await
            .map_err(map_error)
    }

    async fn kill(&self, command: ProcessControlCommand) -> Result<(), ProcessLogicError> {
        self.data
            .kill_process(map_control(command)?)
            .await
            .map_err(map_error)
    }

    async fn detach(
        &self,
        command: ProcessControlCommand,
    ) -> Result<ProcessResult, ProcessLogicError> {
        control_result(&self.data, command, ControlKind::Detach).await
    }

    async fn reattach(
        &self,
        command: ProcessControlCommand,
    ) -> Result<ProcessResult, ProcessLogicError> {
        control_result(&self.data, command, ControlKind::Reattach).await
    }

    async fn list(
        &self,
        authorization: ProcessAuthorization,
    ) -> Result<Vec<ProcessResult>, ProcessLogicError> {
        self.data
            .list_processes(map_authorization(authorization))
            .await
            .map_err(map_error)?
            .into_iter()
            .map(map_record)
            .collect()
    }

    async fn cancel(&self, command: CancelProcessCommand) -> Result<String, ProcessLogicError> {
        self.data
            .cancel_process(ProcessCancelDataRequest {
                identity: map_identity(command.identity),
                cancellation_id: command.cancellation_id,
            })
            .await
            .map_err(map_error)
    }
}

enum ControlKind {
    Wait,
    Detach,
    Reattach,
}

async fn control_result<D: ProcessDataPort>(
    data: &D,
    command: ProcessControlCommand,
    kind: ControlKind,
) -> Result<ProcessResult, ProcessLogicError> {
    let request = map_control(command)?;
    let record = match kind {
        ControlKind::Wait => data.wait_process(request).await,
        ControlKind::Detach => data.detach_process(request).await,
        ControlKind::Reattach => data.reattach_process(request).await,
    }
    .map_err(map_error)?;
    map_record(record)
}

fn validate_start(
    command: &StartProcessCommand,
    config: &ProcessLogicConfig,
) -> Result<(), ProcessLogicError> {
    if command.executable.trim().is_empty() || command.executable.contains('\0') {
        return Err(ProcessLogicError::InvalidExecutable);
    }
    if command.arguments.iter().any(|arg| arg.contains('\0')) {
        return Err(ProcessLogicError::InvalidArgument);
    }
    if command
        .timeout
        .is_some_and(|value| value.is_zero() || value > config.max_timeout)
    {
        return Err(ProcessLogicError::InvalidTimeout);
    }
    if command.output_limit_bytes == 0 || command.output_limit_bytes > config.max_output_bytes {
        return Err(ProcessLogicError::InvalidOutputLimit);
    }
    if let Some(size) = command.terminal_size {
        validate_terminal_size(size)?;
    }
    Ok(())
}

fn validate_terminal_size(size: TerminalSize) -> Result<(), ProcessLogicError> {
    if size.columns == 0 || size.rows == 0 {
        Err(ProcessLogicError::InvalidArgument)
    } else {
        Ok(())
    }
}

fn resolve_cwd(workspace: &Path, requested: Option<PathBuf>) -> Result<PathBuf, ProcessLogicError> {
    let Some(requested) = requested else {
        return Ok(workspace.to_path_buf());
    };
    if requested.is_absolute() {
        return requested
            .starts_with(workspace)
            .then_some(requested)
            .ok_or(ProcessLogicError::WorkingDirectoryEscape);
    }
    if requested.components().any(|part| {
        matches!(
            part,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(ProcessLogicError::WorkingDirectoryEscape);
    }
    Ok(workspace.join(requested))
}

fn filter_environment(
    environment: BTreeMap<String, String>,
    allowlist: &BTreeSet<String>,
    denylist: &BTreeSet<String>,
) -> Result<BTreeMap<String, String>, ProcessLogicError> {
    let denied: BTreeSet<_> = denylist.iter().map(|key| normalize_key(key)).collect();
    let allowed: BTreeSet<_> = allowlist.iter().map(|key| normalize_key(key)).collect();
    for (key, value) in &environment {
        let normalized = normalize_key(key);
        if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
            return Err(ProcessLogicError::InvalidEnvironment);
        }
        let secret_reference = value.strip_prefix("secret://").is_some_and(|reference| {
            !reference.is_empty()
                && reference
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '_')
        });
        if normalized == "PATH"
            || (denied.contains(&normalized) && !secret_reference)
            || (!allowed.is_empty() && !allowed.contains(&normalized))
        {
            return Err(ProcessLogicError::EnvironmentDenied);
        }
    }
    Ok(environment)
}

fn normalize_key(key: &str) -> String {
    if cfg!(windows) {
        key.to_ascii_uppercase()
    } else {
        key.to_owned()
    }
}

fn map_authorization(value: ProcessAuthorization) -> ProcessDataAuthorization {
    ProcessDataAuthorization {
        identity: map_identity(value.identity),
        call_id: value.call_id,
        tool: value.tool,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        cancellation_id: value.cancellation_id,
        canonical_operation: value.canonical_operation,
    }
}

fn map_identity(value: ProcessIdentity) -> ProcessDataIdentity {
    ProcessDataIdentity {
        owner_id: value.owner_id,
        session_id: value.session_id,
    }
}

fn map_control(
    value: ProcessControlCommand,
) -> Result<ProcessControlDataRequest, ProcessLogicError> {
    Ok(ProcessControlDataRequest {
        authorization: map_authorization(value.authorization),
        process_id: map_id(value.process_id)?,
    })
}

fn map_id(value: ProcessId) -> Result<ProcessDataId, ProcessLogicError> {
    ProcessDataId::parse(value.0).map_err(map_error)
}

fn map_record(value: ProcessDataRecord) -> Result<ProcessResult, ProcessLogicError> {
    Ok(ProcessResult {
        process_id: ProcessId::parse(value.process_id.as_str().to_owned())?,
        owner_id: value.owner_id,
        session_id: value.session_id,
        executable: value.executable,
        working_directory: value.working_directory,
        status: match value.state {
            ProcessDataState::Running => ProcessStatus::Running,
            ProcessDataState::Exited => ProcessStatus::Exited,
        },
        exit: value.exit.as_ref().map(map_exit),
        detached: value.detached,
        stdout: value.stdout_projection,
        stderr: value.stderr_projection,
        stdout_truncated: value.stdout_truncated,
        stderr_truncated: value.stderr_truncated,
        logs_removed: value.logs_removed,
        cleanup_failed: value.cleanup_failed,
        terminal: value.terminal,
        terminal_size: value.terminal_size.map(map_data_terminal_size),
        os_process_id: value.os_process_id,
        os_start_time: value.os_start_time,
        recovery_status: match value.recovery_state {
            ProcessDataRecoveryState::Live => ProcessRecoveryStatus::Live,
            ProcessDataRecoveryState::RecoveredRunningUnattached => {
                ProcessRecoveryStatus::RecoveredRunningUnattached
            }
            ProcessDataRecoveryState::RecoveredExited => ProcessRecoveryStatus::RecoveredExited,
            ProcessDataRecoveryState::DispatchUncertain => ProcessRecoveryStatus::DispatchUncertain,
        },
    })
}

fn map_terminal_size(value: TerminalSize) -> ProcessDataTerminalSize {
    ProcessDataTerminalSize {
        columns: value.columns,
        rows: value.rows,
        pixel_width: value.pixel_width,
        pixel_height: value.pixel_height,
    }
}

fn map_data_terminal_size(value: ProcessDataTerminalSize) -> TerminalSize {
    TerminalSize {
        columns: value.columns,
        rows: value.rows,
        pixel_width: value.pixel_width,
        pixel_height: value.pixel_height,
    }
}

fn map_exit(value: &ProcessDataExit) -> ExitStatus {
    ExitStatus {
        code: value.code,
        success: value.success,
        timed_out: value.timed_out,
    }
}

fn map_output(value: ProcessOutputDataRecord) -> OutputRange {
    OutputRange {
        bytes: value.bytes,
        next_offset: value.next_offset,
        retained_bytes: value.retained_bytes,
        truncated: value.truncated,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err owns the data error and logic reduces it to a stable business class"
)]
fn map_error(error: ProcessDataError) -> ProcessLogicError {
    match error {
        ProcessDataError::InvalidProcessId => ProcessLogicError::InvalidProcessId,
        ProcessDataError::Authorization => ProcessLogicError::Authorization,
        ProcessDataError::Ownership => ProcessLogicError::Ownership,
        _ => ProcessLogicError::Operation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denies_path_and_secret_overrides_before_data_mapping() {
        let allowlist = BTreeSet::new();
        let denylist = BTreeSet::from(["OPENAI_API_KEY".to_owned()]);
        assert_eq!(
            filter_environment(
                BTreeMap::from([("PATH".to_owned(), "tampered".to_owned())]),
                &allowlist,
                &denylist,
            ),
            Err(ProcessLogicError::EnvironmentDenied)
        );
        assert_eq!(
            filter_environment(
                BTreeMap::from([("OPENAI_API_KEY".to_owned(), "secret".to_owned())]),
                &allowlist,
                &denylist,
            ),
            Err(ProcessLogicError::EnvironmentDenied)
        );
        if cfg!(windows) {
            assert_eq!(
                filter_environment(
                    BTreeMap::from([("openai_api_key".to_owned(), "secret".to_owned())]),
                    &allowlist,
                    &denylist,
                ),
                Err(ProcessLogicError::EnvironmentDenied)
            );
        }
    }
}
