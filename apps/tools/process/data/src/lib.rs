//! Process datasets, authorization mapping, and dependency error redaction.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use agentmod_process_host_dependency::{
    DependencyAuthorization, DependencyCancelRequest, DependencyCleanupPolicy,
    DependencyExitStatus, DependencyIdentity, DependencyListRequest, DependencyOutputStream,
    DependencyProcessInputRequest, DependencyProcessRecord, DependencyProcessRequest,
    DependencyProcessState, DependencyReadOutputRequest, DependencyReadOutputResponse,
    DependencyRecoveryState, DependencyResizeTerminalRequest, DependencyStartProcessRequest,
    DependencyTerminalSize, ProcessDependencyError, ProcessDependencyPort,
};
use async_trait::async_trait;
use thiserror::Error;

/// Data-owned identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessDataIdentity {
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
}

/// Data-owned authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessDataAuthorization {
    /// Identity.
    pub identity: ProcessDataIdentity,
    /// Call ID.
    pub call_id: String,
    /// Tool.
    pub tool: String,
    /// Digest.
    pub normalized_digest: String,
    /// Opaque grant.
    pub grant: String,
    /// Cancellation ID.
    pub cancellation_id: String,
    /// Canonical operation.
    pub canonical_operation: Vec<u8>,
}

/// Data-owned process ID.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProcessDataId(String);

impl ProcessDataId {
    /// Parses nonempty ID.
    ///
    /// # Errors
    ///
    /// Returns an error for empty identifier text.
    pub fn parse(value: String) -> Result<Self, ProcessDataError> {
        if value.trim().is_empty() {
            Err(ProcessDataError::InvalidProcessId)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns ID.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessDataCleanup {
    /// Retain.
    Retain,
    /// Remove on success.
    RemoveLogsOnSuccess,
    /// Always remove.
    RemoveLogsAlways,
}

/// Data-owned terminal dimensions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessDataTerminalSize {
    /// Columns.
    pub columns: u16,
    /// Rows.
    pub rows: u16,
    /// Cell width in pixels.
    pub pixel_width: u16,
    /// Cell height in pixels.
    pub pixel_height: u16,
}

/// Start data request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartProcessDataRequest {
    /// Authorization.
    pub authorization: ProcessDataAuthorization,
    /// Workspace.
    pub workspace_root: PathBuf,
    /// Cwd.
    pub working_directory: PathBuf,
    /// Requested cwd before workspace resolution.
    pub requested_working_directory: Option<PathBuf>,
    /// Executable.
    pub executable: String,
    /// Args.
    pub arguments: Vec<String>,
    /// Environment.
    pub environment: BTreeMap<String, String>,
    /// Timeout.
    pub timeout: Option<Duration>,
    /// Output bound.
    pub output_limit_bytes: u64,
    /// Cleanup.
    pub cleanup: ProcessDataCleanup,
    /// Foreground.
    pub foreground: bool,
    /// Terminal dimensions when a PTY is requested.
    pub terminal_size: Option<ProcessDataTerminalSize>,
}

/// State.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessDataState {
    /// Running.
    Running,
    /// Exited.
    Exited,
}

/// Data-owned restart-reconciliation classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessDataRecoveryState {
    /// Live in this host.
    Live,
    /// Exact OS identity remains but inherited handles are unavailable.
    RecoveredRunningUnattached,
    /// Recovered as exited.
    RecoveredExited,
    /// Dispatch outcome was uncertain and was not repeated.
    DispatchUncertain,
}

/// Exit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessDataExit {
    /// Code.
    pub code: Option<i32>,
    /// Success.
    pub success: bool,
    /// Timed out.
    pub timed_out: bool,
}

/// Process record.
#[allow(
    clippy::struct_excessive_bools,
    reason = "explicit independent state flags"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessDataRecord {
    /// ID.
    pub process_id: ProcessDataId,
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Executable.
    pub executable: String,
    /// Cwd.
    pub working_directory: PathBuf,
    /// State.
    pub state: ProcessDataState,
    /// Exit.
    pub exit: Option<ProcessDataExit>,
    /// Detached.
    pub detached: bool,
    /// stdout projection.
    pub stdout_projection: Vec<u8>,
    /// stderr projection.
    pub stderr_projection: Vec<u8>,
    /// stdout truncated.
    pub stdout_truncated: bool,
    /// stderr truncated.
    pub stderr_truncated: bool,
    /// logs removed.
    pub logs_removed: bool,
    /// cleanup failed.
    pub cleanup_failed: bool,
    /// PTY marker.
    pub terminal: bool,
    /// Current terminal dimensions.
    pub terminal_size: Option<ProcessDataTerminalSize>,
    /// OS process ID.
    pub os_process_id: Option<u32>,
    /// OS start time.
    pub os_start_time: Option<u64>,
    /// Reconciliation classification.
    pub recovery_state: ProcessDataRecoveryState,
}

/// Stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessDataStream {
    /// stdout.
    Stdout,
    /// stderr.
    Stderr,
    /// Combined PTY stream.
    Terminal,
}

/// Control request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessControlDataRequest {
    /// Authorization.
    pub authorization: ProcessDataAuthorization,
    /// ID.
    pub process_id: ProcessDataId,
}

/// Input request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessInputDataRequest {
    /// Authorization.
    pub authorization: ProcessDataAuthorization,
    /// ID.
    pub process_id: ProcessDataId,
    /// Bytes.
    pub bytes: Vec<u8>,
    /// Close.
    pub close: bool,
}

/// Terminal resize request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResizeProcessTerminalDataRequest {
    /// Authorization.
    pub authorization: ProcessDataAuthorization,
    /// ID.
    pub process_id: ProcessDataId,
    /// Dimensions.
    pub size: ProcessDataTerminalSize,
}

/// Output request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadProcessOutputDataRequest {
    /// Authorization.
    pub authorization: ProcessDataAuthorization,
    /// ID.
    pub process_id: ProcessDataId,
    /// Stream.
    pub stream: ProcessDataStream,
    /// Offset.
    pub offset: u64,
    /// Length.
    pub length: u64,
}

/// Output result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessOutputDataRecord {
    /// Bytes.
    pub bytes: Vec<u8>,
    /// Next.
    pub next_offset: u64,
    /// Retained.
    pub retained_bytes: u64,
    /// Truncated.
    pub truncated: bool,
}

/// Cancel request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessCancelDataRequest {
    /// Identity.
    pub identity: ProcessDataIdentity,
    /// Token.
    pub cancellation_id: String,
}

/// Redacted data error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProcessDataError {
    /// Invalid ID.
    #[error("invalid process ID")]
    InvalidProcessId,
    /// Authorization denied.
    #[error("process authorization denied")]
    Authorization,
    /// Ownership denied.
    #[error("process ownership denied")]
    Ownership,
    /// Bounds.
    #[error("process resource bound exceeded")]
    Bounds,
    /// Lifecycle.
    #[error("process lifecycle operation failed")]
    Lifecycle,
    /// External operation.
    #[error("process external operation failed")]
    External,
}

/// Data interface.
#[async_trait]
pub trait ProcessDataPort: Send + Sync {
    /// Start.
    async fn start_process(
        &self,
        request: StartProcessDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError>;
    /// Input.
    async fn input_process(&self, request: ProcessInputDataRequest)
    -> Result<(), ProcessDataError>;
    /// Resize terminal.
    async fn resize_process_terminal(
        &self,
        request: ResizeProcessTerminalDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError>;
    /// Output.
    async fn read_process_output(
        &self,
        request: ReadProcessOutputDataRequest,
    ) -> Result<ProcessOutputDataRecord, ProcessDataError>;
    /// Wait.
    async fn wait_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError>;
    /// Interrupt.
    async fn interrupt_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<(), ProcessDataError>;
    /// Kill.
    async fn kill_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<(), ProcessDataError>;
    /// Detach.
    async fn detach_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError>;
    /// Reattach.
    async fn reattach_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError>;
    /// List.
    async fn list_processes(
        &self,
        authorization: ProcessDataAuthorization,
    ) -> Result<Vec<ProcessDataRecord>, ProcessDataError>;
    /// Cancel.
    async fn cancel_process(
        &self,
        request: ProcessCancelDataRequest,
    ) -> Result<String, ProcessDataError>;
}

/// Data implementation.
#[derive(Clone)]
pub struct ProcessData<D> {
    dependency: D,
}

impl<D> ProcessData<D> {
    /// Injects dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: ProcessDependencyPort> ProcessDataPort for ProcessData<D> {
    async fn start_process(
        &self,
        request: StartProcessDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError> {
        let response = self
            .dependency
            .start(DependencyStartProcessRequest {
                authorization: map_authorization(request.authorization),
                workspace_root: request.workspace_root,
                working_directory: request.working_directory,
                requested_working_directory: request.requested_working_directory,
                executable: request.executable,
                arguments: request.arguments,
                environment: request.environment,
                timeout: request.timeout,
                output_limit_bytes: request.output_limit_bytes,
                cleanup: map_cleanup(request.cleanup),
                foreground: request.foreground,
                terminal_size: request.terminal_size.map(map_terminal_size),
            })
            .await
            .map_err(map_error)?;
        map_record(response)
    }

    async fn input_process(
        &self,
        request: ProcessInputDataRequest,
    ) -> Result<(), ProcessDataError> {
        self.dependency
            .input(DependencyProcessInputRequest {
                authorization: map_authorization(request.authorization),
                process_id: request.process_id.0,
                bytes: request.bytes,
                close: request.close,
            })
            .await
            .map_err(map_error)
    }

    async fn resize_process_terminal(
        &self,
        request: ResizeProcessTerminalDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError> {
        map_record(
            self.dependency
                .resize(DependencyResizeTerminalRequest {
                    authorization: map_authorization(request.authorization),
                    process_id: request.process_id.0,
                    size: map_terminal_size(request.size),
                })
                .await
                .map_err(map_error)?,
        )
    }

    async fn read_process_output(
        &self,
        request: ReadProcessOutputDataRequest,
    ) -> Result<ProcessOutputDataRecord, ProcessDataError> {
        self.dependency
            .read_output(DependencyReadOutputRequest {
                authorization: map_authorization(request.authorization),
                process_id: request.process_id.0,
                stream: match request.stream {
                    ProcessDataStream::Stdout => DependencyOutputStream::Stdout,
                    ProcessDataStream::Stderr => DependencyOutputStream::Stderr,
                    ProcessDataStream::Terminal => DependencyOutputStream::Terminal,
                },
                offset: request.offset,
                length: request.length,
            })
            .await
            .map(map_output)
            .map_err(map_error)
    }

    async fn wait_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError> {
        map_record(
            self.dependency
                .wait(map_control(request))
                .await
                .map_err(map_error)?,
        )
    }

    async fn interrupt_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<(), ProcessDataError> {
        self.dependency
            .interrupt(map_control(request))
            .await
            .map_err(map_error)
    }

    async fn kill_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<(), ProcessDataError> {
        self.dependency
            .kill(map_control(request))
            .await
            .map_err(map_error)
    }

    async fn detach_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError> {
        map_record(
            self.dependency
                .detach(map_control(request))
                .await
                .map_err(map_error)?,
        )
    }

    async fn reattach_process(
        &self,
        request: ProcessControlDataRequest,
    ) -> Result<ProcessDataRecord, ProcessDataError> {
        map_record(
            self.dependency
                .reattach(map_control(request))
                .await
                .map_err(map_error)?,
        )
    }

    async fn list_processes(
        &self,
        authorization: ProcessDataAuthorization,
    ) -> Result<Vec<ProcessDataRecord>, ProcessDataError> {
        self.dependency
            .list(DependencyListRequest {
                authorization: map_authorization(authorization),
            })
            .await
            .map_err(map_error)?
            .into_iter()
            .map(map_record)
            .collect()
    }

    async fn cancel_process(
        &self,
        request: ProcessCancelDataRequest,
    ) -> Result<String, ProcessDataError> {
        self.dependency
            .cancel(DependencyCancelRequest {
                identity: map_identity(request.identity),
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(map_error)
    }
}

fn map_authorization(value: ProcessDataAuthorization) -> DependencyAuthorization {
    DependencyAuthorization {
        identity: map_identity(value.identity),
        call_id: value.call_id,
        tool: value.tool,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        cancellation_id: value.cancellation_id,
        canonical_operation: value.canonical_operation,
    }
}

fn map_identity(value: ProcessDataIdentity) -> DependencyIdentity {
    DependencyIdentity {
        owner_id: value.owner_id,
        session_id: value.session_id,
    }
}

fn map_control(value: ProcessControlDataRequest) -> DependencyProcessRequest {
    DependencyProcessRequest {
        authorization: map_authorization(value.authorization),
        process_id: value.process_id.0,
    }
}

fn map_cleanup(value: ProcessDataCleanup) -> DependencyCleanupPolicy {
    match value {
        ProcessDataCleanup::Retain => DependencyCleanupPolicy::Retain,
        ProcessDataCleanup::RemoveLogsOnSuccess => DependencyCleanupPolicy::RemoveLogsOnSuccess,
        ProcessDataCleanup::RemoveLogsAlways => DependencyCleanupPolicy::RemoveLogsAlways,
    }
}

fn map_terminal_size(value: ProcessDataTerminalSize) -> DependencyTerminalSize {
    DependencyTerminalSize {
        columns: value.columns,
        rows: value.rows,
        pixel_width: value.pixel_width,
        pixel_height: value.pixel_height,
    }
}

fn map_dependency_terminal_size(value: DependencyTerminalSize) -> ProcessDataTerminalSize {
    ProcessDataTerminalSize {
        columns: value.columns,
        rows: value.rows,
        pixel_width: value.pixel_width,
        pixel_height: value.pixel_height,
    }
}

fn map_record(value: DependencyProcessRecord) -> Result<ProcessDataRecord, ProcessDataError> {
    Ok(ProcessDataRecord {
        process_id: ProcessDataId::parse(value.process_id.as_str().to_owned())?,
        owner_id: value.owner_id,
        session_id: value.session_id,
        executable: value.executable,
        working_directory: value.working_directory,
        state: match value.state {
            DependencyProcessState::Running => ProcessDataState::Running,
            DependencyProcessState::Exited => ProcessDataState::Exited,
        },
        exit: value.exit.as_ref().map(map_exit),
        detached: value.detached,
        stdout_projection: value.stdout_projection,
        stderr_projection: value.stderr_projection,
        stdout_truncated: value.stdout_truncated,
        stderr_truncated: value.stderr_truncated,
        logs_removed: value.logs_removed,
        cleanup_failed: value.cleanup_failed,
        terminal: value.terminal,
        terminal_size: value.terminal_size.map(map_dependency_terminal_size),
        os_process_id: value.os_process_id,
        os_start_time: value.os_start_time,
        recovery_state: match value.recovery_state {
            DependencyRecoveryState::Live => ProcessDataRecoveryState::Live,
            DependencyRecoveryState::RecoveredRunningUnattached => {
                ProcessDataRecoveryState::RecoveredRunningUnattached
            }
            DependencyRecoveryState::RecoveredExited => ProcessDataRecoveryState::RecoveredExited,
            DependencyRecoveryState::DispatchUncertain => {
                ProcessDataRecoveryState::DispatchUncertain
            }
        },
    })
}

fn map_exit(value: &DependencyExitStatus) -> ProcessDataExit {
    ProcessDataExit {
        code: value.code,
        success: value.success,
        timed_out: value.timed_out,
    }
}

fn map_output(value: DependencyReadOutputResponse) -> ProcessOutputDataRecord {
    ProcessOutputDataRecord {
        bytes: value.bytes,
        next_offset: value.next_offset,
        retained_bytes: value.retained_bytes,
        truncated: value.truncated,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err owns the dependency error and this boundary reduces it to a stable class"
)]
fn map_error(error: ProcessDependencyError) -> ProcessDataError {
    match error {
        ProcessDependencyError::AuthorizationDenied
        | ProcessDependencyError::AuthorizationReplay => ProcessDataError::Authorization,
        ProcessDependencyError::OwnershipDenied => ProcessDataError::Ownership,
        ProcessDependencyError::InputTooLarge
        | ProcessDependencyError::InvalidOutputRange
        | ProcessDependencyError::InvalidOutputLimit
        | ProcessDependencyError::InvalidTerminalSize
        | ProcessDependencyError::LengthOverflow
        | ProcessDependencyError::ResourceLimit => ProcessDataError::Bounds,
        ProcessDependencyError::ProcessNotFound
        | ProcessDependencyError::ProcessExited
        | ProcessDependencyError::TerminalRequired
        | ProcessDependencyError::ReattachmentUnavailable
        | ProcessDependencyError::SupervisorStopped => ProcessDataError::Lifecycle,
        _ => ProcessDataError::External,
    }
}
