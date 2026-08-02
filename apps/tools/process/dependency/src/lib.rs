//! Authenticated operating-system process supervision and durable bounded logs.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationKey, ExpectedAuthorization, verify_authorization,
};
use async_trait::async_trait;
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};
use thiserror::Error;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, RwLock, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};
use uuid::Uuid;

#[cfg(windows)]
use tokio::time::sleep;

/// Prepares the exact reconnectable process-host endpoint before binding.
///
/// # Errors
///
/// Fails closed for relative paths, live listeners, symlinks, or non-socket
/// entries. Windows named pipes have no persistent entry to remove.
#[cfg(unix)]
pub fn prepare_local_endpoint(endpoint: &str) -> Result<(), ProcessDependencyError> {
    use std::os::unix::{fs::FileTypeExt, net::UnixStream};

    let path = Path::new(endpoint);
    if endpoint.is_empty() || !path.is_absolute() {
        return Err(ProcessDependencyError::InvalidConfiguration);
    }
    let Some(parent) = path.parent() else {
        return Err(ProcessDependencyError::InvalidConfiguration);
    };
    if !parent.is_dir() {
        return Err(ProcessDependencyError::InvalidConfiguration);
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(ProcessDependencyError::Io),
    };
    if !metadata.file_type().is_socket() {
        return Err(ProcessDependencyError::StorageEscape);
    }
    if UnixStream::connect(path).is_ok() {
        return Err(ProcessDependencyError::ResourceLimit);
    }
    std::fs::remove_file(path).map_err(redacted_io)
}

/// Validates the exact Windows named-pipe endpoint before binding.
///
/// # Errors
///
/// Returns [`ProcessDependencyError::InvalidConfiguration`] for malformed
/// local pipe names.
#[cfg(windows)]
pub fn prepare_local_endpoint(endpoint: &str) -> Result<(), ProcessDependencyError> {
    if !endpoint.starts_with(r"\\.\pipe\") || endpoint.len() <= r"\\.\pipe\".len() {
        return Err(ProcessDependencyError::InvalidConfiguration);
    }
    Ok(())
}

/// Removes the exact Unix socket after graceful host shutdown.
///
/// # Errors
///
/// Refuses to remove anything other than a socket at the configured path.
#[cfg(unix)]
pub fn cleanup_local_endpoint(endpoint: &str) -> Result<(), ProcessDependencyError> {
    use std::os::unix::fs::FileTypeExt;

    let path = Path::new(endpoint);
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(ProcessDependencyError::Io),
    };
    if !metadata.file_type().is_socket() {
        return Err(ProcessDependencyError::StorageEscape);
    }
    std::fs::remove_file(path).map_err(redacted_io)
}

/// Windows named pipes disappear when their final handle closes.
///
/// # Errors
///
/// This platform implementation has no fallible cleanup.
#[cfg(windows)]
#[allow(clippy::unnecessary_wraps)]
pub fn cleanup_local_endpoint(_endpoint: &str) -> Result<(), ProcessDependencyError> {
    Ok(())
}

/// Dependency-owned caller identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyIdentity {
    /// Local owner identity.
    pub owner_id: String,
    /// Session identity.
    pub session_id: String,
}

/// Dependency-owned authorization proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyAuthorization {
    /// Bound identity.
    pub identity: DependencyIdentity,
    /// Runtime call ID.
    pub call_id: String,
    /// Exact tool name.
    pub tool: String,
    /// Caller-provided normalized digest.
    pub normalized_digest: String,
    /// Keyed short-lived grant.
    pub grant: String,
    /// Opaque cancellation ID.
    pub cancellation_id: String,
    /// Deterministic canonical operation bytes.
    pub canonical_operation: Vec<u8>,
}

/// Stable process identifier.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DependencyProcessId(String);

impl DependencyProcessId {
    /// Returns portable identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parses a UUID process identifier.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed UUID text.
    pub fn parse(value: String) -> Result<Self, ProcessDependencyError> {
        Uuid::parse_str(&value)
            .map(|_| Self(value))
            .map_err(|_| ProcessDependencyError::InvalidProcessId)
    }
}

/// Durable-log cleanup policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyCleanupPolicy {
    /// Retain logs.
    Retain,
    /// Remove after success.
    RemoveLogsOnSuccess,
    /// Remove after every exit.
    RemoveLogsAlways,
}

impl DependencyCleanupPolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Retain => "retain",
            Self::RemoveLogsOnSuccess => "remove_logs_on_success",
            Self::RemoveLogsAlways => "remove_logs_always",
        }
    }
}

/// Executable policy decision enforced at the OS boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyExecutablePolicy {
    /// Execute without another approval.
    Allow,
    /// Require approval unavailable at this dependency endpoint.
    Ask,
    /// Deny execution.
    Deny,
}

/// Dependency-owned terminal dimensions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyTerminalSize {
    /// Text columns.
    pub columns: u16,
    /// Text rows.
    pub rows: u16,
    /// Cell width in pixels, or zero when unknown.
    pub pixel_width: u16,
    /// Cell height in pixels, or zero when unknown.
    pub pixel_height: u16,
}

impl DependencyTerminalSize {
    const fn portable(self) -> PtySize {
        PtySize {
            rows: self.rows,
            cols: self.columns,
            pixel_width: self.pixel_width,
            pixel_height: self.pixel_height,
        }
    }
}

/// Authenticated process start request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStartProcessRequest {
    /// Authorization proof.
    pub authorization: DependencyAuthorization,
    /// Approved workspace root.
    pub workspace_root: PathBuf,
    /// Approved working directory.
    pub working_directory: PathBuf,
    /// Provider-visible working-directory selection before workspace resolution.
    pub requested_working_directory: Option<PathBuf>,
    /// Executable passed directly to the OS.
    pub executable: String,
    /// Exact argument vector.
    pub arguments: Vec<String>,
    /// Filtered environment overrides.
    pub environment: BTreeMap<String, String>,
    /// Optional hard runtime limit.
    pub timeout: Option<Duration>,
    /// Per-stream retained-byte limit.
    pub output_limit_bytes: u64,
    /// Cleanup policy.
    pub cleanup: DependencyCleanupPolicy,
    /// Whether start waits and projects output before cleanup.
    pub foreground: bool,
    /// Allocates a terminal when dimensions are present.
    pub terminal_size: Option<DependencyTerminalSize>,
}

/// Process lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyProcessState {
    /// Running.
    Running,
    /// Exited and drained.
    Exited,
}

/// Restart-reconciliation classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyRecoveryState {
    /// Supervised by this process host instance.
    Live,
    /// The exact PID/start-time/executable identity still exists, but its
    /// inherited handles cannot be reconstructed safely.
    RecoveredRunningUnattached,
    /// A durable running record no longer matches a live OS process.
    RecoveredExited,
    /// The host crashed between durable intent and a confirmed dispatch
    /// receipt; execution is never repeated automatically.
    DispatchUncertain,
}

/// Exit status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyExitStatus {
    /// OS exit code.
    pub code: Option<i32>,
    /// Success classification.
    pub success: bool,
    /// Timeout initiated termination.
    pub timed_out: bool,
}

/// Process record scoped to its owner and session.
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent protocol flags are intentionally explicit"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProcessRecord {
    /// Stable process ID.
    pub process_id: DependencyProcessId,
    /// Owner.
    pub owner_id: String,
    /// Session.
    pub session_id: String,
    /// Redacted executable label.
    pub executable: String,
    /// Canonical cwd.
    pub working_directory: PathBuf,
    /// State.
    pub state: DependencyProcessState,
    /// Exit.
    pub exit: Option<DependencyExitStatus>,
    /// Detached marker.
    pub detached: bool,
    /// Captured stdout projection available before cleanup.
    pub stdout_projection: Vec<u8>,
    /// Captured stderr projection available before cleanup.
    pub stderr_projection: Vec<u8>,
    /// Stdout truncated.
    pub stdout_truncated: bool,
    /// Stderr truncated.
    pub stderr_truncated: bool,
    /// Logs removed.
    pub logs_removed: bool,
    /// Cleanup failed after an otherwise completed operation.
    pub cleanup_failed: bool,
    /// Whether the child owns a pseudo-terminal.
    pub terminal: bool,
    /// Current terminal dimensions.
    pub terminal_size: Option<DependencyTerminalSize>,
    /// Operating-system process identifier when available.
    pub os_process_id: Option<u32>,
    /// OS-reported process start time, used with PID and executable to prevent
    /// PID-reuse confusion.
    pub os_start_time: Option<u64>,
    /// Recovery classification.
    pub recovery_state: DependencyRecoveryState,
}

/// Authenticated process control.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProcessRequest {
    /// Authorization proof.
    pub authorization: DependencyAuthorization,
    /// Process ID.
    pub process_id: String,
}

/// Authenticated input request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProcessInputRequest {
    /// Authorization proof.
    pub authorization: DependencyAuthorization,
    /// Process ID.
    pub process_id: String,
    /// Exact bytes.
    pub bytes: Vec<u8>,
    /// Close stdin.
    pub close: bool,
}

/// Output stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyOutputStream {
    /// stdout.
    Stdout,
    /// stderr.
    Stderr,
    /// Combined pseudo-terminal output.
    Terminal,
}

/// Authenticated terminal resize.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyResizeTerminalRequest {
    /// Authorization proof.
    pub authorization: DependencyAuthorization,
    /// Process ID.
    pub process_id: String,
    /// New dimensions.
    pub size: DependencyTerminalSize,
}

/// Authenticated bounded output read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyReadOutputRequest {
    /// Authorization proof.
    pub authorization: DependencyAuthorization,
    /// Process ID.
    pub process_id: String,
    /// Stream.
    pub stream: DependencyOutputStream,
    /// Offset.
    pub offset: u64,
    /// Length.
    pub length: u64,
}

/// Output range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyReadOutputResponse {
    /// Bytes.
    pub bytes: Vec<u8>,
    /// Next offset.
    pub next_offset: u64,
    /// Retained size.
    pub retained_bytes: u64,
    /// Truncation marker.
    pub truncated: bool,
}

/// Authenticated list request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyListRequest {
    /// Authorization proof.
    pub authorization: DependencyAuthorization,
}

/// Identity-scoped cancellation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCancelRequest {
    /// Configured identity.
    pub identity: DependencyIdentity,
    /// Opaque cancellation ID.
    pub cancellation_id: String,
}

/// Hard dependency bounds and security configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessDependencyConfig {
    /// Trusted storage root.
    pub storage_root: PathBuf,
    /// Log root under storage root.
    pub log_root: PathBuf,
    /// Secret reference value loaded as 64 hexadecimal characters.
    pub authorization_key_hex: String,
    /// Bootstrap owner identity; request-supplied identity must match.
    pub owner_id: String,
    /// Bootstrap session identity; request-supplied identity must match.
    pub session_id: String,
    /// Explicit inherited environment allowlist.
    pub inherited_environment_allowlist: BTreeSet<String>,
    /// Max input bytes.
    pub max_input_bytes: usize,
    /// Max range bytes.
    pub max_range_bytes: u64,
    /// Max arguments.
    pub max_arguments: usize,
    /// Max combined argv bytes.
    pub max_argument_bytes: usize,
    /// Max environment entries.
    pub max_environment_entries: usize,
    /// Max combined environment bytes.
    pub max_environment_bytes: usize,
    /// Max concurrently running processes.
    pub max_active_processes: usize,
    /// Max retained bytes across registered logs.
    pub max_total_retained_bytes: u64,
    /// Output-drain deadline.
    pub drain_timeout: Duration,
    /// Maximum time spent sending one stdin frame.
    pub input_write_timeout: Duration,
    /// Maximum retained authorization nonces after expiry pruning.
    pub max_replay_entries: usize,
    /// Maximum completed entries retained in memory.
    pub max_completed_entries: usize,
    /// Maximum simultaneous waiters for one process.
    pub max_waiters_per_process: usize,
    /// Exact executable-name policy, normalized per platform.
    pub executable_policy: BTreeMap<String, DependencyExecutablePolicy>,
    /// Fallback executable decision.
    pub default_executable_policy: DependencyExecutablePolicy,
}

/// Dependency interface.
#[async_trait]
pub trait ProcessDependencyPort: Send + Sync {
    /// Starts a child after grant verification.
    async fn start(
        &self,
        request: DependencyStartProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError>;
    /// Writes input.
    async fn input(
        &self,
        request: DependencyProcessInputRequest,
    ) -> Result<(), ProcessDependencyError>;
    /// Resizes a pseudo-terminal.
    async fn resize(
        &self,
        request: DependencyResizeTerminalRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError>;
    /// Reads output.
    async fn read_output(
        &self,
        request: DependencyReadOutputRequest,
    ) -> Result<DependencyReadOutputResponse, ProcessDependencyError>;
    /// Waits and captures output before cleanup.
    async fn wait(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError>;
    /// Requests graceful/tree termination where supported.
    async fn interrupt(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<(), ProcessDependencyError>;
    /// Forces tree termination where supported.
    async fn kill(&self, request: DependencyProcessRequest) -> Result<(), ProcessDependencyError>;
    /// Detaches.
    async fn detach(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError>;
    /// Reattaches.
    async fn reattach(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError>;
    /// Lists only identity-owned records.
    async fn list(
        &self,
        request: DependencyListRequest,
    ) -> Result<Vec<DependencyProcessRecord>, ProcessDependencyError>;
    /// Counts identity-owned children whose handles remain live in this host.
    async fn active_count(
        &self,
        identity: DependencyIdentity,
    ) -> Result<usize, ProcessDependencyError>;
    /// Cancels by opaque token and identity.
    async fn cancel(
        &self,
        request: DependencyCancelRequest,
    ) -> Result<String, ProcessDependencyError>;
}

#[derive(Clone)]
struct RegistryEntry {
    process_id: DependencyProcessId,
    identity: DependencyIdentity,
    cancellation_id: String,
    executable: String,
    working_directory: PathBuf,
    log_directory: PathBuf,
    output_limit_bytes: u64,
    cleanup: DependencyCleanupPolicy,
    snapshot: Arc<RwLock<ProcessSnapshot>>,
    control: Option<mpsc::Sender<Control>>,
    stdout_truncated: Arc<AtomicBool>,
    stderr_truncated: Arc<AtomicBool>,
    logs_removed: Arc<AtomicBool>,
    cleanup_failed: Arc<AtomicBool>,
    completion: Arc<Mutex<()>>,
    terminal: bool,
    os_process_id: Option<u32>,
    os_start_time: Option<u64>,
    recovery_state: Arc<RwLock<DependencyRecoveryState>>,
    durable: Arc<StdMutex<DurableProcessRecord>>,
}

#[derive(Clone, Debug)]
struct ProcessSnapshot {
    state: DependencyProcessState,
    exit: Option<DependencyExitStatus>,
    detached: bool,
    capture_error: Option<String>,
    terminal_size: Option<DependencyTerminalSize>,
}

enum Control {
    Input {
        bytes: Vec<u8>,
        close: bool,
        response: oneshot::Sender<Result<(), String>>,
    },
    Resize {
        size: DependencyTerminalSize,
        response: oneshot::Sender<Result<(), String>>,
    },
    Interrupt(oneshot::Sender<Result<(), String>>),
    Kill(oneshot::Sender<Result<(), String>>),
    Wait(oneshot::Sender<DependencyExitStatus>),
}

/// Tokio process dependency.
#[derive(Clone)]
pub struct TokioProcessDependency {
    config: ProcessDependencyConfig,
    authorization_key: Arc<AuthorizationKey>,
    registry: Arc<Mutex<BTreeMap<String, RegistryEntry>>>,
    replay: Arc<Mutex<ReplayState>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ReplaySnapshot {
    generation: u64,
    nonces: BTreeMap<String, i64>,
}

#[derive(Debug)]
struct ReplayState {
    directory: PathBuf,
    snapshot: ReplaySnapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DurableLifecycle {
    Dispatching,
    Running,
    Exited,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DurableExitStatus {
    code: Option<i32>,
    success: bool,
    timed_out: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct DurableTerminalSize {
    columns: u16,
    rows: u16,
    pixel_width: u16,
    pixel_height: u16,
}

#[allow(
    clippy::struct_excessive_bools,
    reason = "independent durable lifecycle and output-integrity flags are explicit"
)]
#[derive(Clone, Debug, Deserialize, Serialize)]
struct DurableProcessRecord {
    schema_version: u32,
    generation: u64,
    process_id: String,
    owner_id: String,
    session_id: String,
    executable: String,
    resolved_executable: PathBuf,
    working_directory: PathBuf,
    lifecycle: DurableLifecycle,
    exit: Option<DurableExitStatus>,
    detached: bool,
    terminal: bool,
    terminal_size: Option<DurableTerminalSize>,
    os_process_id: Option<u32>,
    os_start_time: Option<u64>,
    output_limit_bytes: u64,
    cleanup: String,
    stdout_truncated: bool,
    stderr_truncated: bool,
    logs_removed: bool,
    cleanup_failed: bool,
}

impl TokioProcessDependency {
    /// Constructs a secure dependency.
    ///
    /// # Errors
    ///
    /// Returns an error when security configuration or resource bounds are invalid.
    pub fn new(mut config: ProcessDependencyConfig) -> Result<Self, ProcessDependencyError> {
        if config.storage_root.as_os_str().is_empty()
            || config.log_root.as_os_str().is_empty()
            || config.authorization_key_hex.is_empty()
            || config.owner_id.is_empty()
            || config.session_id.is_empty()
            || config.max_input_bytes == 0
            || config.max_range_bytes == 0
            || config.max_arguments == 0
            || config.max_argument_bytes == 0
            || config.max_environment_entries == 0
            || config.max_environment_bytes == 0
            || config.max_active_processes == 0
            || config.max_total_retained_bytes == 0
            || config.drain_timeout.is_zero()
            || config.input_write_timeout.is_zero()
            || config.max_replay_entries == 0
            || config.max_completed_entries == 0
            || config.max_waiters_per_process == 0
        {
            return Err(ProcessDependencyError::InvalidConfiguration);
        }
        let authorization_key = AuthorizationKey::from_hex(&config.authorization_key_hex)
            .map_err(|_| ProcessDependencyError::InvalidConfiguration)?;
        config.authorization_key_hex.clear();
        let mut executable_policy = BTreeMap::new();
        for (name, decision) in std::mem::take(&mut config.executable_policy) {
            let normalized = normalize_executable_policy_key(name.trim());
            if normalized.is_empty()
                || executable_policy
                    .insert(normalized, decision)
                    .is_some_and(|previous| previous != decision)
            {
                return Err(ProcessDependencyError::InvalidConfiguration);
            }
        }
        config.executable_policy = executable_policy;
        let replay = load_replay_state(&config.storage_root)?;
        let registry = load_process_registry(&config)?;
        Ok(Self {
            config,
            authorization_key: Arc::new(authorization_key),
            registry: Arc::new(Mutex::new(registry)),
            replay: Arc::new(Mutex::new(replay)),
        })
    }

    async fn authorize(
        &self,
        authorization: &DependencyAuthorization,
        expected_tool: &str,
        canonical_operation: &[u8],
    ) -> Result<(), ProcessDependencyError> {
        validate_authorization_shape(authorization)?;
        if authorization.identity.owner_id != self.config.owner_id
            || authorization.identity.session_id != self.config.session_id
            || authorization.tool != expected_tool
        {
            return Err(ProcessDependencyError::AuthorizationDenied);
        }
        let digest = ContentHash::digest(canonical_operation);
        if !constant_time_eq(
            digest.to_hex().as_bytes(),
            authorization.normalized_digest.as_bytes(),
        ) {
            return Err(ProcessDependencyError::AuthorizationDenied);
        }
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ProcessDependencyError::AuthorizationDenied)?
            .as_millis();
        let now_millis =
            i64::try_from(now_millis).map_err(|_| ProcessDependencyError::AuthorizationDenied)?;
        let claims = verify_authorization(
            &authorization.grant,
            &self.authorization_key,
            ExpectedAuthorization {
                owner: &authorization.identity.owner_id,
                session: &authorization.identity.session_id,
                call_id: &authorization.call_id,
                action: &authorization.tool,
                normalized_digest: digest,
            },
            TimestampMillis::new(now_millis),
        )
        .map_err(|_| ProcessDependencyError::AuthorizationDenied)?;
        let nonce_key = format!("{}:{}:{}", claims.owner, claims.session, claims.nonce);
        let mut replay = self.replay.lock().await;
        let mut next = replay.snapshot.clone();
        next.nonces.retain(|_, expiry| *expiry >= now_millis);
        if next.nonces.contains_key(&nonce_key) {
            return Err(ProcessDependencyError::AuthorizationReplay);
        }
        if next.nonces.len() >= self.config.max_replay_entries {
            return Err(ProcessDependencyError::ResourceLimit);
        }
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(ProcessDependencyError::ResourceLimit)?;
        next.nonces.insert(nonce_key, claims.expires_at.get());
        persist_replay_snapshot(&replay.directory, &next)?;
        replay.snapshot = next;
        Ok(())
    }

    async fn entry(
        &self,
        raw_id: String,
        identity: &DependencyIdentity,
    ) -> Result<RegistryEntry, ProcessDependencyError> {
        let process_id = DependencyProcessId::parse(raw_id)?;
        let entry = self
            .registry
            .lock()
            .await
            .get(process_id.as_str())
            .cloned()
            .ok_or(ProcessDependencyError::ProcessNotFound)?;
        ensure_owner(&entry, identity)?;
        refresh_recovered_entry(&entry).await?;
        Ok(entry)
    }

    async fn authorized_entry(
        &self,
        request: &DependencyProcessRequest,
        expected_tool: &str,
    ) -> Result<RegistryEntry, ProcessDependencyError> {
        let canonical = canonical_control_operation(
            expected_tool,
            &request.authorization.cancellation_id,
            &request.process_id,
        )?;
        self.authorize(&request.authorization, expected_tool, &canonical)
            .await?;
        self.entry(request.process_id.clone(), &request.authorization.identity)
            .await
    }

    async fn complete_record(
        &self,
        entry: &RegistryEntry,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError> {
        let _completion = entry.completion.lock().await;
        wait_entry(entry).await?;
        let capture_error = entry.snapshot.read().await.capture_error.clone();
        if capture_error.is_some() {
            return Err(ProcessDependencyError::CaptureFailed);
        }
        let stdout_projection = read_entire_bounded(
            &entry.log_directory.join("stdout.log"),
            entry.output_limit_bytes,
        )
        .await?;
        let stderr_projection = read_entire_bounded(
            &entry.log_directory.join("stderr.log"),
            entry.output_limit_bytes,
        )
        .await?;
        let mut record = record_from_entry(entry).await;
        record.stdout_projection = stdout_projection;
        record.stderr_projection = stderr_projection;
        let exit_success = record.exit.as_ref().is_some_and(|exit| exit.success);
        let remove = matches!(entry.cleanup, DependencyCleanupPolicy::RemoveLogsAlways)
            || (matches!(entry.cleanup, DependencyCleanupPolicy::RemoveLogsOnSuccess)
                && exit_success);
        if remove && !entry.logs_removed.load(Ordering::Acquire) {
            if fs::remove_dir_all(&entry.log_directory).await.is_ok() {
                entry.logs_removed.store(true, Ordering::Release);
                record.logs_removed = true;
            } else {
                entry.cleanup_failed.store(true, Ordering::Release);
                record.cleanup_failed = true;
            }
        }
        if !record.logs_removed {
            update_durable_flags(
                &entry.durable,
                &entry.log_directory,
                entry.stdout_truncated.load(Ordering::Acquire),
                entry.stderr_truncated.load(Ordering::Acquire),
                record.cleanup_failed,
            )?;
        }
        self.prune_completed_registry().await;
        Ok(record)
    }

    async fn prune_completed_registry(&self) {
        let mut registry = self.registry.lock().await;
        let completed: Vec<_> = registry
            .iter()
            .filter_map(|(id, entry)| {
                entry
                    .snapshot
                    .try_read()
                    .ok()
                    .and_then(|snapshot| snapshot.exit.is_some().then(|| id.clone()))
            })
            .collect();
        let excess = completed
            .len()
            .saturating_sub(self.config.max_completed_entries);
        for id in completed.into_iter().take(excess) {
            registry.remove(&id);
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the trait implementation keeps each dependency endpoint mapping explicit"
)]
#[async_trait]
impl ProcessDependencyPort for TokioProcessDependency {
    async fn start(
        &self,
        request: DependencyStartProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError> {
        validate_start_request(&request, &self.config)?;
        let expected_tool = match (request.foreground, request.terminal_size.is_some()) {
            (true, false) => "process.run",
            (false, false) => "process.start",
            (true, true) => "process.run_pty",
            (false, true) => "process.start_pty",
        };
        let canonical = canonical_start_operation(&request)?;
        self.authorize(&request.authorization, expected_tool, &canonical)
            .await?;
        let (workspace_root, working_directory, log_root) =
            secure_roots(&request, &self.config).await?;
        if !working_directory.starts_with(&workspace_root) {
            return Err(ProcessDependencyError::WorkingDirectoryEscape);
        }
        let executable = resolve_executable(
            &request.executable,
            &self.config.inherited_environment_allowlist,
        )
        .await?;
        enforce_executable_policy(&request.executable, &executable.identity_path, &self.config)?;
        let environment = resolve_environment(&request.environment)?;
        self.prune_completed_registry().await;
        enforce_capacity(&self.registry, request.output_limit_bytes, &self.config).await?;

        let process_id = DependencyProcessId(Uuid::now_v7().to_string());
        let log_directory = log_root.join(process_id.as_str());
        fs::create_dir(&log_directory).await.map_err(redacted_io)?;
        let stdout_file = match File::create(log_directory.join("stdout.log")).await {
            Ok(file) => file,
            Err(error) => {
                let _ = fs::remove_dir_all(&log_directory).await;
                return Err(redacted_io(error));
            }
        };
        let stderr_file = match File::create(log_directory.join("stderr.log")).await {
            Ok(file) => file,
            Err(error) => {
                let _ = fs::remove_dir_all(&log_directory).await;
                return Err(redacted_io(error));
            }
        };
        let durable = Arc::new(StdMutex::new(DurableProcessRecord {
            schema_version: 1,
            generation: 0,
            process_id: process_id.as_str().to_owned(),
            owner_id: request.authorization.identity.owner_id.clone(),
            session_id: request.authorization.identity.session_id.clone(),
            executable: request.executable.clone(),
            resolved_executable: executable.identity_path.clone(),
            working_directory: working_directory.clone(),
            lifecycle: DurableLifecycle::Dispatching,
            exit: None,
            detached: false,
            terminal: request.terminal_size.is_some(),
            terminal_size: request.terminal_size.map(Into::into),
            os_process_id: None,
            os_start_time: None,
            output_limit_bytes: request.output_limit_bytes,
            cleanup: request.cleanup.as_str().to_owned(),
            stdout_truncated: false,
            stderr_truncated: false,
            logs_removed: false,
            cleanup_failed: false,
        }));
        {
            let mut record = durable
                .lock()
                .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
            persist_durable_record(&log_directory, &mut record)?;
        }
        let stdout_truncated = Arc::new(AtomicBool::new(false));
        let stderr_truncated = Arc::new(AtomicBool::new(false));
        let snapshot = Arc::new(RwLock::new(ProcessSnapshot {
            state: DependencyProcessState::Running,
            exit: None,
            detached: false,
            capture_error: None,
            terminal_size: request.terminal_size,
        }));
        let logs_removed = Arc::new(AtomicBool::new(false));
        let cleanup_failed = Arc::new(AtomicBool::new(false));
        let (control, receiver) = mpsc::channel(32);
        let (os_process_id, os_start_time) = if let Some(terminal_size) = request.terminal_size {
            let stdout_file = stdout_file.into_std().await;
            drop(stderr_file);
            match spawn_terminal_process(
                &executable.invocation_path,
                &executable.identity_path,
                &request.arguments,
                &working_directory,
                &environment,
                &self.config.inherited_environment_allowlist,
                terminal_size,
                stdout_file,
                request.output_limit_bytes,
                Arc::clone(&stdout_truncated),
                receiver,
                Arc::clone(&snapshot),
                request.timeout,
                self.config.drain_timeout,
                self.config.input_write_timeout,
                self.config.max_waiters_per_process,
                Arc::clone(&durable),
                log_directory.clone(),
            ) {
                Ok(identity) => identity,
                Err(error) => {
                    let _ = fs::remove_dir_all(&log_directory).await;
                    return Err(error);
                }
            }
        } else {
            let mut command = command_for_resolved_executable(&executable);
            command
                .args(&request.arguments)
                .current_dir(&working_directory)
                .env_clear()
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            #[cfg(unix)]
            command.process_group(0);
            inherit_safe_environment(&mut command, &self.config.inherited_environment_allowlist);
            for (key, value) in &environment {
                command.env(key, value);
            }
            let mut child = match command.spawn() {
                Ok(child) => child,
                Err(error) => {
                    let _ = fs::remove_dir_all(&log_directory).await;
                    return Err(redacted_io(error));
                }
            };
            let os_process_id = child.id();
            let os_start_time = mark_durable_running(
                &durable,
                &log_directory,
                os_process_id,
                &executable.identity_path,
            )?;
            let stdout = child
                .stdout
                .take()
                .ok_or(ProcessDependencyError::PipeUnavailable)?;
            let stderr = child
                .stderr
                .take()
                .ok_or(ProcessDependencyError::PipeUnavailable)?;
            let stdin = child
                .stdin
                .take()
                .ok_or(ProcessDependencyError::PipeUnavailable)?;
            let stdout_task = tokio::spawn(capture_stream(
                stdout,
                stdout_file,
                request.output_limit_bytes,
                Arc::clone(&stdout_truncated),
            ));
            let stderr_task = tokio::spawn(capture_stream(
                stderr,
                stderr_file,
                request.output_limit_bytes,
                Arc::clone(&stderr_truncated),
            ));
            tokio::spawn(supervise(
                child,
                Some(stdin),
                receiver,
                Arc::clone(&snapshot),
                request.timeout,
                stdout_task,
                stderr_task,
                self.config.drain_timeout,
                self.config.input_write_timeout,
                self.config.max_waiters_per_process,
                Arc::clone(&durable),
                log_directory.clone(),
                Arc::clone(&stdout_truncated),
                Arc::clone(&stderr_truncated),
            ));
            (os_process_id, os_start_time)
        };
        let entry = RegistryEntry {
            process_id: process_id.clone(),
            identity: request.authorization.identity,
            cancellation_id: request.authorization.cancellation_id,
            executable: request.executable,
            working_directory,
            log_directory,
            output_limit_bytes: request.output_limit_bytes,
            cleanup: request.cleanup,
            snapshot,
            control: Some(control),
            stdout_truncated,
            stderr_truncated,
            logs_removed,
            cleanup_failed,
            completion: Arc::new(Mutex::new(())),
            terminal: request.terminal_size.is_some(),
            os_process_id,
            os_start_time,
            recovery_state: Arc::new(RwLock::new(DependencyRecoveryState::Live)),
            durable,
        };
        self.registry
            .lock()
            .await
            .insert(process_id.0.clone(), entry.clone());
        if request.foreground {
            self.complete_record(&entry).await
        } else {
            Ok(record_from_entry(&entry).await)
        }
    }

    async fn input(
        &self,
        request: DependencyProcessInputRequest,
    ) -> Result<(), ProcessDependencyError> {
        if request.bytes.len() > self.config.max_input_bytes {
            return Err(ProcessDependencyError::InputTooLarge);
        }
        let canonical = canonical_input_operation(&request)?;
        self.authorize(&request.authorization, "process.input", &canonical)
            .await?;
        let entry = self
            .entry(request.process_id, &request.authorization.identity)
            .await?;
        send_control(&entry, |response| Control::Input {
            bytes: request.bytes,
            close: request.close,
            response,
        })
        .await
    }

    async fn resize(
        &self,
        request: DependencyResizeTerminalRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError> {
        validate_terminal_size(request.size)?;
        let canonical = canonical_resize_operation(&request)?;
        self.authorize(&request.authorization, "process.resize", &canonical)
            .await?;
        let entry = self
            .entry(request.process_id, &request.authorization.identity)
            .await?;
        if !entry.terminal {
            return Err(ProcessDependencyError::TerminalRequired);
        }
        send_control(&entry, |response| Control::Resize {
            size: request.size,
            response,
        })
        .await?;
        update_durable_terminal_size(&entry.durable, &entry.log_directory, request.size)?;
        Ok(record_from_entry(&entry).await)
    }

    async fn read_output(
        &self,
        request: DependencyReadOutputRequest,
    ) -> Result<DependencyReadOutputResponse, ProcessDependencyError> {
        if request.length == 0 || request.length > self.config.max_range_bytes {
            return Err(ProcessDependencyError::InvalidOutputRange);
        }
        let canonical = canonical_read_operation(&request)?;
        self.authorize(&request.authorization, "process.read", &canonical)
            .await?;
        let entry = self
            .entry(request.process_id, &request.authorization.identity)
            .await?;
        read_output(&entry, request.stream, request.offset, request.length).await
    }

    async fn wait(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError> {
        let entry = self.authorized_entry(&request, "process.wait").await?;
        self.complete_record(&entry).await
    }

    async fn interrupt(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<(), ProcessDependencyError> {
        let entry = self.authorized_entry(&request, "process.interrupt").await?;
        send_control(&entry, Control::Interrupt).await
    }

    async fn kill(&self, request: DependencyProcessRequest) -> Result<(), ProcessDependencyError> {
        let entry = self.authorized_entry(&request, "process.kill").await?;
        send_control(&entry, Control::Kill).await
    }

    async fn detach(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError> {
        let entry = self.authorized_entry(&request, "process.detach").await?;
        entry.snapshot.write().await.detached = true;
        update_durable_attachment(&entry.durable, &entry.log_directory, true)?;
        Ok(record_from_entry(&entry).await)
    }

    async fn reattach(
        &self,
        request: DependencyProcessRequest,
    ) -> Result<DependencyProcessRecord, ProcessDependencyError> {
        let entry = self.authorized_entry(&request, "process.reattach").await?;
        if *entry.recovery_state.read().await != DependencyRecoveryState::Live {
            return Err(ProcessDependencyError::ReattachmentUnavailable);
        }
        entry.snapshot.write().await.detached = false;
        update_durable_attachment(&entry.durable, &entry.log_directory, false)?;
        Ok(record_from_entry(&entry).await)
    }

    async fn list(
        &self,
        request: DependencyListRequest,
    ) -> Result<Vec<DependencyProcessRecord>, ProcessDependencyError> {
        let canonical = canonical_list_operation(&request.authorization.cancellation_id)?;
        self.authorize(&request.authorization, "process.list", &canonical)
            .await?;
        let entries: Vec<_> = self
            .registry
            .lock()
            .await
            .values()
            .filter(|entry| entry.identity == request.authorization.identity)
            .cloned()
            .collect();
        let mut records = Vec::with_capacity(entries.len());
        for entry in entries {
            refresh_recovered_entry(&entry).await?;
            records.push(record_from_entry(&entry).await);
        }
        Ok(records)
    }

    async fn active_count(
        &self,
        identity: DependencyIdentity,
    ) -> Result<usize, ProcessDependencyError> {
        let entries: Vec<_> = self
            .registry
            .lock()
            .await
            .values()
            .filter(|entry| entry.identity == identity)
            .cloned()
            .collect();
        let mut count = 0_usize;
        for entry in entries {
            refresh_recovered_entry(&entry).await?;
            if entry.snapshot.read().await.state == DependencyProcessState::Running
                && *entry.recovery_state.read().await == DependencyRecoveryState::Live
            {
                count = count
                    .checked_add(1)
                    .ok_or(ProcessDependencyError::ResourceLimit)?;
            }
        }
        Ok(count)
    }

    async fn cancel(
        &self,
        request: DependencyCancelRequest,
    ) -> Result<String, ProcessDependencyError> {
        let entries: Vec<_> = self.registry.lock().await.values().cloned().collect();
        let entry = entries
            .into_iter()
            .find(|entry| {
                entry.identity == request.identity
                    && entry.cancellation_id == request.cancellation_id
            })
            .ok_or(ProcessDependencyError::ProcessNotFound)?;
        send_control(&entry, Control::Kill).await?;
        Ok(entry.process_id.as_str().to_owned())
    }
}

/// Reconstructs the canonical start operation from dependency-owned fields.
///
/// # Errors
///
/// Returns an authorization error when the request cannot be represented safely.
pub fn canonical_start_operation(
    request: &DependencyStartProcessRequest,
) -> Result<Vec<u8>, ProcessDependencyError> {
    let tool = match (request.foreground, request.terminal_size.is_some()) {
        (true, false) => "process.run",
        (false, false) => "process.start",
        (true, true) => "process.run_pty",
        (false, true) => "process.start_pty",
    };
    let timeout_ms = request
        .timeout
        .map(|value| u64::try_from(value.as_millis()))
        .transpose()
        .map_err(|_| ProcessDependencyError::AuthorizationDenied)?;
    let arguments = request.terminal_size.map_or_else(
        || {
            json!({
                "executable": request.executable,
                "arguments": request.arguments,
                "working_directory": request.requested_working_directory,
                "environment": request.environment,
                "timeout_ms": timeout_ms,
                "output_limit_bytes": request.output_limit_bytes,
                "cleanup": request.cleanup.as_str(),
            })
        },
        |size| {
            json!({
                "executable": request.executable,
                "arguments": request.arguments,
                "working_directory": request.requested_working_directory,
                "environment": request.environment,
                "timeout_ms": timeout_ms,
                "output_limit_bytes": request.output_limit_bytes,
                "cleanup": request.cleanup.as_str(),
                "terminal": {
                    "columns": size.columns,
                    "rows": size.rows,
                    "pixel_width": size.pixel_width,
                    "pixel_height": size.pixel_height,
                },
            })
        },
    );
    canonical_bytes(tool, &request.authorization.cancellation_id, &arguments)
}

/// Reconstructs a canonical stdin operation.
///
/// # Errors
///
/// Returns an authorization error for non-UTF-8 input or serialization failure.
pub fn canonical_input_operation(
    request: &DependencyProcessInputRequest,
) -> Result<Vec<u8>, ProcessDependencyError> {
    let content = std::str::from_utf8(&request.bytes)
        .map_err(|_| ProcessDependencyError::AuthorizationDenied)?;
    canonical_bytes(
        "process.input",
        &request.authorization.cancellation_id,
        &json!({
            "process_id": request.process_id,
            "content": content,
            "close": request.close,
        }),
    )
}

/// Reconstructs a canonical terminal-resize operation.
///
/// # Errors
///
/// Returns an authorization error when serialization fails.
pub fn canonical_resize_operation(
    request: &DependencyResizeTerminalRequest,
) -> Result<Vec<u8>, ProcessDependencyError> {
    canonical_bytes(
        "process.resize",
        &request.authorization.cancellation_id,
        &json!({
            "process_id": request.process_id,
            "columns": request.size.columns,
            "rows": request.size.rows,
            "pixel_width": request.size.pixel_width,
            "pixel_height": request.size.pixel_height,
        }),
    )
}

/// Reconstructs a canonical output-read operation.
///
/// # Errors
///
/// Returns an authorization error when serialization fails.
pub fn canonical_read_operation(
    request: &DependencyReadOutputRequest,
) -> Result<Vec<u8>, ProcessDependencyError> {
    canonical_bytes(
        "process.read",
        &request.authorization.cancellation_id,
        &json!({
            "process_id": request.process_id,
            "stream": match request.stream {
                DependencyOutputStream::Stdout => "stdout",
                DependencyOutputStream::Stderr => "stderr",
                DependencyOutputStream::Terminal => "terminal",
            },
            "offset": request.offset,
            "length": request.length,
        }),
    )
}

/// Reconstructs a canonical process-control operation.
///
/// # Errors
///
/// Returns an authorization error when serialization fails.
pub fn canonical_control_operation(
    tool: &str,
    cancellation_id: &str,
    process_id: &str,
) -> Result<Vec<u8>, ProcessDependencyError> {
    canonical_bytes(tool, cancellation_id, &json!({ "process_id": process_id }))
}

/// Reconstructs a canonical process-list operation.
///
/// # Errors
///
/// Returns an authorization error when serialization fails.
pub fn canonical_list_operation(cancellation_id: &str) -> Result<Vec<u8>, ProcessDependencyError> {
    canonical_bytes("process.list", cancellation_id, &json!({}))
}

fn canonical_bytes(
    tool: &str,
    cancellation_id: &str,
    arguments: &Value,
) -> Result<Vec<u8>, ProcessDependencyError> {
    serde_json::to_vec(&(tool, cancellation_id, normalize_json(arguments)))
        .map_err(|_| ProcessDependencyError::AuthorizationDenied)
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

fn load_process_registry(
    config: &ProcessDependencyConfig,
) -> Result<BTreeMap<String, RegistryEntry>, ProcessDependencyError> {
    std::fs::create_dir_all(&config.storage_root).map_err(redacted_io)?;
    let storage = std::fs::canonicalize(&config.storage_root).map_err(redacted_io)?;
    if !config.log_root.exists() {
        if !config.log_root.starts_with(&config.storage_root) {
            return Err(ProcessDependencyError::StorageEscape);
        }
        return Ok(BTreeMap::new());
    }
    let log_root = std::fs::canonicalize(&config.log_root).map_err(redacted_io)?;
    if !log_root.starts_with(&storage) {
        return Err(ProcessDependencyError::StorageEscape);
    }
    let mut registry = BTreeMap::new();
    for directory in std::fs::read_dir(&log_root).map_err(redacted_io)? {
        let directory = directory.map_err(redacted_io)?;
        let log_directory = directory.path();
        if !directory.file_type().map_err(redacted_io)?.is_dir() {
            continue;
        }
        let Some(mut durable) = load_latest_durable_record(&log_directory)? else {
            continue;
        };
        if durable.schema_version != 1
            || durable.owner_id != config.owner_id
            || durable.session_id != config.session_id
            || directory.file_name().to_string_lossy() != durable.process_id
        {
            return Err(ProcessDependencyError::RecoveryCorrupt);
        }
        let process_id = DependencyProcessId::parse(durable.process_id.clone())?;
        let (state, recovery_state) = reconcile_durable_record(&mut durable, &log_directory)?;
        let cleanup = match durable.cleanup.as_str() {
            "retain" => DependencyCleanupPolicy::Retain,
            "remove_logs_on_success" => DependencyCleanupPolicy::RemoveLogsOnSuccess,
            "remove_logs_always" => DependencyCleanupPolicy::RemoveLogsAlways,
            _ => return Err(ProcessDependencyError::RecoveryCorrupt),
        };
        let exit = durable.exit.as_ref().map(|exit| DependencyExitStatus {
            code: exit.code,
            success: exit.success,
            timed_out: exit.timed_out,
        });
        let terminal_size = durable.terminal_size.map(DependencyTerminalSize::from);
        let snapshot = ProcessSnapshot {
            state,
            exit,
            detached: durable.detached,
            capture_error: None,
            terminal_size,
        };
        let stdout_truncated = Arc::new(AtomicBool::new(durable.stdout_truncated));
        let stderr_truncated = Arc::new(AtomicBool::new(durable.stderr_truncated));
        let logs_removed = Arc::new(AtomicBool::new(durable.logs_removed));
        let cleanup_failed = Arc::new(AtomicBool::new(durable.cleanup_failed));
        registry.insert(
            process_id.as_str().to_owned(),
            RegistryEntry {
                process_id,
                identity: DependencyIdentity {
                    owner_id: durable.owner_id.clone(),
                    session_id: durable.session_id.clone(),
                },
                cancellation_id: String::new(),
                executable: durable.executable.clone(),
                working_directory: durable.working_directory.clone(),
                log_directory,
                output_limit_bytes: durable.output_limit_bytes,
                cleanup,
                snapshot: Arc::new(RwLock::new(snapshot)),
                control: None,
                stdout_truncated,
                stderr_truncated,
                logs_removed,
                cleanup_failed,
                completion: Arc::new(Mutex::new(())),
                terminal: durable.terminal,
                os_process_id: durable.os_process_id,
                os_start_time: durable.os_start_time,
                recovery_state: Arc::new(RwLock::new(recovery_state)),
                durable: Arc::new(StdMutex::new(durable)),
            },
        );
    }
    Ok(registry)
}

fn reconcile_durable_record(
    durable: &mut DurableProcessRecord,
    log_directory: &Path,
) -> Result<(DependencyProcessState, DependencyRecoveryState), ProcessDependencyError> {
    match durable.lifecycle {
        DurableLifecycle::Exited => Ok((
            DependencyProcessState::Exited,
            DependencyRecoveryState::RecoveredExited,
        )),
        DurableLifecycle::Dispatching => Ok((
            DependencyProcessState::Exited,
            DependencyRecoveryState::DispatchUncertain,
        )),
        DurableLifecycle::Running => {
            let exact_process_exists = durable
                .os_process_id
                .zip(durable.os_start_time)
                .is_some_and(|(pid, start_time)| {
                    inspect_process_identity(pid).is_some_and(|identity| {
                        identity.start_time == start_time
                            && same_executable(&identity.executable, &durable.resolved_executable)
                    })
                });
            if exact_process_exists {
                Ok((
                    DependencyProcessState::Running,
                    DependencyRecoveryState::RecoveredRunningUnattached,
                ))
            } else {
                durable.lifecycle = DurableLifecycle::Exited;
                durable.exit = None;
                persist_durable_record(log_directory, durable)?;
                Ok((
                    DependencyProcessState::Exited,
                    DependencyRecoveryState::RecoveredExited,
                ))
            }
        }
    }
}

struct ProcessIdentityObservation {
    start_time: u64,
    executable: PathBuf,
}

fn inspect_process_identity(process_id: u32) -> Option<ProcessIdentityObservation> {
    let pid = Pid::from_u32(process_id);
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing()
            .with_exe(UpdateKind::Always)
            .without_tasks(),
    );
    let process = system.process(pid)?;
    Some(ProcessIdentityObservation {
        start_time: process.start_time(),
        executable: process.exe()?.to_path_buf(),
    })
}

fn same_executable(left: &Path, right: &Path) -> bool {
    if cfg!(windows) {
        let left = left.to_string_lossy();
        let right = right.to_string_lossy();
        let left = left.strip_prefix(r"\\?\").unwrap_or(&left);
        let right = right.strip_prefix(r"\\?\").unwrap_or(&right);
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

fn mark_durable_running(
    durable: &StdMutex<DurableProcessRecord>,
    log_directory: &Path,
    process_id: Option<u32>,
    executable: &Path,
) -> Result<Option<u64>, ProcessDependencyError> {
    let start_time = process_id
        .and_then(inspect_process_identity)
        .filter(|identity| same_executable(&identity.executable, executable))
        .map(|identity| identity.start_time);
    let mut record = durable
        .lock()
        .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
    record.lifecycle = DurableLifecycle::Running;
    record.os_process_id = process_id;
    record.os_start_time = start_time;
    persist_durable_record(log_directory, &mut record)?;
    Ok(start_time)
}

fn mark_durable_exited(
    durable: &StdMutex<DurableProcessRecord>,
    log_directory: &Path,
    exit: &DependencyExitStatus,
    stdout_truncated: bool,
    stderr_truncated: bool,
) -> Result<(), ProcessDependencyError> {
    let mut record = durable
        .lock()
        .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
    record.lifecycle = DurableLifecycle::Exited;
    record.exit = Some(DurableExitStatus {
        code: exit.code,
        success: exit.success,
        timed_out: exit.timed_out,
    });
    record.stdout_truncated = stdout_truncated;
    record.stderr_truncated = stderr_truncated;
    persist_durable_record(log_directory, &mut record)
}

fn update_durable_attachment(
    durable: &StdMutex<DurableProcessRecord>,
    log_directory: &Path,
    detached: bool,
) -> Result<(), ProcessDependencyError> {
    let mut record = durable
        .lock()
        .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
    record.detached = detached;
    persist_durable_record(log_directory, &mut record)
}

fn update_durable_terminal_size(
    durable: &StdMutex<DurableProcessRecord>,
    log_directory: &Path,
    size: DependencyTerminalSize,
) -> Result<(), ProcessDependencyError> {
    let mut record = durable
        .lock()
        .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
    record.terminal_size = Some(size.into());
    persist_durable_record(log_directory, &mut record)
}

fn update_durable_flags(
    durable: &StdMutex<DurableProcessRecord>,
    log_directory: &Path,
    stdout_truncated: bool,
    stderr_truncated: bool,
    cleanup_failed: bool,
) -> Result<(), ProcessDependencyError> {
    let mut record = durable
        .lock()
        .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
    record.stdout_truncated = stdout_truncated;
    record.stderr_truncated = stderr_truncated;
    record.cleanup_failed = cleanup_failed;
    persist_durable_record(log_directory, &mut record)
}

fn load_latest_durable_record(
    directory: &Path,
) -> Result<Option<DurableProcessRecord>, ProcessDependencyError> {
    let mut records = Vec::new();
    for entry in std::fs::read_dir(directory).map_err(redacted_io)? {
        let path = entry.map_err(redacted_io)?.path();
        let is_record = path.file_name().is_some_and(|name| {
            let name = name.to_string_lossy();
            name.starts_with("process-") && name.ends_with(".json")
        });
        if !is_record {
            continue;
        }
        let bytes = std::fs::read(&path).map_err(redacted_io)?;
        if let Ok(record) = serde_json::from_slice::<DurableProcessRecord>(&bytes) {
            records.push(record);
        } else {
            let quarantine = path.with_extension(format!("json.corrupt-{}", Uuid::now_v7()));
            std::fs::rename(&path, quarantine).map_err(redacted_io)?;
            sync_directory(directory)?;
        }
    }
    records.sort_by_key(|record| record.generation);
    Ok(records.pop())
}

fn persist_durable_record(
    directory: &Path,
    record: &mut DurableProcessRecord,
) -> Result<(), ProcessDependencyError> {
    record.generation = record
        .generation
        .checked_add(1)
        .ok_or(ProcessDependencyError::ResourceLimit)?;
    let bytes = serde_json::to_vec(record).map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
    let path = directory.join(format!("process-{:020}.json", record.generation));
    let temporary = directory.join(format!("process-{}.tmp", Uuid::now_v7()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(redacted_io)?;
    file.write_all(&bytes).map_err(redacted_io)?;
    file.sync_all().map_err(redacted_io)?;
    std::fs::rename(&temporary, &path).map_err(redacted_io)?;
    sync_directory(directory)?;
    for entry in std::fs::read_dir(directory).map_err(redacted_io)? {
        let old = entry.map_err(redacted_io)?.path();
        let is_old_record = old != path
            && old.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                name.starts_with("process-") && name.ends_with(".json")
            });
        if is_old_record {
            let _ = std::fs::remove_file(old);
        }
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), ProcessDependencyError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(redacted_io)
}

#[cfg(windows)]
#[allow(
    clippy::unnecessary_wraps,
    reason = "the platform implementations share one fallible persistence contract"
)]
fn sync_directory(_path: &Path) -> Result<(), ProcessDependencyError> {
    Ok(())
}

impl From<DependencyTerminalSize> for DurableTerminalSize {
    fn from(value: DependencyTerminalSize) -> Self {
        Self {
            columns: value.columns,
            rows: value.rows,
            pixel_width: value.pixel_width,
            pixel_height: value.pixel_height,
        }
    }
}

impl From<DurableTerminalSize> for DependencyTerminalSize {
    fn from(value: DurableTerminalSize) -> Self {
        Self {
            columns: value.columns,
            rows: value.rows,
            pixel_width: value.pixel_width,
            pixel_height: value.pixel_height,
        }
    }
}

fn load_replay_state(storage_root: &Path) -> Result<ReplayState, ProcessDependencyError> {
    let directory = storage_root.join("authorization-replay");
    std::fs::create_dir_all(&directory).map_err(redacted_io)?;
    let mut snapshots = Vec::new();
    for entry in std::fs::read_dir(&directory).map_err(redacted_io)? {
        let path = entry.map_err(redacted_io)?.path();
        if path.extension().is_some_and(|value| value == "json") {
            let bytes = std::fs::read(&path).map_err(redacted_io)?;
            if let Ok(snapshot) = serde_json::from_slice::<ReplaySnapshot>(&bytes) {
                snapshots.push((path, snapshot));
            }
        }
    }
    snapshots.sort_by_key(|(_, snapshot)| snapshot.generation);
    let snapshot = snapshots
        .last()
        .map_or_else(ReplaySnapshot::default, |(_, value)| value.clone());
    for (path, candidate) in snapshots {
        if candidate.generation != snapshot.generation {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(ReplayState {
        directory,
        snapshot,
    })
}

fn persist_replay_snapshot(
    directory: &Path,
    snapshot: &ReplaySnapshot,
) -> Result<(), ProcessDependencyError> {
    let path = directory.join(format!("replay-{:020}.json", snapshot.generation));
    let bytes =
        serde_json::to_vec(snapshot).map_err(|_| ProcessDependencyError::AuthorizationDenied)?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(redacted_io)?;
    std::io::Write::write_all(&mut file, &bytes).map_err(redacted_io)?;
    file.sync_all().map_err(redacted_io)?;
    for entry in std::fs::read_dir(directory).map_err(redacted_io)? {
        let old = entry.map_err(redacted_io)?.path();
        if old != path && old.extension().is_some_and(|value| value == "json") {
            let _ = std::fs::remove_file(old);
        }
    }
    Ok(())
}

fn resolve_environment(
    environment: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ProcessDependencyError> {
    environment
        .iter()
        .map(|(key, value)| {
            if let Some(reference) = value.strip_prefix("secret://") {
                if reference.is_empty()
                    || !reference
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '_')
                {
                    return Err(ProcessDependencyError::InvalidSecretReference);
                }
                let secret = std::env::var(reference)
                    .map_err(|_| ProcessDependencyError::SecretUnavailable)?;
                Ok((key.clone(), secret))
            } else if is_sensitive_environment_key(key) {
                Err(ProcessDependencyError::RawSecretDenied)
            } else {
                Ok((key.clone(), value.clone()))
            }
        })
        .collect()
}

fn enforce_executable_policy(
    requested: &str,
    resolved: &Path,
    config: &ProcessDependencyConfig,
) -> Result<(), ProcessDependencyError> {
    let requested = normalize_executable_policy_key(requested);
    let resolved = normalize_executable_policy_key(&resolved.to_string_lossy());
    let decision = config
        .executable_policy
        .get(&resolved)
        .or_else(|| config.executable_policy.get(&requested))
        .copied()
        .unwrap_or(config.default_executable_policy);
    match decision {
        DependencyExecutablePolicy::Allow => Ok(()),
        DependencyExecutablePolicy::Ask => Err(ProcessDependencyError::ExecutableApprovalRequired),
        DependencyExecutablePolicy::Deny => Err(ProcessDependencyError::ExecutableDenied),
    }
}

fn normalize_executable_policy_key(value: &str) -> String {
    if cfg!(windows) {
        value.replace('/', "\\").to_ascii_lowercase()
    } else {
        value.to_owned()
    }
}

async fn secure_roots(
    request: &DependencyStartProcessRequest,
    config: &ProcessDependencyConfig,
) -> Result<(PathBuf, PathBuf, PathBuf), ProcessDependencyError> {
    fs::create_dir_all(&config.storage_root)
        .await
        .map_err(redacted_io)?;
    let storage = fs::canonicalize(&config.storage_root)
        .await
        .map_err(redacted_io)?;
    fs::create_dir_all(&config.log_root)
        .await
        .map_err(redacted_io)?;
    let log_root = fs::canonicalize(&config.log_root)
        .await
        .map_err(redacted_io)?;
    if !log_root.starts_with(&storage) {
        return Err(ProcessDependencyError::StorageEscape);
    }
    let workspace = fs::canonicalize(&request.workspace_root)
        .await
        .map_err(redacted_io)?;
    let cwd = fs::canonicalize(&request.working_directory)
        .await
        .map_err(redacted_io)?;
    Ok((workspace, cwd, log_root))
}

async fn enforce_capacity(
    registry: &Mutex<BTreeMap<String, RegistryEntry>>,
    requested: u64,
    config: &ProcessDependencyConfig,
) -> Result<(), ProcessDependencyError> {
    let entries: Vec<_> = registry.lock().await.values().cloned().collect();
    let mut active = 0_usize;
    let mut retained = requested
        .checked_mul(2)
        .ok_or(ProcessDependencyError::ResourceLimit)?;
    for entry in entries {
        refresh_recovered_entry(&entry).await?;
        if entry.snapshot.read().await.state == DependencyProcessState::Running {
            active += 1;
        }
        if !entry.logs_removed.load(Ordering::Acquire) {
            retained = retained
                .checked_add(entry.output_limit_bytes.saturating_mul(2))
                .ok_or(ProcessDependencyError::ResourceLimit)?;
        }
    }
    if active >= config.max_active_processes || retained > config.max_total_retained_bytes {
        Err(ProcessDependencyError::ResourceLimit)
    } else {
        Ok(())
    }
}

struct ResolvedExecutable {
    invocation_path: PathBuf,
    identity_path: PathBuf,
}

fn command_for_resolved_executable(executable: &ResolvedExecutable) -> Command {
    #[cfg(unix)]
    {
        let mut command = Command::new(&executable.identity_path);
        command.arg0(&executable.invocation_path);
        command
    }
    #[cfg(not(unix))]
    {
        Command::new(&executable.identity_path)
    }
}

fn revalidate_invocation_identity(
    invocation_path: &Path,
    identity_path: &Path,
) -> Result<(), ProcessDependencyError> {
    let current_identity = std::fs::canonicalize(invocation_path)
        .map_err(|_| ProcessDependencyError::ExecutableNotFound)?;
    if same_executable(&current_identity, identity_path) {
        Ok(())
    } else {
        Err(ProcessDependencyError::ExecutableDenied)
    }
}

async fn resolve_executable(
    executable: &str,
    inherited: &BTreeSet<String>,
) -> Result<ResolvedExecutable, ProcessDependencyError> {
    let candidate = Path::new(executable);
    if candidate.is_absolute() || candidate.components().count() > 1 {
        return resolve_executable_candidate(candidate).await;
    }
    if !inherited.iter().any(|key| normalize_env_key(key) == "PATH") {
        return Err(ProcessDependencyError::ExecutableNotFound);
    }
    let path = std::env::var_os("PATH").ok_or(ProcessDependencyError::ExecutableNotFound)?;
    for directory in std::env::split_paths(&path) {
        for name in executable_names(executable) {
            let candidate = directory.join(name);
            if let Ok(resolved) = resolve_executable_candidate(&candidate).await {
                return Ok(resolved);
            }
        }
    }
    Err(ProcessDependencyError::ExecutableNotFound)
}

async fn resolve_executable_candidate(
    candidate: &Path,
) -> Result<ResolvedExecutable, ProcessDependencyError> {
    let invocation_path = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(redacted_io)?
            .join(candidate)
    };
    let identity_path = fs::canonicalize(&invocation_path)
        .await
        .map_err(|_| ProcessDependencyError::ExecutableNotFound)?;
    if !fs::metadata(&identity_path)
        .await
        .map_err(redacted_io)?
        .is_file()
    {
        return Err(ProcessDependencyError::ExecutableNotFound);
    }
    Ok(ResolvedExecutable {
        invocation_path,
        identity_path,
    })
}

fn executable_names(executable: &str) -> Vec<OsString> {
    #[cfg(windows)]
    {
        if Path::new(executable).extension().is_some() {
            vec![OsString::from(executable)]
        } else {
            vec![
                OsString::from(format!("{executable}.exe")),
                OsString::from(format!("{executable}.cmd")),
                OsString::from(format!("{executable}.bat")),
            ]
        }
    }
    #[cfg(not(windows))]
    {
        vec![OsString::from(executable)]
    }
}

fn inherit_safe_environment(command: &mut Command, allowlist: &BTreeSet<String>) {
    for configured in allowlist {
        let normalized = normalize_env_key(configured);
        for (key, value) in std::env::vars_os() {
            if normalize_env_key(&key.to_string_lossy()) == normalized
                && !is_sensitive_environment_key(&normalized)
            {
                command.env(key, value);
                break;
            }
        }
    }
}

fn inherit_safe_terminal_environment(command: &mut CommandBuilder, allowlist: &BTreeSet<String>) {
    for configured in allowlist {
        let normalized = normalize_env_key(configured);
        for (key, value) in std::env::vars_os() {
            if normalize_env_key(&key.to_string_lossy()) == normalized
                && !is_sensitive_environment_key(&normalized)
            {
                command.env(key, value);
                break;
            }
        }
    }
}

fn normalize_env_key(key: &str) -> String {
    if cfg!(windows) {
        key.to_ascii_uppercase()
    } else {
        key.to_owned()
    }
}

fn is_sensitive_environment_key(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    upper.contains("KEY")
        || upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("CREDENTIAL")
}

async fn refresh_recovered_entry(entry: &RegistryEntry) -> Result<(), ProcessDependencyError> {
    if *entry.recovery_state.read().await != DependencyRecoveryState::RecoveredRunningUnattached {
        return Ok(());
    }
    let expected_executable = entry
        .durable
        .lock()
        .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?
        .resolved_executable
        .clone();
    let exact_process_exists =
        entry
            .os_process_id
            .zip(entry.os_start_time)
            .is_some_and(|(pid, start_time)| {
                inspect_process_identity(pid).is_some_and(|identity| {
                    identity.start_time == start_time
                        && same_executable(&identity.executable, &expected_executable)
                })
            });
    if exact_process_exists {
        return Ok(());
    }
    {
        let mut durable = entry
            .durable
            .lock()
            .map_err(|_| ProcessDependencyError::RecoveryCorrupt)?;
        durable.lifecycle = DurableLifecycle::Exited;
        durable.exit = None;
        persist_durable_record(&entry.log_directory, &mut durable)?;
    }
    {
        let mut snapshot = entry.snapshot.write().await;
        snapshot.state = DependencyProcessState::Exited;
        snapshot.exit = None;
    }
    *entry.recovery_state.write().await = DependencyRecoveryState::RecoveredExited;
    Ok(())
}

fn ensure_owner(
    entry: &RegistryEntry,
    identity: &DependencyIdentity,
) -> Result<(), ProcessDependencyError> {
    if &entry.identity == identity {
        Ok(())
    } else {
        Err(ProcessDependencyError::OwnershipDenied)
    }
}

async fn wait_entry(entry: &RegistryEntry) -> Result<(), ProcessDependencyError> {
    let snapshot = entry.snapshot.read().await;
    if snapshot.state == DependencyProcessState::Exited {
        return Ok(());
    }
    drop(snapshot);
    if entry.snapshot.read().await.exit.is_none() {
        let control = entry
            .control
            .as_ref()
            .ok_or(ProcessDependencyError::ReattachmentUnavailable)?;
        let (sender, receiver) = oneshot::channel();
        if control.send(Control::Wait(sender)).await.is_ok()
            && receiver.await.is_err()
            && entry.snapshot.read().await.exit.is_none()
        {
            return Err(ProcessDependencyError::SupervisorStopped);
        }
        if entry.snapshot.read().await.exit.is_none() {
            return Err(ProcessDependencyError::SupervisorStopped);
        }
    }
    Ok(())
}

async fn send_control<F>(entry: &RegistryEntry, control: F) -> Result<(), ProcessDependencyError>
where
    F: FnOnce(oneshot::Sender<Result<(), String>>) -> Control,
{
    let control_sender = entry
        .control
        .as_ref()
        .ok_or(ProcessDependencyError::ReattachmentUnavailable)?;
    let (sender, receiver) = oneshot::channel();
    control_sender
        .send(control(sender))
        .await
        .map_err(|_| ProcessDependencyError::ProcessExited)?;
    receiver
        .await
        .map_err(|_| ProcessDependencyError::SupervisorStopped)?
        .map_err(|_| ProcessDependencyError::ProcessControl)
}

async fn record_from_entry(entry: &RegistryEntry) -> DependencyProcessRecord {
    let snapshot = entry.snapshot.read().await.clone();
    DependencyProcessRecord {
        process_id: entry.process_id.clone(),
        owner_id: entry.identity.owner_id.clone(),
        session_id: entry.identity.session_id.clone(),
        executable: entry.executable.clone(),
        working_directory: entry.working_directory.clone(),
        state: snapshot.state,
        exit: snapshot.exit,
        detached: snapshot.detached,
        stdout_projection: Vec::new(),
        stderr_projection: Vec::new(),
        stdout_truncated: entry.stdout_truncated.load(Ordering::Acquire),
        stderr_truncated: entry.stderr_truncated.load(Ordering::Acquire),
        logs_removed: entry.logs_removed.load(Ordering::Acquire),
        cleanup_failed: entry.cleanup_failed.load(Ordering::Acquire),
        terminal: entry.terminal,
        terminal_size: snapshot.terminal_size,
        os_process_id: entry.os_process_id,
        os_start_time: entry.os_start_time,
        recovery_state: *entry.recovery_state.read().await,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the dependency boundary keeps all PTY security and resource parameters explicit"
)]
fn spawn_terminal_process(
    executable: &Path,
    executable_identity: &Path,
    arguments: &[String],
    working_directory: &Path,
    environment: &BTreeMap<String, String>,
    inherited_environment_allowlist: &BTreeSet<String>,
    size: DependencyTerminalSize,
    stdout_file: std::fs::File,
    output_limit_bytes: u64,
    stdout_truncated: Arc<AtomicBool>,
    receiver: mpsc::Receiver<Control>,
    snapshot: Arc<RwLock<ProcessSnapshot>>,
    process_timeout: Option<Duration>,
    drain_timeout: Duration,
    input_write_timeout: Duration,
    max_waiters: usize,
    durable: Arc<StdMutex<DurableProcessRecord>>,
    log_directory: PathBuf,
) -> Result<(Option<u32>, Option<u64>), ProcessDependencyError> {
    let pair = native_pty_system()
        .openpty(size.portable())
        .map_err(|_| ProcessDependencyError::Terminal)?;
    // portable-pty derives both the executed path and argv[0] from one field.
    // Revalidate the alias immediately before spawn so multicall dispatch is
    // preserved without accepting a target swapped after policy validation.
    revalidate_invocation_identity(executable, executable_identity)?;
    let mut command = CommandBuilder::new(executable);
    command.args(arguments);
    command.cwd(working_directory);
    command.env_clear();
    inherit_safe_terminal_environment(&mut command, inherited_environment_allowlist);
    for (key, value) in environment {
        command.env(key, value);
    }
    let child = pair
        .slave
        .spawn_command(command)
        .map_err(|_| ProcessDependencyError::Terminal)?;
    let process_id = child.process_id();
    let start_time =
        mark_durable_running(&durable, &log_directory, process_id, executable_identity)?;
    let reader = pair
        .master
        .try_clone_reader()
        .map_err(|_| ProcessDependencyError::Terminal)?;
    let writer = Arc::new(StdMutex::new(
        pair.master
            .take_writer()
            .map_err(|_| ProcessDependencyError::Terminal)?,
    ));
    drop(pair.slave);
    std::thread::Builder::new()
        .name("agentmod-pty-supervisor".to_owned())
        .spawn(move || {
            supervise_terminal(
                child,
                pair.master,
                reader,
                writer,
                stdout_file,
                output_limit_bytes,
                stdout_truncated,
                receiver,
                snapshot,
                process_timeout,
                drain_timeout,
                input_write_timeout,
                max_waiters,
                durable,
                log_directory,
            );
        })
        .map_err(redacted_io)?;
    Ok((process_id, start_time))
}

#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the PTY supervisor owns one isolated child lifecycle and its bounded control loop"
)]
fn supervise_terminal(
    mut child: Box<dyn portable_pty::Child + Send + Sync>,
    master: Box<dyn MasterPty + Send>,
    reader: Box<dyn Read + Send>,
    writer: Arc<StdMutex<Box<dyn Write + Send>>>,
    stdout_file: std::fs::File,
    output_limit_bytes: u64,
    stdout_truncated: Arc<AtomicBool>,
    mut receiver: mpsc::Receiver<Control>,
    snapshot: Arc<RwLock<ProcessSnapshot>>,
    process_timeout: Option<Duration>,
    drain_timeout: Duration,
    input_write_timeout: Duration,
    max_waiters: usize,
    durable: Arc<StdMutex<DurableProcessRecord>>,
    log_directory: PathBuf,
) {
    let (capture_sender, capture_receiver) = std::sync::mpsc::sync_channel(1);
    let capture_truncated = Arc::clone(&stdout_truncated);
    let capture_writer = Arc::clone(&writer);
    let capture_started = std::thread::Builder::new()
        .name("agentmod-pty-capture".to_owned())
        .spawn(move || {
            let result = capture_terminal_stream(
                reader,
                stdout_file,
                output_limit_bytes,
                capture_truncated,
                capture_writer,
            );
            let _ = capture_sender.send(result);
        })
        .is_ok();
    let deadline = process_timeout.map(|duration| std::time::Instant::now() + duration);
    let mut timed_out = false;
    let mut waiters = Vec::new();
    let exit = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                break DependencyExitStatus {
                    code: i32::try_from(status.exit_code()).ok(),
                    success: status.success(),
                    timed_out,
                };
            }
            Err(_) => {
                break DependencyExitStatus {
                    code: None,
                    success: false,
                    timed_out,
                };
            }
            Ok(None) => {}
        }
        if deadline.is_some_and(|value| std::time::Instant::now() >= value) {
            timed_out = true;
            let _ = child.kill();
        }
        while let Ok(control) = receiver.try_recv() {
            match control {
                Control::Input {
                    bytes,
                    close,
                    response,
                } => {
                    let started = std::time::Instant::now();
                    let result = writer
                        .lock()
                        .map_err(|_| "terminal writer unavailable".to_owned())
                        .and_then(|mut writer| {
                            writer
                                .write_all(&bytes)
                                .and_then(|()| writer.flush())
                                .map_err(|_| "input failed".to_owned())
                        })
                        .and_then(|()| {
                            (started.elapsed() <= input_write_timeout)
                                .then_some(())
                                .ok_or_else(|| "input timed out".to_owned())
                        });
                    if close {
                        let _ = response
                            .send(Err("PTY input cannot be closed independently".to_owned()));
                        continue;
                    }
                    let _ = response.send(result);
                }
                Control::Resize { size, response } => {
                    let result = master
                        .resize(size.portable())
                        .map_err(|_| "terminal resize failed".to_owned());
                    if result.is_ok() {
                        snapshot.blocking_write().terminal_size = Some(size);
                    }
                    let _ = response.send(result);
                }
                Control::Interrupt(response) => {
                    let result = writer
                        .lock()
                        .map_err(|_| "interrupt failed".to_owned())
                        .and_then(|mut writer| {
                            writer
                                .write_all(&[3])
                                .and_then(|()| writer.flush())
                                .map_err(|_| "interrupt failed".to_owned())
                        });
                    let _ = response.send(result);
                }
                Control::Kill(response) => {
                    let result = child.kill().map_err(|_| "kill failed".to_owned());
                    let _ = response.send(result);
                }
                Control::Wait(response) if waiters.len() < max_waiters => {
                    waiters.push(response);
                }
                Control::Wait(_) => {}
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    drop(writer);
    drop(master);
    let capture_error =
        if capture_started && matches!(capture_receiver.recv_timeout(drain_timeout), Ok(Ok(()))) {
            None
        } else {
            Some("terminal capture failed or exceeded drain deadline".to_owned())
        };
    {
        let mut state = snapshot.blocking_write();
        state.state = DependencyProcessState::Exited;
        state.exit = Some(exit.clone());
        state.capture_error = capture_error;
    }
    let _ = mark_durable_exited(
        &durable,
        &log_directory,
        &exit,
        stdout_truncated.load(Ordering::Acquire),
        false,
    );
    for waiter in waiters {
        let _ = waiter.send(exit.clone());
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "owned Arc values keep the detached capture thread resources alive"
)]
fn capture_terminal_stream(
    mut reader: Box<dyn Read + Send>,
    mut log: std::fs::File,
    limit: u64,
    truncated: Arc<AtomicBool>,
    writer: Arc<StdMutex<Box<dyn Write + Send>>>,
) -> Result<(), ProcessDependencyError> {
    let mut retained = 0_u64;
    let mut buffer = vec![0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).map_err(redacted_io)?;
        if count == 0 {
            break;
        }
        respond_to_terminal_queries(&buffer[..count], &writer)?;
        let available = limit.saturating_sub(retained);
        let write_count = usize::try_from(
            available
                .min(u64::try_from(count).map_err(|_| ProcessDependencyError::LengthOverflow)?),
        )
        .map_err(|_| ProcessDependencyError::LengthOverflow)?;
        if write_count > 0 {
            log.write_all(&buffer[..write_count]).map_err(redacted_io)?;
            retained = retained
                .checked_add(
                    u64::try_from(write_count)
                        .map_err(|_| ProcessDependencyError::LengthOverflow)?,
                )
                .ok_or(ProcessDependencyError::LengthOverflow)?;
        }
        if write_count < count {
            truncated.store(true, Ordering::Release);
        }
    }
    log.flush().map_err(redacted_io)?;
    log.sync_data().map_err(redacted_io)
}

fn respond_to_terminal_queries(
    bytes: &[u8],
    writer: &StdMutex<Box<dyn Write + Send>>,
) -> Result<(), ProcessDependencyError> {
    if bytes.windows(4).any(|window| window == b"\x1b[6n") {
        let mut writer = writer
            .lock()
            .map_err(|_| ProcessDependencyError::Terminal)?;
        writer.write_all(b"\x1b[1;1R").map_err(redacted_io)?;
        writer.flush().map_err(redacted_io)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn supervise(
    mut child: Child,
    mut stdin: Option<ChildStdin>,
    mut receiver: mpsc::Receiver<Control>,
    snapshot: Arc<RwLock<ProcessSnapshot>>,
    process_timeout: Option<Duration>,
    stdout_task: JoinHandle<Result<(), ProcessDependencyError>>,
    stderr_task: JoinHandle<Result<(), ProcessDependencyError>>,
    drain_timeout: Duration,
    input_write_timeout: Duration,
    max_waiters: usize,
    durable: Arc<StdMutex<DurableProcessRecord>>,
    log_directory: PathBuf,
    stdout_truncated: Arc<AtomicBool>,
    stderr_truncated: Arc<AtomicBool>,
) {
    let deadline = process_timeout.map(|duration| Instant::now() + duration);
    let mut timeout_sleep =
        Box::pin(sleep_until(deadline.unwrap_or_else(|| {
            Instant::now() + Duration::from_secs(31_536_000)
        })));
    let mut timeout_active = deadline.is_some();
    let mut timed_out = false;
    let mut waiters = Vec::new();
    let exit = loop {
        tokio::select! {
            status = child.wait() => {
                break status.map_or(
                    DependencyExitStatus { code: None, success: false, timed_out },
                    |status| DependencyExitStatus {
                        code: status.code(),
                        success: status.success(),
                        timed_out,
                    },
                );
            }
            control = receiver.recv() => {
                match control {
                    Some(Control::Input { bytes, close, response }) => {
                        let result = timeout(input_write_timeout, async {
                            let input = stdin.as_mut().ok_or_else(|| "stdin closed".to_owned())?;
                            input.write_all(&bytes).await.map_err(|_| "input failed".to_owned())?;
                            input.flush().await.map_err(|_| "input failed".to_owned())?;
                            if close { stdin.take(); }
                            Ok(())
                        }).await.map_err(|_| "input timed out".to_owned()).and_then(|value| value);
                        let _ = response.send(result);
                    }
                    Some(Control::Resize { response, .. }) => {
                        let _ = response.send(Err("process has no terminal".to_owned()));
                    }
                    Some(Control::Interrupt(response)) => {
                        let result = interrupt_child(&mut child).await;
                        let _ = response.send(result);
                    }
                    Some(Control::Kill(response)) => {
                        let result = kill_child_tree(&mut child).await;
                        let _ = response.send(result);
                    }
                    Some(Control::Wait(response)) if waiters.len() < max_waiters => {
                        waiters.push(response);
                    }
                    Some(Control::Wait(_)) | None => {}
                }
            }
            () = &mut timeout_sleep, if timeout_active => {
                timeout_active = false;
                timed_out = true;
                let _ = kill_child_tree(&mut child).await;
            }
        }
    };
    let stdout_result = timeout(drain_timeout, stdout_task).await;
    let stderr_result = timeout(drain_timeout, stderr_task).await;
    let capture_error = if capture_ok(&stdout_result) && capture_ok(&stderr_result) {
        None
    } else {
        Some("capture failed or exceeded drain deadline".to_owned())
    };
    {
        let mut state = snapshot.write().await;
        state.state = DependencyProcessState::Exited;
        state.exit = Some(exit.clone());
        state.capture_error = capture_error;
    }
    let _ = mark_durable_exited(
        &durable,
        &log_directory,
        &exit,
        stdout_truncated.load(Ordering::Acquire),
        stderr_truncated.load(Ordering::Acquire),
    );
    for waiter in waiters {
        let _ = waiter.send(exit.clone());
    }
}

fn capture_ok(
    result: &Result<
        Result<Result<(), ProcessDependencyError>, tokio::task::JoinError>,
        tokio::time::error::Elapsed,
    >,
) -> bool {
    matches!(result, Ok(Ok(Ok(()))))
}

async fn interrupt_child(child: &mut Child) -> Result<(), String> {
    #[cfg(windows)]
    {
        let pid = child
            .id()
            .ok_or_else(|| "process ID unavailable".to_owned())?;
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|_| "interrupt failed".to_owned())?;
        if status.success() {
            sleep(Duration::from_millis(500)).await;
            if child
                .try_wait()
                .map_err(|_| "wait failed".to_owned())?
                .is_some()
            {
                return Ok(());
            }
        }
        kill_child_tree(child).await
    }
    #[cfg(not(windows))]
    {
        signal_child_group(child, "-TERM").await.or_else(|_| {
            child
                .start_kill()
                .map_err(|_| "interrupt failed".to_owned())
        })
    }
}

async fn kill_child_tree(child: &mut Child) -> Result<(), String> {
    #[cfg(windows)]
    {
        let pid = child
            .id()
            .ok_or_else(|| "process ID unavailable".to_owned())?;
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map_err(|_| "kill failed".to_owned())?;
        if status.success() {
            Ok(())
        } else {
            child.start_kill().map_err(|_| "kill failed".to_owned())
        }
    }
    #[cfg(not(windows))]
    {
        signal_child_group(child, "-KILL")
            .await
            .or_else(|_| child.start_kill().map_err(|_| "kill failed".to_owned()))
    }
}

#[cfg(not(windows))]
async fn signal_child_group(child: &Child, signal: &str) -> Result<(), String> {
    let pid = child
        .id()
        .ok_or_else(|| "process ID unavailable".to_owned())?;
    let status = Command::new("kill")
        .args([signal, "--", &format!("-{pid}")])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map_err(|_| "process-group signal failed".to_owned())?;
    status
        .success()
        .then_some(())
        .ok_or_else(|| "process-group signal failed".to_owned())
}

async fn capture_stream<R>(
    mut reader: R,
    mut log: File,
    limit: u64,
    truncated: Arc<AtomicBool>,
) -> Result<(), ProcessDependencyError>
where
    R: AsyncRead + Unpin,
{
    let mut retained = 0_u64;
    let mut buffer = vec![0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer).await.map_err(redacted_io)?;
        if count == 0 {
            break;
        }
        let available = limit.saturating_sub(retained);
        let write_count = usize::try_from(
            available
                .min(u64::try_from(count).map_err(|_| ProcessDependencyError::LengthOverflow)?),
        )
        .map_err(|_| ProcessDependencyError::LengthOverflow)?;
        if write_count > 0 {
            log.write_all(&buffer[..write_count])
                .await
                .map_err(redacted_io)?;
            retained = retained
                .checked_add(
                    u64::try_from(write_count)
                        .map_err(|_| ProcessDependencyError::LengthOverflow)?,
                )
                .ok_or(ProcessDependencyError::LengthOverflow)?;
        }
        if write_count < count {
            truncated.store(true, Ordering::Release);
        }
    }
    log.flush().await.map_err(redacted_io)?;
    log.sync_data().await.map_err(redacted_io)
}

async fn read_output(
    entry: &RegistryEntry,
    stream: DependencyOutputStream,
    offset: u64,
    length: u64,
) -> Result<DependencyReadOutputResponse, ProcessDependencyError> {
    if entry.logs_removed.load(Ordering::Acquire) {
        return Err(ProcessDependencyError::OutputUnavailable);
    }
    let path = match stream {
        DependencyOutputStream::Stdout => entry.log_directory.join("stdout.log"),
        DependencyOutputStream::Stderr => entry.log_directory.join("stderr.log"),
        DependencyOutputStream::Terminal => {
            if !entry.terminal {
                return Err(ProcessDependencyError::TerminalRequired);
            }
            entry.log_directory.join("stdout.log")
        }
    };
    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .map_err(redacted_io)?;
    let retained_bytes = file.metadata().await.map_err(redacted_io)?.len();
    if offset > retained_bytes {
        return Err(ProcessDependencyError::InvalidOutputRange);
    }
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(redacted_io)?;
    let count = usize::try_from(retained_bytes.saturating_sub(offset).min(length))
        .map_err(|_| ProcessDependencyError::LengthOverflow)?;
    let mut bytes = vec![0; count];
    file.read_exact(&mut bytes).await.map_err(redacted_io)?;
    Ok(DependencyReadOutputResponse {
        next_offset: offset
            .checked_add(u64::try_from(count).map_err(|_| ProcessDependencyError::LengthOverflow)?)
            .ok_or(ProcessDependencyError::LengthOverflow)?,
        bytes,
        retained_bytes,
        truncated: match stream {
            DependencyOutputStream::Stdout | DependencyOutputStream::Terminal => {
                entry.stdout_truncated.load(Ordering::Acquire)
            }
            DependencyOutputStream::Stderr => entry.stderr_truncated.load(Ordering::Acquire),
        },
    })
}

async fn read_entire_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, ProcessDependencyError> {
    let length = fs::metadata(path).await.map_err(redacted_io)?.len();
    if length > limit {
        return Err(ProcessDependencyError::ResourceLimit);
    }
    fs::read(path).await.map_err(redacted_io)
}

fn validate_start_request(
    request: &DependencyStartProcessRequest,
    config: &ProcessDependencyConfig,
) -> Result<(), ProcessDependencyError> {
    if request.executable.trim().is_empty()
        || request.executable.contains('\0')
        || request.arguments.iter().any(|value| value.contains('\0'))
        || request.arguments.len() > config.max_arguments
        || request.arguments.iter().map(String::len).sum::<usize>() > config.max_argument_bytes
    {
        return Err(ProcessDependencyError::InvalidExecutable);
    }
    if request.workspace_root.as_os_str().is_empty()
        || request.working_directory.as_os_str().is_empty()
    {
        return Err(ProcessDependencyError::InvalidWorkingDirectory);
    }
    if request.output_limit_bytes == 0
        || request.output_limit_bytes > config.max_total_retained_bytes / 2
    {
        return Err(ProcessDependencyError::InvalidOutputLimit);
    }
    if let Some(size) = request.terminal_size {
        validate_terminal_size(size)?;
    }
    if request.environment.len() > config.max_environment_entries
        || request
            .environment
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum::<usize>()
            > config.max_environment_bytes
        || request.environment.iter().any(|(key, value)| {
            key.is_empty()
                || key.contains(['=', '\0'])
                || value.contains('\0')
                || normalize_env_key(key) == "PATH"
        })
    {
        return Err(ProcessDependencyError::InvalidEnvironment);
    }
    Ok(())
}

fn validate_terminal_size(size: DependencyTerminalSize) -> Result<(), ProcessDependencyError> {
    if size.columns == 0 || size.rows == 0 {
        Err(ProcessDependencyError::InvalidTerminalSize)
    } else {
        Ok(())
    }
}

fn validate_authorization_shape(
    authorization: &DependencyAuthorization,
) -> Result<(), ProcessDependencyError> {
    if authorization.identity.owner_id.is_empty()
        || authorization.identity.session_id.is_empty()
        || authorization.call_id.is_empty()
        || authorization.tool.is_empty()
        || authorization.normalized_digest.len() != 64
        || authorization.grant.len() > 4096
        || authorization.cancellation_id.is_empty()
        || authorization.canonical_operation.len() > 1024 * 1024
    {
        Err(ProcessDependencyError::AuthorizationDenied)
    } else {
        Ok(())
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err supplies owned external errors which are deliberately redacted"
)]
fn redacted_io(_error: std::io::Error) -> ProcessDependencyError {
    ProcessDependencyError::Io
}

/// Redacted dependency errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ProcessDependencyError {
    /// Invalid configuration.
    #[error("invalid process dependency configuration")]
    InvalidConfiguration,
    /// Authorization failed.
    #[error("process authorization denied")]
    AuthorizationDenied,
    /// Grant nonce was replayed.
    #[error("process authorization replay denied")]
    AuthorizationReplay,
    /// Owner/session mismatch.
    #[error("process ownership denied")]
    OwnershipDenied,
    /// Storage escaped trusted root.
    #[error("process storage root escape")]
    StorageEscape,
    /// Invalid executable/arguments.
    #[error("invalid executable or arguments")]
    InvalidExecutable,
    /// Executable cannot be resolved.
    #[error("executable cannot be resolved")]
    ExecutableNotFound,
    /// Executable policy requires an approval not present at this boundary.
    #[error("executable requires approval")]
    ExecutableApprovalRequired,
    /// Executable policy denied execution.
    #[error("executable denied")]
    ExecutableDenied,
    /// Invalid cwd.
    #[error("invalid working directory")]
    InvalidWorkingDirectory,
    /// Cwd escape.
    #[error("working directory escape")]
    WorkingDirectoryEscape,
    /// Invalid environment.
    #[error("invalid environment")]
    InvalidEnvironment,
    /// Raw sensitive environment value was denied.
    #[error("raw secret environment value denied")]
    RawSecretDenied,
    /// Secret reference syntax was invalid.
    #[error("invalid secret reference")]
    InvalidSecretReference,
    /// Secret reference could not be resolved.
    #[error("secret reference unavailable")]
    SecretUnavailable,
    /// Invalid output limit.
    #[error("invalid output limit")]
    InvalidOutputLimit,
    /// Invalid process ID.
    #[error("invalid process ID")]
    InvalidProcessId,
    /// Missing process.
    #[error("process not found")]
    ProcessNotFound,
    /// Exited.
    #[error("process exited")]
    ProcessExited,
    /// Pipe unavailable.
    #[error("process pipe unavailable")]
    PipeUnavailable,
    /// Terminal dimensions are invalid.
    #[error("invalid terminal dimensions")]
    InvalidTerminalSize,
    /// Operation requires a pseudo-terminal.
    #[error("process has no pseudo-terminal")]
    TerminalRequired,
    /// Pseudo-terminal setup or control failed.
    #[error("pseudo-terminal operation failed")]
    Terminal,
    /// Durable process recovery data is invalid or inconsistent.
    #[error("process recovery record is corrupt")]
    RecoveryCorrupt,
    /// A recovered process exists but its inherited handles are unavailable.
    #[error("recovered process cannot be controlled by this host")]
    ReattachmentUnavailable,
    /// Input bound.
    #[error("process input exceeds bound")]
    InputTooLarge,
    /// Range invalid.
    #[error("invalid output range")]
    InvalidOutputRange,
    /// Output removed.
    #[error("process output unavailable")]
    OutputUnavailable,
    /// Length overflow.
    #[error("process length overflow")]
    LengthOverflow,
    /// Supervisor stopped.
    #[error("process supervisor stopped")]
    SupervisorStopped,
    /// Control failed.
    #[error("process control failed")]
    ProcessControl,
    /// Capture failed.
    #[error("process output capture failed")]
    CaptureFailed,
    /// Global resource limit.
    #[error("process resource limit exceeded")]
    ResourceLimit,
    /// Redacted external I/O.
    #[error("process dependency I/O failed")]
    Io,
}

#[cfg(test)]
mod tests {
    use agentmod_protocol_support::authorization::{AuthorizationClaims, seal_authorization};

    use super::*;

    #[allow(
        clippy::too_many_arguments,
        reason = "test grant claims are intentionally explicit"
    )]
    fn sign(
        key: &[u8; 32],
        owner: &str,
        session: &str,
        call: &str,
        tool: &str,
        operation: &[u8],
        expiry: i64,
        nonce: &str,
        cancellation: &str,
    ) -> DependencyAuthorization {
        let content_hash = ContentHash::digest(operation);
        let digest = content_hash.to_hex();
        let claims = AuthorizationClaims {
            owner: owner.to_owned(),
            session: session.to_owned(),
            call_id: call.to_owned(),
            action: tool.to_owned(),
            normalized_digest: content_hash,
            issued_at: TimestampMillis::new(expiry - 30_000),
            expires_at: TimestampMillis::new(expiry),
            nonce: nonce.to_owned(),
        };
        let grant = seal_authorization(&claims, &AuthorizationKey::from_bytes(*key)).expect("seal");
        DependencyAuthorization {
            identity: DependencyIdentity {
                owner_id: owner.to_owned(),
                session_id: session.to_owned(),
            },
            call_id: call.to_owned(),
            tool: tool.to_owned(),
            normalized_digest: digest,
            grant,
            cancellation_id: cancellation.to_owned(),
            canonical_operation: operation.to_vec(),
        }
    }

    fn config(root: &Path) -> ProcessDependencyConfig {
        ProcessDependencyConfig {
            storage_root: root.join("storage"),
            log_root: root.join("storage/logs"),
            authorization_key_hex: "07".repeat(32),
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            inherited_environment_allowlist: BTreeSet::from([
                "PATH".to_owned(),
                "SYSTEMROOT".to_owned(),
                "WINDIR".to_owned(),
            ]),
            max_input_bytes: 1024,
            max_range_bytes: 4096,
            max_arguments: 16,
            max_argument_bytes: 4096,
            max_environment_entries: 8,
            max_environment_bytes: 4096,
            max_active_processes: 4,
            max_total_retained_bytes: 64 * 1024,
            drain_timeout: Duration::from_secs(2),
            input_write_timeout: Duration::from_millis(250),
            max_replay_entries: 128,
            max_completed_entries: 8,
            max_waiters_per_process: 4,
            executable_policy: BTreeMap::new(),
            default_executable_policy: DependencyExecutablePolicy::Deny,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_symlink_preserves_multicall_argv_zero_and_canonical_identity() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("root");
        let target = Path::new("/bin/sh");
        let alias = root.path().join("cargo-alias");
        symlink(target, &alias).expect("executable alias");

        let resolved = resolve_executable(alias.to_str().expect("UTF-8 alias"), &BTreeSet::new())
            .await
            .expect("resolved alias");
        assert_eq!(resolved.invocation_path, alias);
        assert_eq!(
            resolved.identity_path,
            std::fs::canonicalize(target).expect("canonical target")
        );
        let mut command = command_for_resolved_executable(&resolved);
        command.args([
            "-c",
            "[ \"$(basename \"$0\")\" = cargo-alias ] || exit 41; printf alias-preserved",
        ]);
        let output = command.output().await.expect("execute alias");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"alias-preserved");

        let replacement = root.path().join("replacement-target");
        std::fs::write(&replacement, b"#!/bin/sh\nexit 0\n").expect("replacement fixture");
        std::fs::remove_file(&alias).expect("remove original alias");
        symlink(&replacement, &alias).expect("swapped executable alias");
        assert_eq!(
            revalidate_invocation_identity(&alias, &resolved.identity_path),
            Err(ProcessDependencyError::ExecutableDenied)
        );
    }

    #[tokio::test]
    async fn rejects_forged_expired_replayed_and_wrong_claims() {
        let root = tempfile::tempdir().expect("root");
        let dependency = TokioProcessDependency::new(config(root.path())).expect("config");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        let now = i64::try_from(now).expect("time");
        let valid = sign(
            &[7; 32],
            "owner",
            "session",
            "call",
            "process.start",
            b"operation",
            now + 30_000,
            "nonce",
            "cancel",
        );
        dependency
            .authorize(&valid, "process.start", b"operation")
            .await
            .expect("valid");
        assert_eq!(
            dependency
                .authorize(&valid, "process.start", b"operation")
                .await,
            Err(ProcessDependencyError::AuthorizationReplay)
        );
        let mut forged = sign(
            &[8; 32],
            "owner",
            "session",
            "call2",
            "process.start",
            b"operation",
            now + 30_000,
            "forged",
            "cancel2",
        );
        assert_eq!(
            dependency
                .authorize(&forged, "process.start", b"operation")
                .await,
            Err(ProcessDependencyError::AuthorizationDenied)
        );
        forged = sign(
            &[7; 32],
            "owner",
            "session",
            "call3",
            "process.start",
            b"operation",
            now - 1,
            "expired",
            "cancel3",
        );
        assert_eq!(
            dependency
                .authorize(&forged, "process.start", b"operation")
                .await,
            Err(ProcessDependencyError::AuthorizationDenied)
        );
        let mut wrong = sign(
            &[7; 32],
            "owner",
            "session",
            "call4",
            "process.start",
            b"operation",
            now + 30_000,
            "wrong",
            "cancel4",
        );
        wrong.identity.owner_id = "other".to_owned();
        assert_eq!(
            dependency
                .authorize(&wrong, "process.start", b"operation")
                .await,
            Err(ProcessDependencyError::AuthorizationDenied)
        );
        let tampered = sign(
            &[7; 32],
            "owner",
            "session",
            "call5",
            "process.start",
            b"operation",
            now + 30_000,
            "tampered",
            "cancel5",
        );
        assert_eq!(
            dependency
                .authorize(&tampered, "process.start", b"different")
                .await,
            Err(ProcessDependencyError::AuthorizationDenied)
        );
    }

    #[tokio::test]
    async fn forged_start_creates_no_logs_and_spawns_nothing() {
        let root = tempfile::tempdir().expect("root");
        let dependency = TokioProcessDependency::new(config(root.path())).expect("config");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        let now = i64::try_from(now).expect("time");
        let authorization = sign(
            &[8; 32],
            "owner",
            "session",
            "forged-start",
            "process.start",
            b"operation",
            now + 30_000,
            "forged-start-nonce",
            "cancel",
        );
        assert_eq!(
            dependency
                .start(DependencyStartProcessRequest {
                    authorization,
                    workspace_root: root.path().to_path_buf(),
                    requested_working_directory: Some(root.path().to_path_buf()),
                    working_directory: root.path().to_path_buf(),
                    executable: std::env::current_exe()
                        .expect("current executable")
                        .to_string_lossy()
                        .into_owned(),
                    arguments: Vec::new(),
                    environment: BTreeMap::new(),
                    timeout: None,
                    output_limit_bytes: 1024,
                    cleanup: DependencyCleanupPolicy::Retain,
                    foreground: false,
                    terminal_size: None,
                })
                .await,
            Err(ProcessDependencyError::AuthorizationDenied)
        );
        assert!(!root.path().join("storage/logs").exists());
    }

    fn recovery_record(
        process_id: &str,
        lifecycle: DurableLifecycle,
        executable: PathBuf,
        os_process_id: Option<u32>,
        os_start_time: Option<u64>,
    ) -> DurableProcessRecord {
        DurableProcessRecord {
            schema_version: 1,
            generation: 0,
            process_id: process_id.to_owned(),
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            executable: executable.to_string_lossy().into_owned(),
            resolved_executable: executable,
            working_directory: std::env::current_dir().expect("cwd"),
            lifecycle,
            exit: None,
            detached: true,
            terminal: false,
            terminal_size: None,
            os_process_id,
            os_start_time,
            output_limit_bytes: 1024,
            cleanup: "retain".to_owned(),
            stdout_truncated: false,
            stderr_truncated: false,
            logs_removed: false,
            cleanup_failed: false,
        }
    }

    #[tokio::test]
    async fn dispatch_intent_is_recovered_without_automatic_redispatch() {
        let root = tempfile::tempdir().expect("root");
        let process_id = Uuid::now_v7().to_string();
        let directory = root.path().join("storage/logs").join(process_id.as_str());
        std::fs::create_dir_all(&directory).expect("directory");
        std::fs::write(directory.join("stdout.log"), []).expect("stdout");
        std::fs::write(directory.join("stderr.log"), []).expect("stderr");
        let mut durable = recovery_record(
            &process_id,
            DurableLifecycle::Dispatching,
            std::env::current_exe().expect("executable"),
            None,
            None,
        );
        persist_durable_record(&directory, &mut durable).expect("persist");

        let dependency = TokioProcessDependency::new(config(root.path())).expect("recover");
        let entry = dependency
            .registry
            .lock()
            .await
            .get(&process_id)
            .cloned()
            .expect("record");
        assert_eq!(
            *entry.recovery_state.read().await,
            DependencyRecoveryState::DispatchUncertain
        );
        assert_eq!(
            entry.snapshot.read().await.state,
            DependencyProcessState::Exited
        );
        assert!(entry.control.is_none());
    }

    #[tokio::test]
    async fn pid_reuse_is_rejected_when_start_time_does_not_match() {
        let root = tempfile::tempdir().expect("root");
        let process_id = Uuid::now_v7().to_string();
        let directory = root.path().join("storage/logs").join(process_id.as_str());
        std::fs::create_dir_all(&directory).expect("directory");
        std::fs::write(directory.join("stdout.log"), []).expect("stdout");
        std::fs::write(directory.join("stderr.log"), []).expect("stderr");
        let observed = inspect_process_identity(std::process::id()).expect("current process");
        let mut durable = recovery_record(
            &process_id,
            DurableLifecycle::Running,
            observed.executable,
            Some(std::process::id()),
            Some(observed.start_time.saturating_add(1)),
        );
        persist_durable_record(&directory, &mut durable).expect("persist");

        let dependency = TokioProcessDependency::new(config(root.path())).expect("recover");
        let entry = dependency
            .registry
            .lock()
            .await
            .get(&process_id)
            .cloned()
            .expect("record");
        assert_eq!(
            *entry.recovery_state.read().await,
            DependencyRecoveryState::RecoveredExited
        );
        assert_eq!(
            entry.snapshot.read().await.state,
            DependencyProcessState::Exited
        );
        assert!(entry.control.is_none());
        assert_eq!(
            load_latest_durable_record(&directory)
                .expect("load")
                .expect("record")
                .lifecycle,
            DurableLifecycle::Exited
        );
    }

    #[tokio::test]
    async fn corrupt_recovery_record_is_quarantined_without_loading_a_process() {
        let root = tempfile::tempdir().expect("root");
        let process_id = Uuid::now_v7().to_string();
        let directory = root.path().join("storage/logs").join(process_id.as_str());
        std::fs::create_dir_all(&directory).expect("directory");
        std::fs::write(directory.join("process-00000000000000000001.json"), b"{")
            .expect("corrupt record");

        let dependency = TokioProcessDependency::new(config(root.path())).expect("recover");
        assert!(dependency.registry.lock().await.is_empty());
        let names: Vec<_> = std::fs::read_dir(&directory)
            .expect("read")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(
            names.iter().any(|name| name.contains(".json.corrupt-")),
            "quarantine file missing: {names:?}"
        );
    }
}
