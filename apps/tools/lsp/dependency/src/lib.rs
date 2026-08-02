//! LSP 3.17 stdio process and JSON-RPC dependency boundary.
//!
//! Implementation choices follow Microsoft's official LSP 3.17 specification:
//! messages use ASCII `Content-Length` headers terminated by `\r\n\r\n` and
//! UTF-8 JSON-RPC bodies; `initialize` is followed by `initialized`; graceful
//! termination is `shutdown` followed by `exit`; cancellation uses the
//! `$/cancelRequest` notification while the original request still receives a
//! response; and diagnostics are collected from
//! `textDocument/publishDiagnostics`. See
//! <https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/>.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use thiserror::Error;

/// Configured language server discovery entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerDefinition {
    /// Stable display ID.
    pub id: String,
    /// Executable path or name passed directly to the operating system.
    pub command: PathBuf,
    /// Arguments passed without shell interpolation.
    pub arguments: Vec<String>,
    /// Supported lowercase file extensions including the leading dot.
    pub extensions: BTreeSet<String>,
    /// LSP language ID.
    pub language_id: String,
    /// Explicit environment additions.
    pub environment: BTreeMap<String, String>,
}

/// Native LSP dependency configuration.
#[derive(Clone, Debug)]
pub struct LspDependencyConfig {
    workspace_root: PathBuf,
    /// Configured server catalogue.
    pub servers: Vec<ServerDefinition>,
    /// Maximum frame body bytes.
    pub max_frame_bytes: usize,
    /// Maximum header bytes.
    pub max_header_bytes: usize,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Mandatory request authorization. `None` denies operation execution.
    pub authorization: Option<AuthorizationConfig>,
}

impl LspDependencyConfig {
    /// Validates and canonicalizes the workspace root and hard bounds.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] when the workspace is unavailable or a
    /// configured bound is zero.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        workspace_root: PathBuf,
        servers: Vec<ServerDefinition>,
        max_frame_bytes: usize,
        request_timeout: Duration,
    ) -> Result<Self, DependencyError> {
        if max_frame_bytes == 0 || request_timeout.is_zero() {
            return Err(DependencyError::InvalidConfiguration(
                "frame bound and timeout must be non-zero",
            ));
        }
        let workspace_root =
            fs::canonicalize(&workspace_root).map_err(|error| DependencyError::Io {
                operation: "canonicalize workspace",
                detail: error.to_string(),
            })?;
        if !workspace_root.is_dir() {
            return Err(DependencyError::InvalidConfiguration(
                "workspace root is not a directory",
            ));
        }
        Ok(Self {
            workspace_root,
            servers,
            max_frame_bytes,
            max_header_bytes: 16 * 1024,
            request_timeout,
            authorization: None,
        })
    }

    /// Canonical workspace root.
    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Installs a mandatory keyed authorization verifier.
    #[must_use]
    pub fn with_authorization(mut self, authorization: AuthorizationConfig) -> Self {
        self.authorization = Some(authorization);
        self
    }
}

/// Dependency-owned authorization trust root.
#[derive(Clone, Debug)]
pub struct AuthorizationConfig {
    /// Expected runtime owner identity.
    pub owner: String,
    /// Expected session identity.
    pub session: String,
    /// BLAKE3 keyed-hash key.
    pub key: [u8; 32],
    /// Maximum accepted future expiry.
    pub maximum_lifetime: Duration,
}

/// Dependency-owned request.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyRequest {
    /// Detect project root by walking upward for known markers.
    DetectProjectRoot {
        /// File or directory within the configured workspace.
        path: String,
    },
    /// Report selected server availability and capabilities.
    Health {
        /// Optional document used for server selection.
        document: Option<String>,
    },
    /// Execute a capability operation.
    Execute {
        /// Request cancellation correlation key.
        cancellation_key: String,
        /// Runtime tool-call identity.
        call_id: String,
        /// Runtime-normalized request digest.
        normalized_digest: String,
        /// Keyed, short-lived, owner/session-bound grant.
        authorization_grant: String,
        /// LSP operation.
        operation: DependencyOperation,
    },
    /// Cancel an active request.
    Cancel {
        /// Correlation key supplied to execute.
        cancellation_key: String,
    },
    /// Gracefully shut down the active language server.
    Shutdown,
}

/// Dependency-owned LSP operation.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyOperation {
    /// Detect a project root within the configured workspace.
    ProjectRoot {
        /// File or directory used as the starting point.
        path: String,
    },
    /// Published diagnostics for an opened document.
    Diagnostics {
        /// Document.
        document: String,
    },
    /// Document symbols.
    DocumentSymbols {
        /// Document.
        document: String,
    },
    /// Workspace symbols.
    WorkspaceSymbols {
        /// Query.
        query: String,
    },
    /// Definition.
    Definition {
        /// Document.
        document: String,
        /// Position.
        position: DependencyPosition,
    },
    /// References.
    References {
        /// Document.
        document: String,
        /// Position.
        position: DependencyPosition,
        /// Include declarations.
        include_declaration: bool,
    },
    /// Hover.
    Hover {
        /// Document.
        document: String,
        /// Position.
        position: DependencyPosition,
    },
    /// Signature help.
    SignatureHelp {
        /// Document.
        document: String,
        /// Position.
        position: DependencyPosition,
    },
    /// Rename proposal.
    Rename {
        /// Document.
        document: String,
        /// Position.
        position: DependencyPosition,
        /// New symbol name.
        new_name: String,
    },
    /// Formatting proposal.
    Formatting {
        /// Document.
        document: String,
        /// Tab size.
        tab_size: u32,
        /// Insert spaces.
        insert_spaces: bool,
    },
    /// Code actions.
    CodeActions {
        /// Document.
        document: String,
        /// Range.
        range: DependencyRange,
        /// Diagnostic codes/messages supplied as context.
        diagnostics: Vec<String>,
    },
}

impl DependencyOperation {
    fn document(&self) -> Option<&str> {
        match self {
            Self::Diagnostics { document }
            | Self::DocumentSymbols { document }
            | Self::Definition { document, .. }
            | Self::References { document, .. }
            | Self::Hover { document, .. }
            | Self::SignatureHelp { document, .. }
            | Self::Rename { document, .. }
            | Self::Formatting { document, .. }
            | Self::CodeActions { document, .. } => Some(document),
            Self::ProjectRoot { .. } | Self::WorkspaceSymbols { .. } => None,
        }
    }
}

/// Zero-based LSP position. Character is a UTF-16 code-unit offset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyPosition {
    /// Zero-based line.
    pub line: u32,
    /// Zero-based UTF-16 character offset.
    pub character: u32,
}

/// LSP range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyRange {
    /// Inclusive start.
    pub start: DependencyPosition,
    /// Exclusive end.
    pub end: DependencyPosition,
}

/// Server availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyAvailability {
    /// Server initialized.
    Ready,
    /// No matching/configured executable is available.
    Unavailable,
}

/// Dependency-owned response.
#[derive(Clone, Debug, PartialEq)]
pub enum DependencyResponse {
    /// Detected root.
    ProjectRoot {
        /// Canonical root.
        root: String,
    },
    /// Health.
    Health {
        /// Availability.
        availability: DependencyAvailability,
        /// Selected server.
        server: Option<String>,
        /// Normalized supported capabilities.
        capabilities: BTreeSet<String>,
        /// Bounded automatic restart count.
        restart_count: u8,
        /// Safe detail.
        detail: String,
    },
    /// Operation unavailable without crashing the host.
    Unavailable {
        /// Safe reason.
        reason: String,
    },
    /// Diagnostics.
    Diagnostics(Vec<DependencyDiagnostic>),
    /// Symbols.
    Symbols(Vec<DependencySymbol>),
    /// Locations.
    Locations(Vec<DependencyLocation>),
    /// Hover.
    Hover(Option<DependencyHover>),
    /// Signature help.
    Signature(Option<DependencySignatureHelp>),
    /// Workspace edit proposal.
    WorkspaceEdit(DependencyWorkspaceEdit),
    /// Text edits.
    TextEdits(Vec<DependencyTextEdit>),
    /// Code actions.
    CodeActions(Vec<DependencyCodeAction>),
    /// Cancellation notification sent.
    Cancelled {
        /// Whether an active request was found.
        active: bool,
    },
    /// Shutdown completed.
    Shutdown,
}

/// Diagnostic record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyDiagnostic {
    /// Canonical document.
    pub path: String,
    /// Range.
    pub range: DependencyRange,
    /// Severity number.
    pub severity: Option<u32>,
    /// Optional code.
    pub code: Option<String>,
    /// Source.
    pub source: Option<String>,
    /// Message.
    pub message: String,
}

/// Symbol record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySymbol {
    /// Name.
    pub name: String,
    /// LSP symbol kind.
    pub kind: u32,
    /// Optional detail.
    pub detail: Option<String>,
    /// Location when supplied.
    pub location: Option<DependencyLocation>,
    /// Selection range for document symbols.
    pub selection_range: Option<DependencyRange>,
}

/// Workspace-contained location.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyLocation {
    /// Canonical file.
    pub path: String,
    /// Range.
    pub range: DependencyRange,
}

/// Hover record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyHover {
    /// Flattened Markdown/plaintext contents.
    pub contents: String,
    /// Optional range.
    pub range: Option<DependencyRange>,
}

/// Signature help record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySignatureHelp {
    /// Signature labels.
    pub signatures: Vec<String>,
    /// Active signature.
    pub active_signature: Option<u32>,
    /// Active parameter.
    pub active_parameter: Option<u32>,
}

/// Text edit proposal. This host never applies it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyTextEdit {
    /// Range.
    pub range: DependencyRange,
    /// Replacement.
    pub new_text: String,
}

/// File-scoped edit proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyFileEdits {
    /// Canonical workspace file.
    pub path: String,
    /// Ordered edits.
    pub edits: Vec<DependencyTextEdit>,
}

/// Workspace edit proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyWorkspaceEdit {
    /// Workspace-contained changes only.
    pub files: Vec<DependencyFileEdits>,
}

/// Code action proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyCodeAction {
    /// Title.
    pub title: String,
    /// Optional kind.
    pub kind: Option<String>,
    /// Optional edit proposal.
    pub edit: Option<DependencyWorkspaceEdit>,
    /// Commands are described but never executed.
    pub command: Option<String>,
}

/// Narrow dependency interface consumed by LSP data.
pub trait LspDependencyPort {
    /// Executes dependency-owned LSP work.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for containment, framing, JSON-RPC,
    /// process, timeout, cancellation, normalization, or lifecycle failures.
    fn execute(&self, request: DependencyRequest) -> Result<DependencyResponse, DependencyError>;
}

/// Native supervised stdio LSP client.
pub struct NativeLspDependency {
    config: LspDependencyConfig,
    supervisor: Mutex<Supervisor>,
    active: Arc<Mutex<BTreeMap<String, ActiveRequest>>>,
    used_grants: Mutex<BTreeSet<String>>,
}

impl NativeLspDependency {
    /// Creates a lazy native dependency. No server process starts until needed.
    #[must_use]
    pub fn new(config: LspDependencyConfig) -> Self {
        Self {
            config,
            supervisor: Mutex::new(Supervisor::default()),
            active: Arc::new(Mutex::new(BTreeMap::new())),
            used_grants: Mutex::new(BTreeSet::new()),
        }
    }

    fn detect_project_root(&self, requested: &str) -> Result<PathBuf, DependencyError> {
        let resolved = self.resolve_workspace_path(requested)?;
        let mut current = if resolved.is_dir() {
            resolved
        } else {
            resolved
                .parent()
                .ok_or(DependencyError::WorkspaceEscape)?
                .to_path_buf()
        };
        let markers = [
            ".git",
            "Cargo.toml",
            "package.json",
            "pyproject.toml",
            "go.mod",
            "pom.xml",
        ];
        loop {
            if markers.iter().any(|marker| current.join(marker).exists()) {
                return Ok(current);
            }
            if current == self.config.workspace_root {
                return Ok(current);
            }
            let Some(parent) = current.parent() else {
                return Ok(self.config.workspace_root.clone());
            };
            if !parent.starts_with(&self.config.workspace_root) {
                return Ok(self.config.workspace_root.clone());
            }
            current = parent.to_path_buf();
        }
    }

    fn resolve_workspace_path(&self, requested: &str) -> Result<PathBuf, DependencyError> {
        if requested.trim().is_empty()
            || Path::new(requested)
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(DependencyError::WorkspaceEscape);
        }
        let candidate = if Path::new(requested).is_absolute() {
            PathBuf::from(requested)
        } else {
            self.config.workspace_root.join(requested)
        };
        let canonical = fs::canonicalize(&candidate).map_err(|error| DependencyError::Io {
            operation: "canonicalize document",
            detail: error.to_string(),
        })?;
        if canonical.starts_with(&self.config.workspace_root) {
            Ok(canonical)
        } else {
            Err(DependencyError::WorkspaceEscape)
        }
    }

    fn selected_server(&self, document: Option<&str>) -> Option<&ServerDefinition> {
        document.map_or_else(
            || self.config.servers.first(),
            |document| {
                let extension = Path::new(document)
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(|value| format!(".{}", value.to_ascii_lowercase()));
                self.config.servers.iter().find(|server| {
                    extension
                        .as_ref()
                        .is_some_and(|value| server.extensions.contains(value))
                })
            },
        )
    }

    fn health(&self, document: Option<&str>) -> Result<DependencyResponse, DependencyError> {
        let Some(server) = self.selected_server(document) else {
            return Ok(DependencyResponse::Health {
                availability: DependencyAvailability::Unavailable,
                server: None,
                capabilities: BTreeSet::new(),
                restart_count: 0,
                detail: "no configured language server matches the request".into(),
            });
        };
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| DependencyError::Poisoned)?;
        if let Some(session) = supervisor.session.as_mut()
            && session.child.try_wait().map_err(process_io)?.is_some()
        {
            supervisor.session = None;
            return Ok(DependencyResponse::Health {
                availability: DependencyAvailability::Unavailable,
                server: Some(server.id.clone()),
                capabilities: BTreeSet::new(),
                restart_count: supervisor.restart_count,
                detail: "language server process exited; next authorized request may restart it"
                    .into(),
            });
        }
        let Some(session) = supervisor.session.as_ref() else {
            return Ok(DependencyResponse::Health {
                availability: DependencyAvailability::Unavailable,
                server: Some(server.id.clone()),
                capabilities: BTreeSet::new(),
                restart_count: supervisor.restart_count,
                detail: "configured; starts lazily after an authorized request".into(),
            });
        };
        Ok(DependencyResponse::Health {
            availability: DependencyAvailability::Ready,
            server: Some(server.id.clone()),
            capabilities: session.capabilities.clone(),
            restart_count: supervisor.restart_count,
            detail: "initialized".into(),
        })
    }

    fn execute_operation(
        &self,
        cancellation_key: &str,
        operation: &DependencyOperation,
    ) -> Result<DependencyResponse, DependencyError> {
        if cancellation_key.trim().is_empty() {
            return Err(DependencyError::InvalidRequest("cancellation key is empty"));
        }
        let resolved_document = operation
            .document()
            .map(|path| self.resolve_workspace_path(path))
            .transpose()?;
        let server = self
            .selected_server(operation.document())
            .ok_or_else(|| DependencyError::ServerUnavailable("no matching server".into()))?
            .clone();
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| DependencyError::Poisoned)?;
        for attempt in 0..=1 {
            supervisor.ensure_session(&self.config, &server)?;
            let result = {
                let session = supervisor.session.as_mut().expect("session initialized");
                let required = operation_capability(operation);
                if !session.capabilities.contains(required) {
                    return Ok(DependencyResponse::Unavailable {
                        reason: format!("selected language server does not advertise {required}"),
                    });
                }
                session.perform(
                    operation,
                    resolved_document.as_deref(),
                    cancellation_key,
                    &self.active,
                    &self.config,
                )
            };
            match result {
                Err(DependencyError::ConnectionClosed) if attempt == 0 => {
                    supervisor.restart(&self.config, &server)?;
                }
                other => return other,
            }
        }
        Err(DependencyError::ConnectionClosed)
    }

    fn authorize(
        &self,
        call_id: &str,
        normalized_digest: &str,
        grant: &str,
        operation: &DependencyOperation,
    ) -> Result<(), DependencyError> {
        let config =
            self.config
                .authorization
                .as_ref()
                .ok_or(DependencyError::AuthorizationDenied(
                    "no authorization trust root is configured",
                ))?;
        let computed = operation_digest(operation)?;
        if !constant_time_eq(computed.as_bytes(), normalized_digest.as_bytes()) {
            return Err(DependencyError::AuthorizationDenied(
                "normalized request digest mismatch",
            ));
        }
        let fields: Vec<_> = grant.split('|').collect();
        if fields.len() != 8 || fields[0] != "v1" {
            return Err(DependencyError::AuthorizationDenied("malformed grant"));
        }
        let expiry = fields[4]
            .parse::<u64>()
            .map_err(|_| DependencyError::AuthorizationDenied("malformed grant expiry"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DependencyError::AuthorizationDenied("system clock invalid"))?
            .as_secs();
        if fields[1] != config.owner
            || fields[2] != config.session
            || fields[3] != call_id
            || fields[6] != normalized_digest
            || expiry < now
            || expiry.saturating_sub(now) > config.maximum_lifetime.as_secs()
        {
            return Err(DependencyError::AuthorizationDenied(
                "grant claims are not valid for this request",
            ));
        }
        let claims = fields[..7].join("|");
        let expected = blake3::keyed_hash(&config.key, claims.as_bytes()).to_hex();
        if !constant_time_eq(expected.as_bytes(), fields[7].as_bytes()) {
            return Err(DependencyError::AuthorizationDenied(
                "grant signature is invalid",
            ));
        }
        let one_time_key = format!("{}|{}", fields[2], fields[5]);
        let mut used = self
            .used_grants
            .lock()
            .map_err(|_| DependencyError::Poisoned)?;
        if !used.insert(one_time_key) {
            return Err(DependencyError::AuthorizationDenied(
                "grant has already been used",
            ));
        }
        Ok(())
    }

    fn cancel(&self, key: &str) -> Result<bool, DependencyError> {
        let active = self.active.lock().map_err(|_| DependencyError::Poisoned)?;
        let Some(request) = active.get(key) else {
            return Ok(false);
        };
        send_message(
            &request.stdin,
            &json!({
                "jsonrpc": "2.0",
                "method": "$/cancelRequest",
                "params": {"id": request.id}
            }),
            self.config.max_frame_bytes,
        )?;
        request.cancelled.store(true, Ordering::SeqCst);
        Ok(true)
    }

    fn shutdown(&self) -> Result<(), DependencyError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| DependencyError::Poisoned)?;
        if let Some(mut session) = supervisor.session.take() {
            session.shutdown(&self.config)?;
        }
        Ok(())
    }
}

impl LspDependencyPort for NativeLspDependency {
    fn execute(&self, request: DependencyRequest) -> Result<DependencyResponse, DependencyError> {
        match request {
            DependencyRequest::DetectProjectRoot { path } => Ok(DependencyResponse::ProjectRoot {
                root: self.detect_project_root(&path)?.display().to_string(),
            }),
            DependencyRequest::Health { document } => self.health(document.as_deref()),
            DependencyRequest::Execute {
                cancellation_key,
                call_id,
                normalized_digest,
                authorization_grant,
                operation,
            } => {
                self.authorize(
                    &call_id,
                    &normalized_digest,
                    &authorization_grant,
                    &operation,
                )?;
                if let DependencyOperation::ProjectRoot { path } = &operation {
                    return Ok(DependencyResponse::ProjectRoot {
                        root: self.detect_project_root(path)?.display().to_string(),
                    });
                }
                match self.execute_operation(&cancellation_key, &operation) {
                    Err(DependencyError::ServerUnavailable(reason)) => {
                        Ok(DependencyResponse::Unavailable { reason })
                    }
                    result => result,
                }
            }
            DependencyRequest::Cancel { cancellation_key } => Ok(DependencyResponse::Cancelled {
                active: self.cancel(&cancellation_key)?,
            }),
            DependencyRequest::Shutdown => {
                self.shutdown()?;
                Ok(DependencyResponse::Shutdown)
            }
        }
    }
}

impl Drop for NativeLspDependency {
    fn drop(&mut self) {
        if let Ok(supervisor) = self.supervisor.get_mut()
            && let Some(mut session) = supervisor.session.take()
        {
            let _ = session.shutdown(&self.config);
        }
    }
}

#[derive(Default)]
struct Supervisor {
    session: Option<Session>,
    server_id: Option<String>,
    restart_count: u8,
}

impl Supervisor {
    fn ensure_session(
        &mut self,
        config: &LspDependencyConfig,
        server: &ServerDefinition,
    ) -> Result<(), DependencyError> {
        if self.session.is_some() && self.server_id.as_deref() == Some(&server.id) {
            return Ok(());
        }
        if let Some(mut old) = self.session.take() {
            let _ = old.shutdown(config);
        }
        self.session = Some(Session::start(config, server)?);
        self.server_id = Some(server.id.clone());
        self.restart_count = 0;
        Ok(())
    }

    fn restart(
        &mut self,
        config: &LspDependencyConfig,
        server: &ServerDefinition,
    ) -> Result<(), DependencyError> {
        if self.restart_count >= 1 {
            return Err(DependencyError::RestartExhausted);
        }
        if let Some(mut session) = self.session.take() {
            session.force_stop();
        }
        self.restart_count += 1;
        self.session = Some(Session::start(config, server)?);
        self.server_id = Some(server.id.clone());
        Ok(())
    }
}

struct ActiveRequest {
    id: u64,
    stdin: Arc<Mutex<ChildStdin>>,
    cancelled: Arc<AtomicBool>,
}

struct Session {
    child: Child,
    stdin: Arc<Mutex<ChildStdin>>,
    incoming: mpsc::Receiver<Result<Value, DependencyError>>,
    next_id: u64,
    capabilities: BTreeSet<String>,
    diagnostics: BTreeMap<String, Vec<DependencyDiagnostic>>,
}

impl Session {
    fn start(
        config: &LspDependencyConfig,
        server: &ServerDefinition,
    ) -> Result<Self, DependencyError> {
        let mut command = Command::new(&server.command);
        command
            .args(&server.arguments)
            .current_dir(&config.workspace_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .envs(&server.environment);
        let mut child = command
            .spawn()
            .map_err(|error| DependencyError::ServerUnavailable(error.to_string()))?;
        let stdin = Arc::new(Mutex::new(
            child.stdin.take().ok_or(DependencyError::MissingPipe)?,
        ));
        let stdout = child.stdout.take().ok_or(DependencyError::MissingPipe)?;
        let (sender, incoming) = mpsc::channel();
        let frame_limit = config.max_frame_bytes;
        let header_limit = config.max_header_bytes;
        thread::Builder::new()
            .name(format!("lsp-reader-{}", server.id))
            .spawn(move || {
                let mut reader = BufReader::new(stdout);
                loop {
                    let frame = read_frame(&mut reader, frame_limit, header_limit);
                    let terminal = frame.is_err();
                    if sender.send(frame).is_err() || terminal {
                        break;
                    }
                }
            })
            .map_err(|error| DependencyError::Io {
                operation: "spawn reader thread",
                detail: error.to_string(),
            })?;
        let mut session = Self {
            child,
            stdin,
            incoming,
            next_id: 1,
            capabilities: BTreeSet::new(),
            diagnostics: BTreeMap::new(),
        };
        let root_uri = path_to_uri(&config.workspace_root);
        let result = session.request_raw(
            "initialize",
            json!({
                "processId": std::process::id(),
                "clientInfo": {"name": "agentmod-lsp-host", "version": env!("CARGO_PKG_VERSION")},
                "locale": "en",
                "rootUri": root_uri,
                "capabilities": {
                    "workspace": {"symbol": {}},
                    "textDocument": {
                        "publishDiagnostics": {},
                        "documentSymbol": {},
                        "definition": {},
                        "references": {},
                        "hover": {"contentFormat": ["markdown", "plaintext"]},
                        "signatureHelp": {},
                        "rename": {},
                        "formatting": {},
                        "codeAction": {}
                    }
                },
                "workspaceFolders": [{"uri": root_uri, "name": "workspace"}]
            }),
            None,
            None,
            config,
        )?;
        session.capabilities = normalize_capabilities(result.get("capabilities"));
        // Servers publish diagnostics through a notification rather than an
        // advertised provider capability. LSP 3.17's `diagnosticProvider`
        // describes the optional pull-diagnostics extension.
        session.capabilities.insert("diagnostics".into());
        session.notify("initialized", json!({}), config.max_frame_bytes)?;
        Ok(session)
    }

    #[allow(clippy::too_many_lines)]
    fn perform(
        &mut self,
        operation: &DependencyOperation,
        document: Option<&Path>,
        cancellation_key: &str,
        active: &Arc<Mutex<BTreeMap<String, ActiveRequest>>>,
        config: &LspDependencyConfig,
    ) -> Result<DependencyResponse, DependencyError> {
        match operation {
            DependencyOperation::ProjectRoot { .. } => Err(DependencyError::InvalidRequest(
                "project-root operation does not use a language server",
            )),
            DependencyOperation::Diagnostics { .. } => {
                let document = document.expect("document operation resolved");
                let uri = path_to_uri(document);
                let text = fs::read_to_string(document).map_err(|error| DependencyError::Io {
                    operation: "read document",
                    detail: error.to_string(),
                })?;
                self.diagnostics.remove(&uri);
                self.notify(
                    "textDocument/didOpen",
                    json!({
                        "textDocument": {
                            "uri": uri,
                            "languageId": language_id(document, config),
                            "version": 1,
                            "text": text
                        }
                    }),
                    config.max_frame_bytes,
                )?;
                self.wait_for_diagnostics(&uri, config)
                    .map(DependencyResponse::Diagnostics)
            }
            DependencyOperation::DocumentSymbols { .. } => {
                let uri = path_to_uri(document.expect("document"));
                let result = self.request_raw(
                    "textDocument/documentSymbol",
                    json!({"textDocument":{"uri":uri}}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_symbols(&result, &config.workspace_root).map(DependencyResponse::Symbols)
            }
            DependencyOperation::WorkspaceSymbols { query } => {
                let result = self.request_raw(
                    "workspace/symbol",
                    json!({"query":query}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_symbols(&result, &config.workspace_root).map(DependencyResponse::Symbols)
            }
            DependencyOperation::Definition { position, .. } => self.location_request(
                "textDocument/definition",
                document,
                *position,
                cancellation_key,
                active,
                config,
            ),
            DependencyOperation::References {
                position,
                include_declaration,
                ..
            } => {
                let uri = path_to_uri(document.expect("document"));
                let result = self.request_raw(
                    "textDocument/references",
                    json!({"textDocument":{"uri":uri},"position":position_json(*position),"context":{"includeDeclaration":include_declaration}}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_locations(&result, &config.workspace_root)
                    .map(DependencyResponse::Locations)
            }
            DependencyOperation::Hover { position, .. } => {
                let uri = path_to_uri(document.expect("document"));
                let result = self.request_raw(
                    "textDocument/hover",
                    json!({"textDocument":{"uri":uri},"position":position_json(*position)}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_hover(&result).map(DependencyResponse::Hover)
            }
            DependencyOperation::SignatureHelp { position, .. } => {
                let uri = path_to_uri(document.expect("document"));
                let result = self.request_raw(
                    "textDocument/signatureHelp",
                    json!({"textDocument":{"uri":uri},"position":position_json(*position)}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_signature(&result).map(DependencyResponse::Signature)
            }
            DependencyOperation::Rename {
                position, new_name, ..
            } => {
                let uri = path_to_uri(document.expect("document"));
                let result = self.request_raw(
                    "textDocument/rename",
                    json!({"textDocument":{"uri":uri},"position":position_json(*position),"newName":new_name}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_workspace_edit(&result, &config.workspace_root)
                    .map(DependencyResponse::WorkspaceEdit)
            }
            DependencyOperation::Formatting {
                tab_size,
                insert_spaces,
                ..
            } => {
                let uri = path_to_uri(document.expect("document"));
                let result = self.request_raw(
                    "textDocument/formatting",
                    json!({"textDocument":{"uri":uri},"options":{"tabSize":tab_size,"insertSpaces":insert_spaces}}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_text_edits(&result).map(DependencyResponse::TextEdits)
            }
            DependencyOperation::CodeActions {
                range, diagnostics, ..
            } => {
                let uri = path_to_uri(document.expect("document"));
                let context: Vec<_> = diagnostics
                    .iter()
                    .map(|message| json!({"range":range_json(*range),"message":message}))
                    .collect();
                let result = self.request_raw(
                    "textDocument/codeAction",
                    json!({"textDocument":{"uri":uri},"range":range_json(*range),"context":{"diagnostics":context}}),
                    Some(cancellation_key),
                    Some(active),
                    config,
                )?;
                normalize_code_actions(&result, &config.workspace_root)
                    .map(DependencyResponse::CodeActions)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn location_request(
        &mut self,
        method: &str,
        document: Option<&Path>,
        position: DependencyPosition,
        cancellation_key: &str,
        active: &Arc<Mutex<BTreeMap<String, ActiveRequest>>>,
        config: &LspDependencyConfig,
    ) -> Result<DependencyResponse, DependencyError> {
        let uri = path_to_uri(document.expect("document"));
        let result = self.request_raw(
            method,
            json!({"textDocument":{"uri":uri},"position":position_json(position)}),
            Some(cancellation_key),
            Some(active),
            config,
        )?;
        normalize_locations(&result, &config.workspace_root).map(DependencyResponse::Locations)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn request_raw(
        &mut self,
        method: &str,
        params: Value,
        cancellation_key: Option<&str>,
        active: Option<&Arc<Mutex<BTreeMap<String, ActiveRequest>>>>,
        config: &LspDependencyConfig,
    ) -> Result<Value, DependencyError> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(DependencyError::IdOverflow)?;
        send_message(
            &self.stdin,
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            config.max_frame_bytes,
        )?;
        let cancelled = Arc::new(AtomicBool::new(false));
        if let (Some(key), Some(active)) = (cancellation_key, active) {
            active
                .lock()
                .map_err(|_| DependencyError::Poisoned)?
                .insert(
                    key.to_owned(),
                    ActiveRequest {
                        id,
                        stdin: Arc::clone(&self.stdin),
                        cancelled: Arc::clone(&cancelled),
                    },
                );
        }
        let deadline = Instant::now() + config.request_timeout;
        let result = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let _ = send_message(
                    &self.stdin,
                    &json!({"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":id}}),
                    config.max_frame_bytes,
                );
                break Err(DependencyError::Timeout);
            }
            let message =
                self.incoming
                    .recv_timeout(remaining)
                    .map_err(|error| match error {
                        mpsc::RecvTimeoutError::Timeout => DependencyError::Timeout,
                        mpsc::RecvTimeoutError::Disconnected => DependencyError::ConnectionClosed,
                    })??;
            if message.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = message.get("error") {
                    break Err(DependencyError::Rpc {
                        code: error.get("code").and_then(Value::as_i64).unwrap_or(-32603),
                        message: error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("language server error")
                            .to_owned(),
                    });
                }
                break Ok(message.get("result").cloned().unwrap_or(Value::Null));
            }
            self.capture_notification(&message, &config.workspace_root)?;
        };
        if let (Some(key), Some(active)) = (cancellation_key, active) {
            active
                .lock()
                .map_err(|_| DependencyError::Poisoned)?
                .remove(key);
        }
        if cancelled.load(Ordering::SeqCst) {
            Err(DependencyError::Cancelled)
        } else {
            result
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn notify(
        &self,
        method: &str,
        params: Value,
        max_frame_bytes: usize,
    ) -> Result<(), DependencyError> {
        send_message(
            &self.stdin,
            &json!({"jsonrpc":"2.0","method":method,"params":params}),
            max_frame_bytes,
        )
    }

    fn wait_for_diagnostics(
        &mut self,
        uri: &str,
        config: &LspDependencyConfig,
    ) -> Result<Vec<DependencyDiagnostic>, DependencyError> {
        let deadline = Instant::now() + config.request_timeout;
        loop {
            if let Some(diagnostics) = self.diagnostics.remove(uri) {
                return Ok(diagnostics);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let message =
                self.incoming
                    .recv_timeout(remaining)
                    .map_err(|error| match error {
                        mpsc::RecvTimeoutError::Timeout => DependencyError::Timeout,
                        mpsc::RecvTimeoutError::Disconnected => DependencyError::ConnectionClosed,
                    })??;
            self.capture_notification(&message, &config.workspace_root)?;
        }
    }

    fn capture_notification(
        &mut self,
        message: &Value,
        workspace: &Path,
    ) -> Result<(), DependencyError> {
        if message.get("method").and_then(Value::as_str) == Some("textDocument/publishDiagnostics")
        {
            let params = message
                .get("params")
                .ok_or(DependencyError::MalformedResponse)?;
            let uri = required_str(params, "uri")?.to_owned();
            let path = contained_uri(&uri, workspace)?;
            let diagnostics = params
                .get("diagnostics")
                .and_then(Value::as_array)
                .ok_or(DependencyError::MalformedResponse)?
                .iter()
                .map(|value| normalize_diagnostic(value, &path))
                .collect::<Result<Vec<_>, _>>()?;
            self.diagnostics.insert(uri, diagnostics);
        }
        Ok(())
    }

    fn shutdown(&mut self, config: &LspDependencyConfig) -> Result<(), DependencyError> {
        if self.child.try_wait().map_err(process_io)?.is_some() {
            return Ok(());
        }
        let _ = self.request_raw("shutdown", json!({}), None, None, config);
        let _ = self.notify("exit", json!({}), config.max_frame_bytes);
        let deadline = Instant::now() + config.request_timeout;
        while Instant::now() < deadline {
            if self.child.try_wait().map_err(process_io)?.is_some() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(5));
        }
        self.force_stop();
        Ok(())
    }

    fn force_stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn send_message(
    stdin: &Arc<Mutex<ChildStdin>>,
    message: &Value,
    max_frame_bytes: usize,
) -> Result<(), DependencyError> {
    let body =
        serde_json::to_vec(message).map_err(|error| DependencyError::Json(error.to_string()))?;
    if body.len() > max_frame_bytes {
        return Err(DependencyError::FrameTooLarge {
            actual: body.len(),
            maximum: max_frame_bytes,
        });
    }
    let mut stdin = stdin.lock().map_err(|_| DependencyError::Poisoned)?;
    write!(stdin, "Content-Length: {}\r\n\r\n", body.len())
        .and_then(|()| stdin.write_all(&body))
        .and_then(|()| stdin.flush())
        .map_err(|error| DependencyError::Io {
            operation: "write server stdin",
            detail: error.to_string(),
        })
}

fn read_frame<R: BufRead>(
    reader: &mut R,
    max_frame_bytes: usize,
    max_header_bytes: usize,
) -> Result<Value, DependencyError> {
    let mut content_length = None;
    let mut header_bytes = 0;
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .map_err(|error| DependencyError::Io {
                operation: "read LSP header",
                detail: error.to_string(),
            })?;
        if read == 0 {
            return Err(DependencyError::ConnectionClosed);
        }
        header_bytes += read;
        if header_bytes > max_header_bytes {
            return Err(DependencyError::HeaderTooLarge);
        }
        if line == "\r\n" {
            break;
        }
        if !line.ends_with("\r\n") {
            return Err(DependencyError::MalformedHeader);
        }
        let (name, value) = line[..line.len() - 2]
            .split_once(": ")
            .ok_or(DependencyError::MalformedHeader)?;
        if name.eq_ignore_ascii_case("Content-Length") {
            if content_length.is_some() {
                return Err(DependencyError::MalformedHeader);
            }
            content_length = Some(
                value
                    .parse::<usize>()
                    .map_err(|_| DependencyError::MalformedHeader)?,
            );
        } else if name.eq_ignore_ascii_case("Content-Type") {
            let lower = value.to_ascii_lowercase();
            if lower.contains("charset=") && !lower.contains("utf-8") && !lower.contains("utf8") {
                return Err(DependencyError::UnsupportedEncoding);
            }
        }
    }
    let length = content_length.ok_or(DependencyError::MissingContentLength)?;
    if length > max_frame_bytes {
        return Err(DependencyError::FrameTooLarge {
            actual: length,
            maximum: max_frame_bytes,
        });
    }
    let mut body = vec![0; length];
    reader
        .read_exact(&mut body)
        .map_err(|error| DependencyError::Io {
            operation: "read LSP body",
            detail: error.to_string(),
        })?;
    serde_json::from_slice(&body).map_err(|error| DependencyError::Json(error.to_string()))
}

fn normalize_capabilities(value: Option<&Value>) -> BTreeSet<String> {
    let Some(object) = value.and_then(Value::as_object) else {
        return BTreeSet::new();
    };
    [
        ("diagnostics", "diagnosticProvider"),
        ("document_symbols", "documentSymbolProvider"),
        ("workspace_symbols", "workspaceSymbolProvider"),
        ("definition", "definitionProvider"),
        ("references", "referencesProvider"),
        ("hover", "hoverProvider"),
        ("signature_help", "signatureHelpProvider"),
        ("rename", "renameProvider"),
        ("formatting", "documentFormattingProvider"),
        ("code_actions", "codeActionProvider"),
    ]
    .into_iter()
    .filter_map(|(normalized, key)| {
        object
            .get(key)
            .filter(|value| !value.is_null() && value.as_bool() != Some(false))
            .map(|_| normalized.to_owned())
    })
    .collect()
}

fn normalize_diagnostic(
    value: &Value,
    path: &Path,
) -> Result<DependencyDiagnostic, DependencyError> {
    Ok(DependencyDiagnostic {
        path: path.display().to_string(),
        range: parse_range(
            value
                .get("range")
                .ok_or(DependencyError::MalformedResponse)?,
        )?,
        severity: value
            .get("severity")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok()),
        code: value.get("code").and_then(value_to_string),
        source: value
            .get("source")
            .and_then(Value::as_str)
            .map(str::to_owned),
        message: required_str(value, "message")?.to_owned(),
    })
}

fn normalize_symbols(
    value: &Value,
    workspace: &Path,
) -> Result<Vec<DependencySymbol>, DependencyError> {
    value
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|symbol| {
            let location = symbol
                .get("location")
                .map(|location| normalize_location(location, workspace))
                .transpose()?;
            Ok(DependencySymbol {
                name: required_str(symbol, "name")?.to_owned(),
                kind: required_u32(symbol, "kind")?,
                detail: symbol
                    .get("detail")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                location,
                selection_range: symbol.get("selectionRange").map(parse_range).transpose()?,
            })
        })
        .collect()
}

fn normalize_locations(
    value: &Value,
    workspace: &Path,
) -> Result<Vec<DependencyLocation>, DependencyError> {
    if value.is_null() {
        return Ok(Vec::new());
    }
    if value.is_array() {
        return value
            .as_array()
            .expect("array")
            .iter()
            .map(|location| normalize_location(location, workspace))
            .collect();
    }
    Ok(vec![normalize_location(value, workspace)?])
}

fn normalize_location(
    value: &Value,
    workspace: &Path,
) -> Result<DependencyLocation, DependencyError> {
    let uri = required_str(value, "uri")?;
    Ok(DependencyLocation {
        path: contained_uri(uri, workspace)?.display().to_string(),
        range: parse_range(
            value
                .get("range")
                .ok_or(DependencyError::MalformedResponse)?,
        )?,
    })
}

fn normalize_hover(value: &Value) -> Result<Option<DependencyHover>, DependencyError> {
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(DependencyHover {
        contents: flatten_markup(
            value
                .get("contents")
                .ok_or(DependencyError::MalformedResponse)?,
        ),
        range: value.get("range").map(parse_range).transpose()?,
    }))
}

fn normalize_signature(value: &Value) -> Result<Option<DependencySignatureHelp>, DependencyError> {
    if value.is_null() {
        return Ok(None);
    }
    let signatures = value
        .get("signatures")
        .and_then(Value::as_array)
        .ok_or(DependencyError::MalformedResponse)?
        .iter()
        .map(|signature| required_str(signature, "label").map(str::to_owned))
        .collect::<Result<_, _>>()?;
    Ok(Some(DependencySignatureHelp {
        signatures,
        active_signature: optional_u32(value, "activeSignature"),
        active_parameter: optional_u32(value, "activeParameter"),
    }))
}

fn normalize_workspace_edit(
    value: &Value,
    workspace: &Path,
) -> Result<DependencyWorkspaceEdit, DependencyError> {
    if value.is_null() {
        return Ok(DependencyWorkspaceEdit { files: Vec::new() });
    }
    if value.get("documentChanges").is_some() {
        return Err(DependencyError::UnsupportedWorkspaceEdit(
            "documentChanges and resource operations require separate runtime handling",
        ));
    }
    let mut files = Vec::new();
    if let Some(changes) = value.get("changes").and_then(Value::as_object) {
        for (uri, edits) in changes {
            files.push(DependencyFileEdits {
                path: contained_uri(uri, workspace)?.display().to_string(),
                edits: normalize_text_edits(edits)?,
            });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(DependencyWorkspaceEdit { files })
}

fn normalize_text_edits(value: &Value) -> Result<Vec<DependencyTextEdit>, DependencyError> {
    value
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|edit| {
            Ok(DependencyTextEdit {
                range: parse_range(
                    edit.get("range")
                        .ok_or(DependencyError::MalformedResponse)?,
                )?,
                new_text: required_str(edit, "newText")?.to_owned(),
            })
        })
        .collect()
}

fn normalize_code_actions(
    value: &Value,
    workspace: &Path,
) -> Result<Vec<DependencyCodeAction>, DependencyError> {
    value
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|action| {
            Ok(DependencyCodeAction {
                title: required_str(action, "title")?.to_owned(),
                kind: action
                    .get("kind")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                edit: action
                    .get("edit")
                    .map(|edit| normalize_workspace_edit(edit, workspace))
                    .transpose()?,
                command: action.get("command").and_then(|command| {
                    command
                        .as_str()
                        .or_else(|| command.get("command").and_then(Value::as_str))
                        .map(str::to_owned)
                }),
            })
        })
        .collect()
}

fn parse_range(value: &Value) -> Result<DependencyRange, DependencyError> {
    Ok(DependencyRange {
        start: parse_position(
            value
                .get("start")
                .ok_or(DependencyError::MalformedResponse)?,
        )?,
        end: parse_position(value.get("end").ok_or(DependencyError::MalformedResponse)?)?,
    })
}

fn parse_position(value: &Value) -> Result<DependencyPosition, DependencyError> {
    Ok(DependencyPosition {
        line: required_u32(value, "line")?,
        character: required_u32(value, "character")?,
    })
}

fn position_json(position: DependencyPosition) -> Value {
    json!({"line":position.line,"character":position.character})
}

fn range_json(range: DependencyRange) -> Value {
    json!({"start":position_json(range.start),"end":position_json(range.end)})
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, DependencyError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(DependencyError::MalformedResponse)
}

fn required_u32(value: &Value, key: &str) -> Result<u32, DependencyError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(DependencyError::MalformedResponse)
}

fn optional_u32(value: &Value, key: &str) -> Option<u32> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn value_to_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
}

fn flatten_markup(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(flatten_markup)
            .collect::<Vec<_>>()
            .join("\n\n"),
        Value::Object(object) => object
            .get("value")
            .or_else(|| object.get("language"))
            .map_or_else(String::new, flatten_markup),
        _ => String::new(),
    }
}

fn language_id(document: &Path, config: &LspDependencyConfig) -> String {
    let extension = document
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!(".{}", value.to_ascii_lowercase()));
    config
        .servers
        .iter()
        .find(|server| {
            extension
                .as_ref()
                .is_some_and(|value| server.extensions.contains(value))
        })
        .map_or_else(|| "plaintext".into(), |server| server.language_id.clone())
}

fn operation_capability(operation: &DependencyOperation) -> &'static str {
    match operation {
        DependencyOperation::ProjectRoot { .. } => "project_root",
        DependencyOperation::Diagnostics { .. } => "diagnostics",
        DependencyOperation::DocumentSymbols { .. } => "document_symbols",
        DependencyOperation::WorkspaceSymbols { .. } => "workspace_symbols",
        DependencyOperation::Definition { .. } => "definition",
        DependencyOperation::References { .. } => "references",
        DependencyOperation::Hover { .. } => "hover",
        DependencyOperation::SignatureHelp { .. } => "signature_help",
        DependencyOperation::Rename { .. } => "rename",
        DependencyOperation::Formatting { .. } => "formatting",
        DependencyOperation::CodeActions { .. } => "code_actions",
    }
}

fn path_to_uri(path: &Path) -> String {
    let mut rendered = path.display().to_string().replace('\\', "/");
    if let Some(value) = rendered.strip_prefix("//?/") {
        rendered = value.to_owned();
    }
    let encoded = percent_encode(&rendered);
    if encoded.starts_with('/') {
        format!("file://{encoded}")
    } else {
        format!("file:///{encoded}")
    }
}

fn contained_uri(uri: &str, workspace: &Path) -> Result<PathBuf, DependencyError> {
    let encoded = uri
        .strip_prefix("file://")
        .ok_or(DependencyError::UnsupportedUri)?;
    let decoded = percent_decode(encoded)?;
    #[cfg(windows)]
    let decoded = if decoded.starts_with('/') && decoded.as_bytes().get(2) == Some(&b':') {
        decoded[1..].to_owned()
    } else {
        decoded
    };
    let canonical =
        fs::canonicalize(PathBuf::from(decoded)).map_err(|_| DependencyError::WorkspaceEscape)?;
    if canonical.starts_with(workspace) {
        Ok(canonical)
    } else {
        Err(DependencyError::WorkspaceEscape)
    }
}

fn percent_encode(value: &str) -> String {
    let mut result = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~/:".contains(&byte) {
            result.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(result, "%{byte:02X}");
        }
    }
    result
}

fn percent_decode(value: &str) -> Result<String, DependencyError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(DependencyError::UnsupportedUri);
            }
            let text = std::str::from_utf8(&bytes[index + 1..index + 3])
                .map_err(|_| DependencyError::UnsupportedUri)?;
            decoded
                .push(u8::from_str_radix(text, 16).map_err(|_| DependencyError::UnsupportedUri)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| DependencyError::UnsupportedUri)
}

/// Computes the canonical digest expected in an execution envelope.
///
/// The digest is lowercase BLAKE3 over a stable JSON object containing the
/// normalized operation. This function is also used by trusted runtime-side
/// grant issuers; the dependency repeats the calculation immediately before
/// any language-server interaction.
///
/// # Errors
///
/// Returns [`DependencyError::Json`] if canonical JSON encoding fails.
pub fn operation_digest(operation: &DependencyOperation) -> Result<String, DependencyError> {
    let value = match operation {
        DependencyOperation::ProjectRoot { path } => {
            json!({"operation":"project_root","path":path})
        }
        DependencyOperation::Diagnostics { document } => {
            json!({"operation":"diagnostics","document":document})
        }
        DependencyOperation::DocumentSymbols { document } => {
            json!({"operation":"document_symbols","document":document})
        }
        DependencyOperation::WorkspaceSymbols { query } => {
            json!({"operation":"workspace_symbols","query":query})
        }
        DependencyOperation::Definition { document, position } => {
            json!({"operation":"definition","document":document,"position":position_json(*position)})
        }
        DependencyOperation::References {
            document,
            position,
            include_declaration,
        } => {
            json!({"operation":"references","document":document,"position":position_json(*position),"include_declaration":include_declaration})
        }
        DependencyOperation::Hover { document, position } => {
            json!({"operation":"hover","document":document,"position":position_json(*position)})
        }
        DependencyOperation::SignatureHelp { document, position } => {
            json!({"operation":"signature_help","document":document,"position":position_json(*position)})
        }
        DependencyOperation::Rename {
            document,
            position,
            new_name,
        } => {
            json!({"operation":"rename","document":document,"position":position_json(*position),"new_name":new_name})
        }
        DependencyOperation::Formatting {
            document,
            tab_size,
            insert_spaces,
        } => {
            json!({"operation":"formatting","document":document,"tab_size":tab_size,"insert_spaces":insert_spaces})
        }
        DependencyOperation::CodeActions {
            document,
            range,
            diagnostics,
        } => {
            json!({"operation":"code_actions","document":document,"range":range_json(*range),"diagnostics":diagnostics})
        }
    };
    serde_json::to_vec(&value)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|error| DependencyError::Json(error.to_string()))
}

/// Creates a grant with the exact format accepted by the dependency verifier.
///
/// Production callers should keep the key in a secret store and issue grants
/// only after the mandatory runtime permission pipeline succeeds.
#[must_use]
pub fn issue_authorization_grant(
    config: &AuthorizationConfig,
    call_id: &str,
    digest: &str,
    nonce: &str,
    expires_at_epoch_seconds: u64,
) -> String {
    let claims = format!(
        "v1|{}|{}|{call_id}|{expires_at_epoch_seconds}|{nonce}|{digest}",
        config.owner, config.session
    );
    let signature = blake3::keyed_hash(&config.key, claims.as_bytes()).to_hex();
    format!("{claims}|{signature}")
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[allow(clippy::needless_pass_by_value)]
fn process_io(error: std::io::Error) -> DependencyError {
    DependencyError::Io {
        operation: "inspect language server process",
        detail: error.to_string(),
    }
}

/// Dependency-layer failure. Messages never include source text or complete LSP
/// payloads, keeping normal diagnostics redacted.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DependencyError {
    /// Configuration invalid.
    #[error("invalid LSP configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// Request invalid.
    #[error("invalid LSP request: {0}")]
    InvalidRequest(&'static str),
    /// Authorization was absent, stale, mismatched, replayed, or invalid.
    #[error("LSP authorization denied: {0}")]
    AuthorizationDenied(&'static str),
    /// No server available.
    #[error("language server unavailable: {0}")]
    ServerUnavailable(String),
    /// Required stdio pipe absent.
    #[error("language server did not expose required stdio pipes")]
    MissingPipe,
    /// Shared state poisoned.
    #[error("language server state lock was poisoned")]
    Poisoned,
    /// Workspace containment violation.
    #[error("LSP URI or path escapes the configured workspace")]
    WorkspaceEscape,
    /// URI scheme/encoding unsupported.
    #[error("unsupported LSP URI")]
    UnsupportedUri,
    /// Content-Length absent.
    #[error("LSP frame is missing Content-Length")]
    MissingContentLength,
    /// Header malformed.
    #[error("malformed LSP header")]
    MalformedHeader,
    /// Header bound exceeded.
    #[error("LSP header exceeds configured bound")]
    HeaderTooLarge,
    /// Frame body bound exceeded.
    #[error("LSP frame is {actual} bytes; maximum is {maximum}")]
    FrameTooLarge {
        /// Actual.
        actual: usize,
        /// Maximum.
        maximum: usize,
    },
    /// Encoding unsupported.
    #[error("LSP content encoding is not UTF-8")]
    UnsupportedEncoding,
    /// JSON invalid.
    #[error("invalid LSP JSON: {0}")]
    Json(String),
    /// External result shape invalid.
    #[error("malformed language server response")]
    MalformedResponse,
    /// Server RPC error.
    #[error("language server RPC error {code}: {message}")]
    Rpc {
        /// Code.
        code: i64,
        /// Redacted server message.
        message: String,
    },
    /// Request timed out.
    #[error("language server request timed out")]
    Timeout,
    /// Explicit cancellation observed.
    #[error("language server request was cancelled")]
    Cancelled,
    /// Connection closed.
    #[error("language server connection closed")]
    ConnectionClosed,
    /// Restart budget exhausted.
    #[error("language server automatic restart budget exhausted")]
    RestartExhausted,
    /// ID overflow.
    #[error("language server request ID overflow")]
    IdOverflow,
    /// Workspace edit form intentionally not applied/accepted.
    #[error("unsupported workspace edit: {0}")]
    UnsupportedWorkspaceEdit(&'static str),
    /// Bounded OS/process/file error.
    #[error("{operation} failed: {detail}")]
    Io {
        /// Operation without private content.
        operation: &'static str,
        /// Redacted detail.
        detail: String,
    },
}

#[cfg(test)]
mod framing_tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn reads_content_length_frame_and_rejects_malformed_or_oversized_frames() {
        let body = br#"{"jsonrpc":"2.0","id":1,"result":null}"#;
        let input = format!("Content-Length: {}\r\n\r\n", body.len())
            .into_bytes()
            .into_iter()
            .chain(body.iter().copied())
            .collect::<Vec<_>>();
        let parsed = read_frame(&mut Cursor::new(input), 1024, 1024).expect("frame");
        assert_eq!(parsed["id"], 1);

        assert_eq!(
            read_frame(
                &mut Cursor::new(b"Content-Type: application/json\r\n\r\n{}".to_vec()),
                1024,
                1024
            ),
            Err(DependencyError::MissingContentLength)
        );
        assert!(matches!(
            read_frame(
                &mut Cursor::new(b"Content-Length: 99\r\n\r\n".to_vec()),
                8,
                1024
            ),
            Err(DependencyError::FrameTooLarge { .. })
        ));
        assert_eq!(
            read_frame(
                &mut Cursor::new(b"Content-Length: 2\n\n{}".to_vec()),
                1024,
                1024
            ),
            Err(DependencyError::MalformedHeader)
        );
    }
}
