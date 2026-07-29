//! CLI endpoint parsing, service mapping, and rendering.

use std::ffi::OsString;

use agentmod_cli_logic::{
    BranchSessionCommand, BranchSessionResult, CancelTurnCommand, CliLogicPort,
    CreateSessionBudgetCommand, CreateSessionCommand, DeferredScheduleCommand, DoctorResult,
    DoctorState, HarnessDescriptorResult, InspectSessionCommand, InspectSessionResult,
    ListSessionsCommand, ResolveApprovalCommand, ResolveApprovalResult, RunDoctorCommand,
    RunTurnCommand, RunTurnResult, RunTurnStream, RunTurnStreamItem,
    ScheduleCommand as LogicScheduleCommand, SchedulePayload, ScheduleResult, ScheduleTrigger,
    SessionEventPageResult, SessionSummaryResult, StyleAvailability, StyleDiagnostic,
    StyleFileCommand, StyleInspectionResult, StyleSourceKind, StyleSummaryResult,
    StyleValidationResult, SubscribeSessionCommand, TurnEvent,
};
use clap::{Parser, Subcommand, ValueEnum};
use thiserror::Error;

/// Clap-owned external command-line request.
#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[command(name = "agentmod", version, about = "Event-driven developer agent")]
pub struct CliArguments {
    /// CLI operation.
    #[command(subcommand)]
    pub command: CliCommand,
}

/// Clap-owned command variants.
#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
#[allow(
    clippy::large_enum_variant,
    reason = "Clap owns this cold-path parsed command and boxing would leak allocation into endpoint mapping"
)]
pub enum CliCommand {
    /// Run one durable agent turn.
    Run {
        /// Exact user prompt.
        prompt: String,
        /// Existing durable session ID.
        #[arg(long)]
        session: String,
        /// Provider adapter.
        #[arg(long, default_value = "deterministic-mock")]
        provider: String,
        /// Model ID.
        #[arg(long, default_value = "mock-model")]
        model: String,
        /// Provider option in `key=value` form; repeatable.
        #[arg(long = "option")]
        options: Vec<String>,
        /// Caller-selected cancellation ID for cross-process cancellation.
        #[arg(long)]
        cancellation_id: Option<String>,
        /// Emit structured JSON.
        #[arg(long, conflicts_with = "stream_json")]
        json: bool,
        /// Emit one JSON object per committed runtime stream item.
        #[arg(long, conflicts_with = "json")]
        stream_json: bool,
    },
    /// Check runtime health and CLI integration.
    Doctor {
        /// Return stable JSON instead of human-readable text.
        #[arg(long)]
        json: bool,
        /// Return a failing exit status when the runtime is degraded.
        #[arg(long)]
        strict: bool,
    },
    /// Manage durable runtime sessions.
    Session {
        /// Session operation.
        #[command(subcommand)]
        command: SessionCommand,
    },
    /// Resolve a durable approval request.
    Approval {
        /// Approval operation.
        #[command(subcommand)]
        command: ApprovalCommand,
    },
    /// Manage durable background schedules.
    Schedule {
        /// Schedule operation.
        #[command(subcommand)]
        command: ScheduleCommand,
    },
    /// Inspect and validate session style manifests.
    Style {
        /// Style operation.
        #[command(subcommand)]
        command: StyleCommand,
    },
    /// Inspect registered harness adapters.
    Harness {
        /// Harness operation.
        #[command(subcommand)]
        command: HarnessCommand,
    },
    /// Cancel one active provider request.
    Cancel {
        /// Cancellation ID supplied to `run`.
        cancellation_id: String,
        /// Safe audit reason.
        #[arg(long, default_value = "cancelled by user")]
        reason: String,
        /// Emit structured JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Durable schedule CLI operations.
#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
#[allow(
    clippy::large_enum_variant,
    reason = "Clap owns this cold-path parsed command and explicit fields preserve endpoint isolation"
)]
pub enum ScheduleCommand {
    /// Create or replace a time-, event-, or process-output-triggered prompt schedule.
    Add {
        /// Stable schedule identifier.
        schedule_id: String,
        /// Existing target session UUID.
        #[arg(long)]
        session: String,
        /// Scheduled prompt.
        #[arg(long)]
        prompt: String,
        /// Persist the turn as a resume-once continuation bound to this schedule.
        #[arg(long)]
        deferred: bool,
        /// Optional absolute continuation expiry in Unix milliseconds.
        #[arg(long, requires = "deferred")]
        expires_at_ms: Option<i64>,
        /// First Unix timestamp in milliseconds. Required for time triggers.
        #[arg(long)]
        at_ms: Option<i64>,
        /// Optional fixed interval in milliseconds. Requires `--at-ms`.
        #[arg(long, requires = "at_ms")]
        every_ms: Option<u64>,
        /// Canonical runtime event type to match.
        #[arg(long, conflicts_with_all = ["at_ms", "every_ms", "process_id", "contains"])]
        on_event: Option<String>,
        /// Exact process identity for an output trigger.
        #[arg(long, requires = "contains", conflicts_with_all = ["at_ms", "every_ms", "on_event"])]
        process_id: Option<String>,
        /// Literal output text to match for `--process-id`.
        #[arg(long, requires = "process_id", conflicts_with_all = ["at_ms", "every_ms", "on_event"])]
        contains: Option<String>,
        /// Explicit idempotency key; generated when omitted.
        #[arg(long)]
        idempotency_id: Option<String>,
        /// Session style.
        #[arg(long, default_value = "persistent-chat")]
        style: String,
        /// Workspace.
        #[arg(long, default_value = ".")]
        workspace: String,
        /// Permission policy.
        #[arg(long, default_value = "interactive")]
        permission_policy: String,
        /// Provider.
        #[arg(long, default_value = "deterministic-mock")]
        provider: String,
        /// Model.
        #[arg(long, default_value = "mock-model")]
        model: String,
        /// Hard token budget.
        #[arg(long, default_value_t = 100_000)]
        token_budget: u64,
        /// Hard cost budget in micro-units.
        #[arg(long, default_value_t = 0)]
        cost_budget_micros: u64,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// List durable schedules.
    List {
        /// Maximum rows.
        #[arg(long, default_value_t = 100)]
        limit: u32,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Remove one durable schedule.
    Remove {
        /// Stable schedule identifier.
        schedule_id: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Atomically claim due occurrences for a headless worker.
    Claim {
        /// Maximum claims.
        #[arg(long, default_value_t = 16)]
        limit: u32,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Claim and execute due prompt schedules through the normal runtime path.
    Run {
        /// Maximum occurrences.
        #[arg(long, default_value_t = 16)]
        limit: u32,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Mark one claimed occurrence terminal.
    Complete {
        /// Deterministic execution hash.
        execution_id: String,
        /// Mark execution failed instead of succeeded.
        #[arg(long)]
        failed: bool,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Session-style CLI operations.
#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
pub enum StyleCommand {
    /// List the bounded style registry.
    List {
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Inspect one style by ID or `id@version`.
    Inspect {
        /// Style selector.
        selector: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Validate a TOML or JSON style manifest file.
    Validate {
        /// Manifest file.
        file: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Compile a TOML or JSON style manifest file.
    Compile {
        /// Manifest file.
        file: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Harness registry CLI operations.
#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
pub enum HarnessCommand {
    /// List registered harnesses.
    List {
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Inspect one harness and its capabilities.
    Inspect {
        /// Stable harness ID.
        id: String,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Durable approval CLI operations.
#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
pub enum ApprovalCommand {
    /// Approve or deny one pending continuation.
    Resolve {
        /// Session containing the continuation.
        session: String,
        /// Opaque continuation identifier.
        continuation: String,
        /// Explicit approval decision.
        decision: ApprovalChoice,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Explicit approval decision accepted by the CLI boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ApprovalChoice {
    /// Allow the pending action to execute.
    Approve,
    /// Deny the pending action without executing it.
    Deny,
}

/// Session CLI operations.
#[derive(Clone, Debug, Eq, PartialEq, Subcommand)]
pub enum SessionCommand {
    /// Create a new durable session.
    Create {
        /// Workspace path.
        #[arg(long, default_value = ".")]
        workspace: String,
        /// Explicit top-level style.
        #[arg(long, default_value = "persistent-chat")]
        style: String,
        /// Explicit harness registry ID.
        #[arg(long)]
        harness: Option<String>,
        /// Explicit memory provider ID.
        #[arg(long)]
        memory: Option<String>,
        /// Explicit compaction strategy ID.
        #[arg(long)]
        compaction: Option<String>,
        /// Maximum loop/research iterations.
        #[arg(long)]
        max_iterations: Option<u32>,
        /// Maximum graph transitions.
        #[arg(long)]
        max_steps: Option<u64>,
        /// Maximum provider tokens.
        #[arg(long)]
        max_tokens: Option<u64>,
        /// Maximum cost in configured currency micros.
        #[arg(long)]
        max_cost_micros: Option<u64>,
        /// Maximum wall-clock duration in milliseconds.
        #[arg(long)]
        max_duration_ms: Option<u64>,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// List dormant-session metadata.
    List {
        /// Maximum rows.
        #[arg(long, default_value_t = 100)]
        limit: u32,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Inspect replay-derived state.
    Inspect {
        /// Durable session ID.
        session: String,
        /// Inclusive sequence; defaults to the verified head.
        #[arg(long)]
        at: Option<u64>,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Replay state without executing side effects.
    Replay {
        /// Durable session ID.
        session: String,
        /// Inclusive sequence; defaults to the verified head.
        #[arg(long)]
        at: Option<u64>,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Create an independently appendable child.
    Branch {
        /// Parent session ID.
        session: String,
        /// Inclusive parent fork sequence.
        #[arg(long)]
        at: u64,
        /// Optional explicit child style.
        #[arg(long)]
        style: Option<String>,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
    /// Read a bounded verified event page after a reconnect cursor.
    Events {
        /// Durable session ID.
        session: String,
        /// Last contiguous sequence already received.
        #[arg(long)]
        after: Option<u64>,
        /// Maximum events in this page.
        #[arg(long, default_value_t = 256)]
        limit: u32,
        /// Emit JSON.
        #[arg(long)]
        json: bool,
    },
}

/// Service-owned doctor request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceDoctorRequest {
    /// Endpoint output selection.
    pub output: ServiceOutputFormat,
    /// Endpoint policy selection.
    pub strict: bool,
    /// Bootstrap-selected runtime endpoint label.
    pub runtime_endpoint: String,
}

/// Service-owned output format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceOutputFormat {
    /// Human-readable terminal text.
    Text,
    /// Stable JSON object.
    Json,
}

/// Service-owned rendered source kind.
#[allow(
    missing_docs,
    reason = "service-local rendering enum variants are self-describing"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStyleSourceKind {
    BuiltIn,
    User,
    Project,
    Plugin,
    Inline,
}

/// Service-owned rendered availability.
#[allow(
    missing_docs,
    reason = "service-local rendering enum variants are self-describing"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceStyleAvailability {
    Available,
    Disabled,
    Invalid,
    Incompatible,
    Conflict,
}

/// Service-owned rendered style diagnostic.
#[allow(
    missing_docs,
    reason = "service-local rendering fields mirror stable output keys"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceStyleDiagnostic {
    pub code: String,
    pub path: String,
    pub message: String,
    pub help: String,
}

/// Service-owned rendered style summary.
#[allow(
    missing_docs,
    reason = "service-local rendering fields mirror stable output keys"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceStyleSummary {
    pub id: String,
    pub version: String,
    pub source: ServiceStyleSourceKind,
    pub availability: ServiceStyleAvailability,
    pub style_content_hash: String,
    pub compiled_cache_key: String,
    pub required_capabilities: Vec<String>,
}

/// Service-owned rendered inspection result.
#[allow(
    missing_docs,
    reason = "service-local rendering fields mirror stable output keys"
)]
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceStyleInspection {
    pub summary: ServiceStyleSummary,
    pub source_locator: String,
    pub manifest: serde_json::Value,
    pub compiled: Option<serde_json::Value>,
    pub diagnostics: Vec<ServiceStyleDiagnostic>,
}

/// Service-owned rendered validation result.
#[allow(
    missing_docs,
    reason = "service-local rendering fields mirror stable output keys"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceStyleValidation {
    pub valid: bool,
    pub diagnostics: Vec<ServiceStyleDiagnostic>,
}

/// Service-owned command response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCommandResponse {
    /// Fully rendered endpoint output.
    pub output: String,
    /// Portable process exit code.
    pub exit_code: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ServiceCreateSessionInput {
    workspace: String,
    style: String,
    harness: Option<String>,
    memory: Option<String>,
    compaction: Option<String>,
    budgets: Option<ServiceBudgetOverrides>,
    json: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_field_names,
    reason = "explicit budget names preserve unambiguous service-to-logic mapping"
)]
struct ServiceBudgetOverrides {
    max_iterations: Option<u32>,
    max_steps: Option<u64>,
    max_tokens: Option<u64>,
    max_cost_micros: Option<u64>,
    max_duration_ms: Option<u64>,
}

const fn budget_input(
    max_iterations: Option<u32>,
    max_steps: Option<u64>,
    max_tokens: Option<u64>,
    max_cost_micros: Option<u64>,
    max_duration_ms: Option<u64>,
) -> Option<ServiceBudgetOverrides> {
    if max_iterations.is_none()
        && max_steps.is_none()
        && max_tokens.is_none()
        && max_cost_micros.is_none()
        && max_duration_ms.is_none()
    {
        None
    } else {
        Some(ServiceBudgetOverrides {
            max_iterations,
            max_steps,
            max_tokens,
            max_cost_micros,
            max_duration_ms,
        })
    }
}

/// Parsed endpoint invocation, either complete or incrementally rendered.
pub enum ServiceInvocation {
    /// A complete command response.
    Complete(ServiceCommandResponse),
    /// A live newline-delimited JSON stream.
    Stream(ServiceCommandStream),
}

/// Service-owned live turn stream.
pub struct ServiceCommandStream {
    logic: RunTurnStream,
}

impl ServiceCommandStream {
    /// Receives and renders the next committed runtime item.
    ///
    /// `None` means that the runtime closed the stream after its terminal item.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when the logic stream fails or JSON rendering
    /// cannot serialize the service-owned response.
    #[must_use]
    pub fn next(&self) -> Option<Result<String, ServiceError>> {
        self.logic.next().map(|result| {
            result
                .map_err(|error| ServiceError::Logic {
                    detail: error.to_string(),
                })
                .and_then(render_turn_stream_item)
        })
    }
}

/// CLI service bootstrap configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliServiceConfig {
    /// Safe label for the selected runtime transport.
    pub runtime_endpoint_label: String,
}

/// Endpoint-facing CLI service.
#[derive(Clone, Debug)]
pub struct CliService<L> {
    logic: L,
    config: CliServiceConfig,
}

impl<L> CliService<L> {
    /// Creates a CLI service with injected logic and endpoint configuration.
    #[must_use]
    pub const fn new(logic: L, config: CliServiceConfig) -> Self {
        Self { logic, config }
    }
}

impl<L> CliService<L>
where
    L: CliLogicPort,
{
    /// Parses external arguments and starts either a complete or streaming
    /// endpoint invocation.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when argument parsing or business execution
    /// fails.
    pub fn start_from<I, T>(&self, arguments: I) -> Result<ServiceInvocation, ServiceError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let arguments =
            CliArguments::try_parse_from(arguments).map_err(|error| ServiceError::Arguments {
                detail: error.to_string(),
            })?;
        if !matches!(
            &arguments.command,
            CliCommand::Run {
                stream_json: true,
                ..
            }
        ) {
            return self.execute(arguments).map(ServiceInvocation::Complete);
        }
        let CliCommand::Run {
            prompt,
            session,
            provider,
            model,
            options,
            cancellation_id,
            stream_json: _,
            ..
        } = arguments.command
        else {
            unreachable!("stream selection was checked above");
        };
        let command =
            map_run_turn_command(prompt, &session, provider, model, options, cancellation_id)?;
        let logic = self
            .logic
            .run_turn_stream(command)
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        Ok(ServiceInvocation::Stream(ServiceCommandStream { logic }))
    }

    /// Parses external arguments and executes the mapped service request.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when argument parsing, business execution, or
    /// endpoint rendering fails.
    pub fn run_from<I, T>(&self, arguments: I) -> Result<ServiceCommandResponse, ServiceError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let arguments =
            CliArguments::try_parse_from(arguments).map_err(|error| ServiceError::Arguments {
                detail: error.to_string(),
            })?;
        self.execute(arguments)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the endpoint router explicitly maps every CLI command into service-owned calls"
    )]
    fn execute(&self, arguments: CliArguments) -> Result<ServiceCommandResponse, ServiceError> {
        match arguments.command {
            CliCommand::Run {
                prompt,
                session,
                provider,
                model,
                options,
                cancellation_id,
                json,
                stream_json,
            } => self.run_turn(
                prompt,
                &session,
                provider,
                model,
                options,
                cancellation_id,
                json || stream_json,
            ),
            CliCommand::Doctor { json, strict } => self.doctor(ServiceDoctorRequest {
                output: if json {
                    ServiceOutputFormat::Json
                } else {
                    ServiceOutputFormat::Text
                },
                strict,
                runtime_endpoint: self.config.runtime_endpoint_label.clone(),
            }),
            CliCommand::Session { command } => match command {
                SessionCommand::Create {
                    workspace,
                    style,
                    harness,
                    memory,
                    compaction,
                    max_iterations,
                    max_steps,
                    max_tokens,
                    max_cost_micros,
                    max_duration_ms,
                    json,
                } => self.create_session(ServiceCreateSessionInput {
                    workspace,
                    style,
                    harness,
                    memory,
                    compaction,
                    budgets: budget_input(
                        max_iterations,
                        max_steps,
                        max_tokens,
                        max_cost_micros,
                        max_duration_ms,
                    ),
                    json,
                }),
                SessionCommand::List { limit, json } => self.list_sessions(limit, json),
                SessionCommand::Inspect { session, at, json } => {
                    self.inspect_session(&session, at, false, json)
                }
                SessionCommand::Replay { session, at, json } => {
                    self.inspect_session(&session, at, true, json)
                }
                SessionCommand::Branch {
                    session,
                    at,
                    style,
                    json,
                } => self.branch_session(&session, at, style, json),
                SessionCommand::Events {
                    session,
                    after,
                    limit,
                    json,
                } => self.subscribe_session(&session, after, limit, json),
            },
            CliCommand::Approval { command } => match command {
                ApprovalCommand::Resolve {
                    session,
                    continuation,
                    decision,
                    json,
                } => self.resolve_approval(
                    &session,
                    continuation,
                    decision == ApprovalChoice::Approve,
                    json,
                ),
            },
            CliCommand::Schedule { command } => match command {
                ScheduleCommand::Add {
                    schedule_id,
                    session,
                    prompt,
                    deferred,
                    expires_at_ms,
                    at_ms,
                    every_ms,
                    on_event,
                    process_id,
                    contains,
                    idempotency_id,
                    style,
                    workspace,
                    permission_policy,
                    provider,
                    model,
                    token_budget,
                    cost_budget_micros,
                    json,
                } => self.add_schedule(
                    schedule_id,
                    &session,
                    prompt,
                    deferred,
                    expires_at_ms,
                    at_ms,
                    every_ms,
                    on_event,
                    process_id,
                    contains,
                    idempotency_id,
                    style,
                    workspace,
                    permission_policy,
                    provider,
                    model,
                    token_budget,
                    cost_budget_micros,
                    json,
                ),
                ScheduleCommand::List { limit, json } => self.list_schedules(limit, json),
                ScheduleCommand::Remove { schedule_id, json } => {
                    self.remove_schedule(&schedule_id, json)
                }
                ScheduleCommand::Claim { limit, json } => self.claim_schedules(limit, json),
                ScheduleCommand::Run { limit, json } => self.run_due_schedules(limit, json),
                ScheduleCommand::Complete {
                    execution_id,
                    failed,
                    json,
                } => self.complete_scheduled_execution(&execution_id, !failed, json),
            },
            CliCommand::Style { command } => match command {
                StyleCommand::List { json } => self.list_styles(json),
                StyleCommand::Inspect { selector, json } => self.inspect_style(selector, json),
                StyleCommand::Validate { file, json } => self.validate_style(file, json),
                StyleCommand::Compile { file, json } => self.compile_style(file, json),
            },
            CliCommand::Harness { command } => match command {
                HarnessCommand::List { json } => self.list_harnesses(json),
                HarnessCommand::Inspect { id, json } => self.inspect_harness(&id, json),
            },
            CliCommand::Cancel {
                cancellation_id,
                reason,
                json,
            } => self.cancel_turn(&cancellation_id, reason, json),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the CLI endpoint explicitly maps every user-visible schedule policy field"
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "the schedule endpoint maps trigger, identity, deferred continuation, and rendering fields explicitly"
    )]
    fn add_schedule(
        &self,
        schedule_id: String,
        session: &str,
        prompt: String,
        deferred: bool,
        expires_at_ms: Option<i64>,
        at_ms: Option<i64>,
        every_ms: Option<u64>,
        on_event: Option<String>,
        process_id: Option<String>,
        contains: Option<String>,
        idempotency_id: Option<String>,
        style: String,
        workspace: String,
        permission_policy: String,
        provider: String,
        model: String,
        token_budget: u64,
        cost_budget_micros: u64,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let session_id = session.parse().map_err(|_| ServiceError::Arguments {
            detail: String::from("session must be a valid UUID"),
        })?;
        let trigger = match (at_ms, every_ms, on_event, process_id, contains) {
            (Some(at_ms), None, None, None, None) => ScheduleTrigger::AtMillis(at_ms),
            (Some(starts_at_ms), Some(every_ms), None, None, None) => ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            (None, None, Some(event_type), None, None) => {
                ScheduleTrigger::RuntimeEvent { event_type }
            }
            (None, None, None, Some(process_id), Some(contains)) => {
                ScheduleTrigger::ProcessOutput {
                    process_id,
                    contains,
                }
            }
            _ => {
                return Err(ServiceError::Arguments {
                    detail: String::from(
                        "schedule add requires exactly one trigger: --at-ms, --on-event, or --process-id with --contains",
                    ),
                });
            }
        };
        let trigger_key = match &trigger {
            ScheduleTrigger::AtMillis(value) => format!("at:{value}"),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => format!("interval:{starts_at_ms}:{every_ms}"),
            ScheduleTrigger::RuntimeEvent { event_type } => format!("event:{event_type}"),
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => format!("output:{process_id}:{contains}"),
        };
        let idempotency_id = idempotency_id.unwrap_or_else(|| {
            let digest = blake3::hash(format!("{schedule_id}:{trigger_key}").as_bytes());
            format!("cli:{}", digest.to_hex())
        });
        let result = if deferred {
            if matches!(trigger, ScheduleTrigger::Interval { .. }) {
                return Err(ServiceError::Arguments {
                    detail: String::from(
                        "deferred schedules are resume-once and do not support --every-ms",
                    ),
                });
            }
            let identity = blake3::hash(
                format!("{schedule_id}:{trigger_key}:{idempotency_id}:deferred").as_bytes(),
            );
            let continuation_id = uuid_string_from_hash(identity.as_bytes());
            let cancellation_identity =
                blake3::hash(format!("{continuation_id}:cancellation").as_bytes());
            let cancellation_id = uuid_string_from_hash(cancellation_identity.as_bytes())
                .parse()
                .map_err(|_| ServiceError::Arguments {
                    detail: String::from("generated cancellation identity is invalid"),
                })?;
            self.logic
                .defer_schedule(DeferredScheduleCommand {
                    schedule: LogicScheduleCommand {
                        schedule_id,
                        session_id,
                        idempotency_id,
                        style,
                        workspace,
                        permission_policy,
                        provider,
                        model,
                        token_budget,
                        cost_budget_micros,
                        trigger,
                        payload: SchedulePayload::Continuation {
                            continuation_id: continuation_id.clone(),
                        },
                        active: true,
                    },
                    continuation_id,
                    prompt,
                    cancellation_id,
                    expires_at_ms,
                })
                .map_err(logic_error)?
        } else {
            self.logic
                .upsert_schedule(LogicScheduleCommand {
                    schedule_id,
                    session_id,
                    idempotency_id,
                    style,
                    workspace,
                    permission_policy,
                    provider,
                    model,
                    token_budget,
                    cost_budget_micros,
                    trigger,
                    payload: SchedulePayload::Prompt { prompt },
                    active: true,
                })
                .map_err(logic_error)?
        };
        let value = serde_json::json!({
            "schedule_id": result.schedule_id,
            "replayed": result.replayed
        });
        Ok(render_value(
            value,
            json,
            format!(
                "schedule {} stored{}",
                result.schedule_id,
                if result.replayed { " (replayed)" } else { "" }
            ),
        ))
    }

    fn list_schedules(
        &self,
        limit: u32,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let schedules = self.logic.list_schedules(limit).map_err(logic_error)?;
        let values = schedules
            .iter()
            .map(render_schedule_value)
            .collect::<Vec<_>>();
        let text = schedules
            .iter()
            .map(|schedule| {
                format!(
                    "{}\t{}\t{}\t{}",
                    schedule.schedule_id, schedule.session_id, schedule.provider, schedule.model
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(render_value(
            serde_json::json!({"schedules": values}),
            json,
            text,
        ))
    }

    fn remove_schedule(
        &self,
        schedule_id: &str,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let existed = self
            .logic
            .remove_schedule(schedule_id)
            .map_err(logic_error)?;
        Ok(render_value(
            serde_json::json!({"schedule_id": schedule_id, "existed": existed}),
            json,
            if existed {
                format!("schedule {schedule_id} removed")
            } else {
                format!("schedule {schedule_id} did not exist")
            },
        ))
    }

    fn claim_schedules(
        &self,
        limit: u32,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let executions = self.logic.claim_due_schedules(limit).map_err(logic_error)?;
        let values = executions
            .iter()
            .map(|execution| {
                serde_json::json!({
                    "execution_id": execution.execution_id,
                    "scheduled_for_ms": execution.scheduled_for_ms,
                    "claimed_at_ms": execution.claimed_at_ms,
                    "schedule": render_schedule_value(&execution.schedule)
                })
            })
            .collect::<Vec<_>>();
        let text = executions
            .iter()
            .map(|execution| {
                format!(
                    "{}\t{}\t{}",
                    execution.execution_id,
                    execution.schedule.schedule_id,
                    execution.scheduled_for_ms
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(render_value(
            serde_json::json!({"executions": values}),
            json,
            text,
        ))
    }

    fn complete_scheduled_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let changed = self
            .logic
            .complete_scheduled_execution(execution_id, succeeded)
            .map_err(logic_error)?;
        Ok(render_value(
            serde_json::json!({
                "execution_id": execution_id,
                "succeeded": succeeded,
                "changed": changed
            }),
            json,
            format!(
                "scheduled execution {execution_id} {}",
                if changed {
                    "completed"
                } else {
                    "was already terminal"
                }
            ),
        ))
    }

    fn run_due_schedules(
        &self,
        limit: u32,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let runs = self.logic.run_due_schedules(limit).map_err(logic_error)?;
        let values = runs
            .iter()
            .map(|run| {
                serde_json::json!({
                    "execution_id": run.execution_id,
                    "schedule_id": run.schedule_id,
                    "terminal": run.terminal,
                    "succeeded": run.succeeded,
                    "last_committed_sequence": run.last_committed_sequence,
                    "awaiting_continuation": run.awaiting_continuation,
                    "error": run.error
                })
            })
            .collect::<Vec<_>>();
        let text = runs
            .iter()
            .map(|run| {
                format!(
                    "{}\t{}\t{}",
                    run.execution_id,
                    run.schedule_id,
                    if run.succeeded {
                        "succeeded"
                    } else if run.terminal {
                        "failed"
                    } else {
                        "pending"
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(render_value(
            serde_json::json!({"runs": values}),
            json,
            text,
        ))
    }

    fn list_styles(&self, json: bool) -> Result<ServiceCommandResponse, ServiceError> {
        let styles = self.logic.list_styles().map_err(logic_error)?;
        let styles = styles
            .into_iter()
            .map(map_style_summary)
            .collect::<Vec<_>>();
        Ok(render_style_list(&styles, json))
    }

    fn list_harnesses(&self, json: bool) -> Result<ServiceCommandResponse, ServiceError> {
        let harnesses = self.logic.list_harnesses().map_err(logic_error)?;
        Ok(render_harnesses(&harnesses, "harness_list", json))
    }

    fn inspect_harness(
        &self,
        id: &str,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let harness = self.logic.inspect_harness(id).map_err(logic_error)?;
        Ok(render_harnesses(
            std::slice::from_ref(&harness),
            "harness_inspect",
            json,
        ))
    }

    fn inspect_style(
        &self,
        selector: String,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let inspection = self
            .logic
            .inspect_style(agentmod_cli_logic::InspectStyleCommand { selector })
            .map_err(logic_error)?;
        render_style_inspection(&map_style_inspection(inspection), "style_inspect", json)
    }

    fn validate_style(
        &self,
        file: String,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let validation = self
            .logic
            .validate_style(StyleFileCommand { file })
            .map_err(logic_error)?;
        Ok(render_style_validation(
            &map_style_validation(validation),
            json,
        ))
    }

    fn compile_style(
        &self,
        file: String,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let inspection = self
            .logic
            .compile_style(StyleFileCommand { file })
            .map_err(logic_error)?;
        render_style_inspection(&map_style_inspection(inspection), "style_compile", json)
    }

    fn inspect_session(
        &self,
        session: &str,
        at: Option<u64>,
        replay: bool,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let session_id = session.parse().map_err(|_| ServiceError::Arguments {
            detail: String::from("session must be a valid UUID"),
        })?;
        let at = at
            .map(agentmod_primitives::Sequence::new)
            .transpose()
            .map_err(|_| ServiceError::Arguments {
                detail: String::from("sequence must be greater than zero"),
            })?;
        let result = self
            .logic
            .inspect_session(InspectSessionCommand {
                session_id,
                at,
                replay,
            })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        Ok(ServiceCommandResponse {
            output: render_inspection(&result, replay, json)?,
            exit_code: 0,
        })
    }

    fn subscribe_session(
        &self,
        session: &str,
        after: Option<u64>,
        limit: u32,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let session_id = session.parse().map_err(|_| ServiceError::Arguments {
            detail: String::from("session must be a valid UUID"),
        })?;
        let after = after
            .map(agentmod_primitives::Sequence::new)
            .transpose()
            .map_err(|_| ServiceError::Arguments {
                detail: String::from("after sequence must be greater than zero"),
            })?;
        let page = self
            .logic
            .subscribe_session(SubscribeSessionCommand {
                session_id,
                after,
                limit,
            })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        Ok(ServiceCommandResponse {
            output: render_session_events(&page, json)?,
            exit_code: 0,
        })
    }

    fn branch_session(
        &self,
        session: &str,
        at: u64,
        style: Option<String>,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let session_id = session.parse().map_err(|_| ServiceError::Arguments {
            detail: String::from("session must be a valid UUID"),
        })?;
        let at = agentmod_primitives::Sequence::new(at).map_err(|_| ServiceError::Arguments {
            detail: String::from("sequence must be greater than zero"),
        })?;
        let result = self
            .logic
            .branch_session(BranchSessionCommand {
                session_id,
                at,
                style,
            })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        Ok(ServiceCommandResponse {
            output: render_branch(&result, json)?,
            exit_code: 0,
        })
    }

    fn cancel_turn(
        &self,
        cancellation_id: &str,
        reason: String,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let cancellation_id = cancellation_id
            .parse()
            .map_err(|_| ServiceError::Arguments {
                detail: String::from("cancellation ID must be a valid UUID"),
            })?;
        self.logic
            .cancel_turn(CancelTurnCommand {
                cancellation_id,
                reason,
            })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        Ok(ServiceCommandResponse {
            output: if json {
                serde_json::to_string(&serde_json::json!({
                    "command":"cancel",
                    "cancelled":true,
                    "cancellation_id":cancellation_id.to_string(),
                }))
                .map_err(|error| ServiceError::Rendering {
                    detail: error.to_string(),
                })?
            } else {
                format!("cancelled {cancellation_id}")
            },
            exit_code: 0,
        })
    }

    fn resolve_approval(
        &self,
        session: &str,
        continuation: String,
        approved: bool,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let session_id = session.parse().map_err(|_| ServiceError::Arguments {
            detail: String::from("session must be a valid UUID"),
        })?;
        let result = self
            .logic
            .resolve_approval(ResolveApprovalCommand {
                session_id,
                continuation_id: continuation,
                approved,
            })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        let output = if json {
            render_approval_json(&result)?
        } else {
            render_approval_text(&result)
        };
        Ok(ServiceCommandResponse {
            output,
            exit_code: 0,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the endpoint keeps each independently validated CLI field explicit"
    )]
    fn run_turn(
        &self,
        prompt: String,
        session: &str,
        provider: String,
        model: String,
        options: Vec<String>,
        cancellation_id: Option<String>,
        json_output: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let command =
            map_run_turn_command(prompt, session, provider, model, options, cancellation_id)?;
        let result = self
            .logic
            .run_turn(command)
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        let output = if json_output {
            render_turn_json(&result)?
        } else {
            render_turn_text(&result)
        };
        Ok(ServiceCommandResponse {
            output,
            exit_code: 0,
        })
    }

    fn create_session(
        &self,
        request: ServiceCreateSessionInput,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let result = self
            .logic
            .create_session(CreateSessionCommand {
                workspace: request.workspace,
                style: request.style,
                harness: request.harness,
                memory: request.memory,
                compaction: request.compaction,
                budgets: request.budgets.map(|budgets| CreateSessionBudgetCommand {
                    max_iterations: budgets.max_iterations,
                    max_steps: budgets.max_steps,
                    max_tokens: budgets.max_tokens,
                    max_cost_micros: budgets.max_cost_micros,
                    max_duration_ms: budgets.max_duration_ms,
                }),
            })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        let output = if request.json {
            serde_json::to_string(&serde_json::json!({
                "command": "session_create",
                "session_id": result.session_id.to_string(),
            }))
            .map_err(|error| ServiceError::Rendering {
                detail: error.to_string(),
            })?
        } else {
            format!("created session {}", result.session_id)
        };
        Ok(ServiceCommandResponse {
            output,
            exit_code: 0,
        })
    }

    fn list_sessions(
        &self,
        limit: u32,
        json: bool,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        let sessions = self
            .logic
            .list_sessions(ListSessionsCommand { limit })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        let output = if json {
            render_sessions_json(&sessions)?
        } else {
            render_sessions_text(&sessions)
        };
        Ok(ServiceCommandResponse {
            output,
            exit_code: 0,
        })
    }

    /// Executes the service-owned doctor endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError`] when the service request is invalid, logic
    /// fails, or the response cannot be rendered.
    pub fn doctor(
        &self,
        request: ServiceDoctorRequest,
    ) -> Result<ServiceCommandResponse, ServiceError> {
        if request.runtime_endpoint.trim().is_empty() {
            return Err(ServiceError::InvalidRuntimeEndpoint);
        }
        let result = self
            .logic
            .run_doctor(RunDoctorCommand {
                strict: request.strict,
                runtime_endpoint: request.runtime_endpoint,
            })
            .map_err(|error| ServiceError::Logic {
                detail: error.to_string(),
            })?;
        let output = match request.output {
            ServiceOutputFormat::Text => render_text(&result),
            ServiceOutputFormat::Json => render_json(&result)?,
        };
        Ok(ServiceCommandResponse {
            output,
            exit_code: u8::from(!result.successful),
        })
    }
}

fn state_label(state: DoctorState) -> &'static str {
    match state {
        DoctorState::Ready => "ready",
        DoctorState::Degraded => "degraded",
        DoctorState::Unavailable => "unavailable",
    }
}

fn map_style_summary(summary: StyleSummaryResult) -> ServiceStyleSummary {
    ServiceStyleSummary {
        id: summary.id,
        version: summary.version,
        source: match summary.source {
            StyleSourceKind::BuiltIn => ServiceStyleSourceKind::BuiltIn,
            StyleSourceKind::User => ServiceStyleSourceKind::User,
            StyleSourceKind::Project => ServiceStyleSourceKind::Project,
            StyleSourceKind::Plugin => ServiceStyleSourceKind::Plugin,
            StyleSourceKind::Inline => ServiceStyleSourceKind::Inline,
        },
        availability: match summary.availability {
            StyleAvailability::Available => ServiceStyleAvailability::Available,
            StyleAvailability::Disabled => ServiceStyleAvailability::Disabled,
            StyleAvailability::Invalid => ServiceStyleAvailability::Invalid,
            StyleAvailability::Incompatible => ServiceStyleAvailability::Incompatible,
            StyleAvailability::Conflict => ServiceStyleAvailability::Conflict,
        },
        style_content_hash: summary.style_content_hash,
        compiled_cache_key: summary.compiled_cache_key,
        required_capabilities: summary.required_capabilities,
    }
}

fn map_style_diagnostic(diagnostic: StyleDiagnostic) -> ServiceStyleDiagnostic {
    ServiceStyleDiagnostic {
        code: diagnostic.code,
        path: diagnostic.path,
        message: diagnostic.message,
        help: diagnostic.help,
    }
}

fn map_style_inspection(inspection: StyleInspectionResult) -> ServiceStyleInspection {
    ServiceStyleInspection {
        summary: map_style_summary(inspection.summary),
        source_locator: inspection.source_locator,
        manifest: inspection.manifest,
        compiled: inspection.compiled,
        diagnostics: inspection
            .diagnostics
            .into_iter()
            .map(map_style_diagnostic)
            .collect(),
    }
}

fn map_style_validation(validation: StyleValidationResult) -> ServiceStyleValidation {
    ServiceStyleValidation {
        valid: validation.valid,
        diagnostics: validation
            .diagnostics
            .into_iter()
            .map(map_style_diagnostic)
            .collect(),
    }
}

fn style_source_label(source: ServiceStyleSourceKind) -> &'static str {
    match source {
        ServiceStyleSourceKind::BuiltIn => "built_in",
        ServiceStyleSourceKind::User => "user",
        ServiceStyleSourceKind::Project => "project",
        ServiceStyleSourceKind::Plugin => "plugin",
        ServiceStyleSourceKind::Inline => "inline",
    }
}

fn style_availability_label(availability: ServiceStyleAvailability) -> &'static str {
    match availability {
        ServiceStyleAvailability::Available => "available",
        ServiceStyleAvailability::Disabled => "disabled",
        ServiceStyleAvailability::Invalid => "invalid",
        ServiceStyleAvailability::Incompatible => "incompatible",
        ServiceStyleAvailability::Conflict => "conflict",
    }
}

fn render_style_diagnostic(diagnostic: &ServiceStyleDiagnostic) -> serde_json::Value {
    serde_json::json!({
        "code": diagnostic.code,
        "path": diagnostic.path,
        "message": diagnostic.message,
        "help": diagnostic.help,
    })
}

fn render_style_summary(summary: &ServiceStyleSummary) -> serde_json::Value {
    serde_json::json!({
        "id": summary.id,
        "version": summary.version,
        "source": style_source_label(summary.source),
        "availability": style_availability_label(summary.availability),
        "style_content_hash": summary.style_content_hash,
        "compiled_cache_key": summary.compiled_cache_key,
        "required_capabilities": summary.required_capabilities,
    })
}

fn render_style_list(styles: &[ServiceStyleSummary], json: bool) -> ServiceCommandResponse {
    let value = serde_json::json!({
        "command": "style_list",
        "styles": styles.iter().map(render_style_summary).collect::<Vec<_>>(),
    });
    let text = if styles.is_empty() {
        String::from("no styles")
    } else {
        styles
            .iter()
            .map(|style| {
                format!(
                    "{}@{}\t{}\t{}\t{}\t{}",
                    style.id,
                    style.version,
                    style_availability_label(style.availability),
                    style_source_label(style.source),
                    style.style_content_hash,
                    style.compiled_cache_key,
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    render_value(value, json, text)
}

fn render_harnesses(
    harnesses: &[HarnessDescriptorResult],
    command: &str,
    json: bool,
) -> ServiceCommandResponse {
    let rows = harnesses
        .iter()
        .map(|harness| {
            serde_json::json!({
                "id": harness.id,
                "version": harness.version,
                "availability": harness.availability,
                "capability_set_hash": harness.capability_set_hash,
                "capabilities": harness.capabilities,
            })
        })
        .collect::<Vec<_>>();
    let text = if harnesses.is_empty() {
        String::from("no harnesses")
    } else {
        harnesses
            .iter()
            .map(|harness| {
                format!(
                    "{}@{}\t{}\t{}\t{}",
                    harness.id,
                    harness.version,
                    harness.availability,
                    harness.capability_set_hash,
                    harness.capabilities.join(","),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    render_value(
        serde_json::json!({"command": command, "harnesses": rows}),
        json,
        text,
    )
}

fn render_style_inspection(
    inspection: &ServiceStyleInspection,
    command: &str,
    json: bool,
) -> Result<ServiceCommandResponse, ServiceError> {
    let value = serde_json::json!({
        "command": command,
        "summary": render_style_summary(&inspection.summary),
        "source_locator": inspection.source_locator,
        "manifest": inspection.manifest,
        "compiled": inspection.compiled,
        "diagnostics": inspection.diagnostics.iter().map(render_style_diagnostic).collect::<Vec<_>>(),
    });
    let text = if json {
        value.to_string()
    } else {
        let manifest = serde_json::to_string_pretty(&inspection.manifest).map_err(|error| {
            ServiceError::Rendering {
                detail: error.to_string(),
            }
        })?;
        let compiled = inspection.compiled.as_ref().map_or_else(
            || String::from("none"),
            |value| {
                serde_json::to_string_pretty(value)
                    .unwrap_or_else(|_| String::from("<unrenderable>"))
            },
        );
        format!(
            "{}@{}\navailability: {}\nsource: {}\nsource_locator: {}\nstyle_content_hash: {}\ncompiled_cache_key: {}\nmanifest:\n{}\ncompiled:\n{}\ndiagnostics:\n{}",
            inspection.summary.id,
            inspection.summary.version,
            style_availability_label(inspection.summary.availability),
            style_source_label(inspection.summary.source),
            inspection.source_locator,
            inspection.summary.style_content_hash,
            inspection.summary.compiled_cache_key,
            manifest,
            compiled,
            inspection
                .diagnostics
                .iter()
                .map(|diagnostic| format!(
                    "{} {}: {}",
                    diagnostic.code, diagnostic.path, diagnostic.message
                ))
                .collect::<Vec<_>>()
                .join("\n"),
        )
    };
    Ok(ServiceCommandResponse {
        output: text,
        exit_code: 0,
    })
}

fn render_style_validation(
    validation: &ServiceStyleValidation,
    json: bool,
) -> ServiceCommandResponse {
    let value = serde_json::json!({
        "command": "style_validate",
        "valid": validation.valid,
        "diagnostics": validation.diagnostics.iter().map(render_style_diagnostic).collect::<Vec<_>>(),
    });
    let text = if json {
        value.to_string()
    } else if validation.diagnostics.is_empty() {
        String::from("style manifest is valid")
    } else {
        validation
            .diagnostics
            .iter()
            .map(|diagnostic| {
                format!(
                    "{} {}: {}",
                    diagnostic.code, diagnostic.path, diagnostic.message
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    ServiceCommandResponse {
        output: text,
        exit_code: 0,
    }
}

fn render_text(result: &DoctorResult) -> String {
    let mut lines = vec![format!("doctor: {}", state_label(result.state))];
    lines.extend(result.checks.iter().map(|check| {
        format!(
            "{}: {} ({})",
            check.name,
            state_label(check.state),
            check.detail
        )
    }));
    lines.join("\n")
}

fn render_json(result: &DoctorResult) -> Result<String, ServiceError> {
    let checks: Vec<_> = result
        .checks
        .iter()
        .map(|check| {
            serde_json::json!({
                "name": check.name,
                "state": state_label(check.state),
                "detail": check.detail,
            })
        })
        .collect();
    serde_json::to_string(&serde_json::json!({
        "command": "doctor",
        "state": state_label(result.state),
        "successful": result.successful,
        "checks": checks,
    }))
    .map_err(|error| ServiceError::Rendering {
        detail: error.to_string(),
    })
}

fn render_sessions_text(sessions: &[SessionSummaryResult]) -> String {
    if sessions.is_empty() {
        return String::from("no sessions");
    }
    sessions
        .iter()
        .map(|session| {
            format!(
                "{}\t{}\t{}\t{}\t{}",
                session.id,
                session.state,
                session.style,
                session.sequence.get(),
                session.workspace_label
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_sessions_json(sessions: &[SessionSummaryResult]) -> Result<String, ServiceError> {
    let rows = sessions
        .iter()
        .map(|session| {
            serde_json::json!({
                "id": session.id.to_string(),
                "workspace": session.workspace_label,
                "style": session.style,
                "sequence": session.sequence.get(),
                "state": session.state,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&serde_json::json!({
        "command": "session_list",
        "sessions": rows,
    }))
    .map_err(|error| ServiceError::Rendering {
        detail: error.to_string(),
    })
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the layer error at the service boundary"
)]
fn logic_error(error: agentmod_cli_logic::LogicError) -> ServiceError {
    ServiceError::Logic {
        detail: error.to_string(),
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "the renderer consumes its endpoint-owned alternatives"
)]
fn render_value(value: serde_json::Value, json: bool, text: String) -> ServiceCommandResponse {
    ServiceCommandResponse {
        output: if json { value.to_string() } else { text },
        exit_code: 0,
    }
}

fn render_schedule_value(value: &ScheduleResult) -> serde_json::Value {
    let trigger = match &value.trigger {
        ScheduleTrigger::AtMillis(at) => serde_json::json!({"kind": "at_millis", "value": at}),
        ScheduleTrigger::Interval {
            starts_at_ms,
            every_ms,
        } => serde_json::json!({
            "kind": "interval",
            "value": {"starts_at_ms": starts_at_ms, "every_ms": every_ms}
        }),
        ScheduleTrigger::RuntimeEvent { event_type } => serde_json::json!({
            "kind": "runtime_event",
            "value": {"event_type": event_type}
        }),
        ScheduleTrigger::ProcessOutput {
            process_id,
            contains,
        } => serde_json::json!({
            "kind": "process_output",
            "value": {"process_id": process_id, "contains": contains}
        }),
    };
    let payload = match &value.payload {
        SchedulePayload::Prompt { prompt } => {
            serde_json::json!({"kind": "prompt", "value": {"prompt": prompt}})
        }
        SchedulePayload::Continuation { continuation_id } => serde_json::json!({
            "kind": "continuation",
            "value": {"continuation_id": continuation_id}
        }),
    };
    serde_json::json!({
        "schedule_id": value.schedule_id,
        "session_id": value.session_id,
        "idempotency_id": value.idempotency_id,
        "style": value.style,
        "workspace": value.workspace,
        "permission_policy": value.permission_policy,
        "provider": value.provider,
        "model": value.model,
        "token_budget": value.token_budget,
        "cost_budget_micros": value.cost_budget_micros,
        "trigger": trigger,
        "payload": payload,
        "active": value.active
    })
}

fn uuid_string_from_hash(bytes: &[u8; 32]) -> String {
    let hex = blake3::Hash::from_bytes(*bytes).to_hex();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn render_inspection(
    result: &InspectSessionResult,
    replay: bool,
    json: bool,
) -> Result<String, ServiceError> {
    if json {
        return serde_json::to_string(&serde_json::json!({
            "command": if replay { "session_replay" } else { "session_inspect" },
            "session_id": result.session_id.to_string(),
            "head_sequence": result.head_sequence.get(),
            "inspected_sequence": result.inspected_sequence.get(),
            "event_count": result.event_count,
            "state": result.state,
        }))
        .map_err(|error| ServiceError::Rendering {
            detail: error.to_string(),
        });
    }
    serde_json::to_string_pretty(&result.state).map_err(|error| ServiceError::Rendering {
        detail: error.to_string(),
    })
}

fn render_branch(result: &BranchSessionResult, json: bool) -> Result<String, ServiceError> {
    if json {
        return serde_json::to_string(&serde_json::json!({
            "command": "session_branch",
            "session_id": result.session_id.to_string(),
            "parent_session_id": result.parent_session_id.to_string(),
            "fork_sequence": result.fork_sequence.get(),
            "child_head_sequence": result.child_head_sequence.get(),
        }))
        .map_err(|error| ServiceError::Rendering {
            detail: error.to_string(),
        });
    }
    Ok(format!(
        "branched {} from {} at sequence {} (child head {})",
        result.session_id,
        result.parent_session_id,
        result.fork_sequence.get(),
        result.child_head_sequence.get()
    ))
}

fn render_session_events(
    page: &SessionEventPageResult,
    json: bool,
) -> Result<String, ServiceError> {
    if json {
        return serde_json::to_string(&serde_json::json!({
            "command": "session_events",
            "events": page.events.iter().map(|event| serde_json::json!({
                "sequence": event.sequence.get(),
                "event_type": event.event_type,
                "payload": event.payload,
            })).collect::<Vec<_>>(),
            "head_sequence": page.head_sequence.get(),
            "last_delivered_sequence": page
                .last_delivered_sequence
                .map(agentmod_primitives::Sequence::get),
            "has_more": page.has_more,
        }))
        .map_err(|error| ServiceError::Rendering {
            detail: error.to_string(),
        });
    }
    let mut lines = page
        .events
        .iter()
        .map(|event| format!("{} {}", event.sequence.get(), event.event_type))
        .collect::<Vec<_>>();
    lines.push(format!(
        "head={} next_after={} has_more={}",
        page.head_sequence.get(),
        page.last_delivered_sequence.map_or_else(
            || String::from("none"),
            |sequence| sequence.get().to_string()
        ),
        page.has_more
    ));
    Ok(lines.join("\n"))
}

fn render_turn_text(result: &RunTurnResult) -> String {
    let text: String = result
        .events
        .iter()
        .filter_map(|event| match event {
            TurnEvent::Text(value) => Some(value.as_str()),
            _ => None,
        })
        .collect();
    if text.is_empty() {
        result.awaiting_continuation.as_ref().map_or_else(
            || String::from("turn completed without visible output"),
            |continuation| format!("turn awaiting continuation {continuation}"),
        )
    } else {
        text
    }
}

fn render_turn_json(result: &RunTurnResult) -> Result<String, ServiceError> {
    let events: Vec<_> = result.events.iter().map(render_turn_event).collect();
    serde_json::to_string(&serde_json::json!({
        "command": "run",
        "events": events,
        "first_committed_sequence": result.first_committed_sequence.get(),
        "last_committed_sequence": result.last_committed_sequence.get(),
        "awaiting_continuation": result.awaiting_continuation,
    }))
    .map_err(|error| ServiceError::Rendering {
        detail: error.to_string(),
    })
}

fn render_turn_stream_item(item: RunTurnStreamItem) -> Result<String, ServiceError> {
    let value = match item {
        RunTurnStreamItem::Event {
            event,
            committed_sequence,
        } => serde_json::json!({
            "command": "run_event",
            "committed_sequence": committed_sequence.get(),
            "event": render_turn_event(&event),
        }),
        RunTurnStreamItem::Complete {
            first_committed_sequence,
            last_committed_sequence,
            awaiting_continuation,
        } => serde_json::json!({
            "command": "run_complete",
            "first_committed_sequence": first_committed_sequence.get(),
            "last_committed_sequence": last_committed_sequence.get(),
            "awaiting_continuation": awaiting_continuation,
        }),
    };
    serde_json::to_string(&value).map_err(|error| ServiceError::Rendering {
        detail: error.to_string(),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the boundary maps each independently validated CLI field explicitly"
)]
fn map_run_turn_command(
    prompt: String,
    session: &str,
    provider: String,
    model: String,
    options: Vec<String>,
    cancellation_id: Option<String>,
) -> Result<RunTurnCommand, ServiceError> {
    let session_id = session.parse().map_err(|_| ServiceError::Arguments {
        detail: String::from("session must be a valid UUID"),
    })?;
    let mut mapped_options = serde_json::Map::new();
    for option in options {
        let (key, raw) = option
            .split_once('=')
            .ok_or_else(|| ServiceError::Arguments {
                detail: format!("provider option `{option}` must use key=value"),
            })?;
        if key.trim().is_empty() {
            return Err(ServiceError::Arguments {
                detail: String::from("provider option key is empty"),
            });
        }
        let value =
            serde_json::from_str(raw).unwrap_or_else(|_| serde_json::Value::String(raw.to_owned()));
        mapped_options.insert(key.to_owned(), value);
    }
    Ok(RunTurnCommand {
        session_id,
        prompt,
        provider,
        model,
        options: serde_json::Value::Object(mapped_options),
        cancellation_id: cancellation_id
            .map(|value| {
                value.parse().map_err(|_| ServiceError::Arguments {
                    detail: String::from("cancellation ID must be a valid UUID"),
                })
            })
            .transpose()?,
    })
}

fn render_approval_text(result: &ResolveApprovalResult) -> String {
    if !result.transitioned {
        return String::from("approval was already resolved");
    }
    let text: String = result
        .events
        .iter()
        .filter_map(|event| match event {
            TurnEvent::Text(value) => Some(value.as_str()),
            _ => None,
        })
        .collect();
    if text.is_empty() {
        result.awaiting_continuation.as_ref().map_or_else(
            || String::from("approval resolved"),
            |continuation| format!("approval resolved; awaiting continuation {continuation}"),
        )
    } else {
        text
    }
}

fn render_approval_json(result: &ResolveApprovalResult) -> Result<String, ServiceError> {
    let events: Vec<_> = result.events.iter().map(render_turn_event).collect();
    serde_json::to_string(&serde_json::json!({
        "command": "approval_resolve",
        "transitioned": result.transitioned,
        "events": events,
        "last_committed_sequence": result
            .last_committed_sequence
            .map(agentmod_primitives::Sequence::get),
        "awaiting_continuation": result.awaiting_continuation,
    }))
    .map_err(|error| ServiceError::Rendering {
        detail: error.to_string(),
    })
}

fn render_turn_event(event: &TurnEvent) -> serde_json::Value {
    match event {
        TurnEvent::Started => serde_json::json!({"event": "started"}),
        TurnEvent::Text(text) => serde_json::json!({"event": "text", "text": text}),
        TurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => serde_json::json!({
            "event": "tool_delta",
            "call_id": call_id,
            "name": name,
            "arguments": arguments
        }),
        TurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => serde_json::json!({
            "event": "tool_proposed",
            "continuation_id": continuation_id,
            "call_id": call_id,
            "tool": tool,
            "arguments": arguments
        }),
        TurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        } => serde_json::json!({
            "event": "completed",
            "reason": reason,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens
        }),
        TurnEvent::Cancelled => serde_json::json!({"event": "cancelled"}),
        TurnEvent::Failed {
            code,
            message,
            retryable,
        } => serde_json::json!({
            "event": "failed",
            "code": code,
            "message": message,
            "retryable": retryable
        }),
    }
}

/// CLI endpoint failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ServiceError {
    /// Clap rejected the external command line.
    #[error("invalid command line: {detail}")]
    Arguments {
        /// Safe parser diagnostic.
        detail: String,
    },
    /// Bootstrap supplied no runtime endpoint.
    #[error("configured runtime endpoint is empty")]
    InvalidRuntimeEndpoint,
    /// The requested business use case failed.
    #[error("CLI operation failed: {detail}")]
    Logic {
        /// Sanitized logic diagnostic.
        detail: String,
    },
    /// A service result could not be serialized.
    #[error("CLI output rendering failed: {detail}")]
    Rendering {
        /// Safe serialization diagnostic.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_cli_logic::{
        CreateSessionResult, DoctorCheck, LogicError, ScheduleStoreResult, SessionSummaryResult,
    };
    use agentmod_primitives::{Sequence, SessionId};
    use uuid::Uuid;

    use super::*;

    struct MockLogic {
        state: DoctorState,
        successful: bool,
        observed: RefCell<Vec<RunDoctorCommand>>,
        observed_create: RefCell<Vec<CreateSessionCommand>>,
        observed_schedules: RefCell<Vec<LogicScheduleCommand>>,
        observed_deferred: RefCell<Vec<DeferredScheduleCommand>>,
    }

    impl CliLogicPort for MockLogic {
        fn run_doctor(&self, command: RunDoctorCommand) -> Result<DoctorResult, LogicError> {
            self.observed.borrow_mut().push(command);
            Ok(DoctorResult {
                state: self.state,
                successful: self.successful,
                checks: vec![DoctorCheck {
                    name: "runtime".into(),
                    state: self.state,
                    detail: "runtime version test".into(),
                }],
            })
        }

        fn create_session(
            &self,
            command: CreateSessionCommand,
        ) -> Result<CreateSessionResult, LogicError> {
            self.observed_create.borrow_mut().push(command);
            Ok(CreateSessionResult {
                session_id: SessionId::from_uuid(Uuid::from_u128(1)),
            })
        }

        fn list_sessions(
            &self,
            _command: ListSessionsCommand,
        ) -> Result<Vec<SessionSummaryResult>, LogicError> {
            Ok(vec![SessionSummaryResult {
                id: SessionId::from_uuid(Uuid::from_u128(1)),
                workspace_label: String::from("workspace"),
                style: String::from("persistent-chat"),
                sequence: Sequence::FIRST,
                state: String::from("active"),
            }])
        }

        fn inspect_session(
            &self,
            command: InspectSessionCommand,
        ) -> Result<InspectSessionResult, LogicError> {
            Ok(InspectSessionResult {
                session_id: command.session_id,
                head_sequence: Sequence::new(3).expect("sequence"),
                inspected_sequence: command.at.unwrap_or(Sequence::new(3).expect("sequence")),
                event_count: command.at.map_or(3, Sequence::get),
                state: serde_json::json!({"lifecycle": "active"}),
            })
        }

        fn subscribe_session(
            &self,
            command: SubscribeSessionCommand,
        ) -> Result<SessionEventPageResult, LogicError> {
            Ok(SessionEventPageResult {
                events: vec![],
                head_sequence: Sequence::new(3).expect("sequence"),
                last_delivered_sequence: command.after,
                has_more: false,
            })
        }

        fn branch_session(
            &self,
            command: BranchSessionCommand,
        ) -> Result<BranchSessionResult, LogicError> {
            Ok(BranchSessionResult {
                session_id: SessionId::from_uuid(Uuid::from_u128(2)),
                parent_session_id: command.session_id,
                fork_sequence: command.at,
                child_head_sequence: Sequence::new(2).expect("sequence"),
            })
        }

        fn run_turn(&self, _command: RunTurnCommand) -> Result<RunTurnResult, LogicError> {
            Ok(RunTurnResult {
                events: vec![
                    TurnEvent::Started,
                    TurnEvent::Text("done".into()),
                    TurnEvent::Completed {
                        reason: "stop".into(),
                        input_tokens: 2,
                        output_tokens: 1,
                    },
                ],
                first_committed_sequence: Sequence::new(2).expect("sequence"),
                last_committed_sequence: Sequence::new(3).expect("sequence"),
                awaiting_continuation: None,
            })
        }

        fn run_turn_stream(&self, _command: RunTurnCommand) -> Result<RunTurnStream, LogicError> {
            Err(LogicError::TurnData)
        }

        fn cancel_turn(&self, _command: CancelTurnCommand) -> Result<(), LogicError> {
            Ok(())
        }

        fn resolve_approval(
            &self,
            _command: ResolveApprovalCommand,
        ) -> Result<ResolveApprovalResult, LogicError> {
            Ok(ResolveApprovalResult {
                transitioned: true,
                events: vec![TurnEvent::Text(String::from("resumed"))],
                last_committed_sequence: Some(Sequence::new(4).expect("sequence")),
                awaiting_continuation: None,
            })
        }

        fn upsert_schedule(
            &self,
            schedule: LogicScheduleCommand,
        ) -> Result<ScheduleStoreResult, LogicError> {
            let schedule_id = schedule.schedule_id.clone();
            self.observed_schedules.borrow_mut().push(schedule);
            Ok(ScheduleStoreResult {
                schedule_id,
                replayed: false,
            })
        }

        fn defer_schedule(
            &self,
            command: DeferredScheduleCommand,
        ) -> Result<ScheduleStoreResult, LogicError> {
            let schedule_id = command.schedule.schedule_id.clone();
            self.observed_deferred.borrow_mut().push(command);
            Ok(ScheduleStoreResult {
                schedule_id,
                replayed: false,
            })
        }
    }

    fn service(state: DoctorState, successful: bool) -> CliService<MockLogic> {
        CliService::new(
            MockLogic {
                state,
                successful,
                observed: RefCell::new(Vec::new()),
                observed_create: RefCell::new(Vec::new()),
                observed_schedules: RefCell::new(Vec::new()),
                observed_deferred: RefCell::new(Vec::new()),
            },
            CliServiceConfig {
                runtime_endpoint_label: "local-runtime".into(),
            },
        )
    }

    #[test]
    fn clap_doctor_maps_through_service_and_logic_types() {
        let service = service(DoctorState::Ready, true);
        let response = service
            .run_from(["agentmod", "doctor", "--strict"])
            .expect("doctor");
        assert_eq!(response.exit_code, 0);
        assert!(response.output.contains("doctor: ready"));
        assert_eq!(
            service.logic.observed.into_inner(),
            vec![RunDoctorCommand {
                strict: true,
                runtime_endpoint: "local-runtime".into(),
            }]
        );
    }

    #[test]
    fn json_output_is_stable_and_structured() {
        let response = service(DoctorState::Degraded, true)
            .run_from(["agentmod", "doctor", "--json"])
            .expect("doctor");
        let json: serde_json::Value = serde_json::from_str(&response.output).expect("valid JSON");
        assert_eq!(json["command"], "doctor");
        assert_eq!(json["state"], "degraded");
        assert_eq!(response.exit_code, 0);
    }

    #[test]
    fn unsuccessful_business_result_maps_to_failing_exit_code() {
        let response = service(DoctorState::Unavailable, false)
            .run_from(["agentmod", "doctor"])
            .expect("doctor");
        assert_eq!(response.exit_code, 1);
    }

    #[test]
    fn unknown_command_is_rejected_at_service_boundary() {
        let error = service(DoctorState::Ready, true)
            .run_from(["agentmod", "unknown"])
            .expect_err("invalid command");
        assert!(matches!(error, ServiceError::Arguments { .. }));
    }

    #[test]
    fn session_create_and_list_render_stable_json() {
        let service = service(DoctorState::Ready, true);
        let created = service
            .run_from([
                "agentmod",
                "session",
                "create",
                "--memory",
                "sqlite-fts",
                "--compaction",
                "sliding_window",
                "--max-iterations",
                "3",
                "--max-steps",
                "40",
                "--max-tokens",
                "100000",
                "--max-cost-micros",
                "1000000",
                "--max-duration-ms",
                "60000",
                "--json",
            ])
            .expect("create");
        assert!(created.output.contains("session_create"));
        assert_eq!(
            service.logic.observed_create.borrow()[0],
            CreateSessionCommand {
                workspace: String::from("."),
                style: String::from("persistent-chat"),
                harness: None,
                memory: Some(String::from("sqlite-fts")),
                compaction: Some(String::from("sliding_window")),
                budgets: Some(CreateSessionBudgetCommand {
                    max_iterations: Some(3),
                    max_steps: Some(40),
                    max_tokens: Some(100_000),
                    max_cost_micros: Some(1_000_000),
                    max_duration_ms: Some(60_000),
                }),
            }
        );
        let listed = service
            .run_from(["agentmod", "session", "list", "--json"])
            .expect("list");
        let json: serde_json::Value = serde_json::from_str(&listed.output).expect("json");
        assert_eq!(json["sessions"][0]["style"], "persistent-chat");
    }

    #[test]
    fn approval_resolution_requires_an_explicit_decision() {
        let service = service(DoctorState::Ready, true);
        let response = service
            .run_from([
                "agentmod",
                "approval",
                "resolve",
                "00000000-0000-0000-0000-000000000001",
                "continuation-1",
                "approve",
                "--json",
            ])
            .expect("approval");
        let json: serde_json::Value =
            serde_json::from_str(&response.output).expect("approval JSON");
        assert_eq!(json["command"], "approval_resolve");
        assert_eq!(json["transitioned"], true);
        assert_eq!(json["events"][0]["text"], "resumed");
    }

    #[test]
    fn run_parses_provider_options_and_renders_json() {
        let response = service(DoctorState::Ready, true)
            .run_from([
                "agentmod",
                "run",
                "hello",
                "--session",
                "00000000-0000-0000-0000-000000000001",
                "--option",
                "temperature=0",
                "--json",
            ])
            .expect("run");
        let json: serde_json::Value = serde_json::from_str(&response.output).expect("json");
        assert_eq!(json["command"], "run");
        assert_eq!(json["last_committed_sequence"], 3);
        assert_eq!(json["events"][1]["text"], "done");
    }

    #[test]
    fn schedule_add_maps_runtime_event_trigger() {
        let service = service(DoctorState::Ready, true);
        service
            .run_from([
                "agentmod",
                "schedule",
                "add",
                "on-model-complete",
                "--session",
                "00000000-0000-0000-0000-000000000001",
                "--prompt",
                "review the completed turn",
                "--on-event",
                "model.response_completed",
                "--json",
            ])
            .expect("schedule");
        let schedules = service.logic.observed_schedules.borrow();
        assert_eq!(schedules.len(), 1);
        assert_eq!(
            schedules[0].trigger,
            ScheduleTrigger::RuntimeEvent {
                event_type: String::from("model.response_completed")
            }
        );
    }

    #[test]
    fn schedule_add_maps_exact_process_output_trigger() {
        let service = service(DoctorState::Ready, true);
        service
            .run_from([
                "agentmod",
                "schedule",
                "add",
                "on-ready",
                "--session",
                "00000000-0000-0000-0000-000000000001",
                "--prompt",
                "inspect the ready service",
                "--process-id",
                "process-42",
                "--contains",
                "READY",
            ])
            .expect("schedule");
        let schedules = service.logic.observed_schedules.borrow();
        assert_eq!(
            schedules[0].trigger,
            ScheduleTrigger::ProcessOutput {
                process_id: String::from("process-42"),
                contains: String::from("READY")
            }
        );
    }

    #[test]
    fn schedule_add_deferred_maps_resume_once_continuation() {
        let service = service(DoctorState::Ready, true);
        service
            .run_from([
                "agentmod",
                "schedule",
                "add",
                "deferred-ready",
                "--session",
                "00000000-0000-0000-0000-000000000001",
                "--prompt",
                "continue after ready",
                "--process-id",
                "process-42",
                "--contains",
                "READY",
                "--deferred",
                "--expires-at-ms",
                "9999999999999",
            ])
            .expect("deferred schedule");
        let deferred = service.logic.observed_deferred.borrow();
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].expires_at_ms, Some(9_999_999_999_999));
        assert!(matches!(
            deferred[0].schedule.payload,
            SchedulePayload::Continuation { .. }
        ));
        assert_eq!(
            deferred[0].schedule.trigger,
            ScheduleTrigger::ProcessOutput {
                process_id: String::from("process-42"),
                contains: String::from("READY"),
            }
        );
    }

    #[test]
    fn schedule_add_rejects_missing_trigger() {
        let error = service(DoctorState::Ready, true)
            .run_from([
                "agentmod",
                "schedule",
                "add",
                "missing-trigger",
                "--session",
                "00000000-0000-0000-0000-000000000001",
                "--prompt",
                "must not be stored",
            ])
            .expect_err("missing trigger");
        assert!(matches!(error, ServiceError::Arguments { .. }));
    }

    #[test]
    fn style_commands_parse_and_render_stable_json() {
        let parsed = CliArguments::try_parse_from(["agentmod", "style", "inspect", "calm@1"])
            .expect("style command");
        assert!(matches!(
            parsed.command,
            CliCommand::Style {
                command: StyleCommand::Inspect { selector, json: false }
            } if selector == "calm@1"
        ));
        let response = render_style_validation(
            &ServiceStyleValidation {
                valid: false,
                diagnostics: vec![ServiceStyleDiagnostic {
                    code: String::from("style.id.required"),
                    path: String::from("id"),
                    message: String::from("style id is required"),
                    help: String::from("set id"),
                }],
            },
            true,
        );
        let value: serde_json::Value = serde_json::from_str(&response.output).expect("json");
        assert_eq!(value["command"], "style_validate");
        assert_eq!(value["valid"], false);
        assert_eq!(value["diagnostics"][0]["code"], "style.id.required");
        assert_eq!(response.exit_code, 0);
    }
}
