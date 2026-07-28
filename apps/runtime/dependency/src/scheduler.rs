//! Supervised scheduler-worker adapter owned by the runtime dependency layer.
#![allow(
    missing_docs,
    reason = "dependency-local scheduler transport records are self-describing"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the scheduler adapter exposes one documented closed error taxonomy"
)]

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{Arc, Mutex},
};

use agentmod_scheduler_protocol::{
    CURRENT_PROTOCOL_VERSION, SchedulePayload, ScheduleSpec, ScheduleTrigger, ScheduledExecution,
    SchedulerCommand, SchedulerResponse,
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencyScheduleTrigger {
    AtMillis(i64),
    Interval {
        starts_at_ms: i64,
        every_ms: u64,
    },
    RuntimeEvent {
        event_type: String,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DependencySchedulePayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyRuntimeSchedule {
    pub schedule_id: String,
    pub session_id: String,
    pub idempotency_id: String,
    pub style: String,
    pub workspace: String,
    pub permission_policy: String,
    pub provider: String,
    pub model: String,
    pub token_budget: u64,
    pub cost_budget_micros: u64,
    pub trigger: DependencyScheduleTrigger,
    pub payload: DependencySchedulePayload,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScheduledExecution {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub claimed_at_ms: i64,
    pub schedule: DependencyRuntimeSchedule,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyScheduleStoreResult {
    pub schedule_id: String,
    pub replayed: bool,
}

pub trait RuntimeSchedulerDependencyPort: Send + Sync {
    fn upsert(
        &self,
        schedule: DependencyRuntimeSchedule,
    ) -> Result<DependencyScheduleStoreResult, RuntimeSchedulerDependencyError>;
    fn remove(&self, schedule_id: &str) -> Result<bool, RuntimeSchedulerDependencyError>;
    fn list(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyRuntimeSchedule>, RuntimeSchedulerDependencyError>;
    fn claim_due(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError>;
    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError>;
    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError>;
    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeSchedulerDependencyError>;
}

#[derive(Clone, Debug)]
pub struct ProcessSchedulerDependencyConfig {
    pub program: String,
    pub arguments: Vec<String>,
    pub state_root: PathBuf,
    pub authentication_token: String,
    pub maximum_frame_bytes: usize,
}

struct Connection {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Clone)]
pub struct ProcessSchedulerDependency {
    config: Arc<ProcessSchedulerDependencyConfig>,
    connection: Arc<Mutex<Connection>>,
}

impl ProcessSchedulerDependency {
    #[must_use]
    pub fn generate_authentication_token() -> String {
        format!("{}{}", uuid::Uuid::now_v7(), uuid::Uuid::now_v7())
    }

    pub fn new(
        config: ProcessSchedulerDependencyConfig,
    ) -> Result<Self, RuntimeSchedulerDependencyError> {
        if config.program.trim().is_empty()
            || config.program.contains('\0')
            || config.arguments.iter().any(|value| value.contains('\0'))
            || config.state_root.as_os_str().is_empty()
            || config.authentication_token.len() < 32
            || config.maximum_frame_bytes == 0
        {
            return Err(RuntimeSchedulerDependencyError::InvalidConfiguration);
        }
        let connection = connect(&config)?;
        let dependency = Self {
            config: Arc::new(config),
            connection: Arc::new(Mutex::new(connection)),
        };
        match dependency.request(&SchedulerCommand::Negotiate {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            capabilities: vec!["durable_schedules".to_owned()],
            authentication_token: dependency.config.authentication_token.clone(),
        })? {
            SchedulerResponse::Negotiated {
                protocol_version,
                capabilities,
            } if protocol_version == CURRENT_PROTOCOL_VERSION
                && capabilities
                    .iter()
                    .any(|value| value == "durable_schedules") =>
            {
                Ok(dependency)
            }
            SchedulerResponse::Error { code, .. } => {
                Err(RuntimeSchedulerDependencyError::Remote(code))
            }
            _ => Err(RuntimeSchedulerDependencyError::Protocol),
        }
    }

    fn request(
        &self,
        command: &SchedulerCommand,
    ) -> Result<SchedulerResponse, RuntimeSchedulerDependencyError> {
        let bytes =
            serde_json::to_vec(&command).map_err(|_| RuntimeSchedulerDependencyError::Protocol)?;
        if bytes.len() > self.config.maximum_frame_bytes {
            return Err(RuntimeSchedulerDependencyError::FrameTooLarge);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RuntimeSchedulerDependencyError::Transport)?;
        connection
            .input
            .write_all(&bytes)
            .and_then(|()| connection.input.write_all(b"\n"))
            .and_then(|()| connection.input.flush())
            .map_err(|_| RuntimeSchedulerDependencyError::Transport)?;
        let mut line = String::new();
        connection
            .output
            .read_line(&mut line)
            .map_err(|_| RuntimeSchedulerDependencyError::Transport)?;
        if line.is_empty() {
            return Err(RuntimeSchedulerDependencyError::Transport);
        }
        if line.len() > self.config.maximum_frame_bytes {
            return Err(RuntimeSchedulerDependencyError::FrameTooLarge);
        }
        serde_json::from_str(&line).map_err(|_| RuntimeSchedulerDependencyError::Protocol)
    }
}

impl RuntimeSchedulerDependencyPort for ProcessSchedulerDependency {
    fn upsert(
        &self,
        schedule: DependencyRuntimeSchedule,
    ) -> Result<DependencyScheduleStoreResult, RuntimeSchedulerDependencyError> {
        match self.request(&SchedulerCommand::Upsert {
            schedule: Box::new(to_wire_schedule(schedule)),
        })? {
            SchedulerResponse::Stored {
                schedule_id,
                replayed,
            } => Ok(DependencyScheduleStoreResult {
                schedule_id,
                replayed,
            }),
            response => remote_result(response),
        }
    }

    fn remove(&self, schedule_id: &str) -> Result<bool, RuntimeSchedulerDependencyError> {
        match self.request(&SchedulerCommand::Remove {
            schedule_id: schedule_id.to_owned(),
        })? {
            SchedulerResponse::Removed { existed } => Ok(existed),
            response => remote_result(response),
        }
    }

    fn list(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyRuntimeSchedule>, RuntimeSchedulerDependencyError> {
        match self.request(&SchedulerCommand::List { limit })? {
            SchedulerResponse::Schedules { schedules } => {
                Ok(schedules.into_iter().map(from_wire_schedule).collect())
            }
            response => remote_result(response),
        }
    }

    fn claim_due(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        match self.request(&SchedulerCommand::ClaimDue { limit })? {
            SchedulerResponse::Executions { executions } => {
                Ok(executions.into_iter().map(from_wire_execution).collect())
            }
            response => remote_result(response),
        }
    }

    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        match self.request(&SchedulerCommand::FireRuntimeEvent {
            event_id: event_id.to_owned(),
            event_type: event_type.to_owned(),
        })? {
            SchedulerResponse::Executions { executions } => {
                Ok(executions.into_iter().map(from_wire_execution).collect())
            }
            response => remote_result(response),
        }
    }

    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        match self.request(&SchedulerCommand::FireProcessOutput {
            output_id: output_id.to_owned(),
            process_id: process_id.to_owned(),
            output: output.to_owned(),
        })? {
            SchedulerResponse::Executions { executions } => {
                Ok(executions.into_iter().map(from_wire_execution).collect())
            }
            response => remote_result(response),
        }
    }

    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeSchedulerDependencyError> {
        match self.request(&SchedulerCommand::CompleteExecution {
            execution_id: execution_id.to_owned(),
            succeeded,
        })? {
            SchedulerResponse::ExecutionCompleted { changed } => Ok(changed),
            response => remote_result(response),
        }
    }
}

fn connect(
    config: &ProcessSchedulerDependencyConfig,
) -> Result<Connection, RuntimeSchedulerDependencyError> {
    let mut command = Command::new(&config.program);
    command
        .args(&config.arguments)
        .env_clear()
        .envs(host_environment())
        .env("AGENTMOD_SCHEDULER_ROOT", &config.state_root)
        .env(
            "AGENTMOD_SCHEDULER_AUTH_TOKEN",
            &config.authentication_token,
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        std::os::windows::process::CommandExt::creation_flags(&mut command, CREATE_NO_WINDOW);
    }
    let mut child = command
        .spawn()
        .map_err(|_| RuntimeSchedulerDependencyError::Transport)?;
    let input = child
        .stdin
        .take()
        .ok_or(RuntimeSchedulerDependencyError::Transport)?;
    let output = child
        .stdout
        .take()
        .map(BufReader::new)
        .ok_or(RuntimeSchedulerDependencyError::Transport)?;
    Ok(Connection {
        child,
        input,
        output,
    })
}

fn host_environment() -> BTreeMap<String, String> {
    [
        "PATH",
        "PATHEXT",
        "SYSTEMROOT",
        "WINDIR",
        "COMSPEC",
        "TMP",
        "TEMP",
        "TMPDIR",
        "LANG",
        "LC_ALL",
    ]
    .into_iter()
    .filter_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| (name.to_owned(), value))
    })
    .collect()
}

fn to_wire_schedule(value: DependencyRuntimeSchedule) -> ScheduleSpec {
    ScheduleSpec {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            DependencyScheduleTrigger::AtMillis(value) => ScheduleTrigger::AtMillis(value),
            DependencyScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            DependencyScheduleTrigger::RuntimeEvent { event_type } => {
                ScheduleTrigger::RuntimeEvent { event_type }
            }
            DependencyScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            DependencySchedulePayload::Prompt { prompt } => SchedulePayload::Prompt { prompt },
            DependencySchedulePayload::Continuation { continuation_id } => {
                SchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_wire_schedule(value: ScheduleSpec) -> DependencyRuntimeSchedule {
    DependencyRuntimeSchedule {
        schedule_id: value.schedule_id,
        session_id: value.session_id,
        idempotency_id: value.idempotency_id,
        style: value.style,
        workspace: value.workspace,
        permission_policy: value.permission_policy,
        provider: value.provider,
        model: value.model,
        token_budget: value.token_budget,
        cost_budget_micros: value.cost_budget_micros,
        trigger: match value.trigger {
            ScheduleTrigger::AtMillis(value) => DependencyScheduleTrigger::AtMillis(value),
            ScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => DependencyScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleTrigger::RuntimeEvent { event_type } => {
                DependencyScheduleTrigger::RuntimeEvent { event_type }
            }
            ScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => DependencyScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match value.payload {
            SchedulePayload::Prompt { prompt } => DependencySchedulePayload::Prompt { prompt },
            SchedulePayload::Continuation { continuation_id } => {
                DependencySchedulePayload::Continuation { continuation_id }
            }
        },
        active: value.active,
    }
}

fn from_wire_execution(value: ScheduledExecution) -> DependencyScheduledExecution {
    DependencyScheduledExecution {
        execution_id: value.execution_id,
        scheduled_for_ms: value.scheduled_for_ms,
        claimed_at_ms: value.claimed_at_ms,
        schedule: from_wire_schedule(value.schedule),
    }
}

fn remote_result<T>(response: SchedulerResponse) -> Result<T, RuntimeSchedulerDependencyError> {
    match response {
        SchedulerResponse::Error { code, .. } => Err(RuntimeSchedulerDependencyError::Remote(code)),
        _ => Err(RuntimeSchedulerDependencyError::Protocol),
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RuntimeSchedulerDependencyError {
    #[error("invalid scheduler adapter configuration")]
    InvalidConfiguration,
    #[error("scheduler transport failed")]
    Transport,
    #[error("scheduler protocol failed")]
    Protocol,
    #[error("scheduler frame exceeded configured bound")]
    FrameTooLarge,
    #[error("scheduler rejected request: {0}")]
    Remote(String),
}
