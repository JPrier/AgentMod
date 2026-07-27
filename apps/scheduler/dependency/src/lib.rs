//! Crash-safe schedule files, occurrence claims, and system-clock access.
#![allow(
    missing_docs,
    reason = "dependency-local storage records are exhaustively named and architecture-documented"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the dependency port exposes one documented closed error taxonomy"
)]

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Dependency trigger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DependencyTrigger {
    /// One time.
    AtMillis(i64),
    /// Recurring.
    Interval { starts_at_ms: i64, every_ms: u64 },
    /// Runtime event.
    RuntimeEvent { event_type: String },
    /// Process output.
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

/// Dependency payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DependencyPayload {
    /// Background prompt.
    Prompt { prompt: String },
    /// Continuation wakeup.
    Continuation { continuation_id: String },
}

/// Dependency-owned persisted schedule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DependencySchedule {
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
    pub trigger: DependencyTrigger,
    pub payload: DependencyPayload,
    pub active: bool,
}

/// Claimed occurrence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyExecution {
    pub execution_id: String,
    pub scheduled_for_ms: i64,
    pub schedule: DependencySchedule,
}

/// Store result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStoreResult {
    pub schedule_id: String,
    pub replayed: bool,
}

/// Persistence boundary.
pub trait SchedulerDependencyPort: Send + Sync {
    fn upsert(
        &self,
        schedule: DependencySchedule,
    ) -> Result<DependencyStoreResult, SchedulerDependencyError>;
    fn remove(&self, schedule_id: &str) -> Result<bool, SchedulerDependencyError>;
    fn list(&self, limit: usize) -> Result<Vec<DependencySchedule>, SchedulerDependencyError>;
    fn claim_due(&self, limit: usize)
    -> Result<Vec<DependencyExecution>, SchedulerDependencyError>;
    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError>;
    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError>;
    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, SchedulerDependencyError>;
    fn health(&self) -> Result<(), SchedulerDependencyError>;
}

#[derive(Clone)]
pub struct FileSchedulerDependency {
    root: PathBuf,
}

impl FileSchedulerDependency {
    /// Creates durable roots.
    ///
    /// # Errors
    ///
    /// Rejects an empty or unavailable root.
    pub fn new(root: PathBuf) -> Result<Self, SchedulerDependencyError> {
        if root.as_os_str().is_empty() {
            return Err(SchedulerDependencyError::Configuration);
        }
        fs::create_dir_all(root.join("schedules"))
            .and_then(|()| fs::create_dir_all(root.join("executions")))
            .map_err(|_| SchedulerDependencyError::Storage)?;
        recover_interrupted_replacements(&root.join("schedules"))?;
        Ok(Self { root })
    }

    fn locked<T>(
        &self,
        operation: impl FnOnce() -> Result<T, SchedulerDependencyError>,
    ) -> Result<T, SchedulerDependencyError> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(".scheduler.lock"))
            .map_err(|_| SchedulerDependencyError::Storage)?;
        lock.lock_exclusive()
            .map_err(|_| SchedulerDependencyError::Storage)?;
        let result = operation();
        let unlock = FileExt::unlock(&lock).map_err(|_| SchedulerDependencyError::Storage);
        match (result, unlock) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    fn schedule_path(&self, id: &str) -> Result<PathBuf, SchedulerDependencyError> {
        validate_id(id)?;
        Ok(self.root.join("schedules").join(format!("{id}.json")))
    }

    fn load_state(path: &Path) -> Result<ScheduleState, SchedulerDependencyError> {
        let bytes = fs::read(path).map_err(|_| SchedulerDependencyError::Storage)?;
        let stored: StoredSchedule =
            serde_json::from_slice(&bytes).map_err(|_| SchedulerDependencyError::Corrupt)?;
        let checksum = checksum(&stored.state)?;
        if checksum != stored.checksum {
            return Err(SchedulerDependencyError::Corrupt);
        }
        Ok(stored.state)
    }

    fn write_state(path: &Path, state: &ScheduleState) -> Result<(), SchedulerDependencyError> {
        write_json_atomic(
            path,
            &StoredSchedule {
                checksum: checksum(state)?,
                state: state.clone(),
            },
        )
    }

    fn claim(
        &self,
        schedule: &DependencySchedule,
        source: &str,
        scheduled_for_ms: i64,
    ) -> Result<Option<DependencyExecution>, SchedulerDependencyError> {
        let occurrence = if source == "time" {
            scheduled_for_ms.to_string()
        } else {
            source.to_owned()
        };
        let digest =
            blake3::hash(format!("{}\0{source}\0{occurrence}", schedule.schedule_id).as_bytes())
                .to_hex()
                .to_string();
        let execution = DependencyExecution {
            execution_id: digest.clone(),
            scheduled_for_ms,
            schedule: schedule.clone(),
        };
        let stored = StoredExecution {
            checksum: checksum(&execution_record(&execution, ExecutionStatus::Claimed))?,
            record: execution_record(&execution, ExecutionStatus::Claimed),
        };
        let path = self.root.join("executions").join(format!("{digest}.json"));
        let bytes = serde_json::to_vec(&stored).map_err(|_| SchedulerDependencyError::Storage)?;
        let mut file = match OpenOptions::new().create_new(true).write(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return Ok(None),
            Err(_) => return Err(SchedulerDependencyError::Storage),
        };
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| SchedulerDependencyError::Storage)?;
        Ok(Some(execution))
    }

    fn event_matches(
        &self,
        source_id: &str,
        scheduled_for_ms: i64,
        predicate: impl Fn(&DependencyTrigger) -> bool,
    ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError> {
        self.locked(|| {
            let mut result = Vec::new();
            for schedule in self.list_unlocked(10_000)? {
                if schedule.active
                    && predicate(&schedule.trigger)
                    && let Some(execution) = self.claim(&schedule, source_id, scheduled_for_ms)?
                {
                    result.push(execution);
                }
            }
            Ok(result)
        })
    }

    fn list_unlocked(
        &self,
        limit: usize,
    ) -> Result<Vec<DependencySchedule>, SchedulerDependencyError> {
        let mut paths = fs::read_dir(self.root.join("schedules"))
            .map_err(|_| SchedulerDependencyError::Storage)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|value| value == "json"))
            .collect::<Vec<_>>();
        paths.sort();
        paths
            .into_iter()
            .take(limit)
            .map(|path| Self::load_state(&path).map(|state| state.schedule))
            .collect()
    }
}

impl SchedulerDependencyPort for FileSchedulerDependency {
    fn upsert(
        &self,
        schedule: DependencySchedule,
    ) -> Result<DependencyStoreResult, SchedulerDependencyError> {
        validate_schedule(&schedule)?;
        self.locked(|| {
            let path = self.schedule_path(&schedule.schedule_id)?;
            if path.exists() {
                let current = Self::load_state(&path)?;
                if current.schedule.idempotency_id == schedule.idempotency_id {
                    if current.schedule == schedule {
                        return Ok(DependencyStoreResult {
                            schedule_id: schedule.schedule_id,
                            replayed: true,
                        });
                    }
                    return Err(SchedulerDependencyError::IdempotencyConflict);
                }
            }
            let next_due_ms = match schedule.trigger {
                DependencyTrigger::AtMillis(value) => Some(value),
                DependencyTrigger::Interval { starts_at_ms, .. } => Some(starts_at_ms),
                DependencyTrigger::RuntimeEvent { .. }
                | DependencyTrigger::ProcessOutput { .. } => None,
            };
            Self::write_state(
                &path,
                &ScheduleState {
                    schedule: schedule.clone(),
                    next_due_ms,
                    completed: false,
                },
            )?;
            Ok(DependencyStoreResult {
                schedule_id: schedule.schedule_id,
                replayed: false,
            })
        })
    }

    fn remove(&self, schedule_id: &str) -> Result<bool, SchedulerDependencyError> {
        self.locked(|| {
            let path = self.schedule_path(schedule_id)?;
            match fs::remove_file(path) {
                Ok(()) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                Err(_) => Err(SchedulerDependencyError::Storage),
            }
        })
    }

    fn list(&self, limit: usize) -> Result<Vec<DependencySchedule>, SchedulerDependencyError> {
        self.locked(|| self.list_unlocked(limit))
    }

    fn claim_due(
        &self,
        limit: usize,
    ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError> {
        let now = now_millis()?;
        self.locked(|| {
            let mut result = Vec::new();
            let mut paths = fs::read_dir(self.root.join("schedules"))
                .map_err(|_| SchedulerDependencyError::Storage)?
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|value| value == "json"))
                .collect::<Vec<_>>();
            paths.sort();
            for path in paths {
                if result.len() >= limit {
                    break;
                }
                let mut state = Self::load_state(&path)?;
                if !state.schedule.active || state.completed {
                    continue;
                }
                while result.len() < limit {
                    let Some(due) = state.next_due_ms.filter(|due| *due <= now) else {
                        break;
                    };
                    if let Some(execution) = self.claim(&state.schedule, "time", due)? {
                        result.push(execution);
                    }
                    match state.schedule.trigger {
                        DependencyTrigger::AtMillis(_) => {
                            state.completed = true;
                            state.next_due_ms = None;
                        }
                        DependencyTrigger::Interval { every_ms, .. } => {
                            let delta = i64::try_from(every_ms)
                                .map_err(|_| SchedulerDependencyError::Invalid)?;
                            state.next_due_ms = due.checked_add(delta);
                        }
                        DependencyTrigger::RuntimeEvent { .. }
                        | DependencyTrigger::ProcessOutput { .. } => {
                            return Err(SchedulerDependencyError::Corrupt);
                        }
                    }
                }
                Self::write_state(&path, &state)?;
            }
            Ok(result)
        })
    }

    fn fire_runtime_event(
        &self,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError> {
        validate_id(event_id)?;
        let now = now_millis()?;
        self.event_matches(event_id, now, |trigger| {
            matches!(trigger, DependencyTrigger::RuntimeEvent { event_type: expected } if expected == event_type)
        })
    }

    fn fire_process_output(
        &self,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<DependencyExecution>, SchedulerDependencyError> {
        validate_id(output_id)?;
        let now = now_millis()?;
        self.event_matches(output_id, now, |trigger| {
            matches!(
                trigger,
                DependencyTrigger::ProcessOutput {
                    process_id: expected,
                    contains,
                } if expected == process_id && output.contains(contains)
            )
        })
    }

    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, SchedulerDependencyError> {
        validate_hash(execution_id)?;
        self.locked(|| {
            let path = self
                .root
                .join("executions")
                .join(format!("{execution_id}.json"));
            let bytes = fs::read(&path).map_err(|_| SchedulerDependencyError::NotFound)?;
            let stored: StoredExecution =
                serde_json::from_slice(&bytes).map_err(|_| SchedulerDependencyError::Corrupt)?;
            if checksum(&stored.record)? != stored.checksum {
                return Err(SchedulerDependencyError::Corrupt);
            }
            let desired = if succeeded {
                self.root
                    .join("executions")
                    .join(format!("{execution_id}.succeeded"))
            } else {
                self.root
                    .join("executions")
                    .join(format!("{execution_id}.failed"))
            };
            let opposite = if succeeded {
                self.root
                    .join("executions")
                    .join(format!("{execution_id}.failed"))
            } else {
                self.root
                    .join("executions")
                    .join(format!("{execution_id}.succeeded"))
            };
            if opposite.exists() {
                return Err(SchedulerDependencyError::TerminalConflict);
            }
            let mut marker = match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(desired)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Ok(false);
                }
                Err(_) => return Err(SchedulerDependencyError::Storage),
            };
            if stored.record.status != ExecutionStatus::Claimed {
                return Err(SchedulerDependencyError::Corrupt);
            }
            marker
                .write_all(b"complete\n")
                .and_then(|()| marker.sync_all())
                .map_err(|_| SchedulerDependencyError::Storage)?;
            Ok(true)
        })
    }

    fn health(&self) -> Result<(), SchedulerDependencyError> {
        fs::read_dir(&self.root)
            .map(|_| ())
            .map_err(|_| SchedulerDependencyError::Storage)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ScheduleState {
    schedule: DependencySchedule,
    next_due_ms: Option<i64>,
    completed: bool,
}

#[derive(Deserialize, Serialize)]
struct StoredSchedule {
    checksum: String,
    state: ScheduleState,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum ExecutionStatus {
    Claimed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ExecutionRecord {
    execution_id: String,
    scheduled_for_ms: i64,
    schedule: DependencySchedule,
    status: ExecutionStatus,
}

#[derive(Deserialize, Serialize)]
struct StoredExecution {
    checksum: String,
    record: ExecutionRecord,
}

fn execution_record(value: &DependencyExecution, status: ExecutionStatus) -> ExecutionRecord {
    ExecutionRecord {
        execution_id: value.execution_id.clone(),
        scheduled_for_ms: value.scheduled_for_ms,
        schedule: value.schedule.clone(),
        status,
    }
}

fn validate_schedule(value: &DependencySchedule) -> Result<(), SchedulerDependencyError> {
    for id in [&value.schedule_id, &value.session_id, &value.idempotency_id] {
        validate_id(id)?;
    }
    if value.style.trim().is_empty()
        || value.workspace.trim().is_empty()
        || value.permission_policy.trim().is_empty()
        || value.provider.trim().is_empty()
        || value.model.trim().is_empty()
        || value.token_budget == 0
    {
        return Err(SchedulerDependencyError::Invalid);
    }
    match &value.trigger {
        DependencyTrigger::Interval { every_ms, .. } if *every_ms == 0 => {
            Err(SchedulerDependencyError::Invalid)
        }
        DependencyTrigger::RuntimeEvent { event_type } if event_type.trim().is_empty() => {
            Err(SchedulerDependencyError::Invalid)
        }
        DependencyTrigger::ProcessOutput {
            process_id,
            contains,
        } if process_id.trim().is_empty() || contains.is_empty() => {
            Err(SchedulerDependencyError::Invalid)
        }
        _ => Ok(()),
    }
}

fn validate_id(value: &str) -> Result<(), SchedulerDependencyError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b':'))
    {
        Err(SchedulerDependencyError::Invalid)
    } else {
        Ok(())
    }
}

fn validate_hash(value: &str) -> Result<(), SchedulerDependencyError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(SchedulerDependencyError::Invalid)
    }
}

fn now_millis() -> Result<i64, SchedulerDependencyError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SchedulerDependencyError::Clock)?
            .as_millis(),
    )
    .map_err(|_| SchedulerDependencyError::Clock)
}

fn checksum<T: Serialize>(value: &T) -> Result<String, SchedulerDependencyError> {
    serde_json::to_vec(value)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|_| SchedulerDependencyError::Storage)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), SchedulerDependencyError> {
    let nonce = uuid::Uuid::now_v7();
    let temporary = path.with_extension(format!("{nonce}.tmp"));
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(SchedulerDependencyError::Invalid)?;
    let backup = path.with_file_name(format!("{file_name}.{nonce}.backup"));
    let bytes = serde_json::to_vec(value).map_err(|_| SchedulerDependencyError::Storage)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|_| SchedulerDependencyError::Storage)?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| SchedulerDependencyError::Storage)?;
    drop(file);
    if path.exists() {
        fs::rename(path, &backup).map_err(|_| SchedulerDependencyError::Storage)?;
    }
    if fs::rename(&temporary, path).is_err() {
        if backup.exists() {
            let _ = fs::rename(&backup, path);
        }
        let _ = fs::remove_file(&temporary);
        return Err(SchedulerDependencyError::Storage);
    }
    if backup.exists() {
        fs::remove_file(&backup).map_err(|_| SchedulerDependencyError::Storage)?;
    }
    Ok(())
}

fn recover_interrupted_replacements(directory: &Path) -> Result<(), SchedulerDependencyError> {
    let mut backups = fs::read_dir(directory)
        .map_err(|_| SchedulerDependencyError::Storage)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "backup")
        })
        .collect::<Vec<_>>();
    backups.sort();
    for backup in backups {
        let Some(file_name) = backup.file_name().and_then(|value| value.to_str()) else {
            return Err(SchedulerDependencyError::Corrupt);
        };
        let Some((stem, _)) = file_name.rsplit_once('.') else {
            return Err(SchedulerDependencyError::Corrupt);
        };
        let Some((schedule_name, _)) = stem.rsplit_once('.') else {
            return Err(SchedulerDependencyError::Corrupt);
        };
        let destination = directory.join(schedule_name);
        if destination.exists() {
            fs::remove_file(&backup).map_err(|_| SchedulerDependencyError::Storage)?;
        } else {
            fs::rename(&backup, destination).map_err(|_| SchedulerDependencyError::Storage)?;
        }
    }
    Ok(())
}

/// Dependency failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum SchedulerDependencyError {
    #[error("invalid scheduler dependency configuration")]
    Configuration,
    #[error("invalid scheduler dependency request")]
    Invalid,
    #[error("scheduler idempotency key conflicts")]
    IdempotencyConflict,
    #[error("scheduler execution terminal state conflicts")]
    TerminalConflict,
    #[error("scheduler record is missing")]
    NotFound,
    #[error("scheduler record is corrupt")]
    Corrupt,
    #[error("scheduler storage failed")]
    Storage,
    #[error("scheduler clock failed")]
    Clock,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::{
        DependencyPayload, DependencySchedule, DependencyTrigger, FileSchedulerDependency,
        SchedulerDependencyError, SchedulerDependencyPort,
    };

    #[test]
    fn one_time_claim_is_durable_idempotent_and_completes_once() {
        let root = TempDir::new().expect("root");
        let dependency =
            FileSchedulerDependency::new(root.path().to_path_buf()).expect("dependency");
        let schedule = fixture("once", DependencyTrigger::AtMillis(0));
        assert!(!dependency.upsert(schedule.clone()).expect("store").replayed);
        assert!(dependency.upsert(schedule).expect("replay").replayed);

        let claimed = dependency.claim_due(10).expect("claim");
        assert_eq!(claimed.len(), 1);
        assert!(dependency.claim_due(10).expect("second claim").is_empty());
        let restarted = FileSchedulerDependency::new(root.path().to_path_buf()).expect("restart");
        assert!(restarted.claim_due(10).expect("restart claim").is_empty());
        assert!(
            restarted
                .complete_execution(&claimed[0].execution_id, true)
                .expect("complete")
        );
        assert!(
            !restarted
                .complete_execution(&claimed[0].execution_id, true)
                .expect("idempotent complete")
        );
        assert_eq!(
            restarted.complete_execution(&claimed[0].execution_id, false),
            Err(SchedulerDependencyError::TerminalConflict)
        );
    }

    #[test]
    fn existing_schedule_can_be_replaced_and_interrupted_backup_recovers() {
        let root = TempDir::new().expect("root");
        let dependency =
            FileSchedulerDependency::new(root.path().to_path_buf()).expect("dependency");
        let original = fixture("replace", DependencyTrigger::AtMillis(i64::MAX));
        dependency.upsert(original.clone()).expect("original");
        let mut replacement = original;
        replacement.idempotency_id = "idem:replacement".to_owned();
        replacement.model = "replacement-model".to_owned();
        dependency.upsert(replacement).expect("replacement");
        assert_eq!(
            dependency.list(10).expect("list")[0].model,
            "replacement-model"
        );

        let schedule_path = root.path().join("schedules").join("replace.json");
        let backup_path = root
            .path()
            .join("schedules")
            .join("replace.json.interrupted.backup");
        fs::rename(&schedule_path, &backup_path).expect("simulate interrupted replacement");
        let restarted = FileSchedulerDependency::new(root.path().to_path_buf()).expect("restart");
        assert_eq!(
            restarted.list(10).expect("recovered")[0].model,
            "replacement-model"
        );
        assert!(!backup_path.exists());
    }

    #[test]
    fn event_and_process_triggers_use_source_ids_for_deduplication() {
        let root = TempDir::new().expect("root");
        let dependency =
            FileSchedulerDependency::new(root.path().to_path_buf()).expect("dependency");
        dependency
            .upsert(fixture(
                "event",
                DependencyTrigger::RuntimeEvent {
                    event_type: "tool.execution_completed".to_owned(),
                },
            ))
            .expect("event");
        dependency
            .upsert(fixture(
                "output",
                DependencyTrigger::ProcessOutput {
                    process_id: "process:1".to_owned(),
                    contains: "READY".to_owned(),
                },
            ))
            .expect("output");

        assert_eq!(
            dependency
                .fire_runtime_event("event:1", "tool.execution_completed")
                .expect("fire")
                .len(),
            1
        );
        assert!(
            dependency
                .fire_runtime_event("event:1", "tool.execution_completed")
                .expect("dedupe")
                .is_empty()
        );
        assert_eq!(
            dependency
                .fire_process_output("output:1", "process:1", "server READY")
                .expect("process")
                .len(),
            1
        );
        assert!(
            dependency
                .fire_process_output("output:2", "process:1", "not yet")
                .expect("no match")
                .is_empty()
        );
    }

    #[test]
    fn recurring_backlog_is_bounded_and_corruption_is_rejected() {
        let root = TempDir::new().expect("root");
        let dependency =
            FileSchedulerDependency::new(root.path().to_path_buf()).expect("dependency");
        dependency
            .upsert(fixture(
                "recurring",
                DependencyTrigger::Interval {
                    starts_at_ms: 0,
                    every_ms: 1_000,
                },
            ))
            .expect("recurring");
        let first = dependency.claim_due(3).expect("first");
        let second = dependency.claim_due(3).expect("second");
        assert_eq!(first.len(), 3);
        assert_eq!(second.len(), 3);
        assert_ne!(first[2].execution_id, second[0].execution_id);

        fs::write(
            root.path().join("schedules").join("recurring.json"),
            b"{\"tampered\":true}",
        )
        .expect("tamper");
        assert_eq!(dependency.list(10), Err(SchedulerDependencyError::Corrupt));
    }

    fn fixture(id: &str, trigger: DependencyTrigger) -> DependencySchedule {
        DependencySchedule {
            schedule_id: id.to_owned(),
            session_id: "session:1".to_owned(),
            idempotency_id: format!("idempotency:{id}"),
            style: "persistent-chat".to_owned(),
            workspace: "workspace".to_owned(),
            permission_policy: "safe-background".to_owned(),
            provider: "deterministic-mock".to_owned(),
            model: "mock".to_owned(),
            token_budget: 100,
            cost_budget_micros: 0,
            trigger,
            payload: DependencyPayload::Prompt {
                prompt: "scheduled work".to_owned(),
            },
            active: true,
        }
    }
}
