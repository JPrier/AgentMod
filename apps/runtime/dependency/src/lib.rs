//! Runtime-owned external adapters.

pub mod artifact;
pub mod cancellation;
pub mod continuation;
pub mod fixture_file;
pub mod harness;
pub mod harness_registry;
pub mod identity;
pub mod journal;
pub mod local_rpc;
pub mod memory;
pub mod plugin;
pub mod plugin_receipt;
pub mod process_tool;
pub mod provider_completion_receipt;
pub mod receipt;
pub mod registry;
pub mod scheduler;
pub mod snapshot;
pub mod style;
pub mod supervised;
pub mod tool;
pub mod workspace;

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Dependency-layer storage health request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStorageHealthRequest {
    /// Root configured for canonical session directories.
    pub storage_root: PathBuf,
}

/// Dependency-layer storage health response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStorageHealthResponse {
    /// Whether the configured root is usable by the current process.
    pub available: bool,
    /// Normalized display form; not a business workspace path.
    pub location: String,
}

/// Narrow runtime dependency interface consumed only by runtime data.
pub trait RuntimeDependencyPort {
    /// Examines the configured canonical storage dependency.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] for an invalid root or unavailable parent.
    fn check_storage(
        &self,
        request: DependencyStorageHealthRequest,
    ) -> Result<DependencyStorageHealthResponse, DependencyError>;
}

/// Local filesystem implementation assembled by the runtime binary.
#[derive(Clone, Debug)]
pub struct LocalRuntimeDependencies;

impl RuntimeDependencyPort for LocalRuntimeDependencies {
    fn check_storage(
        &self,
        request: DependencyStorageHealthRequest,
    ) -> Result<DependencyStorageHealthResponse, DependencyError> {
        validate_storage_root(&request.storage_root)?;
        let available = if request.storage_root.exists() {
            request.storage_root.is_dir()
        } else {
            let configured_parent = request
                .storage_root
                .parent()
                .ok_or(DependencyError::StorageRootHasNoParent)?;
            let parent = if configured_parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                configured_parent
            };
            parent.is_dir()
        };
        Ok(DependencyStorageHealthResponse {
            available,
            location: request.storage_root.to_string_lossy().into_owned(),
        })
    }
}

impl scheduler::RuntimeSchedulerDependencyPort for LocalRuntimeDependencies {
    fn upsert(
        &self,
        _schedule: scheduler::DependencyRuntimeSchedule,
    ) -> Result<scheduler::DependencyScheduleStoreResult, scheduler::RuntimeSchedulerDependencyError>
    {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }

    fn remove(
        &self,
        _schedule_id: &str,
    ) -> Result<bool, scheduler::RuntimeSchedulerDependencyError> {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }

    fn list(
        &self,
        _limit: u32,
    ) -> Result<Vec<scheduler::DependencyRuntimeSchedule>, scheduler::RuntimeSchedulerDependencyError>
    {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }

    fn claim_due(
        &self,
        _limit: u32,
    ) -> Result<
        Vec<scheduler::DependencyScheduledExecution>,
        scheduler::RuntimeSchedulerDependencyError,
    > {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }

    fn list_pending_executions(
        &self,
        _limit: u32,
    ) -> Result<
        Vec<scheduler::DependencyScheduledExecution>,
        scheduler::RuntimeSchedulerDependencyError,
    > {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }

    fn fire_runtime_event(
        &self,
        _source_session_id: &str,
        _event_id: &str,
        _event_type: &str,
    ) -> Result<
        Vec<scheduler::DependencyScheduledExecution>,
        scheduler::RuntimeSchedulerDependencyError,
    > {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }

    fn fire_process_output(
        &self,
        _source_session_id: &str,
        _output_id: &str,
        _process_id: &str,
        _output: &str,
    ) -> Result<
        Vec<scheduler::DependencyScheduledExecution>,
        scheduler::RuntimeSchedulerDependencyError,
    > {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }

    fn complete_execution(
        &self,
        _execution_id: &str,
        _succeeded: bool,
    ) -> Result<bool, scheduler::RuntimeSchedulerDependencyError> {
        Err(scheduler::RuntimeSchedulerDependencyError::InvalidConfiguration)
    }
}

fn validate_storage_root(path: &Path) -> Result<(), DependencyError> {
    if path.as_os_str().is_empty() {
        Err(DependencyError::EmptyStorageRoot)
    } else {
        Ok(())
    }
}

/// Runtime external-adapter failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DependencyError {
    /// Configuration supplied no storage root.
    #[error("runtime storage root is empty")]
    EmptyStorageRoot,
    /// A relative/root-only configuration has no usable parent.
    #[error("runtime storage root has no parent")]
    StorageRootHasNoParent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_dependency_reports_temp_root_available() {
        let directory = tempfile::tempdir().expect("temp directory");
        let response = LocalRuntimeDependencies
            .check_storage(DependencyStorageHealthRequest {
                storage_root: directory.path().to_owned(),
            })
            .expect("health check");
        assert!(response.available);
        assert!(!response.location.is_empty());
    }

    #[test]
    fn empty_root_is_rejected() {
        assert_eq!(
            LocalRuntimeDependencies.check_storage(DependencyStorageHealthRequest {
                storage_root: PathBuf::new(),
            }),
            Err(DependencyError::EmptyStorageRoot)
        );
    }

    #[test]
    fn absent_relative_storage_uses_current_directory_as_parent() {
        let name = format!("agentmod-absent-{}", uuid::Uuid::now_v7());
        let response = LocalRuntimeDependencies
            .check_storage(DependencyStorageHealthRequest {
                storage_root: PathBuf::from(name),
            })
            .expect("relative root");
        assert!(response.available);
    }
}
