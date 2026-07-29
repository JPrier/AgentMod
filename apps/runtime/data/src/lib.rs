//! Runtime business dataset construction.

pub mod artifact;
pub mod continuation;
pub mod harness;
pub mod identity;
pub mod journal;
pub mod memory;
pub mod registry;
pub mod scheduler;
pub mod snapshot;
pub mod style;
pub mod tool;

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use agentmod_runtime_dependency::{
    DependencyError, DependencyStorageHealthRequest, RuntimeDependencyPort,
};
use thiserror::Error;

/// Data-layer request for the runtime health dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealthDataRequest {
    /// Configured canonical session directory root.
    pub session_storage_root: PathBuf,
}

/// Normalized data-layer health record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealthDataRecord {
    /// Whether canonical storage is available.
    pub canonical_storage_available: bool,
    /// Safe storage label for diagnostics.
    pub storage_label: String,
}

/// Narrow data interface consumed by runtime logic.
pub trait RuntimeDataPort {
    /// Builds the business-facing runtime health dataset.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when the injected storage dependency fails.
    fn runtime_health(
        &self,
        request: RuntimeHealthDataRequest,
    ) -> Result<RuntimeHealthDataRecord, DataError>;
}

/// Runtime data implementation routing to injected dependencies.
#[derive(Clone, Debug)]
pub struct RuntimeData<D> {
    dependency: D,
    style_cache: Arc<Mutex<BTreeMap<String, style::CachedSessionStyle>>>,
    memory: Option<memory::RuntimeMemoryData>,
    artifacts: Option<artifact::RuntimeArtifactData>,
}

impl<D> RuntimeData<D> {
    /// Creates runtime data with a concrete dependency implementation.
    #[must_use]
    pub fn new(dependency: D) -> Self {
        Self {
            dependency,
            style_cache: Arc::new(Mutex::new(BTreeMap::new())),
            memory: None,
            artifacts: None,
        }
    }

    /// Adds the explicit first-party memory-provider router used by live turns.
    #[must_use]
    pub fn with_memory(mut self, memory: memory::RuntimeMemoryData) -> Self {
        self.memory = Some(memory);
        self
    }

    /// Adds the explicit first-party immutable artifact router.
    #[must_use]
    pub fn with_artifacts(mut self, artifacts: artifact::RuntimeArtifactData) -> Self {
        self.artifacts = Some(artifacts);
        self
    }
}

impl<D> artifact::ArtifactDataPort for RuntimeData<D> {
    fn persist_artifact(
        &self,
        request: artifact::PersistArtifactDataRequest,
    ) -> Result<artifact::PersistedArtifactDataRecord, artifact::ArtifactDataError> {
        self.artifacts
            .as_ref()
            .ok_or(artifact::ArtifactDataError::InvalidRequest)?
            .persist_artifact(request)
    }

    fn inspect_artifact(
        &self,
        request: artifact::InspectArtifactDataRequest,
    ) -> Result<artifact::PersistedArtifactDataRecord, artifact::ArtifactDataError> {
        self.artifacts
            .as_ref()
            .ok_or(artifact::ArtifactDataError::InvalidRequest)?
            .inspect_artifact(request)
    }
}

impl<D> memory::MemoryDataPort for RuntimeData<D> {
    fn write_memory(
        &self,
        request: memory::WriteMemoryDataRequest,
    ) -> Result<memory::WriteMemoryDataRecord, memory::MemoryDataError> {
        self.memory
            .as_ref()
            .ok_or(memory::MemoryDataError::InvalidProvider)?
            .write_memory(request)
    }

    fn retrieve_memory(
        &self,
        request: memory::RetrieveMemoryDataRequest,
    ) -> Result<Vec<memory::RetrievedMemoryDataRecord>, memory::MemoryDataError> {
        self.memory
            .as_ref()
            .ok_or(memory::MemoryDataError::InvalidProvider)?
            .retrieve_memory(request)
    }
}

impl<D> RuntimeDataPort for RuntimeData<D>
where
    D: RuntimeDependencyPort,
{
    fn runtime_health(
        &self,
        request: RuntimeHealthDataRequest,
    ) -> Result<RuntimeHealthDataRecord, DataError> {
        let dependency_request = DependencyStorageHealthRequest {
            storage_root: request.session_storage_root,
        };
        let response = self
            .dependency
            .check_storage(dependency_request)
            .map_err(DataError::StorageDependency)?;
        Ok(RuntimeHealthDataRecord {
            canonical_storage_available: response.available,
            storage_label: response.location,
        })
    }
}

/// Runtime data-layer failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DataError {
    /// Canonical storage adapter failed.
    #[error("canonical storage dependency failed: {0}")]
    StorageDependency(DependencyError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_dependency::{DependencyStorageHealthResponse, RuntimeDependencyPort};

    use super::*;

    #[derive(Default)]
    struct MockDependency {
        observed: RefCell<Vec<DependencyStorageHealthRequest>>,
    }

    impl RuntimeDependencyPort for MockDependency {
        fn check_storage(
            &self,
            request: DependencyStorageHealthRequest,
        ) -> Result<DependencyStorageHealthResponse, DependencyError> {
            self.observed.borrow_mut().push(request);
            Ok(DependencyStorageHealthResponse {
                available: true,
                location: "fixture-sessions".into(),
            })
        }
    }

    #[test]
    fn maps_data_request_to_dependency_and_normalizes_result() {
        let data = RuntimeData::new(MockDependency::default());
        let record = data
            .runtime_health(RuntimeHealthDataRequest {
                session_storage_root: PathBuf::from("sessions"),
            })
            .expect("health record");
        assert_eq!(
            record,
            RuntimeHealthDataRecord {
                canonical_storage_available: true,
                storage_label: "fixture-sessions".into()
            }
        );
        assert_eq!(
            data.dependency.observed.into_inner(),
            vec![DependencyStorageHealthRequest {
                storage_root: PathBuf::from("sessions")
            }]
        );
    }
}
