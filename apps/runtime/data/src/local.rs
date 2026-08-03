//! Data-owned construction of the local runtime adapter.
//!
//! Runtime logic uses this opaque data interface in production-path tests
//! without importing dependency-layer implementations.

use std::{path::PathBuf, sync::Arc};

use agentmod_runtime_dependency::{
    LocalRuntimeDependencies, cancellation::RuntimeCancellationDependency,
    plugin_receipt::FilePluginNodeReceiptDependency,
};
use thiserror::Error;

use crate::{
    RuntimeData,
    artifact::{ArtifactDataPort, RuntimeArtifactData},
    cancellation::{
        RuntimeCancellationControlDataPort, RuntimeCancellationData, RuntimeCancellationDataPort,
    },
    execution_plan::ExecutionPlanDataPort,
    fixture_file::FixtureFileDataPort,
    identity::EventIdentityDataPort,
    journal::JournalEventDataPort,
    node_executor::{NodeExecutorDataPort, RuntimeNodeExecutorData},
    plugin_receipt::{PluginNodeReceiptDataPort, RuntimePluginNodeReceiptData},
    registry::SessionRegistryDataPort,
    style::SessionStyleDataPort,
    workspace::WorkspaceLeaseDataPort,
};

/// Complete local data surface required by runtime production-path tests.
pub trait LocalRuntimeDataPort:
    Clone
    + Send
    + Sync
    + ArtifactDataPort
    + EventIdentityDataPort
    + ExecutionPlanDataPort
    + FixtureFileDataPort
    + JournalEventDataPort
    + NodeExecutorDataPort
    + PluginNodeReceiptDataPort
    + RuntimeCancellationControlDataPort
    + RuntimeCancellationDataPort
    + SessionRegistryDataPort
    + SessionStyleDataPort
    + WorkspaceLeaseDataPort
{
}

impl<T> LocalRuntimeDataPort for T where
    T: Clone
        + Send
        + Sync
        + ArtifactDataPort
        + EventIdentityDataPort
        + ExecutionPlanDataPort
        + FixtureFileDataPort
        + JournalEventDataPort
        + NodeExecutorDataPort
        + PluginNodeReceiptDataPort
        + RuntimeCancellationControlDataPort
        + RuntimeCancellationDataPort
        + SessionRegistryDataPort
        + SessionStyleDataPort
        + WorkspaceLeaseDataPort
{
}

/// Constructs the local runtime data adapter without optional providers.
#[must_use]
pub fn local_runtime_data() -> impl LocalRuntimeDataPort {
    RuntimeData::new(LocalRuntimeDependencies)
}

/// Constructs local runtime data with one exact immutable executor registry.
#[must_use]
pub fn local_runtime_data_with_node_executors(
    registry: RuntimeNodeExecutorData,
) -> impl LocalRuntimeDataPort {
    RuntimeData::new(LocalRuntimeDependencies).with_node_executors(registry)
}

/// Constructs local runtime data with first-party artifact persistence.
#[must_use]
pub fn local_runtime_data_with_artifacts() -> impl LocalRuntimeDataPort {
    RuntimeData::new(LocalRuntimeDependencies).with_artifacts(RuntimeArtifactData::first_party())
}

/// Constructs local runtime data with durable plugin receipts and cancellation.
///
/// # Errors
///
/// Returns [`LocalRuntimeDataError`] when the durable receipt root cannot be
/// initialized.
pub fn local_plugin_runtime_data(
    receipt_root: PathBuf,
) -> Result<impl LocalRuntimeDataPort, LocalRuntimeDataError> {
    let receipts = FilePluginNodeReceiptDependency::new(receipt_root)
        .map_err(|_| LocalRuntimeDataError::ReceiptInitialization)?;
    Ok(RuntimeData::new(LocalRuntimeDependencies)
        .with_plugin_node_receipts(RuntimePluginNodeReceiptData::new(Arc::new(receipts)))
        .with_runtime_cancellations(RuntimeCancellationData::new(Arc::new(
            RuntimeCancellationDependency::default(),
        ))))
}

/// Local data adapter construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalRuntimeDataError {
    /// Durable plugin receipt storage could not be initialized.
    #[error("local plugin receipt storage initialization failed")]
    ReceiptInitialization,
}
