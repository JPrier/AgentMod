//! Runtime business dataset construction.

pub mod artifact;
pub mod cancellation;
pub mod child_message;
pub mod continuation;
pub mod execution_plan;
pub mod fixture_file;
pub mod harness;
pub mod harness_registry;
pub mod identity;
pub mod journal;
pub mod local;
pub mod mcp_oauth;
pub mod memory;
pub mod node_executor;
pub mod plugin;
pub mod plugin_receipt;
pub mod provider_receipt;
pub mod registry;
pub mod scheduler;
pub mod snapshot;
pub mod style;
pub mod tool;
pub mod workspace;

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
    node_executors: Option<node_executor::RuntimeNodeExecutorData>,
    artifacts: Option<artifact::RuntimeArtifactData>,
    plugins: Option<plugin::RuntimePluginData>,
    plugin_node_receipts: Option<plugin_receipt::RuntimePluginNodeReceiptData>,
    provider_receipts: Option<provider_receipt::RuntimeProviderCompletionReceiptData>,
    cancellations: Option<cancellation::RuntimeCancellationData>,
}

impl<D> RuntimeData<D> {
    /// Creates runtime data with a concrete dependency implementation.
    #[must_use]
    pub fn new(dependency: D) -> Self {
        Self {
            dependency,
            style_cache: Arc::new(Mutex::new(BTreeMap::new())),
            memory: None,
            node_executors: None,
            artifacts: None,
            plugins: None,
            plugin_node_receipts: None,
            provider_receipts: None,
            cancellations: None,
        }
    }

    /// Adds the explicit first-party memory-provider router used by live turns.
    #[must_use]
    pub fn with_memory(mut self, memory: memory::RuntimeMemoryData) -> Self {
        self.memory = Some(memory);
        self
    }

    /// Adds the immutable node-executor capability registry assembled by the
    /// runtime composition root.
    #[must_use]
    pub fn with_node_executors(
        mut self,
        node_executors: node_executor::RuntimeNodeExecutorData,
    ) -> Self {
        self.node_executors = Some(node_executors);
        self
    }

    /// Adds the explicit first-party immutable artifact router.
    #[must_use]
    pub fn with_artifacts(mut self, artifacts: artifact::RuntimeArtifactData) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    /// Adds the isolated plugin-host router used by live style composition.
    #[must_use]
    pub fn with_plugins(mut self, plugins: plugin::RuntimePluginData) -> Self {
        self.plugins = Some(plugins);
        self
    }

    /// Adds durable plugin-node terminal receipt storage.
    #[must_use]
    pub fn with_plugin_node_receipts(
        mut self,
        receipts: plugin_receipt::RuntimePluginNodeReceiptData,
    ) -> Self {
        self.plugin_node_receipts = Some(receipts);
        self
    }

    /// Adds durable provider terminal receipt storage.
    #[must_use]
    pub fn with_provider_receipts(
        mut self,
        receipts: provider_receipt::RuntimeProviderCompletionReceiptData,
    ) -> Self {
        self.provider_receipts = Some(receipts);
        self
    }

    /// Adds the explicit runtime cancellation source.
    #[must_use]
    pub fn with_runtime_cancellations(
        mut self,
        cancellations: cancellation::RuntimeCancellationData,
    ) -> Self {
        self.cancellations = Some(cancellations);
        self
    }
}

#[async_trait::async_trait]
impl<D: Send + Sync> plugin::PluginDataPort for RuntimeData<D> {
    fn plugin_version(&self, plugin_id: &str) -> Result<String, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .plugin_version(plugin_id)
    }

    fn plugin_configuration_reference(
        &self,
        plugin_id: &str,
    ) -> Result<agentmod_primitives::ContentHash, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .plugin_configuration_reference(plugin_id)
    }

    fn context_transform_declaration(
        &self,
        plugin_id: &str,
        transform_id: &str,
        transform_version: &str,
    ) -> Result<plugin::PluginContextTransformDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .context_transform_declaration(plugin_id, transform_id, transform_version)
    }

    fn node_executor_declaration(
        &self,
        plugin_id: &str,
        executor_id: &str,
        executor_version: &str,
        node_kind: &str,
    ) -> Result<plugin::PluginNodeExecutorDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .node_executor_declaration(plugin_id, executor_id, executor_version, node_kind)
    }

    fn memory_provider_declaration(
        &self,
        plugin_id: &str,
        provider_id: &str,
        provider_version: &str,
    ) -> Result<plugin::PluginMemoryProviderDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .memory_provider_declaration(plugin_id, provider_id, provider_version)
    }

    fn compactor_declaration(
        &self,
        plugin_id: &str,
        compactor_id: &str,
        compactor_version: &str,
    ) -> Result<plugin::PluginCompactorDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .compactor_declaration(plugin_id, compactor_id, compactor_version)
    }

    async fn activate_plugins(
        &self,
        request: plugin::ActivatePluginsDataRequest,
    ) -> Result<plugin::ActivatedPluginsDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .activate_plugins(request)
            .await
    }

    async fn change_plugin_lifecycle(
        &self,
        request: plugin::ChangePluginLifecycleDataRequest,
    ) -> Result<plugin::PluginLifecycleDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .change_plugin_lifecycle(request)
            .await
    }

    async fn invoke_plugin(
        &self,
        request: plugin::InvokePluginDataRequest,
    ) -> Result<plugin::PluginDecisionDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .invoke_plugin(request)
            .await
    }

    async fn observe_event(
        &self,
        request: plugin::ObservePluginDataRequest,
    ) -> Result<plugin::PluginObservationDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .observe_event(request)
            .await
    }

    async fn invoke_node_executor(
        &self,
        request: plugin::InvokePluginNodeExecutorDataRequest,
    ) -> Result<plugin::PluginNodeOutcomeDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .invoke_node_executor(request)
            .await
    }

    async fn invoke_context_transform(
        &self,
        request: plugin::InvokePluginContextTransformDataRequest,
    ) -> Result<plugin::PluginContextTransformProposalDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .invoke_context_transform(request)
            .await
    }

    async fn retrieve_memory(
        &self,
        request: plugin::RetrievePluginMemoryDataRequest,
    ) -> Result<plugin::PluginMemoryRetrieveProposalDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .retrieve_memory(request)
            .await
    }

    async fn write_memory(
        &self,
        request: plugin::WritePluginMemoryDataRequest,
    ) -> Result<plugin::PluginMemoryWriteReceiptDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .write_memory(request)
            .await
    }

    async fn compact_context(
        &self,
        request: plugin::CompactPluginContextDataRequest,
    ) -> Result<plugin::PluginCompactionProposalDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .compact_context(request)
            .await
    }

    async fn persist_plugin_node_state(
        &self,
        request: plugin::PersistPluginNodeStateDataRequest,
    ) -> Result<plugin::PluginNodeStateReceiptDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .persist_plugin_node_state(request)
            .await
    }

    async fn load_plugin_node_state(
        &self,
        request: plugin::LoadPluginNodeStateDataRequest,
    ) -> Result<plugin::LoadedPluginNodeStateDataRecord, plugin::PluginDataError> {
        self.plugins
            .as_ref()
            .ok_or(plugin::PluginDataError::Unavailable)?
            .load_plugin_node_state(request)
            .await
    }
}

impl<D: Send + Sync> plugin_receipt::PluginNodeReceiptDataPort for RuntimeData<D> {
    fn load_plugin_node_receipt(
        &self,
        identity: plugin_receipt::PluginNodeReceiptDataIdentity,
    ) -> Result<
        Option<plugin_receipt::PluginNodeReceiptDataRecord>,
        plugin_receipt::PluginNodeReceiptDataError,
    > {
        self.plugin_node_receipts
            .as_ref()
            .ok_or(plugin_receipt::PluginNodeReceiptDataError::Unavailable)?
            .load_plugin_node_receipt(identity)
    }

    fn store_plugin_node_receipt(
        &self,
        request: plugin_receipt::StorePluginNodeReceiptDataRequest,
    ) -> Result<
        plugin_receipt::PluginNodeReceiptDataRecord,
        plugin_receipt::PluginNodeReceiptDataError,
    > {
        self.plugin_node_receipts
            .as_ref()
            .ok_or(plugin_receipt::PluginNodeReceiptDataError::Unavailable)?
            .store_plugin_node_receipt(request)
    }
}

impl<D: Send + Sync> provider_receipt::ProviderCompletionReceiptDataPort for RuntimeData<D> {
    fn load_provider_completion_receipt(
        &self,
        identity: provider_receipt::ProviderCompletionReceiptDataIdentity,
    ) -> Result<
        Option<provider_receipt::ProviderCompletionReceiptDataRecord>,
        provider_receipt::ProviderCompletionReceiptDataError,
    > {
        self.provider_receipts
            .as_ref()
            .ok_or(provider_receipt::ProviderCompletionReceiptDataError::Unavailable)?
            .load_provider_completion_receipt(identity)
    }

    fn store_provider_completion_receipt(
        &self,
        request: provider_receipt::StoreProviderCompletionReceiptDataRequest,
    ) -> Result<
        provider_receipt::ProviderCompletionReceiptDataRecord,
        provider_receipt::ProviderCompletionReceiptDataError,
    > {
        self.provider_receipts
            .as_ref()
            .ok_or(provider_receipt::ProviderCompletionReceiptDataError::Unavailable)?
            .store_provider_completion_receipt(request)
    }
}

impl<D: Send + Sync> cancellation::RuntimeCancellationDataPort for RuntimeData<D> {
    fn cancellation_requested(
        &self,
        request: cancellation::RuntimeCancellationDataRequest,
    ) -> Result<bool, cancellation::RuntimeCancellationDataError> {
        self.cancellations
            .as_ref()
            .ok_or(cancellation::RuntimeCancellationDataError::Unavailable)?
            .cancellation_requested(request)
    }
}

impl<D: Send + Sync> cancellation::RuntimeCancellationControlDataPort for RuntimeData<D> {
    fn request_runtime_cancellation(
        &self,
        command: cancellation::RequestRuntimeCancellationDataCommand,
    ) -> Result<(), cancellation::RuntimeCancellationDataError> {
        self.cancellations
            .as_ref()
            .ok_or(cancellation::RuntimeCancellationDataError::Unavailable)?
            .request_runtime_cancellation(command)
    }

    fn clear_runtime_cancellation(
        &self,
        command: cancellation::ClearRuntimeCancellationDataCommand,
    ) -> Result<bool, cancellation::RuntimeCancellationDataError> {
        self.cancellations
            .as_ref()
            .ok_or(cancellation::RuntimeCancellationDataError::Unavailable)?
            .clear_runtime_cancellation(command)
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

    fn read_artifact_range(
        &self,
        request: artifact::ReadArtifactRangeDataRequest,
    ) -> Result<artifact::ReadArtifactRangeDataRecord, artifact::ArtifactDataError> {
        self.artifacts
            .as_ref()
            .ok_or(artifact::ArtifactDataError::InvalidRequest)?
            .read_artifact_range(request)
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

impl<D> node_executor::NodeExecutorDataPort for RuntimeData<D> {
    fn list_node_executors(
        &self,
        request: node_executor::ListNodeExecutorsDataRequest,
    ) -> Result<Vec<node_executor::NodeExecutorDataRecord>, node_executor::NodeExecutorDataError>
    {
        self.node_executors
            .as_ref()
            .ok_or(node_executor::NodeExecutorDataError::Unavailable)?
            .list_node_executors(request)
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
    use std::{cell::RefCell, collections::BTreeSet, sync::Arc};

    use agentmod_primitives::ContentHash;
    use agentmod_runtime_dependency::{
        DependencyStorageHealthResponse, RuntimeDependencyPort, plugin as plugin_dependency,
    };
    use async_trait::async_trait;
    use serde_json::json;

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

    struct PluginDependency;

    #[async_trait]
    impl plugin_dependency::RuntimePluginDependencyPort for PluginDependency {
        async fn negotiate(
            &self,
            _session_id: String,
            _runtime_api_version: String,
            capabilities: BTreeSet<String>,
        ) -> Result<BTreeSet<String>, plugin_dependency::PluginDependencyError> {
            Ok(capabilities)
        }

        async fn validate_set(
            &self,
            _session_id: String,
            _manifests_json: Vec<String>,
        ) -> Result<Vec<String>, plugin_dependency::PluginDependencyError> {
            Err(plugin_dependency::PluginDependencyError::InvalidRequest)
        }

        async fn load(
            &self,
            _request: plugin_dependency::DependencyPluginLoadRequest,
        ) -> Result<
            plugin_dependency::DependencyPluginLoadResult,
            plugin_dependency::PluginDependencyError,
        > {
            Err(plugin_dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke(
            &self,
            _request: plugin_dependency::DependencyPluginInvocationRequest,
        ) -> Result<
            (plugin_dependency::DependencyPluginDecision, u8),
            plugin_dependency::PluginDependencyError,
        > {
            Err(plugin_dependency::PluginDependencyError::InvalidRequest)
        }

        async fn observe(
            &self,
            _request: plugin_dependency::DependencyPluginObservationRequest,
        ) -> Result<
            plugin_dependency::DependencyPluginObservationResult,
            plugin_dependency::PluginDependencyError,
        > {
            Err(plugin_dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke_node_executor(
            &self,
            _request: plugin_dependency::DependencyPluginNodeInvocationRequest,
        ) -> Result<
            plugin_dependency::DependencyPluginNodeOutcome,
            plugin_dependency::PluginDependencyError,
        > {
            Err(plugin_dependency::PluginDependencyError::InvalidRequest)
        }

        async fn shutdown(&self) {}
    }

    fn plugin_facade() -> RuntimeData<()> {
        let operation = plugin::PluginMemoryOperationDataRecord {
            handler: String::from("retrieve"),
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(r#"{"type":"object"}"#),
            timeout_ms: 500,
            failure_policy: String::from("reject"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            idempotent: true,
            tool_permissions: BTreeSet::new(),
            network_permissions: BTreeSet::new(),
            state_scope: String::from("session"),
            external_effects: false,
        };
        RuntimeData::new(()).with_plugins(plugin::RuntimePluginData::new(
            Arc::new(PluginDependency),
            vec![plugin::PluginManifestDataRecord {
                id: String::from("fixture.memory"),
                version: String::from("1.0.0"),
                category: String::from("memory"),
                class: String::from("blocking"),
                provided_capabilities: BTreeSet::new(),
                subscribed_events: BTreeSet::new(),
                timeout_ms: 500,
                failure_policy: String::from("reject"),
                canonical_manifest_json: String::from("{}"),
                configuration: json!({}),
                configuration_reference: ContentHash::digest(b"{}"),
                node_executors: Vec::new(),
                context_transforms: Vec::new(),
                memory_providers: vec![plugin::PluginMemoryProviderDataRecord {
                    provider_id: String::from("fixture.provider"),
                    version: String::from("1.0.0"),
                    runtime_api: String::from("^0.1"),
                    capabilities: BTreeSet::new(),
                    retrieve: operation,
                    write: None,
                    declaration_hash: ContentHash::digest(b"provider"),
                }],
                compactors: vec![plugin::PluginCompactorDataRecord {
                    compactor_id: String::from("fixture.compactor"),
                    version: String::from("1.0.0"),
                    runtime_api: String::from("^0.1"),
                    handler: String::from("compact"),
                    capabilities: BTreeSet::new(),
                    input_schema: String::from(r#"{"type":"object"}"#),
                    output_schema: String::from(r#"{"type":"object"}"#),
                    timeout_ms: 500,
                    failure_policy: String::from("reject"),
                    max_attempts: 1,
                    retry_backoff_ms: 0,
                    idempotent: true,
                    tool_permissions: BTreeSet::new(),
                    network_permissions: BTreeSet::new(),
                    state_scope: String::from("session"),
                    external_effects: false,
                    declaration_hash: ContentHash::digest(b"compactor"),
                }],
            }],
        ))
    }

    fn operation_binding(
        declaration_hash: ContentHash,
    ) -> plugin::PluginOperationBindingDataRecord {
        plugin::PluginOperationBindingDataRecord {
            plugin_id: String::from("fixture.memory"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("invocation-1"),
            operation_id: String::from("operation-1"),
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: None,
            declaration_hash,
            configuration_reference: ContentHash::digest(b"configuration"),
            request_hash: ContentHash::digest(b"request"),
            idempotency_key: String::from("idempotency-1"),
            attempt: 1,
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

    #[tokio::test]
    async fn production_facade_forwards_plugin_memory_declarations_and_invocation() {
        use plugin::PluginDataPort as _;

        let data = plugin_facade();
        assert_eq!(
            data.memory_provider_declaration("fixture.memory", "fixture.provider", "1.0.0")
                .expect("provider declaration")
                .declaration_hash,
            ContentHash::digest(b"provider")
        );
        assert_eq!(
            data.compactor_declaration("fixture.memory", "fixture.compactor", "1.0.0")
                .expect("compactor declaration")
                .declaration_hash,
            ContentHash::digest(b"compactor")
        );

        let retrieve = plugin::RetrievePluginMemoryDataRequest {
            binding: operation_binding(ContentHash::digest(b"provider")),
            provider_id: String::from("fixture.provider"),
            provider_version: String::from("1.0.0"),
            handler: String::from("retrieve"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            timeout_ms: 500,
            input: plugin::PluginMemoryRetrieveInputDataRecord {
                query: String::from("query"),
                scopes: BTreeSet::from([plugin::PluginMemoryScopeData::Session]),
                max_items: 1,
                max_bytes: 1024,
                artifacts: Vec::new(),
                references: Vec::new(),
                parameters: json!({}),
            },
            readable_state: json!({}),
            cancellation_id: String::from("cancel-1"),
        };
        assert_eq!(
            data.retrieve_memory(retrieve).await,
            Err(plugin::PluginDataError::Inactive)
        );

        let value = json!({"fact": "approved"});
        let write = plugin::WritePluginMemoryDataRequest {
            binding: operation_binding(ContentHash::digest(b"provider")),
            provider_id: String::from("fixture.provider"),
            provider_version: String::from("1.0.0"),
            handler: String::from("write"),
            timeout_ms: 500,
            input: plugin::PluginMemoryWriteInputDataRecord {
                scope: plugin::PluginMemoryScopeData::Session,
                boundary: plugin::PluginMemoryWriteBoundaryData::IterationCompletion,
                value_hash: ContentHash::digest(
                    &serde_json::to_vec(&value).expect("approved value"),
                ),
                value,
                artifacts: Vec::new(),
                references: Vec::new(),
                security_classification: plugin::PluginSecurityClassificationData::Private,
                parameters: json!({}),
            },
            readable_state: json!({}),
            cancellation_id: String::from("cancel-write"),
        };
        assert_eq!(
            data.write_memory(write).await,
            Err(plugin::PluginDataError::Inactive)
        );

        let projection = json!([]);
        let compact = plugin::CompactPluginContextDataRequest {
            binding: operation_binding(ContentHash::digest(b"compactor")),
            compactor_id: String::from("fixture.compactor"),
            compactor_version: String::from("1.0.0"),
            handler: String::from("compact"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            timeout_ms: 500,
            input: plugin::PluginCompactionInputDataRecord {
                projection_hash: ContentHash::digest(
                    &serde_json::to_vec(&projection).expect("projection"),
                ),
                projection,
                required_references: Vec::new(),
                required_artifacts: Vec::new(),
                preservation_requirements: BTreeSet::new(),
                max_replacement_bytes: 1024,
                max_projection_tokens: 64,
                parameters: json!({}),
            },
            readable_state: json!({}),
            cancellation_id: String::from("cancel-2"),
        };
        assert_eq!(
            data.compact_context(compact).await,
            Err(plugin::PluginDataError::Inactive)
        );
    }
}
