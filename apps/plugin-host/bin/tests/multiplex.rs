//! Process proof for correlated concurrent plugin-host transport and cancellation.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use agentmod_plugin_protocol as protocol;
use agentmod_plugin_sdk as plugin_sdk;
use agentmod_primitives::ContentHash;
use agentmod_runtime_dependency::plugin::{
    DependencyCancelPluginInvocationRequest, DependencyLoadPluginNodeStateRequest,
    DependencyPersistPluginNodeStateRequest, DependencyPluginContextTransformInvocationRequest,
    DependencyPluginContextTransformLifecycle, DependencyPluginInvocationCancellationStatus,
    DependencyPluginInvocationCancellationTarget, DependencyPluginInvocationRequest,
    DependencyPluginLifecycleAction, DependencyPluginLifecycleRequest, DependencyPluginLoadRequest,
    DependencyPluginMemoryRetrieveInput, DependencyPluginMemoryRetrieveRequest,
    DependencyPluginMemoryScope, DependencyPluginMemoryWriteBoundary,
    DependencyPluginMemoryWriteInput, DependencyPluginMemoryWriteRequest,
    DependencyPluginNodeInvocationRequest, DependencyPluginNodeStateScope,
    DependencyPluginOperationBinding, DependencyPluginSecurityClassification,
    PluginDependencyError, ProcessPluginDependency, ProcessPluginDependencyConfig,
    RuntimePluginDependencyPort,
};
use serde_json::json;

const KEY: [u8; 32] = [7; 32];

struct LoadedFixture {
    dependency: ProcessPluginDependency,
    marker: PathBuf,
    configuration_reference: ContentHash,
}

fn operation(
    handler: &str,
    idempotency: protocol::PluginOperationIdempotency,
) -> protocol::PluginMemoryRetrieveDeclaration {
    protocol::PluginMemoryRetrieveDeclaration {
        handler: handler.to_owned(),
        input_schema: String::from(r#"{"type":"object"}"#),
        output_schema: String::from(r#"{"type":"object"}"#),
        timeout_ms: 10_000,
        failure_policy: protocol::PluginOperationFailurePolicy::Reject,
        idempotency,
        required_permissions: protocol::PluginOperationPermissions::default(),
        state_scope: protocol::PluginOperationStateScope::Session,
        external_effects: false,
    }
}

fn manifest(
    worker: &Path,
    plugin_id: &str,
    provider_id: &str,
    retrieve_handler: &str,
    write_handler: Option<&str>,
) -> protocol::PluginManifest {
    protocol::PluginManifest {
        schema_version: 1,
        id: plugin_id.to_owned(),
        version: String::from("1.0.0"),
        runtime_api: String::from("^0.1"),
        category: String::from("memory"),
        scope: String::from("session"),
        class: protocol::PluginClass::Extension,
        entrypoint: protocol::PluginEntrypoint {
            program: worker.to_string_lossy().into_owned(),
            arguments: Vec::new(),
        },
        required_capabilities: BTreeSet::new(),
        provided_capabilities: BTreeSet::new(),
        subscribed_events: BTreeSet::new(),
        read_authority: BTreeSet::new(),
        proposed_write_authority: BTreeSet::new(),
        tool_permissions: BTreeSet::new(),
        network_permissions: BTreeSet::new(),
        after: BTreeSet::new(),
        before: BTreeSet::new(),
        stage: 0,
        priority: 0,
        timeout_ms: 10_000,
        failure_policy: String::from("reject"),
        max_attempts: 1,
        retry_backoff_ms: 0,
        state_migration_version: 1,
        configuration_schema: protocol::PluginConfigurationSchema {
            id: String::from("fixture.configuration"),
            version: 1,
            required: true,
            inline_json: String::from(
                r#"{"type":"object","required":["marker_path"],"properties":{"marker_path":{"type":"string"}},"additionalProperties":false}"#,
            ),
        },
        node_executors: Vec::new(),
        context_transforms: Vec::new(),
        memory_providers: vec![protocol::PluginMemoryProviderDeclaration {
            provider_id: provider_id.to_owned(),
            version: String::from("1.0.0"),
            runtime_api: String::from("^0.1"),
            capabilities: Vec::new(),
            retrieve: operation(
                retrieve_handler,
                protocol::PluginOperationIdempotency::Idempotent,
            ),
            write: write_handler.map(|handler| protocol::PluginMemoryWriteDeclaration {
                handler: handler.to_owned(),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 10_000,
                failure_policy: protocol::PluginOperationFailurePolicy::Reject,
                idempotency: protocol::PluginOperationIdempotency::NonIdempotent,
                required_permissions: protocol::PluginOperationPermissions::default(),
                state_scope: protocol::PluginOperationStateScope::Session,
                external_effects: false,
            }),
        }],
        compactors: Vec::new(),
    }
}

fn node_manifest(
    worker: &Path,
    plugin_id: &str,
    node_idempotency: protocol::NodeExecutorIdempotency,
    node_external_effects: bool,
) -> protocol::PluginManifest {
    let mut value = manifest(worker, plugin_id, "unused.provider", "retrieve", None);
    value.category = String::from("graph_node");
    value.memory_providers.clear();
    value.node_executors = vec![protocol::PluginNodeExecutorDeclaration {
        executor_id: String::from("fixture.executor"),
        version: String::from("1.0.0"),
        runtime_api: String::from("^0.1"),
        node_kind: String::from("fixture_node"),
        handler: String::from("slow_node"),
        capabilities: BTreeSet::new(),
        input_schema: String::from(r#"{"type":"object"}"#),
        output_schema: String::from(r#"{"type":"object"}"#),
        timeout_ms: 10_000,
        failure_policy: String::from("reject"),
        max_attempts: 1,
        retry_backoff_ms: 0,
        idempotency: node_idempotency,
        tool_permissions: BTreeSet::new(),
        network_permissions: BTreeSet::new(),
        state_scope: String::from("session"),
        external_effects: node_external_effects,
    }];
    value
}

fn context_manifest(worker: &Path, plugin_id: &str) -> protocol::PluginManifest {
    let mut value = manifest(worker, plugin_id, "unused.provider", "retrieve", None);
    value.category = String::from("context_transform");
    value.memory_providers.clear();
    value.context_transforms = vec![protocol::PluginContextTransformDeclaration {
        transform_id: String::from("fixture.transform"),
        version: String::from("1.0.0"),
        runtime_api: String::from("^0.1"),
        handler: String::from("slow_context"),
        lifecycle: protocol::ContextTransformLifecycle::BeforeModelRequest,
        capabilities: BTreeSet::new(),
        input_schema: String::from(r#"{"type":"object"}"#),
        output_schema: String::from(r#"{"type":"object"}"#),
        timeout_ms: 10_000,
        failure_policy: String::from("reject"),
        max_attempts: 1,
        retry_backoff_ms: 0,
        idempotency: protocol::ContextTransformIdempotency::Idempotent,
        tool_permissions: BTreeSet::new(),
        network_permissions: BTreeSet::new(),
        state_scope: String::from("session"),
        external_effects: false,
    }];
    value
}

fn interceptor_manifest(worker: &Path, plugin_id: &str) -> protocol::PluginManifest {
    let mut value = manifest(worker, plugin_id, "unused.provider", "retrieve", None);
    value.category = String::from("interceptor");
    value.class = protocol::PluginClass::Blocking;
    value.memory_providers.clear();
    value.subscribed_events = BTreeSet::from([String::from("action.proposed")]);
    value
}

fn fixture(root: &Path) -> LoadedFixture {
    let host = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-host"));
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    let marker = root.join("dispatches.log");
    let dependency = ProcessPluginDependency::new(ProcessPluginDependencyConfig {
        program: host.to_string_lossy().into_owned(),
        arguments: Vec::new(),
        owner_id: String::from("owner"),
        runtime_api_version: String::from("0.1.0"),
        sessions_root: root.join("sessions"),
        executable_roots: vec![
            host.parent().expect("host root").to_owned(),
            worker.parent().expect("worker root").to_owned(),
        ],
        authorization_key: KEY,
        maximum_frame_bytes: protocol::MAX_PLUGIN_FRAME_BYTES,
        request_timeout: Duration::from_secs(15),
    })
    .expect("runtime plugin dependency");
    LoadedFixture {
        dependency,
        marker: marker.clone(),
        configuration_reference: ContentHash::digest(
            &serde_json::to_vec(&json!({"marker_path":marker.to_string_lossy()}))
                .expect("configuration"),
        ),
    }
}

async fn load_plugin(
    fixture: &LoadedFixture,
    worker: &Path,
    plugin_id: &str,
    provider_id: &str,
    retrieve_handler: &str,
    write_handler: Option<&str>,
) -> ContentHash {
    let manifest = manifest(
        worker,
        plugin_id,
        provider_id,
        retrieve_handler,
        write_handler,
    );
    let declaration_hash = manifest.memory_providers[0]
        .declaration_hash()
        .expect("declaration hash");
    fixture
        .dependency
        .load(DependencyPluginLoadRequest {
            session_id: String::from("session-1"),
            manifest_json: serde_json::to_string(&manifest).expect("manifest"),
            configuration: json!({"marker_path":fixture.marker.to_string_lossy()}),
            cancellation_id: format!("load-{plugin_id}"),
        })
        .await
        .expect("load plugin");
    declaration_hash
}

fn retrieve_request(
    fixture: &LoadedFixture,
    plugin_id: &str,
    provider_id: &str,
    handler: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
) -> DependencyPluginMemoryRetrieveRequest {
    let input = DependencyPluginMemoryRetrieveInput {
        query: String::from("query"),
        scopes: BTreeSet::from([DependencyPluginMemoryScope::Session]),
        max_items: 4,
        max_bytes: 4096,
        artifacts: Vec::new(),
        references: Vec::new(),
        parameters: json!({}),
    };
    let wire_input = protocol::PluginMemoryRetrieveRequest {
        query: input.query.clone(),
        scopes: BTreeSet::from([protocol::PluginMemoryScope::Session]),
        max_items: input.max_items,
        max_bytes: input.max_bytes,
        artifacts: Vec::new(),
        references: Vec::new(),
        parameters: input.parameters.clone(),
    };
    let readable_state = json!({});
    let mut request_binding = binding(
        fixture,
        plugin_id,
        invocation_id,
        declaration_hash,
        ContentHash::from_bytes([0; 32]),
    );
    request_binding.request_hash = protocol::plugin_memory_retrieve_request_hash(
        &protocol_binding(&request_binding),
        provider_id,
        "1.0.0",
        handler,
        10_000,
        protocol::PluginOperationIdempotency::Idempotent,
        &wire_input,
        &readable_state,
    )
    .expect("complete retrieve request hash");
    DependencyPluginMemoryRetrieveRequest {
        binding: request_binding,
        provider_id: provider_id.to_owned(),
        provider_version: String::from("1.0.0"),
        handler: handler.to_owned(),
        max_attempts: 1,
        retry_backoff: Duration::ZERO,
        timeout: Duration::from_secs(10),
        input,
        readable_state,
        cancellation_id: format!("invoke-{invocation_id}"),
    }
}

fn write_request(
    fixture: &LoadedFixture,
    plugin_id: &str,
    provider_id: &str,
    handler: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
) -> DependencyPluginMemoryWriteRequest {
    let value = json!({"remember":"once"});
    let value_hash = ContentHash::digest(&serde_json::to_vec(&value).expect("value"));
    let input = DependencyPluginMemoryWriteInput {
        scope: DependencyPluginMemoryScope::Session,
        boundary: DependencyPluginMemoryWriteBoundary::IterationCompletion,
        value: value.clone(),
        value_hash,
        artifacts: Vec::new(),
        references: Vec::new(),
        security_classification: DependencyPluginSecurityClassification::Private,
        parameters: json!({}),
    };
    let wire_input = protocol::PluginMemoryWriteRequest {
        scope: protocol::PluginMemoryScope::Session,
        boundary: protocol::PluginMemoryWriteBoundary::IterationCompletion,
        value,
        value_hash,
        artifacts: Vec::new(),
        references: Vec::new(),
        security_classification: protocol::PluginSecurityClassification::Private,
        parameters: json!({}),
    };
    let readable_state = json!({});
    let mut request_binding = binding(
        fixture,
        plugin_id,
        invocation_id,
        declaration_hash,
        ContentHash::from_bytes([0; 32]),
    );
    request_binding.request_hash = protocol::plugin_memory_write_request_hash(
        &protocol_binding(&request_binding),
        provider_id,
        "1.0.0",
        handler,
        10_000,
        protocol::PluginOperationIdempotency::NonIdempotent,
        &wire_input,
        &readable_state,
    )
    .expect("complete write request hash");
    DependencyPluginMemoryWriteRequest {
        binding: request_binding,
        provider_id: provider_id.to_owned(),
        provider_version: String::from("1.0.0"),
        handler: handler.to_owned(),
        timeout: Duration::from_secs(10),
        input,
        readable_state,
        cancellation_id: format!("invoke-{invocation_id}"),
    }
}

fn binding(
    fixture: &LoadedFixture,
    plugin_id: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
    request_hash: ContentHash,
) -> DependencyPluginOperationBinding {
    DependencyPluginOperationBinding {
        plugin_id: plugin_id.to_owned(),
        plugin_version: String::from("1.0.0"),
        invocation_id: invocation_id.to_owned(),
        operation_id: format!("operation-{invocation_id}"),
        session_id: String::from("session-1"),
        run_id: String::from("run-1"),
        node_id: Some(String::from("memory-node")),
        declaration_hash,
        configuration_reference: fixture.configuration_reference,
        request_hash,
        idempotency_key: format!("invoke-key-{invocation_id}"),
        attempt: 1,
    }
}

fn protocol_binding(
    binding: &DependencyPluginOperationBinding,
) -> protocol::PluginOperationBinding {
    protocol::PluginOperationBinding {
        plugin_id: binding.plugin_id.clone(),
        plugin_version: binding.plugin_version.clone(),
        invocation_id: binding.invocation_id.clone(),
        operation_id: binding.operation_id.clone(),
        session_id: binding.session_id.clone(),
        run_id: binding.run_id.clone(),
        node_id: binding.node_id.clone(),
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        request_hash: binding.request_hash,
        idempotency_key: binding.idempotency_key.clone(),
        attempt: binding.attempt,
    }
}

fn exact_cancellation_target(
    plugin_id: &str,
    invocation_id: &str,
    operation_id: &str,
    declaration_hash: ContentHash,
    request_hash: ContentHash,
) -> DependencyPluginInvocationCancellationTarget {
    DependencyPluginInvocationCancellationTarget {
        session_id: String::from("session-1"),
        run_id: String::from("run-1"),
        plugin_id: plugin_id.to_owned(),
        plugin_version: String::from("1.0.0"),
        invocation_id: invocation_id.to_owned(),
        invocation_digest: protocol::plugin_invocation_identity_digest(
            "session-1",
            "run-1",
            plugin_id,
            "1.0.0",
            invocation_id,
            operation_id,
            declaration_hash,
            request_hash,
        )
        .expect("exact invocation identity"),
        operation_id: operation_id.to_owned(),
        declaration_hash,
        request_hash,
    }
}

fn node_declaration_hash(declaration: &protocol::PluginNodeExecutorDeclaration) -> ContentHash {
    let value = plugin_sdk::NodeExecutorManifest {
        executor_id: declaration.executor_id.clone(),
        version: declaration.version.clone(),
        runtime_api: declaration.runtime_api.clone(),
        node_kind: declaration.node_kind.clone(),
        handler: declaration.handler.clone(),
        capabilities: declaration.capabilities.iter().cloned().collect(),
        input_schema: declaration.input_schema.clone(),
        output_schema: declaration.output_schema.clone(),
        timeout_ms: declaration.timeout_ms,
        failure_policy: plugin_sdk::FailurePolicy::Reject,
        idempotency: match declaration.idempotency {
            protocol::NodeExecutorIdempotency::Idempotent => {
                plugin_sdk::NodeExecutorIdempotency::Idempotent
            }
            protocol::NodeExecutorIdempotency::NonIdempotent => {
                plugin_sdk::NodeExecutorIdempotency::NonIdempotent
            }
        },
        required_permissions: plugin_sdk::PermissionManifest {
            tools: declaration.tool_permissions.iter().cloned().collect(),
            network: declaration.network_permissions.iter().cloned().collect(),
        },
        state_scope: plugin_sdk::PluginScope::Session,
        external_effects: declaration.external_effects,
    };
    ContentHash::digest(&serde_json::to_vec(&value).expect("SDK node declaration"))
}

fn context_declaration_hash(
    declaration: &protocol::PluginContextTransformDeclaration,
) -> ContentHash {
    let value = plugin_sdk::ContextTransformManifest {
        transform_id: declaration.transform_id.clone(),
        version: declaration.version.clone(),
        runtime_api: declaration.runtime_api.clone(),
        handler: declaration.handler.clone(),
        lifecycle: plugin_sdk::ContextTransformLifecycle::BeforeModelRequest,
        capabilities: declaration.capabilities.iter().cloned().collect(),
        input_schema: declaration.input_schema.clone(),
        output_schema: declaration.output_schema.clone(),
        timeout_ms: declaration.timeout_ms,
        failure_policy: plugin_sdk::FailurePolicy::Reject,
        idempotency: plugin_sdk::ContextTransformIdempotency::Idempotent,
        required_permissions: plugin_sdk::PermissionManifest {
            tools: declaration.tool_permissions.iter().cloned().collect(),
            network: declaration.network_permissions.iter().cloned().collect(),
        },
        state_scope: plugin_sdk::PluginScope::Session,
        external_effects: declaration.external_effects,
    };
    ContentHash::digest(&serde_json::to_vec(&value).expect("SDK context declaration"))
}

fn node_request(
    fixture: &LoadedFixture,
    plugin_id: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
) -> DependencyPluginNodeInvocationRequest {
    let input = json!({"task":"bounded"});
    let readable_state = json!({"classification":"internal"});
    let request_hash = protocol::plugin_node_executor_invocation_request_hash(
        plugin_id,
        invocation_id,
        "fixture.executor",
        "1.0.0",
        "fixture_node",
        "slow_node",
        10_000,
        fixture.configuration_reference,
        &input,
        &readable_state,
    )
    .expect("node request hash");
    DependencyPluginNodeInvocationRequest {
        cancellation_target: exact_cancellation_target(
            plugin_id,
            invocation_id,
            "fixture.executor",
            declaration_hash,
            request_hash,
        ),
        session_id: String::from("session-1"),
        plugin_id: plugin_id.to_owned(),
        invocation_id: invocation_id.to_owned(),
        executor_id: String::from("fixture.executor"),
        executor_version: String::from("1.0.0"),
        timeout_ms: 10_000,
        configuration_reference: fixture.configuration_reference,
        node_kind: String::from("fixture_node"),
        handler: String::from("slow_node"),
        input,
        readable_state,
        cancellation_id: format!("invoke-{invocation_id}"),
    }
}

fn context_request(
    fixture: &LoadedFixture,
    plugin_id: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
) -> DependencyPluginContextTransformInvocationRequest {
    let input = json!({"projection":[]});
    let readable_state = json!({});
    let request_hash = protocol::plugin_context_transform_invocation_request_hash(
        plugin_id,
        invocation_id,
        "fixture.transform",
        "1.0.0",
        "before_model_request",
        "slow_context",
        10_000,
        fixture.configuration_reference,
        &input,
        &readable_state,
    )
    .expect("context request hash");
    DependencyPluginContextTransformInvocationRequest {
        cancellation_target: exact_cancellation_target(
            plugin_id,
            invocation_id,
            "fixture.transform",
            declaration_hash,
            request_hash,
        ),
        session_id: String::from("session-1"),
        plugin_id: plugin_id.to_owned(),
        invocation_id: invocation_id.to_owned(),
        transform_id: String::from("fixture.transform"),
        transform_version: String::from("1.0.0"),
        timeout_ms: 10_000,
        configuration_reference: fixture.configuration_reference,
        lifecycle: DependencyPluginContextTransformLifecycle::BeforeModelRequest,
        handler: String::from("slow_context"),
        input,
        readable_state,
        cancellation_id: format!("invoke-{invocation_id}"),
    }
}

fn interceptor_request(
    plugin_id: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
) -> DependencyPluginInvocationRequest {
    let proposal = json!({"action":{"kind":"fixture"}});
    let readable_state = json!({"session_id":"session-1"});
    let request_hash = protocol::plugin_interceptor_invocation_request_hash(
        plugin_id,
        invocation_id,
        "slow_interceptor",
        "action.proposed",
        &proposal,
        &readable_state,
    )
    .expect("interceptor request hash");
    DependencyPluginInvocationRequest {
        cancellation_target: exact_cancellation_target(
            plugin_id,
            invocation_id,
            "slow_interceptor",
            declaration_hash,
            request_hash,
        ),
        session_id: String::from("session-1"),
        plugin_id: plugin_id.to_owned(),
        invocation_id: invocation_id.to_owned(),
        handler: String::from("slow_interceptor"),
        kind: String::from("action.proposed"),
        payload: proposal,
        readable_state,
        cancellation_id: format!("invoke-{invocation_id}"),
    }
}

fn state_write_request(
    fixture: &LoadedFixture,
    plugin_id: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
    state: serde_json::Value,
) -> DependencyPersistPluginNodeStateRequest {
    let invocation_digest = ContentHash::digest(b"canonical node invocation");
    let state_hash = ContentHash::digest(&serde_json::to_vec(&state).expect("state"));
    let idempotency_key = String::from("state-write-1");
    let nonce = String::from("state-write-nonce-1");
    let cancellation_id = String::from("state-write-cancel-1");
    let request_hash = protocol::plugin_node_state_persist_request_hash(
        plugin_id,
        invocation_id,
        invocation_digest,
        "fixture.executor",
        "1.0.0",
        declaration_hash,
        fixture.configuration_reference,
        "session",
        0,
        None,
        &state,
        state_hash,
        &idempotency_key,
    )
    .expect("state write request hash");
    let cancellation_target = exact_cancellation_target(
        plugin_id,
        invocation_id,
        "fixture.executor:state-write",
        declaration_hash,
        request_hash,
    );
    let action_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            "session-1",
            plugin_id,
            invocation_id,
            invocation_digest,
            "fixture.executor",
            "1.0.0",
            declaration_hash,
            fixture.configuration_reference,
            DependencyPluginNodeStateScope::Session,
            0_u64,
            Option::<ContentHash>::None,
            state_hash,
            &idempotency_key,
        ))
        .expect("state write action"),
    );
    let authorization_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            &cancellation_target,
            action_digest,
            &nonce,
            &cancellation_id,
            &idempotency_key,
        ))
        .expect("state write authorization"),
    );
    DependencyPersistPluginNodeStateRequest {
        cancellation_target,
        session_id: String::from("session-1"),
        plugin_id: plugin_id.to_owned(),
        invocation_id: invocation_id.to_owned(),
        invocation_digest,
        executor_id: String::from("fixture.executor"),
        executor_version: String::from("1.0.0"),
        executor_declaration_hash: declaration_hash,
        configuration_reference: fixture.configuration_reference,
        state_scope: DependencyPluginNodeStateScope::Session,
        prior_generation: 0,
        prior_state_hash: None,
        state,
        state_hash,
        action_digest,
        authorization_digest,
        nonce,
        cancellation_id,
        idempotency_key,
    }
}

fn state_read_request(
    fixture: &LoadedFixture,
    plugin_id: &str,
    invocation_id: &str,
    declaration_hash: ContentHash,
    generation: u64,
    state_hash: ContentHash,
) -> DependencyLoadPluginNodeStateRequest {
    let invocation_digest = ContentHash::digest(b"canonical later node invocation");
    let idempotency_key = String::from("state-read-1");
    let nonce = String::from("state-read-nonce-1");
    let cancellation_id = String::from("state-read-cancel-1");
    let request_hash = protocol::plugin_node_state_load_request_hash(
        plugin_id,
        invocation_id,
        invocation_digest,
        "fixture.executor",
        "1.0.0",
        declaration_hash,
        fixture.configuration_reference,
        "session",
        generation,
        state_hash,
        &idempotency_key,
    )
    .expect("state read request hash");
    let cancellation_target = exact_cancellation_target(
        plugin_id,
        invocation_id,
        "fixture.executor:state-read",
        declaration_hash,
        request_hash,
    );
    let action_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            "session-1",
            plugin_id,
            invocation_id,
            invocation_digest,
            "fixture.executor",
            "1.0.0",
            declaration_hash,
            fixture.configuration_reference,
            DependencyPluginNodeStateScope::Session,
            generation,
            state_hash,
            &idempotency_key,
        ))
        .expect("state read action"),
    );
    let authorization_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            &cancellation_target,
            action_digest,
            &nonce,
            &cancellation_id,
            &idempotency_key,
        ))
        .expect("state read authorization"),
    );
    DependencyLoadPluginNodeStateRequest {
        cancellation_target,
        session_id: String::from("session-1"),
        plugin_id: plugin_id.to_owned(),
        invocation_id: invocation_id.to_owned(),
        invocation_digest,
        executor_id: String::from("fixture.executor"),
        executor_version: String::from("1.0.0"),
        executor_declaration_hash: declaration_hash,
        configuration_reference: fixture.configuration_reference,
        state_scope: DependencyPluginNodeStateScope::Session,
        expected_generation: generation,
        expected_state_hash: state_hash,
        action_digest,
        authorization_digest,
        nonce,
        cancellation_id,
        idempotency_key,
    }
}

fn cancellation_request(
    binding: &DependencyPluginOperationBinding,
    suffix: &str,
) -> DependencyCancelPluginInvocationRequest {
    DependencyCancelPluginInvocationRequest {
        target: cancellation_target(binding),
        reason_code: String::from("user_cancelled"),
        nonce: format!("cancel-nonce-{suffix}"),
        idempotency_key: format!("cancel-key-{suffix}"),
        cancellation_id: format!("cancel-lineage-{suffix}"),
    }
}

fn target_cancellation_request(
    target: DependencyPluginInvocationCancellationTarget,
    suffix: &str,
) -> DependencyCancelPluginInvocationRequest {
    DependencyCancelPluginInvocationRequest {
        target,
        reason_code: String::from("user_cancelled"),
        nonce: format!("cancel-nonce-{suffix}"),
        idempotency_key: format!("cancel-key-{suffix}"),
        cancellation_id: format!("cancel-lineage-{suffix}"),
    }
}

fn cancellation_target(
    binding: &DependencyPluginOperationBinding,
) -> DependencyPluginInvocationCancellationTarget {
    DependencyPluginInvocationCancellationTarget {
        session_id: binding.session_id.clone(),
        run_id: binding.run_id.clone(),
        plugin_id: binding.plugin_id.clone(),
        plugin_version: binding.plugin_version.clone(),
        invocation_id: binding.invocation_id.clone(),
        invocation_digest: protocol::plugin_invocation_identity_digest(
            &binding.session_id,
            &binding.run_id,
            &binding.plugin_id,
            &binding.plugin_version,
            &binding.invocation_id,
            &binding.operation_id,
            binding.declaration_hash,
            binding.request_hash,
        )
        .expect("invocation digest"),
        operation_id: binding.operation_id.clone(),
        declaration_hash: binding.declaration_hash,
        request_hash: binding.request_hash,
    }
}

async fn wait_for_dispatch(marker: &Path, invocation_id: &str) {
    for _ in 0..500 {
        let contents = tokio::fs::read_to_string(marker).await.unwrap_or_default();
        if contents.lines().any(|line| line == invocation_id) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("invocation did not reach isolated worker: {invocation_id}");
}

async fn dispatch_count(marker: &Path, invocation_id: &str) -> usize {
    tokio::fs::read_to_string(marker)
        .await
        .unwrap_or_default()
        .lines()
        .filter(|line| *line == invocation_id)
        .count()
}

#[tokio::test]
async fn live_cancel_preempts_worker_replays_receipt_and_preserves_write_ambiguity() {
    let root = tempfile::tempdir().expect("root");
    let fixture = fixture(root.path());
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));

    let retrieve_hash = load_plugin(
        &fixture,
        &worker,
        "fixture.slow.retrieve",
        "slow.provider",
        "slow_retrieve",
        None,
    )
    .await;
    let retrieve = retrieve_request(
        &fixture,
        "fixture.slow.retrieve",
        "slow.provider",
        "slow_retrieve",
        "retrieve-live",
        retrieve_hash,
    );
    let retrieve_binding = retrieve.binding.clone();
    let running_retrieve = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.retrieve_memory(retrieve).await })
    };
    wait_for_dispatch(&fixture.marker, "retrieve-live").await;

    let cancel = cancellation_request(&retrieve_binding, "retrieve");
    let receipt = fixture
        .dependency
        .cancel_plugin_invocation(cancel.clone())
        .await
        .expect("live cancellation");
    assert_eq!(
        receipt.status,
        DependencyPluginInvocationCancellationStatus::Signalled
    );
    assert_eq!(
        fixture
            .dependency
            .cancel_plugin_invocation(cancel)
            .await
            .expect("exact receipt replay"),
        receipt
    );
    assert!(matches!(
        running_retrieve.await.expect("retrieve task"),
        Err(PluginDependencyError::Rejected { ref code, .. }) if code == "cancelled"
    ));
    assert_eq!(dispatch_count(&fixture.marker, "retrieve-live").await, 1);

    let write_hash = load_plugin(
        &fixture,
        &worker,
        "fixture.slow.write",
        "write.provider",
        "unused_retrieve",
        Some("slow_write"),
    )
    .await;
    let write = write_request(
        &fixture,
        "fixture.slow.write",
        "write.provider",
        "slow_write",
        "write-live",
        write_hash,
    );
    let write_binding = write.binding.clone();
    let running_write = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.write_memory(write).await })
    };
    wait_for_dispatch(&fixture.marker, "write-live").await;
    fixture
        .dependency
        .cancel_plugin_invocation(cancellation_request(&write_binding, "write"))
        .await
        .expect("write cancellation signal");
    assert_eq!(
        running_write.await.expect("write task"),
        Err(PluginDependencyError::AmbiguousMemoryWrite)
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(dispatch_count(&fixture.marker, "write-live").await, 1);
    fixture.dependency.shutdown().await;
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one process proof keeps three authenticated invocation classes and their cancellation receipts adjacent"
)]
async fn authenticated_cancel_preempts_interceptor_node_and_context_processes() {
    let root = tempfile::tempdir().expect("root");
    let fixture = fixture(root.path());
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    let node_plugin_id = "fixture.execution.node";
    let context_plugin_id = "fixture.execution.context";
    let interceptor_plugin_id = "fixture.execution.interceptor";
    let mut node_manifest = node_manifest(
        &worker,
        node_plugin_id,
        protocol::NodeExecutorIdempotency::Idempotent,
        false,
    );
    node_manifest
        .entrypoint
        .arguments
        .push(fixture.marker.to_string_lossy().into_owned());
    let node_declaration_hash = node_declaration_hash(&node_manifest.node_executors[0]);
    let mut context_manifest = context_manifest(&worker, context_plugin_id);
    context_manifest
        .entrypoint
        .arguments
        .push(fixture.marker.to_string_lossy().into_owned());
    let context_declaration_hash =
        context_declaration_hash(&context_manifest.context_transforms[0]);
    let mut interceptor_manifest = interceptor_manifest(&worker, interceptor_plugin_id);
    interceptor_manifest
        .entrypoint
        .arguments
        .push(fixture.marker.to_string_lossy().into_owned());
    for (manifest, cancellation_id) in [
        (node_manifest, "load-execution-node"),
        (context_manifest, "load-execution-context"),
        (interceptor_manifest, "load-execution-interceptor"),
    ] {
        fixture
            .dependency
            .load(DependencyPluginLoadRequest {
                session_id: String::from("session-1"),
                manifest_json: serde_json::to_string(&manifest).expect("manifest"),
                configuration: json!({"marker_path":fixture.marker.to_string_lossy()}),
                cancellation_id: cancellation_id.to_owned(),
            })
            .await
            .expect("load execution plugin");
    }

    let interceptor = interceptor_request(
        interceptor_plugin_id,
        "interceptor-live",
        ContentHash::digest(b"style interceptor declaration"),
    );
    let interceptor_target = interceptor.cancellation_target.clone();
    let running_interceptor = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.invoke(interceptor).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !running_interceptor.is_finished(),
        "slow interceptor must remain active before cancellation"
    );
    let interceptor_cancel =
        target_cancellation_request(interceptor_target, "interceptor-execution");
    let interceptor_receipt = fixture
        .dependency
        .cancel_plugin_invocation(interceptor_cancel.clone())
        .await
        .expect("interceptor cancellation");
    assert_eq!(
        fixture
            .dependency
            .cancel_plugin_invocation(interceptor_cancel)
            .await
            .expect("interceptor cancellation replay"),
        interceptor_receipt
    );
    assert!(matches!(
        running_interceptor.await.expect("interceptor task"),
        Err(PluginDependencyError::Rejected { ref code, .. }) if code == "cancelled"
    ));

    let node = node_request(&fixture, node_plugin_id, "node-live", node_declaration_hash);
    let node_target = node.cancellation_target.clone();
    let running_node = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.invoke_node_executor(node).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !running_node.is_finished(),
        "node terminated before cancellation"
    );
    fixture
        .dependency
        .cancel_plugin_invocation(target_cancellation_request(node_target, "node-execution"))
        .await
        .expect("node cancellation");
    assert!(matches!(
        running_node.await.expect("node task"),
        Err(PluginDependencyError::Rejected { ref code, .. }) if code == "cancelled"
    ));

    let context = context_request(
        &fixture,
        context_plugin_id,
        "context-live",
        context_declaration_hash,
    );
    let context_target = context.cancellation_target.clone();
    let running_context = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.invoke_context_transform(context).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !running_context.is_finished(),
        "context transform terminated before cancellation"
    );
    fixture
        .dependency
        .cancel_plugin_invocation(target_cancellation_request(
            context_target,
            "context-execution",
        ))
        .await
        .expect("context cancellation");
    assert!(matches!(
        running_context.await.expect("context task"),
        Err(PluginDependencyError::Rejected { ref code, .. }) if code == "cancelled"
    ));

    let mut substituted = node_request(
        &fixture,
        node_plugin_id,
        "node-config-substitution",
        node_declaration_hash,
    );
    substituted.configuration_reference = ContentHash::digest(b"other configuration");
    let input = substituted.input.clone();
    let readable_state = substituted.readable_state.clone();
    let substituted_hash = protocol::plugin_node_executor_invocation_request_hash(
        node_plugin_id,
        &substituted.invocation_id,
        &substituted.executor_id,
        &substituted.executor_version,
        &substituted.node_kind,
        &substituted.handler,
        substituted.timeout_ms,
        substituted.configuration_reference,
        &input,
        &readable_state,
    )
    .expect("substituted request hash");
    substituted.cancellation_target = exact_cancellation_target(
        node_plugin_id,
        &substituted.invocation_id,
        &substituted.executor_id,
        node_declaration_hash,
        substituted_hash,
    );
    assert!(
        fixture
            .dependency
            .invoke_node_executor(substituted)
            .await
            .is_err(),
        "an independently self-consistent configuration substitution must fail closed"
    );
    fixture.dependency.shutdown().await;
}

#[tokio::test]
async fn non_idempotent_node_cancel_is_ambiguous_and_never_auto_relaunches() {
    let root = tempfile::tempdir().expect("root");
    let fixture = fixture(root.path());
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    let plugin_id = "fixture.ambiguous.node";
    let mut ambiguous_manifest = node_manifest(
        &worker,
        plugin_id,
        protocol::NodeExecutorIdempotency::NonIdempotent,
        true,
    );
    ambiguous_manifest
        .entrypoint
        .arguments
        .push(fixture.marker.to_string_lossy().into_owned());
    let declaration_hash = node_declaration_hash(&ambiguous_manifest.node_executors[0]);
    fixture
        .dependency
        .load(DependencyPluginLoadRequest {
            session_id: String::from("session-1"),
            manifest_json: serde_json::to_string(&ambiguous_manifest).expect("manifest"),
            configuration: json!({"marker_path":fixture.marker.to_string_lossy()}),
            cancellation_id: String::from("load-ambiguous-node"),
        })
        .await
        .expect("load ambiguous node");
    let node = node_request(&fixture, plugin_id, "node-ambiguous", declaration_hash);
    let target = node.cancellation_target.clone();
    let running = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.invoke_node_executor(node).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !running.is_finished(),
        "non-idempotent node terminated before cancellation"
    );
    fixture
        .dependency
        .cancel_plugin_invocation(target_cancellation_request(target, "ambiguous-node"))
        .await
        .expect("signal ambiguous node");
    assert!(matches!(
        running.await.expect("ambiguous node task"),
        Err(PluginDependencyError::Rejected { ref code, .. }) if code == "ambiguous_execution"
    ));
    fixture.dependency.shutdown().await;
}

#[tokio::test]
async fn disable_and_quarantine_preempt_in_flight_plugins_and_reject_future_work() {
    let root = tempfile::tempdir().expect("root");
    let fixture = fixture(root.path());
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    for (plugin_id, action, reason_code) in [
        (
            "fixture.lifecycle.disable",
            DependencyPluginLifecycleAction::Disable,
            None,
        ),
        (
            "fixture.lifecycle.quarantine",
            DependencyPluginLifecycleAction::Quarantine,
            Some(String::from("integrity_failure")),
        ),
    ] {
        let mut manifest = node_manifest(
            &worker,
            plugin_id,
            protocol::NodeExecutorIdempotency::Idempotent,
            false,
        );
        manifest
            .entrypoint
            .arguments
            .push(fixture.marker.to_string_lossy().into_owned());
        let declaration_hash = node_declaration_hash(&manifest.node_executors[0]);
        let configuration = json!({"marker_path":fixture.marker.to_string_lossy()});
        let configuration_reference =
            ContentHash::digest(&serde_json::to_vec(&configuration).expect("configuration"));
        fixture
            .dependency
            .load(DependencyPluginLoadRequest {
                session_id: String::from("session-1"),
                manifest_json: serde_json::to_string(&manifest).expect("manifest"),
                configuration,
                cancellation_id: format!("load-{plugin_id}"),
            })
            .await
            .expect("load lifecycle plugin");
        let node = node_request(
            &fixture,
            plugin_id,
            &format!("{plugin_id}-active"),
            declaration_hash,
        );
        let running = {
            let dependency = fixture.dependency.clone();
            tokio::spawn(async move { dependency.invoke_node_executor(node).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !running.is_finished(),
            "lifecycle fixture must be active before management"
        );
        let changed = fixture
            .dependency
            .change_plugin_lifecycle(DependencyPluginLifecycleRequest {
                session_id: String::from("session-1"),
                plugin_id: plugin_id.to_owned(),
                plugin_version: String::from("1.0.0"),
                configuration_reference,
                action,
                reason_code: reason_code.clone(),
                cancellation_id: format!("manage-{plugin_id}"),
            })
            .await
            .expect("change lifecycle");
        assert_eq!(changed.plugin_id, plugin_id);
        assert_eq!(
            changed.state,
            match action {
                DependencyPluginLifecycleAction::Disable => "disabled",
                DependencyPluginLifecycleAction::Enable
                | DependencyPluginLifecycleAction::Unquarantine => "active",
                DependencyPluginLifecycleAction::Quarantine => "quarantined",
            }
        );
        assert!(matches!(
            running.await.expect("managed node task"),
            Err(PluginDependencyError::Rejected { ref code, .. }) if code == "cancelled"
        ));
        let future = node_request(
            &fixture,
            plugin_id,
            &format!("{plugin_id}-future"),
            declaration_hash,
        );
        assert!(matches!(
            fixture.dependency.invoke_node_executor(future).await,
            Err(PluginDependencyError::Rejected { ref code, .. }) if code == "operation_failed"
        ));
    }
    fixture.dependency.shutdown().await;
}

#[tokio::test]
async fn node_state_write_and_read_replay_exact_receipts_and_reject_substitution() {
    let root = tempfile::tempdir().expect("root");
    let fixture = fixture(root.path());
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    let plugin_id = "fixture.execution.state";
    let node_manifest = node_manifest(
        &worker,
        plugin_id,
        protocol::NodeExecutorIdempotency::Idempotent,
        false,
    );
    let declaration_hash = node_declaration_hash(&node_manifest.node_executors[0]);
    fixture
        .dependency
        .load(DependencyPluginLoadRequest {
            session_id: String::from("session-1"),
            manifest_json: serde_json::to_string(&node_manifest).expect("manifest"),
            configuration: json!({"marker_path":fixture.marker.to_string_lossy()}),
            cancellation_id: String::from("load-state-node"),
        })
        .await
        .expect("load state node");

    let write = state_write_request(
        &fixture,
        plugin_id,
        "state-node-invocation",
        declaration_hash,
        json!({"cursor":1}),
    );
    let first = fixture
        .dependency
        .persist_plugin_node_state(write.clone())
        .await
        .expect("state write");
    let replay = fixture
        .dependency
        .persist_plugin_node_state(write.clone())
        .await
        .expect("state write replay");
    assert!(replay.replayed);
    assert_eq!(replay.receipt_id, first.receipt_id);
    assert_eq!(replay.receipt_digest, first.receipt_digest);

    let mut substituted = write;
    substituted.configuration_reference = ContentHash::digest(b"other configuration");
    assert!(
        fixture
            .dependency
            .persist_plugin_node_state(substituted)
            .await
            .is_err(),
        "state configuration substitution must fail closed"
    );

    let read = state_read_request(
        &fixture,
        plugin_id,
        "state-later-invocation",
        declaration_hash,
        first.generation,
        first.state_hash,
    );
    let loaded = fixture
        .dependency
        .load_plugin_node_state(read.clone())
        .await
        .expect("state read");
    assert_eq!(loaded.state, json!({"cursor":1}));
    let loaded_replay = fixture
        .dependency
        .load_plugin_node_state(read)
        .await
        .expect("state read replay");
    assert!(loaded_replay.receipt.replayed);
    assert_eq!(
        loaded_replay.receipt.receipt_digest,
        loaded.receipt.receipt_digest
    );
    fixture.dependency.shutdown().await;
}

#[tokio::test]
async fn concurrent_out_of_order_responses_route_exactly() {
    let root = tempfile::tempdir().expect("root");
    let fixture = fixture(root.path());
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    let delayed_hash = load_plugin(
        &fixture,
        &worker,
        "fixture.delayed",
        "delayed.provider",
        "delayed_retrieve",
        None,
    )
    .await;
    let fast_hash = load_plugin(
        &fixture,
        &worker,
        "fixture.fast",
        "fast.provider",
        "fast_retrieve",
        None,
    )
    .await;
    let delayed = retrieve_request(
        &fixture,
        "fixture.delayed",
        "delayed.provider",
        "delayed_retrieve",
        "delayed-call",
        delayed_hash,
    );
    let fast = retrieve_request(
        &fixture,
        "fixture.fast",
        "fast.provider",
        "fast_retrieve",
        "fast-call",
        fast_hash,
    );
    let delayed_task = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.retrieve_memory(delayed).await })
    };
    wait_for_dispatch(&fixture.marker, "delayed-call").await;
    let fast_result = fixture
        .dependency
        .retrieve_memory(fast)
        .await
        .expect("fast response");
    assert_eq!(fast_result.binding.invocation_id, "fast-call");
    let delayed_result = delayed_task
        .await
        .expect("delayed task")
        .expect("delayed response");
    assert_eq!(delayed_result.binding.invocation_id, "delayed-call");
    fixture.dependency.shutdown().await;
}

#[tokio::test]
async fn cancellation_correlation_substitution_fails_closed() {
    let root = tempfile::tempdir().expect("root");
    let fixture = fixture(root.path());
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    let slow_hash = load_plugin(
        &fixture,
        &worker,
        "fixture.substitution",
        "substitution.provider",
        "slow_retrieve",
        None,
    )
    .await;
    let slow = retrieve_request(
        &fixture,
        "fixture.substitution",
        "substitution.provider",
        "slow_retrieve",
        "substitution-call",
        slow_hash,
    );
    let binding = slow.binding.clone();
    let running = {
        let dependency = fixture.dependency.clone();
        tokio::spawn(async move { dependency.retrieve_memory(slow).await })
    };
    wait_for_dispatch(&fixture.marker, "substitution-call").await;
    let mut substituted = cancellation_request(&binding, "substituted");
    substituted.target.request_hash = ContentHash::digest(b"other-request");
    let substitution = fixture
        .dependency
        .cancel_plugin_invocation(substituted)
        .await;
    assert!(
        substitution.is_err(),
        "a target hash substitution must fail closed: {substitution:?}"
    );
    fixture
        .dependency
        .cancel_plugin_invocation(cancellation_request(&binding, "exact"))
        .await
        .expect("exact cancellation after substitution");
    assert!(matches!(
        running.await.expect("slow task"),
        Err(PluginDependencyError::Rejected { ref code, .. }) if code == "cancelled"
    ));
    assert_eq!(
        dispatch_count(&fixture.marker, "substitution-call").await,
        1
    );
    fixture.dependency.shutdown().await;
}

#[tokio::test]
async fn unknown_response_correlation_closes_every_process_waiter() {
    let root = tempfile::tempdir().expect("root");
    let host = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-corrupt-host-fixture"));
    let worker = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-multiplex-fixture"));
    let dependency = ProcessPluginDependency::new(ProcessPluginDependencyConfig {
        program: host.to_string_lossy().into_owned(),
        arguments: Vec::new(),
        owner_id: String::from("owner"),
        runtime_api_version: String::from("0.1.0"),
        sessions_root: root.path().join("sessions"),
        executable_roots: vec![
            host.parent().expect("host root").to_owned(),
            worker.parent().expect("worker root").to_owned(),
        ],
        authorization_key: KEY,
        maximum_frame_bytes: protocol::MAX_PLUGIN_FRAME_BYTES,
        request_timeout: Duration::from_secs(5),
    })
    .expect("runtime plugin dependency");
    let marker = root.path().join("unused.log");
    let request = |plugin_id: &str, provider_id: &str| DependencyPluginLoadRequest {
        session_id: String::from("session-1"),
        manifest_json: serde_json::to_string(&manifest(
            &worker,
            plugin_id,
            provider_id,
            "fast_retrieve",
            None,
        ))
        .expect("manifest"),
        configuration: json!({"marker_path":marker.to_string_lossy()}),
        cancellation_id: format!("load-{plugin_id}"),
    };
    let first = {
        let dependency = dependency.clone();
        let request = request("fixture.correlation.one", "correlation.one");
        tokio::spawn(async move { dependency.load(request).await })
    };
    let second = {
        let dependency = dependency.clone();
        let request = request("fixture.correlation.two", "correlation.two");
        tokio::spawn(async move { dependency.load(request).await })
    };

    assert!(matches!(
        first.await.expect("first waiter"),
        Err(PluginDependencyError::InvalidResponse)
    ));
    assert!(matches!(
        second.await.expect("second waiter"),
        Err(PluginDependencyError::InvalidResponse)
    ));
    dependency.shutdown().await;
}
