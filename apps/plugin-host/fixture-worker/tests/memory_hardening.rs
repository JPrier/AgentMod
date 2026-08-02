//! Adversarial process tests for plugin-host memory and compaction hardening.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_plugin_host_dependency::{
    DependencyAuthorization, DependencyCancelInvocationRequest, DependencyConfigurationSchema,
    DependencyEntrypoint, DependencyInvocationCancellationStatus,
    DependencyInvocationCancellationTarget, DependencyLoadRequest, DependencyManifest,
    DependencyMemoryProviderDeclaration, DependencyMemoryRetrieveRequest,
    DependencyMemoryWriteRequest, DependencyOperationBinding, DependencyOperationDeclaration,
    DependencyOperationIdempotency, DependencyPluginClass, IsolatedPluginDependency,
    PluginDependencyConfig, PluginDependencyError, PluginDependencyPort,
    cancellation_action_digest, invocation_identity_digest,
};
use agentmod_plugin_protocol as protocol;
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use serde::Serialize;
use serde_json::{Value, json};
use tempfile::TempDir;
use uuid::Uuid;

const KEY: [u8; 32] = [7; 32];

struct Fixture {
    _root: TempDir,
    dependency: IsolatedPluginDependency,
    configuration: Value,
    configuration_reference: ContentHash,
    declaration_hash: ContentHash,
    marker: PathBuf,
    plugin_id: String,
    provider_id: String,
    handler: String,
}

fn operation(
    handler: &str,
    idempotency: DependencyOperationIdempotency,
) -> DependencyOperationDeclaration {
    let non_idempotent = idempotency == DependencyOperationIdempotency::NonIdempotent;
    DependencyOperationDeclaration {
        handler: handler.to_owned(),
        input_schema: String::from(r#"{"type":"object"}"#),
        output_schema: String::from(
            r#"{"type":"object","required":["accepted"],"properties":{"accepted":{"type":"boolean"}},"additionalProperties":false}"#,
        ),
        timeout_ms: 50,
        failure_policy: String::from(if non_idempotent { "reject" } else { "retry" }),
        max_attempts: if non_idempotent { 1 } else { 3 },
        retry_backoff_ms: u64::from(!non_idempotent),
        idempotency,
        tool_permissions: Vec::new(),
        network_permissions: Vec::new(),
        state_scope: String::from("session"),
        external_effects: false,
    }
}

fn manifest(
    executable: &Path,
    plugin_id: &str,
    provider_id: &str,
    handler: &str,
    write: bool,
) -> DependencyManifest {
    let selected_operation = operation(
        handler,
        if write {
            DependencyOperationIdempotency::NonIdempotent
        } else {
            DependencyOperationIdempotency::Idempotent
        },
    );
    DependencyManifest {
        schema_version: 1,
        id: plugin_id.to_owned(),
        version: String::from("1.0.0"),
        runtime_api: String::from("^0.1"),
        category: String::from("memory"),
        scope: String::from("session"),
        class: DependencyPluginClass::Extension,
        entrypoint: DependencyEntrypoint {
            program: executable.to_string_lossy().into_owned(),
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
        timeout_ms: 500,
        failure_policy: String::from("reject"),
        max_attempts: 1,
        retry_backoff_ms: 0,
        state_migration_version: 1,
        configuration_schema: DependencyConfigurationSchema {
            id: String::from("fixture.memory.configuration"),
            version: 1,
            required: true,
            inline_json: String::from(
                r#"{"type":"object","required":["marker_path"],"properties":{"marker_path":{"type":"string"}},"additionalProperties":false}"#,
            ),
        },
        node_executors: Vec::new(),
        context_transforms: Vec::new(),
        memory_providers: vec![DependencyMemoryProviderDeclaration {
            provider_id: provider_id.to_owned(),
            version: String::from("1.0.0"),
            runtime_api: String::from("^0.1"),
            capabilities: Vec::new(),
            retrieve: if write {
                operation(
                    "unused_retrieve",
                    DependencyOperationIdempotency::Idempotent,
                )
            } else {
                selected_operation.clone()
            },
            write: write.then_some(selected_operation),
        }],
        compactors: Vec::new(),
    }
}

fn now_millis() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("timestamp")
}

fn authorization<T: Serialize>(
    action: &str,
    operation: &T,
    call_id: &str,
    cancellation_id: &str,
) -> DependencyAuthorization {
    let digest = ContentHash::digest(&serde_json::to_vec(operation).expect("operation bytes"));
    let now = now_millis();
    let grant = seal_authorization(
        &AuthorizationClaims {
            owner: String::from("owner"),
            session: String::from("session-1"),
            call_id: call_id.to_owned(),
            action: action.to_owned(),
            normalized_digest: digest,
            issued_at: TimestampMillis::new(now),
            expires_at: TimestampMillis::new(now + 30_000),
            nonce: Uuid::now_v7().to_string(),
        },
        &AuthorizationKey::from_bytes(KEY),
    )
    .expect("grant");
    DependencyAuthorization {
        owner_id: String::from("owner"),
        session_id: String::from("session-1"),
        call_id: call_id.to_owned(),
        normalized_digest: digest.to_hex(),
        grant,
        cancellation_id: cancellation_id.to_owned(),
    }
}

async fn fixture(handler: &str, write: bool) -> Fixture {
    let root = tempfile::tempdir().expect("fixture root");
    let executable = PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-fixture-worker"));
    let executable_root = executable.parent().expect("executable root").to_owned();
    let marker = root.path().join("memory-invocations.log");
    let configuration = json!({"marker_path":marker.to_string_lossy()});
    let configuration_reference =
        ContentHash::digest(&serde_json::to_vec(&configuration).expect("configuration bytes"));
    let plugin_id = format!("fixture.memory.{}", Uuid::now_v7());
    let provider_id = String::from("fixture.memory.provider");
    let manifest = manifest(&executable, &plugin_id, &provider_id, handler, write);
    let declaration_hash = ContentHash::digest(
        &serde_json::to_vec(&manifest.memory_providers[0]).expect("declaration bytes"),
    );
    let dependency = IsolatedPluginDependency::new(PluginDependencyConfig {
        runtime_api_version: String::from("0.1.0"),
        protocol_version: protocol::CURRENT_PROTOCOL_VERSION,
        available_capabilities: BTreeSet::new(),
        owner_id: String::from("owner"),
        session_id: String::from("session-1"),
        authorization_key_hex: "07".repeat(32),
        state_root: root.path().join("state"),
        executable_roots: vec![executable_root],
        observer_queue_capacity: 4,
        max_response_bytes: 1024 * 1024,
        rate_limit_per_minute: 100,
        max_restarts: 4,
        audit_capacity: 64,
    })
    .await
    .expect("dependency");
    let auth = authorization(
        "plugin.load",
        &(&manifest, &configuration),
        "load-fixture",
        "load-cancel",
    );
    dependency
        .load(DependencyLoadRequest {
            manifest,
            configuration: configuration.clone(),
            authorization: auth,
        })
        .await
        .expect("load fixture");
    Fixture {
        _root: root,
        dependency,
        configuration,
        configuration_reference,
        declaration_hash,
        marker,
        plugin_id,
        provider_id,
        handler: handler.to_owned(),
    }
}

fn binding(fixture: &Fixture, invocation_id: &str) -> DependencyOperationBinding {
    DependencyOperationBinding {
        plugin_id: fixture.plugin_id.clone(),
        plugin_version: String::from("1.0.0"),
        invocation_id: invocation_id.to_owned(),
        operation_id: format!("operation-{invocation_id}"),
        session_id: String::from("session-1"),
        run_id: String::from("run-1"),
        node_id: Some(String::from("memory-node")),
        declaration_hash: fixture.declaration_hash,
        configuration_reference: fixture.configuration_reference,
        request_hash: ContentHash::from_bytes([0; 32]),
        idempotency_key: format!("key-{invocation_id}"),
        attempt: 1,
    }
}

fn protocol_binding(binding: &DependencyOperationBinding) -> protocol::PluginOperationBinding {
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

fn cancellation_request(
    binding: &DependencyOperationBinding,
    reason_code: &str,
    nonce: &str,
    idempotency_key: &str,
    cancellation_id: &str,
) -> DependencyCancelInvocationRequest {
    let mut target = DependencyInvocationCancellationTarget {
        session_id: binding.session_id.clone(),
        run_id: binding.run_id.clone(),
        plugin_id: binding.plugin_id.clone(),
        plugin_version: binding.plugin_version.clone(),
        invocation_id: binding.invocation_id.clone(),
        invocation_digest: ContentHash::digest(b"pending"),
        operation_id: binding.operation_id.clone(),
        declaration_hash: binding.declaration_hash,
        request_hash: binding.request_hash,
    };
    target.invocation_digest = invocation_identity_digest(&target).expect("invocation digest");
    let action_digest = cancellation_action_digest(
        &target,
        reason_code,
        nonce,
        idempotency_key,
        cancellation_id,
    )
    .expect("cancellation action");
    let call_id = format!("cancel-call-{}", Uuid::now_v7());
    let now = now_millis();
    let grant = seal_authorization(
        &AuthorizationClaims {
            owner: String::from("owner"),
            session: binding.session_id.clone(),
            call_id: call_id.clone(),
            action: String::from("plugin.invocation.cancel"),
            normalized_digest: action_digest,
            issued_at: TimestampMillis::new(now),
            expires_at: TimestampMillis::new(now + 30_000),
            nonce: nonce.to_owned(),
        },
        &AuthorizationKey::from_bytes(KEY),
    )
    .expect("cancellation grant");
    DependencyCancelInvocationRequest {
        target,
        reason_code: reason_code.to_owned(),
        action_digest,
        nonce: nonce.to_owned(),
        idempotency_key: idempotency_key.to_owned(),
        authorization: DependencyAuthorization {
            owner_id: String::from("owner"),
            session_id: binding.session_id.clone(),
            call_id,
            normalized_digest: action_digest.to_hex(),
            grant,
            cancellation_id: cancellation_id.to_owned(),
        },
    }
}

fn retrieve_request(
    fixture: &Fixture,
    invocation_id: &str,
    cancellation_id: &str,
) -> DependencyMemoryRetrieveRequest {
    let typed_request = protocol::PluginMemoryRetrieveRequest {
        query: String::from("current goal"),
        scopes: BTreeSet::from([protocol::PluginMemoryScope::Session]),
        max_items: 4,
        max_bytes: 4096,
        artifacts: Vec::new(),
        references: Vec::new(),
        parameters: json!({}),
    };
    let request = serde_json::to_value(&typed_request).expect("retrieve request");
    let readable_state = json!({});
    let mut binding = binding(fixture, invocation_id);
    binding.request_hash = protocol::plugin_memory_retrieve_request_hash(
        &protocol_binding(&binding),
        &fixture.provider_id,
        "1.0.0",
        &fixture.handler,
        50,
        protocol::PluginOperationIdempotency::Idempotent,
        &typed_request,
        &readable_state,
    )
    .expect("complete retrieve request hash");
    let authorization = authorization(
        "plugin.memory.retrieve.invoke",
        &(
            &binding,
            fixture.provider_id.as_str(),
            "1.0.0",
            fixture.handler.as_str(),
            50_u64,
            DependencyOperationIdempotency::Idempotent,
            &request,
            &readable_state,
            cancellation_id,
        ),
        &format!("call-{invocation_id}"),
        cancellation_id,
    );
    DependencyMemoryRetrieveRequest {
        binding,
        provider_id: fixture.provider_id.clone(),
        provider_version: String::from("1.0.0"),
        handler: fixture.handler.clone(),
        timeout_ms: 50,
        idempotency: DependencyOperationIdempotency::Idempotent,
        request,
        readable_state,
        authorization,
    }
}

fn write_request(
    fixture: &Fixture,
    invocation_id: &str,
    cancellation_id: &str,
) -> DependencyMemoryWriteRequest {
    let value = json!({"remember":"exactly once"});
    let value_hash = ContentHash::digest(&serde_json::to_vec(&value).expect("memory value bytes"));
    let typed_request = protocol::PluginMemoryWriteRequest {
        scope: protocol::PluginMemoryScope::Session,
        boundary: protocol::PluginMemoryWriteBoundary::IterationCompletion,
        value,
        value_hash,
        artifacts: Vec::new(),
        references: Vec::new(),
        security_classification: protocol::PluginSecurityClassification::Private,
        parameters: json!({}),
    };
    let request = serde_json::to_value(&typed_request).expect("write request");
    let readable_state = json!({});
    let mut binding = binding(fixture, invocation_id);
    binding.request_hash = protocol::plugin_memory_write_request_hash(
        &protocol_binding(&binding),
        &fixture.provider_id,
        "1.0.0",
        &fixture.handler,
        50,
        protocol::PluginOperationIdempotency::NonIdempotent,
        &typed_request,
        &readable_state,
    )
    .expect("complete write request hash");
    let authorization = authorization(
        "plugin.memory.write.invoke",
        &(
            &binding,
            fixture.provider_id.as_str(),
            "1.0.0",
            fixture.handler.as_str(),
            50_u64,
            DependencyOperationIdempotency::NonIdempotent,
            &request,
            &readable_state,
            cancellation_id,
        ),
        &format!("call-{invocation_id}"),
        cancellation_id,
    );
    DependencyMemoryWriteRequest {
        binding,
        provider_id: fixture.provider_id.clone(),
        provider_version: String::from("1.0.0"),
        handler: fixture.handler.clone(),
        timeout_ms: 50,
        idempotency: DependencyOperationIdempotency::NonIdempotent,
        request,
        readable_state,
        authorization,
    }
}

async fn marker_lines(path: &Path) -> usize {
    tokio::fs::read_to_string(path)
        .await
        .unwrap_or_default()
        .lines()
        .count()
}

#[tokio::test]
async fn retry_declaration_timeout_launches_worker_exactly_once_and_audits_timeout() {
    let fixture = fixture("timeout_memory_retrieve", false).await;
    let result = fixture
        .dependency
        .invoke_memory_retrieve(retrieve_request(
            &fixture,
            "retrieve-timeout",
            "cancel-timeout",
        ))
        .await;
    assert_eq!(result, Err(PluginDependencyError::Timeout));
    assert_eq!(marker_lines(&fixture.marker).await, 1);
    let audit = fixture
        .dependency
        .audits()
        .await
        .into_iter()
        .last()
        .expect("terminal audit");
    assert_eq!(audit.operation, "memory_retrieve");
    assert_eq!(audit.outcome, "timeout");
    assert_eq!(audit.attempts, 1);
}

#[tokio::test]
async fn cancellation_identity_is_signed_and_substitution_is_rejected_before_dispatch() {
    let fixture = fixture("timeout_memory_retrieve", false).await;
    let mut request = retrieve_request(&fixture, "retrieve-cancel-substitution", "cancel-a");
    request.authorization.cancellation_id = String::from("cancel-b");
    assert_eq!(
        fixture.dependency.invoke_memory_retrieve(request).await,
        Err(PluginDependencyError::Authorization)
    );
    assert_eq!(marker_lines(&fixture.marker).await, 0);
    let audit = fixture
        .dependency
        .audits()
        .await
        .into_iter()
        .last()
        .expect("rejection audit");
    assert_eq!(audit.outcome, "rejected");
    assert_eq!(audit.attempts, 0);
}

#[tokio::test]
async fn loaded_configuration_is_immutable_and_binding_substitution_never_dispatches() {
    let fixture = fixture("timeout_memory_retrieve", false).await;
    let record = fixture
        .dependency
        .get(fixture.plugin_id.clone())
        .await
        .expect("loaded record");
    let exact_auth = authorization(
        "plugin.load",
        &(&record.manifest, &fixture.configuration),
        "reload-exact",
        "reload-exact-cancel",
    );
    let exact = fixture
        .dependency
        .load(DependencyLoadRequest {
            manifest: record.manifest.clone(),
            configuration: fixture.configuration.clone(),
            authorization: exact_auth,
        })
        .await
        .expect("exact reload");
    assert_eq!(exact.attempts, 0);

    let alternate = json!({"marker_path":fixture.marker.with_extension("other").to_string_lossy()});
    let drift_auth = authorization(
        "plugin.load",
        &(&record.manifest, &alternate),
        "reload-drift",
        "reload-drift-cancel",
    );
    assert_eq!(
        fixture
            .dependency
            .load(DependencyLoadRequest {
                manifest: record.manifest,
                configuration: alternate,
                authorization: drift_auth,
            })
            .await,
        Err(PluginDependencyError::ConfigurationDrift)
    );

    let mut substituted = retrieve_request(
        &fixture,
        "retrieve-configuration-substitution",
        "cancel-substitution",
    );
    substituted.binding.configuration_reference = ContentHash::digest(b"other configuration");
    substituted.authorization = authorization(
        "plugin.memory.retrieve.invoke",
        &(
            &substituted.binding,
            substituted.provider_id.as_str(),
            substituted.provider_version.as_str(),
            substituted.handler.as_str(),
            substituted.idempotency,
            &substituted.request,
            &substituted.readable_state,
            substituted.authorization.cancellation_id.as_str(),
        ),
        "call-configuration-substitution",
        &substituted.authorization.cancellation_id,
    );
    assert_eq!(
        fixture.dependency.invoke_memory_retrieve(substituted).await,
        Err(PluginDependencyError::Invalid)
    );
    assert_eq!(marker_lines(&fixture.marker).await, 0);
}

#[tokio::test]
async fn every_post_dispatch_write_receipt_failure_is_ambiguous_and_never_retried() {
    for handler in [
        "invalid_memory_write",
        "invalid_record_memory_write",
        "invalid_receipt_memory_write",
        "oversized_receipt_memory_write",
        "wrong_hash_memory_write",
        "wrong_identity_memory_write",
        "timeout_memory_write",
    ] {
        let fixture = fixture(handler, true).await;
        let result = fixture
            .dependency
            .invoke_memory_write(write_request(
                &fixture,
                &format!("write-{handler}"),
                &format!("cancel-{handler}"),
            ))
            .await;
        assert_eq!(
            result,
            Err(PluginDependencyError::Ambiguous),
            "handler {handler}"
        );
        assert_eq!(marker_lines(&fixture.marker).await, 1, "handler {handler}");
        let audit = fixture
            .dependency
            .audits()
            .await
            .into_iter()
            .last()
            .expect("terminal audit");
        assert_eq!(audit.operation, "memory_write");
        assert_eq!(audit.outcome, "ambiguous_write");
        assert_eq!(audit.attempts, 1);
        assert!(
            !format!("{audit:?}").contains("exactly once"),
            "audit leaked payload"
        );
    }

    let cancelled = fixture("timeout_memory_write", true).await;
    let request = write_request(&cancelled, "write-cancelled", "cancel-write-live");
    let cancel_request = cancellation_request(
        &request.binding,
        "user_cancelled",
        "cancel-write-nonce",
        "cancel-write-key",
        "cancel-write-live",
    );
    let running = {
        let dependency = cancelled.dependency.clone();
        tokio::spawn(async move { dependency.invoke_memory_write(request).await })
    };
    for _ in 0..100 {
        if marker_lines(&cancelled.marker).await == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let receipt = cancelled
        .dependency
        .cancel_invocation(cancel_request)
        .await
        .expect("cancel approved write");
    assert_eq!(
        receipt.status,
        DependencyInvocationCancellationStatus::Signalled
    );
    assert_eq!(
        running.await.expect("write task"),
        Err(PluginDependencyError::Ambiguous),
        "a cancellation signal is not an original-operation terminal receipt"
    );
    assert_eq!(marker_lines(&cancelled.marker).await, 1);
}

#[tokio::test]
async fn terminal_success_crash_malformed_and_cancellation_are_redacted_and_single_attempt() {
    let success = fixture("memory_write", true).await;
    let (_, attempts) = success
        .dependency
        .invoke_memory_write(write_request(&success, "write-success", "cancel-success"))
        .await
        .expect("terminal receipt");
    assert_eq!(attempts, 1);
    assert_eq!(marker_lines(&success.marker).await, 1);
    assert_eq!(
        success
            .dependency
            .audits()
            .await
            .last()
            .expect("success audit")
            .outcome,
        "completed"
    );

    for (handler, expected_error, expected_outcome) in [
        (
            "crash_memory_retrieve",
            PluginDependencyError::Crashed,
            "crashed",
        ),
        (
            "invalid_memory_retrieve",
            PluginDependencyError::MalformedResponse,
            "malformed_result",
        ),
    ] {
        let fixture = fixture(handler, false).await;
        assert_eq!(
            fixture
                .dependency
                .invoke_memory_retrieve(retrieve_request(
                    &fixture,
                    &format!("retrieve-{handler}"),
                    &format!("cancel-{handler}"),
                ))
                .await,
            Err(expected_error)
        );
        let audit = fixture
            .dependency
            .audits()
            .await
            .into_iter()
            .last()
            .expect("terminal audit");
        assert_eq!(audit.outcome, expected_outcome);
        assert_eq!(audit.attempts, 1);
    }

    let cancelled = fixture("timeout_memory_retrieve", false).await;
    let invocation_id = String::from("retrieve-cancelled");
    let request = retrieve_request(&cancelled, &invocation_id, "cancel-live");
    let cancel_request = cancellation_request(
        &request.binding,
        "user_cancelled",
        "cancel-live-nonce",
        "cancel-live-key",
        "cancel-live",
    );
    let running = {
        let dependency = cancelled.dependency.clone();
        tokio::spawn(async move { dependency.invoke_memory_retrieve(request).await })
    };
    for _ in 0..100 {
        if marker_lines(&cancelled.marker).await == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let receipt = cancelled
        .dependency
        .cancel_invocation(cancel_request)
        .await
        .expect("cancel active retrieval");
    assert_eq!(
        receipt.status,
        DependencyInvocationCancellationStatus::Signalled
    );
    assert_eq!(
        running.await.expect("retrieval task"),
        Err(PluginDependencyError::Cancelled)
    );
    let audits = cancelled.dependency.audits().await;
    assert!(audits.iter().any(|audit| {
        audit.operation == "memory_retrieve" && audit.outcome == "cancelled" && audit.attempts == 1
    }));
    assert!(audits.iter().any(|audit| {
        audit.operation == "cancel_invocation"
            && audit.outcome == "signalled"
            && audit.attempts == 1
    }));
}
