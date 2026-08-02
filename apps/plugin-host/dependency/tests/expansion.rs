//! Process-level plugin host expansion tests: graph nodes, memory,
//! compaction, context transforms, durable observer delivery, restart
//! recovery, and lifecycle state changes through the real fixture worker.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use agentmod_plugin_host_dependency::{
    DependencyCompactionDeclaration, DependencyConfigurationSchema, DependencyContextTransform,
    DependencyContextTransformBoundary, DependencyEntrypoint, DependencyManifest,
    DependencyMemoryDeclaration, DependencyMemoryItem, DependencyMemoryResult,
    DependencyNodeExecutor, DependencyObservationRequest, DependencyObserverDelivery,
    DependencyPluginClass, DependencyPluginStatus, DurableDeliveryRecord, IsolatedPluginDependency,
    PluginDependencyConfig, PluginDependencyError, PluginDependencyPort,
};
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

fn worker_program() -> PathBuf {
    let executable = if cfg!(windows) {
        "agentmod-plugin-fixture-worker.exe"
    } else {
        "agentmod-plugin-fixture-worker"
    };
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let program = workspace.join("target/debug").join(executable);
    assert!(
        program.is_file(),
        "fixture worker must be built: run `cargo build -p agentmod-plugin-fixture-worker`"
    );
    program
}

fn dependency_config(root: &Path) -> PluginDependencyConfig {
    PluginDependencyConfig {
        runtime_api_version: "0.1.0".to_owned(),
        protocol_version: 2,
        available_capabilities: BTreeSet::from([
            "events".to_owned(),
            "tools".to_owned(),
            "plugin_state".to_owned(),
            "memory".to_owned(),
            "compaction".to_owned(),
            "context".to_owned(),
            "graph_nodes".to_owned(),
        ]),
        owner_id: "owner".to_owned(),
        session_id: "session".to_owned(),
        authorization_key_hex: "07".repeat(32),
        state_root: root.join("state"),
        executable_roots: vec![
            worker_program()
                .parent()
                .expect("worker parent")
                .to_path_buf(),
        ],
        observer_queue_capacity: 64,
        max_response_bytes: 4096,
        rate_limit_per_minute: 120,
        max_restarts: 1,
        audit_capacity: 128,
    }
}

/// Mints a short-lived grant bound to the exact normalized operation tuple.
fn mint<O: Serialize>(
    action: &str,
    operation: &O,
) -> agentmod_plugin_host_dependency::DependencyAuthorization {
    let key = AuthorizationKey::from_hex(&"07".repeat(32)).expect("key");
    let digest = ContentHash::digest(&serde_json::to_vec(operation).expect("operation json"));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let claims = AuthorizationClaims {
        owner: "owner".to_owned(),
        session: "session".to_owned(),
        call_id: Uuid::now_v7().to_string(),
        action: action.to_owned(),
        normalized_digest: digest,
        issued_at: TimestampMillis::new(now),
        expires_at: TimestampMillis::new(now.saturating_add(30_000)),
        nonce: Uuid::now_v7().to_string(),
    };
    let grant = seal_authorization(&claims, &key).expect("seal");
    agentmod_plugin_host_dependency::DependencyAuthorization {
        owner_id: "owner".to_owned(),
        session_id: "session".to_owned(),
        call_id: claims.call_id.clone(),
        normalized_digest: digest.to_hex(),
        grant,
        cancellation_id: claims.call_id,
    }
}

fn manifest_base(id: &str, class: DependencyPluginClass) -> DependencyManifest {
    DependencyManifest {
        schema_version: 1,
        id: id.to_owned(),
        version: "1.0.0".to_owned(),
        runtime_api: "0.1.0".to_owned(),
        category: match class {
            DependencyPluginClass::Observer => "observer",
            DependencyPluginClass::GraphNode => "graph_node",
            DependencyPluginClass::Memory => "memory",
            DependencyPluginClass::Compaction => "compaction",
            DependencyPluginClass::ContextTransform => "context_transform",
            _ => "interceptor",
        }
        .to_owned(),
        scope: "session".to_owned(),
        class,
        entrypoint: DependencyEntrypoint {
            program: worker_program().to_string_lossy().into_owned(),
            arguments: Vec::new(),
        },
        required_capabilities: BTreeSet::new(),
        provided_capabilities: BTreeSet::new(),
        subscribed_events: match class {
            DependencyPluginClass::Observer => {
                BTreeSet::from(["tool.execution_completed".to_owned()])
            }
            _ => BTreeSet::new(),
        },
        read_authority: BTreeSet::from(["session_state".to_owned()]),
        proposed_write_authority: BTreeSet::new(),
        tool_permissions: BTreeSet::new(),
        network_permissions: BTreeSet::new(),
        after: BTreeSet::new(),
        before: BTreeSet::new(),
        stage: 0,
        priority: 0,
        timeout_ms: 5_000,
        failure_policy: if class == DependencyPluginClass::Observer {
            "continue"
        } else {
            "reject"
        }
        .to_owned(),
        max_attempts: 1,
        retry_backoff_ms: 0,
        state_migration_version: 1,
        configuration_schema: DependencyConfigurationSchema {
            id: format!("{id}.config"),
            version: 1,
            required: false,
            inline_json: "{\"type\":\"object\",\"additionalProperties\":false}".to_owned(),
        },
        node_executors: if class == DependencyPluginClass::GraphNode {
            vec![DependencyNodeExecutor {
                executor_id: "fixture.node".to_owned(),
                version: "1.0.0".to_owned(),
                node_kind: "emit_event".to_owned(),
                runtime_api: "^1.0".to_owned(),
                required_capabilities: BTreeSet::from(["events".to_owned()]),
                input_schema: "{\"type\":\"object\"}".to_owned(),
                output_schema: "{\"type\":\"object\"}".to_owned(),
                timeout_ms: 3_000,
                failure_policy: "reject".to_owned(),
                idempotent: true,
                external_effect: false,
                read_authority: BTreeSet::from(["session_state".to_owned()]),
                state_scope: "plugin_state".to_owned(),
            }]
        } else {
            Vec::new()
        },
        memory: if class == DependencyPluginClass::Memory {
            Some(DependencyMemoryDeclaration {
                scopes: BTreeSet::from(["session".to_owned(), "project".to_owned()]),
                capabilities: BTreeSet::from(["retrieve".to_owned(), "write".to_owned()]),
                bounded_bytes: 1024 * 1024,
            })
        } else {
            None
        },
        compaction: if class == DependencyPluginClass::Compaction {
            Some(DependencyCompactionDeclaration {
                strategy_id: "fixture.plugin-summary".to_owned(),
                idempotent: true,
                bounded_bytes: 64 * 1024,
            })
        } else {
            None
        },
        context_transforms: if class == DependencyPluginClass::ContextTransform {
            vec![DependencyContextTransform {
                transform_id: "fixture.anonymize".to_owned(),
                boundary: DependencyContextTransformBoundary::BeforeProviderProjection,
                stage: 10,
                priority: 5,
                before: BTreeSet::new(),
                after: BTreeSet::new(),
            }]
        } else {
            Vec::new()
        },
        observer_delivery: DependencyObserverDelivery::BestEffort,
    }
}

async fn load_plugin(dependency: &IsolatedPluginDependency, manifest: DependencyManifest) {
    let configuration = json!({});
    let auth = mint("plugin.load", &(&manifest, &configuration));
    dependency
        .load(agentmod_plugin_host_dependency::DependencyLoadRequest {
            manifest,
            configuration,
            authorization: auth,
        })
        .await
        .expect("plugin must load");
}

#[tokio::test]
async fn graph_node_memory_compaction_and_transform_run_through_real_worker() {
    let root = tempfile::tempdir().expect("root");
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("dependency");

    load_plugin(
        &dependency,
        manifest_base("fixture.graph-node", DependencyPluginClass::GraphNode),
    )
    .await;
    let auth = mint(
        "plugin.execute_node",
        &(
            &"fixture.graph-node",
            &"node-invocation-1",
            &"fixture.node",
            &"graph-node-1",
            &"emit_event",
            &json!({"event":"node"}),
            &json!({}),
            &json!({"session_id":"session"}),
        ),
    );
    let (value, attempts) = dependency
        .execute_node(
            agentmod_plugin_host_dependency::DependencyNodeExecutionRequest {
                plugin_id: "fixture.graph-node".to_owned(),
                invocation_id: "node-invocation-1".to_owned(),
                executor_id: "fixture.node".to_owned(),
                node_id: "graph-node-1".to_owned(),
                node_kind: "emit_event".to_owned(),
                input: json!({"event":"node"}),
                variables: json!({}),
                readable_state: json!({"session_id":"session"}),
                authorization: auth,
            },
        )
        .await
        .expect("node execution");
    assert_eq!(value["ok"], json!(true));
    assert!(attempts >= 1);
    assert_eq!(
        dependency
            .get("fixture.graph-node".to_owned())
            .await
            .expect("record")
            .manifest
            .class,
        DependencyPluginClass::GraphNode
    );

    load_plugin(
        &dependency,
        manifest_base("fixture.memory", DependencyPluginClass::Memory),
    )
    .await;
    let describe_auth = mint(
        "plugin.memory_describe",
        &(
            &"fixture.memory",
            &"memory-describe-1",
            &"describe",
            &"",
            &"",
            &0,
            &Vec::<DependencyMemoryItem>::new(),
        ),
    );
    let (describe, _) = dependency
        .memory(
            "describe".to_owned(),
            agentmod_plugin_host_dependency::DependencyMemoryRequest {
                plugin_id: "fixture.memory".to_owned(),
                invocation_id: "memory-describe-1".to_owned(),
                scope: String::new(),
                query: String::new(),
                limit: 0,
                entries: Vec::new(),
                authorization: describe_auth,
            },
        )
        .await
        .expect("memory describe");
    assert!(matches!(
        describe,
        agentmod_plugin_host_dependency::DependencyMemoryResult::Describe { .. }
    ));
    let retrieve_auth = mint(
        "plugin.memory_retrieve",
        &(
            &"fixture.memory",
            &"memory-retrieve-1",
            &"retrieve",
            &"session",
            &"fixture",
            &5,
            &Vec::<DependencyMemoryItem>::new(),
        ),
    );
    let (retrieved, _) = dependency
        .memory(
            "retrieve".to_owned(),
            agentmod_plugin_host_dependency::DependencyMemoryRequest {
                plugin_id: "fixture.memory".to_owned(),
                invocation_id: "memory-retrieve-1".to_owned(),
                scope: "session".to_owned(),
                query: "fixture".to_owned(),
                limit: 5,
                entries: Vec::new(),
                authorization: retrieve_auth,
            },
        )
        .await
        .expect("memory retrieve");
    let DependencyMemoryResult::Retrieve { items } = &retrieved else {
        panic!("expected retrieval");
    };
    assert_eq!(items[0].reference, "fixture-item-1");
    let entries = vec![DependencyMemoryItem {
        reference: "fixture-item-2".to_owned(),
        content: "approved content".to_owned(),
        score: None,
        created_at_ms: 1_700_000_000_000,
    }];
    let commit_auth = mint(
        "plugin.memory_commit_write",
        &(
            &"fixture.memory",
            &"memory-commit-1",
            &"commit_write",
            &"session",
            &"",
            &0,
            &entries,
        ),
    );
    let (committed, _) = dependency
        .memory(
            "commit_write".to_owned(),
            agentmod_plugin_host_dependency::DependencyMemoryRequest {
                plugin_id: "fixture.memory".to_owned(),
                invocation_id: "memory-commit-1".to_owned(),
                scope: "session".to_owned(),
                query: String::new(),
                limit: 0,
                entries,
                authorization: commit_auth,
            },
        )
        .await
        .expect("memory commit");
    assert!(matches!(
        committed,
        agentmod_plugin_host_dependency::DependencyMemoryResult::Commit { retained: true, .. }
    ));
    let health_auth = mint(
        "plugin.memory_health",
        &(
            &"fixture.memory",
            &"memory-health-1",
            &"health",
            &"",
            &"",
            &0,
            &Vec::<DependencyMemoryItem>::new(),
        ),
    );
    let (health, _) = dependency
        .memory(
            "health".to_owned(),
            agentmod_plugin_host_dependency::DependencyMemoryRequest {
                plugin_id: "fixture.memory".to_owned(),
                invocation_id: "memory-health-1".to_owned(),
                scope: String::new(),
                query: String::new(),
                limit: 0,
                entries: Vec::new(),
                authorization: health_auth,
            },
        )
        .await
        .expect("memory health");
    assert!(matches!(
        health,
        agentmod_plugin_host_dependency::DependencyMemoryResult::Health { healthy: true, .. }
    ));

    load_plugin(
        &dependency,
        manifest_base("fixture.compaction", DependencyPluginClass::Compaction),
    )
    .await;
    let compaction_auth = mint(
        "plugin.compaction_propose",
        &(
            &"fixture.compaction",
            &"compaction-1",
            &1_u64,
            &10_u64,
            &"abc123",
            &json!([{"kind":"message"}]),
            &json!({"strategy":"summary"}),
        ),
    );
    let (replacement, size, _) = dependency
        .compaction_propose(
            agentmod_plugin_host_dependency::DependencyCompactionRequest {
                plugin_id: "fixture.compaction".to_owned(),
                invocation_id: "compaction-1".to_owned(),
                source_range_start: 1,
                source_range_end: 10,
                source_range_hash: "abc123".to_owned(),
                current_entries: json!([{"kind":"message"}]),
                proposal: json!({"strategy":"summary"}),
                authorization: compaction_auth,
            },
        )
        .await
        .expect("compaction proposal");
    assert_eq!(replacement["preserved"], json!(true));
    assert_eq!(size, 256);

    load_plugin(
        &dependency,
        manifest_base("fixture.transform", DependencyPluginClass::ContextTransform),
    )
    .await;
    let transform_auth = mint(
        "plugin.context_transform",
        &(
            &"fixture.transform",
            &"transform-1",
            &"fixture.anonymize",
            &DependencyContextTransformBoundary::BeforeProviderProjection,
            &json!({"text":"secret"}),
        ),
    );
    let (transformed, _) = dependency
        .context_transform(
            agentmod_plugin_host_dependency::DependencyContextTransformRequest {
                plugin_id: "fixture.transform".to_owned(),
                invocation_id: "transform-1".to_owned(),
                transform_id: "fixture.anonymize".to_owned(),
                boundary: DependencyContextTransformBoundary::BeforeProviderProjection,
                payload: json!({"text":"secret"}),
                authorization: transform_auth,
            },
        )
        .await
        .expect("context transform");
    assert_eq!(transformed["applied"], json!(true));
}

#[tokio::test]
async fn at_least_once_delivery_is_durable_and_recovers_after_restart() {
    let root = tempfile::tempdir().expect("root");
    let mut manifest = manifest_base("fixture.durable", DependencyPluginClass::Observer);
    manifest.observer_delivery = DependencyObserverDelivery::AtLeastOnce {
        max_attempts: 3,
        retry_backoff_ms: 10,
    };
    {
        let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
            .await
            .expect("dependency");
        load_plugin(&dependency, manifest.clone()).await;
        let observe_auth = mint(
            "plugin.observe",
            &(
                &"fixture.durable",
                &"delivery-1",
                &"observe:tool.execution_completed",
                &"tool.execution_completed",
                &json!({"sequence": 7}),
                &7_u64,
                &7_u64,
            ),
        );
        dependency
            .observe(DependencyObservationRequest {
                plugin_id: "fixture.durable".to_owned(),
                invocation_id: "delivery-1".to_owned(),
                handler: "observe:tool.execution_completed".to_owned(),
                event_type: "tool.execution_completed".to_owned(),
                event: json!({"sequence": 7}),
                event_range_start: 7,
                event_range_end: 7,
                authorization: observe_auth,
            })
            .await
            .expect("observe accepted");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let records = dependency.deliveries().await;
            if records
                .iter()
                .any(|record| record.terminal.as_deref() == Some("observer_delivery_completed"))
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "delivery did not complete: {records:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    // Restart: a fresh dependency over the same durable state root must find a
    // completed delivery and not redeliver it.
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("restarted dependency");
    let requeued = dependency.recover_deliveries().await.expect("recovery");
    assert_eq!(requeued, 0);
    let records = dependency.deliveries().await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].terminal.as_deref(),
        Some("observer_delivery_completed")
    );
}

#[tokio::test]
async fn pending_at_least_once_delivery_requeues_and_completes_after_restart() {
    let root = tempfile::tempdir().expect("root");
    let state_root = root.path().join("state");
    std::fs::create_dir_all(&state_root).expect("state dir");
    let pending = DurableDeliveryRecord {
        delivery_id: "recovered-delivery-1".to_owned(),
        plugin_id: "fixture.durable".to_owned(),
        handler: "observe:tool.execution_completed".to_owned(),
        event_type: "tool.execution_completed".to_owned(),
        event: json!({"sequence": 9}),
        event_range_start: 9,
        event_range_end: 9,
        attempts: 0,
        max_attempts: 3,
        retry_backoff_ms: 10,
        next_retry_at_ms: 1_700_000_000_000,
        terminal: None,
    };
    std::fs::write(
        state_root.join("deliveries.json"),
        serde_json::to_vec(&vec![pending]).expect("delivery json"),
    )
    .expect("write deliveries");
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("dependency");
    let mut manifest = manifest_base("fixture.durable", DependencyPluginClass::Observer);
    manifest.observer_delivery = DependencyObserverDelivery::AtLeastOnce {
        max_attempts: 3,
        retry_backoff_ms: 10,
    };
    load_plugin(&dependency, manifest).await;
    let requeued = dependency.recover_deliveries().await.expect("recovery");
    assert_eq!(requeued, 1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let records = dependency.deliveries().await;
        if records
            .iter()
            .any(|record| record.terminal.as_deref() == Some("observer_delivery_completed"))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "recovered delivery did not complete: {records:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let records = dependency.deliveries().await;
    let record = records
        .iter()
        .find(|record| record.delivery_id == "recovered-delivery-1")
        .expect("recovered record");
    assert_eq!(
        record.terminal.as_deref(),
        Some("observer_delivery_completed")
    );
    // Exactly one delivery attempt: the idempotency key prevented a duplicate
    // execution after recovery.
    assert_eq!(record.attempts, 1);
}

#[tokio::test]
async fn ambiguous_pending_delivery_past_retry_budget_fails_closed() {
    let root = tempfile::tempdir().expect("root");
    let state_root = root.path().join("state");
    std::fs::create_dir_all(&state_root).expect("state dir");
    let pending = DurableDeliveryRecord {
        delivery_id: "exhausted-delivery-1".to_owned(),
        plugin_id: "fixture.durable".to_owned(),
        handler: "observe:tool.execution_completed".to_owned(),
        event_type: "tool.execution_completed".to_owned(),
        event: json!({"sequence": 11}),
        event_range_start: 11,
        event_range_end: 11,
        attempts: 3,
        max_attempts: 3,
        retry_backoff_ms: 10,
        next_retry_at_ms: 1_700_000_000_000,
        terminal: None,
    };
    std::fs::write(
        state_root.join("deliveries.json"),
        serde_json::to_vec(&vec![pending]).expect("delivery json"),
    )
    .expect("write deliveries");
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("dependency");
    let mut manifest = manifest_base("fixture.durable", DependencyPluginClass::Observer);
    manifest.observer_delivery = DependencyObserverDelivery::AtLeastOnce {
        max_attempts: 3,
        retry_backoff_ms: 10,
    };
    load_plugin(&dependency, manifest).await;
    let requeued = dependency.recover_deliveries().await.expect("recovery");
    assert_eq!(requeued, 0);
    let records = dependency.deliveries().await;
    assert_eq!(
        records[0].terminal.as_deref(),
        Some("observer_delivery_failed")
    );
    // No delivery was executed: the worker never wrote a marker.
    let marker = state_root
        .parent()
        .expect("parent")
        .join("fixture-observer-received.log");
    assert!(!marker.exists());
}

#[tokio::test]
async fn at_most_once_delivery_deduplicates_by_invocation_id() {
    let root = tempfile::tempdir().expect("root");
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("dependency");
    let mut manifest = manifest_base("fixture.once", DependencyPluginClass::Observer);
    manifest.observer_delivery = DependencyObserverDelivery::AtMostOnce;
    load_plugin(&dependency, manifest).await;
    let request = DependencyObservationRequest {
        plugin_id: "fixture.once".to_owned(),
        invocation_id: "once-delivery-1".to_owned(),
        handler: "observe:tool.execution_completed".to_owned(),
        event_type: "tool.execution_completed".to_owned(),
        event: json!({"sequence": 1}),
        event_range_start: 1,
        event_range_end: 1,
        authorization: mint(
            "plugin.observe",
            &(
                &"fixture.once",
                &"once-delivery-1",
                &"observe:tool.execution_completed",
                &"tool.execution_completed",
                &json!({"sequence": 1}),
                &1_u64,
                &1_u64,
            ),
        ),
    };
    let first = dependency.observe(request.clone()).await.expect("first");
    assert!(first.accepted);
    let second_request = DependencyObservationRequest {
        authorization: mint(
            "plugin.observe",
            &(
                &"fixture.once",
                &"once-delivery-1",
                &"observe:tool.execution_completed",
                &"tool.execution_completed",
                &json!({"sequence": 1}),
                &1_u64,
                &1_u64,
            ),
        ),
        ..request
    };
    let second = dependency.observe(second_request).await.expect("second");
    assert!(!second.accepted);
    assert_eq!(second.dropped, 1);
}

#[tokio::test]
async fn lifecycle_disable_quarantine_unquarantine_and_reload_transition_status() {
    let root = tempfile::tempdir().expect("root");
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("dependency");
    load_plugin(
        &dependency,
        manifest_base("fixture.lifecycle", DependencyPluginClass::GraphNode),
    )
    .await;
    let disabled = dependency
        .disable(
            agentmod_plugin_host_dependency::DependencyStateChangeRequest {
                plugin_id: "fixture.lifecycle".to_owned(),
                reason: None,
                authorization: mint("plugin.disable", &"fixture.lifecycle"),
            },
        )
        .await
        .expect("disable");
    assert_eq!(disabled.outcome, "disabled");
    assert_eq!(
        dependency
            .get("fixture.lifecycle".to_owned())
            .await
            .expect("record")
            .status,
        DependencyPluginStatus::Disabled
    );
    let quarantined = dependency
        .quarantine(
            agentmod_plugin_host_dependency::DependencyStateChangeRequest {
                plugin_id: "fixture.lifecycle".to_owned(),
                reason: Some("invalid_response".to_owned()),
                authorization: mint(
                    "plugin.quarantine",
                    &(&"fixture.lifecycle", &Some("invalid_response".to_owned())),
                ),
            },
        )
        .await
        .expect("quarantine");
    assert_eq!(quarantined.outcome, "invalid_response");
    let restored = dependency
        .unquarantine(
            agentmod_plugin_host_dependency::DependencyStateChangeRequest {
                plugin_id: "fixture.lifecycle".to_owned(),
                reason: None,
                authorization: mint("plugin.unquarantine", &"fixture.lifecycle"),
            },
        )
        .await
        .expect("unquarantine");
    assert_eq!(restored.outcome, "active");
    let reloaded = dependency
        .reload(
            agentmod_plugin_host_dependency::DependencyStateChangeRequest {
                plugin_id: "fixture.lifecycle".to_owned(),
                reason: None,
                authorization: mint("plugin.reload", &"fixture.lifecycle"),
            },
        )
        .await
        .expect("reload");
    assert_eq!(reloaded.outcome, "reloaded");
    assert_eq!(
        dependency
            .get("fixture.lifecycle".to_owned())
            .await
            .expect("record")
            .status,
        DependencyPluginStatus::Active
    );
    let audits = dependency.audits().await;
    assert!(audits.iter().any(|audit| audit.outcome == "reloaded"));
}

#[tokio::test]
async fn wrong_class_operations_are_rejected() {
    let root = tempfile::tempdir().expect("root");
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("dependency");
    load_plugin(
        &dependency,
        manifest_base("fixture.graph-node", DependencyPluginClass::GraphNode),
    )
    .await;
    let auth = mint(
        "plugin.memory_describe",
        &(
            &"fixture.graph-node",
            &"wrong-class-1",
            &"describe",
            &"",
            &"",
            &0,
            &Vec::<DependencyMemoryItem>::new(),
        ),
    );
    let result = dependency
        .memory(
            "describe".to_owned(),
            agentmod_plugin_host_dependency::DependencyMemoryRequest {
                plugin_id: "fixture.graph-node".to_owned(),
                invocation_id: "wrong-class-1".to_owned(),
                scope: String::new(),
                query: String::new(),
                limit: 0,
                entries: Vec::new(),
                authorization: auth,
            },
        )
        .await;
    assert!(matches!(result, Err(PluginDependencyError::WrongClass)));
}
#[tokio::test]
async fn loaded_catalog_restores_after_host_restart() {
    let root = tempfile::tempdir().expect("root");
    {
        let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
            .await
            .expect("dependency");
        load_plugin(
            &dependency,
            manifest_base("fixture.restored", DependencyPluginClass::GraphNode),
        )
        .await;
        assert!(
            root.path().join("state/loaded.json.gen-0.json").exists()
                || std::fs::read_dir(root.path().join("state"))
                    .expect("state dir")
                    .any(|entry| entry
                        .expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .starts_with("loaded.json.gen-"))
        );
    }
    // A fresh host over the same state root restores the durable catalog and
    // can execute nodes without a new activation request.
    let dependency = IsolatedPluginDependency::new(dependency_config(root.path()))
        .await
        .expect("restarted dependency");
    let restored = dependency.restore_loaded_plugins().await.expect("restore");
    assert_eq!(restored, 1);
    let record = dependency
        .get("fixture.restored".to_owned())
        .await
        .expect("record");
    assert_eq!(record.manifest.class, DependencyPluginClass::GraphNode);
    assert_eq!(record.status, DependencyPluginStatus::Active);
}
