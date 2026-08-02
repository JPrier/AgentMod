//! Acceptance tests for plugin-provided graph nodes, memory, compaction,
//! context transforms, and observer delivery declarations.

use agentmod_plugin_sdk::{
    Entrypoint, FailurePolicy, IsolationMode, PluginCategory, PluginClassification,
    PluginCompactionDeclaration, PluginContextTransformBoundary, PluginContextTransformDeclaration,
    PluginIdentity, PluginManifest, PluginMemoryDeclaration, PluginNodeExecutor,
    PluginObserverDelivery, PluginScope, TrustLevel, ValidationContext, parse_toml,
    validate_manifest,
};

fn context() -> ValidationContext {
    ValidationContext {
        runtime_api_version: "1.0.0".to_owned(),
        available_capabilities: vec!["events".to_owned(), "model".to_owned()],
        maximum_timeout_ms: 300_000,
    }
}

fn manifest(category: PluginCategory) -> PluginManifest {
    let mut manifest = PluginManifest {
        schema_version: 1,
        identity: PluginIdentity {
            id: "fixture.expansion".to_owned(),
            version: "1.0.0".to_owned(),
            runtime_api: "^1.0".to_owned(),
        },
        category,
        scope: PluginScope::Session,
        classification: PluginClassification::Blocking,
        entrypoint: Entrypoint::Process {
            program: "fixture-worker".to_owned(),
            args: Vec::new(),
        },
        trust: TrustLevel::ApprovedThirdParty,
        isolation: IsolationMode::Process,
        required_capabilities: vec!["events".to_owned()],
        provided_capabilities: Vec::new(),
        subscribed_events: Vec::new(),
        authorities: Default::default(),
        permissions: Default::default(),
        ordering: Default::default(),
        configuration: agentmod_plugin_sdk::ConfigurationSchemaMetadata {
            schema_id: "fixture.config".to_owned(),
            schema_version: 1,
            required: false,
            source: agentmod_plugin_sdk::ConfigurationSchemaSource::InlineJson {
                document: "{\"type\":\"object\"}".to_owned(),
            },
        },
        failure_policy: FailurePolicy::Reject,
        timeout_ms: 5_000,
        state_migration_version: 1,
        node_executors: Vec::new(),
        memory: None,
        compaction: None,
        context_transforms: Vec::new(),
        observer_delivery: PluginObserverDelivery::BestEffort,
    };
    match category {
        PluginCategory::GraphNode => manifest.node_executors.push(PluginNodeExecutor {
            executor_id: "fixture.node".to_owned(),
            version: "1.0.0".to_owned(),
            node_kind: "emit_event".to_owned(),
            runtime_api: "^1.0".to_owned(),
            required_capabilities: vec!["events".to_owned()],
            input_schema: "{\"type\":\"object\",\"additionalProperties\":false}".to_owned(),
            output_schema: "{\"type\":\"object\",\"additionalProperties\":true}".to_owned(),
            timeout_ms: 3_000,
            failure_policy: "reject".to_owned(),
            idempotent: true,
            external_effect: false,
            read_authority: vec!["session_state".to_owned()],
            state_scope: "plugin_state".to_owned(),
        }),
        PluginCategory::Memory => {
            manifest.memory = Some(PluginMemoryDeclaration {
                scopes: vec!["session".to_owned(), "project".to_owned()],
                capabilities: vec!["retrieve".to_owned(), "write".to_owned()],
                bounded_bytes: 1024 * 1024,
            })
        }
        PluginCategory::Compaction => {
            manifest.compaction = Some(PluginCompactionDeclaration {
                strategy_id: "fixture.plugin-summary".to_owned(),
                idempotent: true,
                bounded_bytes: 64 * 1024,
            });
        }
        PluginCategory::ContextTransform => {
            manifest
                .context_transforms
                .push(PluginContextTransformDeclaration {
                    transform_id: "fixture.anonymize".to_owned(),
                    boundary: PluginContextTransformBoundary::BeforeProviderProjection,
                    stage: 10,
                    priority: 5,
                    before: Vec::new(),
                    after: Vec::new(),
                });
        }
        _ => {}
    }
    manifest
}

#[test]
fn graph_node_executor_declaration_is_accepted_and_round_trips() {
    let validated = validate_manifest(&manifest(PluginCategory::GraphNode), &context())
        .expect("graph node plugin must be valid");
    assert_eq!(
        validated.manifest().node_executors[0].executor_id,
        "fixture.node"
    );
    assert!(validated.manifest().node_executors[0].idempotent);
}

#[test]
fn graph_node_plugin_without_executor_is_rejected() {
    let mut manifest = manifest(PluginCategory::GraphNode);
    manifest.node_executors.clear();
    let errors = validate_manifest(&manifest, &context()).expect_err("must fail");
    assert!(
        errors
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code == "PLUG025")
    );
}

#[test]
fn memory_declaration_requires_memory_category() {
    let mut manifest = manifest(PluginCategory::Memory);
    manifest.category = PluginCategory::Interceptor;
    let errors = validate_manifest(&manifest, &context()).expect_err("must fail");
    assert!(
        errors
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code == "PLUG026")
    );
}

#[test]
fn compaction_declaration_requires_compaction_category() {
    let mut manifest = manifest(PluginCategory::Compaction);
    manifest.category = PluginCategory::Tool;
    let errors = validate_manifest(&manifest, &context()).expect_err("must fail");
    assert!(
        errors
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code == "PLUG027")
    );
}

#[test]
fn context_transform_plugin_requires_declared_transform() {
    let mut manifest = manifest(PluginCategory::ContextTransform);
    manifest.context_transforms.clear();
    let errors = validate_manifest(&manifest, &context()).expect_err("must fail");
    assert!(
        errors
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code == "PLUG028")
    );
}

#[test]
fn at_least_once_observer_delivery_requires_bounded_retry_policy() {
    let mut manifest = manifest(PluginCategory::Observer);
    manifest.observer_delivery = PluginObserverDelivery::AtLeastOnce {
        max_attempts: 99,
        retry_backoff_ms: 400_000,
    };
    let errors = validate_manifest(&manifest, &context()).expect_err("must fail");
    assert!(
        errors
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code == "PLUG029")
    );
}

#[test]
fn observer_delivery_defaults_round_trip_through_toml() {
    let toml = agentmod_plugin_sdk::to_toml(&manifest(PluginCategory::Observer))
        .expect("TOML serialization");
    let parsed = parse_toml(&toml).expect("TOML parse");
    assert!(matches!(
        parsed.observer_delivery,
        PluginObserverDelivery::BestEffort
    ));
}
