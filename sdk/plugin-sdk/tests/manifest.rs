//! Acceptance, golden-format, and parser property tests.

use agentmod_plugin_sdk::{
    CompactorManifest, ContextTransformIdempotency, ContextTransformLifecycle,
    ContextTransformManifest, Entrypoint, FailurePolicy, IsolationMode, MemoryProviderManifest,
    MemoryRetrieveManifest, MemoryWriteManifest, NodeExecutorIdempotency, NodeExecutorManifest,
    PermissionManifest, PluginCategory, PluginClassification, PluginManifest,
    PluginOperationIdempotency, PluginScope, TrustLevel, ValidationContext, parse_json, parse_toml,
    to_json, to_toml, validate_manifest, validate_plugin_set,
};
use proptest::prelude::*;

const GOLDEN_TOML: &str = include_str!("golden/authorized-interceptor.toml");
const GOLDEN_JSON: &str = include_str!("golden/authorized-interceptor.json");

fn context() -> ValidationContext {
    ValidationContext {
        runtime_api_version: "1.2.0".to_owned(),
        available_capabilities: vec!["runtime.events".to_owned()],
        maximum_timeout_ms: 30_000,
    }
}

#[test]
fn graph_node_executor_metadata_is_exact_and_authority_bounded() {
    let mut candidate = manifest();
    candidate.category = PluginCategory::GraphNode;
    candidate.subscribed_events.clear();
    candidate.provided_capabilities.push("node.echo".into());
    candidate.permissions.tools.push("tools.echo".into());
    candidate.node_executors.push(NodeExecutorManifest {
        executor_id: "fixture.echo".into(),
        version: "2.1.0".into(),
        runtime_api: "^1.2".into(),
        node_kind: "plugin_echo".into(),
        handler: "execute_echo".into(),
        capabilities: vec!["node.echo".into()],
        input_schema: r#"{"type":"object"}"#.into(),
        output_schema: r#"{"type":"object","required":["echo"]}"#.into(),
        timeout_ms: 500,
        failure_policy: FailurePolicy::Reject,
        idempotency: NodeExecutorIdempotency::NonIdempotent,
        required_permissions: PermissionManifest {
            tools: vec!["tools.echo".into()],
            network: Vec::new(),
        },
        state_scope: PluginScope::Invocation,
        external_effects: false,
    });
    validate_manifest(&candidate, &context()).expect("exact executor declaration");

    candidate.node_executors[0]
        .required_permissions
        .network
        .push("undeclared.example".into());
    let report = validate_manifest(&candidate, &context()).expect_err("authority must be bounded");
    assert!(codes(&report).contains(&"PLUG032"));
}

fn context_transform_manifest() -> PluginManifest {
    let mut candidate = manifest();
    candidate.category = PluginCategory::ContextTransform;
    candidate.classification = PluginClassification::Blocking;
    candidate.entrypoint = Entrypoint::Process {
        program: "agentmod-context-transform".into(),
        args: Vec::new(),
    };
    candidate.trust = TrustLevel::ApprovedThirdParty;
    candidate.isolation = IsolationMode::Process;
    candidate.subscribed_events.clear();
    candidate.authorities.proposed_write.clear();
    candidate
        .provided_capabilities
        .push("context.redaction".into());
    candidate.permissions.tools.push("artifact.read".into());
    candidate.context_transforms.push(ContextTransformManifest {
        transform_id: "fixture.redact".into(),
        version: "1.0.0".into(),
        runtime_api: "^1.2".into(),
        handler: "redact_projection".into(),
        lifecycle: ContextTransformLifecycle::BeforeModelRequest,
        capabilities: vec!["context.redaction".into()],
        input_schema: r#"{"type":"object","required":["projection"]}"#.into(),
        output_schema: r#"{"type":"object","required":["replacement"]}"#.into(),
        timeout_ms: 500,
        failure_policy: FailurePolicy::Reject,
        idempotency: ContextTransformIdempotency::Idempotent,
        required_permissions: PermissionManifest {
            tools: vec!["artifact.read".into()],
            network: Vec::new(),
        },
        state_scope: PluginScope::ModelCall,
        external_effects: false,
    });
    candidate
}

#[test]
fn context_transform_declaration_is_exact_bounded_pure_and_idempotent() {
    let candidate = context_transform_manifest();
    validate_manifest(&candidate, &context()).expect("exact context-transform declaration");

    let mut wrong_category = candidate.clone();
    wrong_category.category = PluginCategory::Tool;
    assert!(
        codes(&validate_manifest(&wrong_category, &context()).expect_err("category"))
            .contains(&"PLUG033")
    );

    let mut duplicate = candidate.clone();
    duplicate
        .context_transforms
        .push(duplicate.context_transforms[0].clone());
    assert!(
        codes(&validate_manifest(&duplicate, &context()).expect_err("duplicate"))
            .contains(&"PLUG034")
    );

    let mut incompatible = candidate.clone();
    incompatible.context_transforms[0].runtime_api = "^9.0".into();
    assert!(
        codes(&validate_manifest(&incompatible, &context()).expect_err("runtime API"))
            .contains(&"PLUG036")
    );

    let mut invalid_timeout = candidate.clone();
    invalid_timeout.context_transforms[0].timeout_ms = 0;
    assert!(
        codes(&validate_manifest(&invalid_timeout, &context()).expect_err("timeout"))
            .contains(&"PLUG037")
    );

    let mut missing_capability = candidate.clone();
    missing_capability.context_transforms[0]
        .capabilities
        .push("context.missing".into());
    assert!(
        codes(&validate_manifest(&missing_capability, &context()).expect_err("capability"))
            .contains(&"PLUG038")
    );

    let mut invalid_schema = candidate.clone();
    invalid_schema.context_transforms[0].output_schema = String::from("[]");
    assert!(
        codes(&validate_manifest(&invalid_schema, &context()).expect_err("schema"))
            .contains(&"PLUG039")
    );

    let mut excess_permission = candidate.clone();
    excess_permission.context_transforms[0]
        .required_permissions
        .network
        .push("undeclared.example".into());
    assert!(
        codes(&validate_manifest(&excess_permission, &context()).expect_err("permission"))
            .contains(&"PLUG040")
    );

    let mut non_idempotent = candidate.clone();
    non_idempotent.context_transforms[0].idempotency = ContextTransformIdempotency::NonIdempotent;
    assert!(
        codes(&validate_manifest(&non_idempotent, &context()).expect_err("idempotency"))
            .contains(&"PLUG041")
    );

    let mut effectful = candidate;
    effectful.context_transforms[0].external_effects = true;
    assert!(
        codes(&validate_manifest(&effectful, &context()).expect_err("external effects"))
            .contains(&"PLUG041")
    );
}

fn memory_provider_manifest() -> PluginManifest {
    let mut candidate = manifest();
    candidate.category = PluginCategory::Memory;
    candidate.subscribed_events.clear();
    candidate
        .provided_capabilities
        .push("memory.semantic".into());
    candidate.memory_providers.push(MemoryProviderManifest {
        provider_id: "fixture.semantic".into(),
        version: "1.4.0".into(),
        runtime_api: "^1.2".into(),
        capabilities: vec!["memory.semantic".into()],
        retrieve: MemoryRetrieveManifest {
            handler: "retrieve_memory".into(),
            input_schema: r#"{"type":"object","required":["query","scopes"]}"#.into(),
            output_schema: r#"{"type":"object","required":["items"]}"#.into(),
            timeout_ms: 500,
            failure_policy: FailurePolicy::Retry {
                max_attempts: 2,
                backoff_ms: 10,
            },
            idempotency: PluginOperationIdempotency::Idempotent,
            required_permissions: PermissionManifest {
                tools: vec!["filesystem.read".into()],
                network: Vec::new(),
            },
            state_scope: PluginScope::Session,
            external_effects: false,
        },
        write: Some(MemoryWriteManifest {
            handler: "write_memory".into(),
            input_schema: r#"{"type":"object","required":["scope","content"]}"#.into(),
            output_schema: r#"{"type":"object","required":["reference","retained"]}"#.into(),
            timeout_ms: 750,
            failure_policy: FailurePolicy::Reject,
            idempotency: PluginOperationIdempotency::NonIdempotent,
            required_permissions: PermissionManifest {
                tools: vec!["filesystem.read".into()],
                network: Vec::new(),
            },
            state_scope: PluginScope::Session,
            external_effects: true,
        }),
    });
    candidate
}

#[test]
fn memory_provider_declarations_are_exact_bounded_and_operation_specific() {
    let candidate = memory_provider_manifest();
    validate_manifest(&candidate, &context()).expect("exact memory-provider declaration");
    assert_eq!(
        parse_json(&to_json(&candidate).expect("memory JSON")).expect("parse memory JSON"),
        candidate
    );
    assert_eq!(
        parse_toml(&to_toml(&candidate).expect("memory TOML")).expect("parse memory TOML"),
        candidate
    );

    let mut wrong_category = candidate.clone();
    wrong_category.category = PluginCategory::Tool;
    assert!(
        codes(&validate_manifest(&wrong_category, &context()).expect_err("category"))
            .contains(&"PLUG042")
    );

    let mut duplicate = candidate.clone();
    duplicate
        .memory_providers
        .push(duplicate.memory_providers[0].clone());
    assert!(
        codes(&validate_manifest(&duplicate, &context()).expect_err("duplicate"))
            .contains(&"PLUG043")
    );

    let mut bad_identity = candidate.clone();
    bad_identity.memory_providers[0].version = "not-semver".into();
    assert!(
        codes(&validate_manifest(&bad_identity, &context()).expect_err("identity"))
            .contains(&"PLUG044")
    );

    let mut bad_handler = candidate.clone();
    bad_handler.memory_providers[0].retrieve.handler = "not-a-symbol".into();
    assert!(
        codes(&validate_manifest(&bad_handler, &context()).expect_err("handler"))
            .contains(&"PLUG046")
    );

    let mut bad_schema = candidate.clone();
    bad_schema.memory_providers[0].retrieve.output_schema = "[]".into();
    assert!(
        codes(&validate_manifest(&bad_schema, &context()).expect_err("schema"))
            .contains(&"PLUG046")
    );

    let mut bad_bound = candidate.clone();
    bad_bound.memory_providers[0].retrieve.timeout_ms = candidate.timeout_ms + 1;
    assert!(
        codes(&validate_manifest(&bad_bound, &context()).expect_err("timeout"))
            .contains(&"PLUG046")
    );

    let mut incompatible = candidate.clone();
    incompatible.memory_providers[0].runtime_api = "^9.0".into();
    assert!(
        codes(&validate_manifest(&incompatible, &context()).expect_err("runtime API"))
            .contains(&"PLUG045")
    );

    let mut excess_permission = candidate.clone();
    excess_permission.memory_providers[0]
        .retrieve
        .required_permissions
        .network
        .push("undeclared.example".into());
    assert!(
        codes(&validate_manifest(&excess_permission, &context()).expect_err("permission"))
            .contains(&"PLUG047")
    );

    let mut effectful_retrieval = candidate.clone();
    effectful_retrieval.memory_providers[0]
        .retrieve
        .external_effects = true;
    assert!(
        codes(&validate_manifest(&effectful_retrieval, &context()).expect_err("retrieval effects"))
            .contains(&"PLUG048")
    );

    let mut non_idempotent_retrieval = candidate.clone();
    non_idempotent_retrieval.memory_providers[0]
        .retrieve
        .idempotency = PluginOperationIdempotency::NonIdempotent;
    assert!(
        codes(
            &validate_manifest(&non_idempotent_retrieval, &context())
                .expect_err("retrieval idempotency")
        )
        .contains(&"PLUG048")
    );

    let mut retrying_non_idempotent_write = candidate;
    let write = retrying_non_idempotent_write.memory_providers[0]
        .write
        .as_mut()
        .expect("write declaration");
    write.failure_policy = FailurePolicy::Retry {
        max_attempts: 2,
        backoff_ms: 10,
    };
    assert!(
        codes(
            &validate_manifest(&retrying_non_idempotent_write, &context())
                .expect_err("write retry")
        )
        .contains(&"PLUG048")
    );
}

fn compactor_manifest() -> PluginManifest {
    let mut candidate = manifest();
    candidate.category = PluginCategory::Compaction;
    candidate.subscribed_events.clear();
    candidate
        .provided_capabilities
        .push("compaction.semantic".into());
    candidate.compactors.push(CompactorManifest {
        compactor_id: "fixture.semantic".into(),
        version: "2.0.0".into(),
        runtime_api: "^1.2".into(),
        handler: "compact_projection".into(),
        capabilities: vec!["compaction.semantic".into()],
        input_schema: r#"{"type":"object","required":["projection","limits"]}"#.into(),
        output_schema: r#"{"type":"object","required":["replacement"]}"#.into(),
        timeout_ms: 600,
        failure_policy: FailurePolicy::Retry {
            max_attempts: 2,
            backoff_ms: 10,
        },
        idempotency: PluginOperationIdempotency::Idempotent,
        required_permissions: PermissionManifest {
            tools: vec!["filesystem.read".into()],
            network: Vec::new(),
        },
        state_scope: PluginScope::Session,
        external_effects: false,
    });
    candidate
}

#[test]
fn compactor_declarations_are_exact_bounded_pure_and_idempotent() {
    let candidate = compactor_manifest();
    validate_manifest(&candidate, &context()).expect("exact compactor declaration");
    assert_eq!(
        parse_json(&to_json(&candidate).expect("compactor JSON")).expect("parse compactor JSON"),
        candidate
    );
    assert_eq!(
        parse_toml(&to_toml(&candidate).expect("compactor TOML")).expect("parse compactor TOML"),
        candidate
    );

    let mut wrong_category = candidate.clone();
    wrong_category.category = PluginCategory::Tool;
    assert!(
        codes(&validate_manifest(&wrong_category, &context()).expect_err("category"))
            .contains(&"PLUG049")
    );

    let mut duplicate = candidate.clone();
    duplicate.compactors.push(duplicate.compactors[0].clone());
    assert!(
        codes(&validate_manifest(&duplicate, &context()).expect_err("duplicate"))
            .contains(&"PLUG050")
    );

    let mut bad_handler = candidate.clone();
    bad_handler.compactors[0].handler = "bad-handler".into();
    assert!(
        codes(&validate_manifest(&bad_handler, &context()).expect_err("handler"))
            .contains(&"PLUG051")
    );

    let mut bad_schema = candidate.clone();
    bad_schema.compactors[0].input_schema = "{".into();
    assert!(
        codes(&validate_manifest(&bad_schema, &context()).expect_err("schema"))
            .contains(&"PLUG053")
    );

    let mut bad_bound = candidate.clone();
    bad_bound.compactors[0].timeout_ms = 0;
    assert!(
        codes(&validate_manifest(&bad_bound, &context()).expect_err("timeout"))
            .contains(&"PLUG053")
    );

    let mut incompatible = candidate.clone();
    incompatible.compactors[0].runtime_api = "^9.0".into();
    assert!(
        codes(&validate_manifest(&incompatible, &context()).expect_err("runtime API"))
            .contains(&"PLUG052")
    );

    let mut excess_scope = candidate.clone();
    excess_scope.compactors[0].state_scope = PluginScope::Runtime;
    assert!(
        codes(&validate_manifest(&excess_scope, &context()).expect_err("state scope"))
            .contains(&"PLUG054")
    );

    let mut non_idempotent = candidate.clone();
    non_idempotent.compactors[0].idempotency = PluginOperationIdempotency::NonIdempotent;
    assert!(
        codes(&validate_manifest(&non_idempotent, &context()).expect_err("idempotency"))
            .contains(&"PLUG055")
    );

    let mut effectful = candidate;
    effectful.compactors[0].external_effects = true;
    assert!(
        codes(&validate_manifest(&effectful, &context()).expect_err("effects"))
            .contains(&"PLUG055")
    );
}

#[test]
fn declaration_hash_inputs_are_deterministic_and_cover_complete_declarations() {
    let memory = memory_provider_manifest().memory_providers.remove(0);
    let memory_bytes = memory
        .declaration_hash_input()
        .expect("memory declaration bytes");
    assert_eq!(
        memory_bytes,
        memory
            .declaration_hash_input()
            .expect("stable memory declaration bytes")
    );
    assert_eq!(
        memory_bytes,
        serde_json::to_vec(&memory).expect("complete memory declaration JSON")
    );
    for changed in [
        {
            let mut value = memory.clone();
            value.version = "1.4.1".into();
            value
        },
        {
            let mut value = memory.clone();
            value.retrieve.handler = "retrieve_memory_v2".into();
            value
        },
        {
            let mut value = memory.clone();
            value.write.as_mut().expect("write").external_effects = false;
            value
        },
    ] {
        assert_ne!(
            memory_bytes,
            changed
                .declaration_hash_input()
                .expect("changed memory declaration bytes")
        );
    }

    let compactor = compactor_manifest().compactors.remove(0);
    let compactor_bytes = compactor
        .declaration_hash_input()
        .expect("compactor declaration bytes");
    assert_eq!(
        compactor_bytes,
        compactor
            .declaration_hash_input()
            .expect("stable compactor declaration bytes")
    );
    assert_eq!(
        compactor_bytes,
        serde_json::to_vec(&compactor).expect("complete compactor declaration JSON")
    );
    let mut changed = compactor;
    changed
        .required_permissions
        .network
        .push("api.example".into());
    assert_ne!(
        compactor_bytes,
        changed
            .declaration_hash_input()
            .expect("changed compactor declaration bytes")
    );
}

fn manifest() -> PluginManifest {
    parse_toml(GOLDEN_TOML).expect("golden manifest must parse")
}

fn codes(error: &agentmod_plugin_sdk::ValidationReport) -> Vec<&'static str> {
    error
        .diagnostics()
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect()
}

#[test]
fn golden_toml_and_json_are_equivalent_and_round_trip() {
    let from_toml = parse_toml(GOLDEN_TOML).expect("TOML golden must parse");
    let from_json = parse_json(GOLDEN_JSON).expect("JSON golden must parse");
    assert_eq!(from_toml, from_json);

    let encoded_toml = to_toml(&from_toml).expect("manifest must serialize as TOML");
    let encoded_json = to_json(&from_json).expect("manifest must serialize as JSON");
    assert_eq!(
        parse_toml(&encoded_toml).expect("serialized TOML must parse"),
        from_toml
    );
    assert_eq!(
        parse_json(&encoded_json).expect("serialized JSON must parse"),
        from_json
    );
    assert_eq!(encoded_json.trim(), GOLDEN_JSON.trim());
}

#[test]
fn authorized_blocking_interceptor_is_accepted() {
    let validated = validate_manifest(&manifest(), &context()).expect("manifest must be valid");
    assert_eq!(validated.manifest().identity.id, "agentmod.audit");
}

#[test]
fn observer_requesting_canonical_write_is_rejected_at_stable_path() {
    let mut candidate = manifest();
    candidate.category = PluginCategory::Observer;
    candidate.classification = PluginClassification::Observer;
    candidate.scope = PluginScope::Runtime;
    candidate.entrypoint = Entrypoint::Process {
        program: "agentmod-observer".to_owned(),
        args: Vec::new(),
    };
    candidate.trust = TrustLevel::ApprovedThirdParty;
    candidate.isolation = IsolationMode::Process;
    candidate.failure_policy = FailurePolicy::Continue;

    let report = validate_manifest(&candidate, &context()).expect_err("write must be rejected");
    assert!(codes(&report).contains(&"PLUG016"));
    let diagnostic = report
        .diagnostics()
        .iter()
        .find(|diagnostic| diagnostic.code == "PLUG016")
        .expect("observer authority diagnostic");
    assert_eq!(
        diagnostic.path,
        "plugins[agentmod.audit].authorities.proposed_write[0]"
    );
}

#[test]
fn missing_capability_prevents_activation() {
    let mut candidate = manifest();
    candidate
        .required_capabilities
        .push("runtime.nonexistent".to_owned());

    let report = validate_manifest(&candidate, &context()).expect_err("capability is missing");
    assert!(codes(&report).contains(&"PLUG012"));
}

#[test]
fn ordering_cycle_is_rejected_by_event_pipeline_compiler() {
    let mut alpha = manifest();
    alpha.identity.id = "plugin.alpha".to_owned();
    alpha.provided_capabilities.clear();
    alpha.ordering.before = vec!["plugin.beta".to_owned()];

    let mut beta = manifest();
    beta.identity.id = "plugin.beta".to_owned();
    beta.provided_capabilities.clear();
    beta.ordering.before = vec!["plugin.alpha".to_owned()];

    let report =
        validate_plugin_set(&[alpha, beta], &context()).expect_err("cycle must be rejected");
    assert_eq!(codes(&report), vec!["PLUG022"]);
    assert_eq!(report.diagnostics()[0].path, "plugins.ordering");
}

#[test]
fn missing_ordering_dependency_is_rejected() {
    let mut candidate = manifest();
    candidate.ordering.after = vec!["plugin.absent".to_owned()];

    let report = validate_plugin_set(&[candidate], &context()).expect_err("dependency is missing");
    assert_eq!(codes(&report), vec!["PLUG021"]);
    assert_eq!(
        report.diagnostics()[0].path,
        "plugins[agentmod.audit].ordering.after"
    );
}

#[test]
fn out_of_process_plugins_require_process_entrypoints() {
    let mut candidate = manifest();
    candidate.isolation = IsolationMode::Process;

    let report = validate_manifest(&candidate, &context()).expect_err("entrypoint must mismatch");
    assert!(codes(&report).contains(&"PLUG007"));
}

#[test]
fn unknown_fields_are_rejected_in_both_formats() {
    let json = GOLDEN_JSON.replacen(
        "\"schema_version\": 1,",
        "\"schema_version\": 1, \"unknown\": true,",
        1,
    );
    assert!(parse_json(&json).is_err());

    let toml = format!("unknown = true\n{GOLDEN_TOML}");
    assert!(parse_toml(&toml).is_err());
}

#[test]
fn diagnostics_have_deterministic_code_path_message_order() {
    let mut candidate = manifest();
    candidate.schema_version = 9;
    candidate.identity.id = "--INVALID".to_owned();
    candidate.timeout_ms = 0;
    candidate.state_migration_version = 0;

    let first = validate_manifest(&candidate, &context()).expect_err("manifest must be invalid");
    let second = validate_manifest(&candidate, &context()).expect_err("manifest must be invalid");
    assert_eq!(first, second);
    assert!(first.diagnostics().windows(2).all(|pair| {
        (
            pair[0].code,
            pair[0].path.as_str(),
            pair[0].message.as_str(),
        ) <= (
            pair[1].code,
            pair[1].path.as_str(),
            pair[1].message.as_str(),
        )
    }));
}

#[test]
fn duplicate_capabilities_and_subscriptions_are_rejected() {
    let mut candidate = manifest();
    candidate
        .required_capabilities
        .push("runtime.events".to_owned());
    candidate
        .subscribed_events
        .push("tool.call.proposed".to_owned());

    let report = validate_manifest(&candidate, &context()).expect_err("duplicates must fail");
    assert_eq!(codes(&report), vec!["PLUG011", "PLUG014"]);
}

proptest! {
    #[test]
    fn parsers_never_panic_on_arbitrary_bounded_text(input in ".{0,4096}") {
        let _ = parse_toml(&input);
        let _ = parse_json(&input);
    }

    #[test]
    fn timeout_validation_matches_documented_bounds(timeout_ms in any::<u64>()) {
        let mut candidate = manifest();
        candidate.timeout_ms = timeout_ms;
        let result = validate_manifest(&candidate, &context());
        let timeout_error = result
            .as_ref()
            .err()
            .is_some_and(|report| codes(report).contains(&"PLUG009"));
        prop_assert_eq!(timeout_error, timeout_ms == 0 || timeout_ms > 30_000);
    }
}
