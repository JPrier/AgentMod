//! Plugin authority and ordering validation through the real dependency adapter.

use std::collections::BTreeSet;

use agentmod_plugin_host_dependency::{
    DependencyConfigurationSchema, DependencyEntrypoint, DependencyManifest, DependencyPluginClass,
    IsolatedPluginDependency, PluginDependencyConfig, PluginDependencyError, PluginDependencyPort,
};

#[tokio::test]
async fn observer_canonical_writes_and_ordering_cycles_are_rejected() {
    let root = tempfile::tempdir().expect("root");
    let dependency = IsolatedPluginDependency::new(PluginDependencyConfig {
        runtime_api_version: "0.1.0".to_owned(),
        protocol_version: 1,
        available_capabilities: BTreeSet::from(["events".to_owned()]),
        owner_id: "owner".to_owned(),
        session_id: "session".to_owned(),
        authorization_key_hex: "07".repeat(32),
        state_root: root.path().join("state"),
        executable_roots: vec![root.path().to_path_buf()],
        observer_queue_capacity: 4,
        max_response_bytes: 4096,
        rate_limit_per_minute: 10,
        max_restarts: 1,
        audit_capacity: 16,
    })
    .await
    .expect("dependency");
    let mut observer = manifest("observer", DependencyPluginClass::Observer);
    observer
        .proposed_write_authority
        .insert("canonical_state".to_owned());
    assert!(matches!(
        dependency.validate_set(vec![observer]).await,
        Err(PluginDependencyError::Validation(_))
    ));

    let mut first = manifest("first", DependencyPluginClass::Blocking);
    first.before.insert("second".to_owned());
    let mut second = manifest("second", DependencyPluginClass::Blocking);
    second.before.insert("first".to_owned());
    assert!(matches!(
        dependency.validate_set(vec![first, second]).await,
        Err(PluginDependencyError::Validation(_))
    ));
}

fn manifest(id: &str, class: DependencyPluginClass) -> DependencyManifest {
    DependencyManifest {
        schema_version: 1,
        id: id.to_owned(),
        version: "1.0.0".to_owned(),
        runtime_api: "0.1.0".to_owned(),
        category: match class {
            DependencyPluginClass::Observer => "observer",
            _ => "interceptor",
        }
        .to_owned(),
        scope: "session".to_owned(),
        class,
        entrypoint: DependencyEntrypoint {
            program: "unused".to_owned(),
            arguments: Vec::new(),
        },
        required_capabilities: BTreeSet::new(),
        provided_capabilities: BTreeSet::new(),
        subscribed_events: BTreeSet::from(["session.created".to_owned()]),
        read_authority: BTreeSet::from(["session_state".to_owned()]),
        proposed_write_authority: BTreeSet::new(),
        tool_permissions: BTreeSet::new(),
        network_permissions: BTreeSet::new(),
        after: BTreeSet::new(),
        before: BTreeSet::new(),
        stage: 0,
        priority: 0,
        timeout_ms: 1_000,
        failure_policy: "reject".to_owned(),
        max_attempts: 1,
        retry_backoff_ms: 0,
        state_migration_version: 1,
        configuration_schema: DependencyConfigurationSchema {
            id: format!("{id}.config"),
            version: 1,
            required: false,
            inline_json: "{\"type\":\"object\",\"additionalProperties\":false}".to_owned(),
        },
        node_executors: Vec::new(),
        context_transforms: Vec::new(),
        memory_providers: Vec::new(),
        compactors: Vec::new(),
    }
}
