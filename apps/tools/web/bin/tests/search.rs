//! Full-layer authenticated mock-search regression.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_primitives::{CancellationId, ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};
use agentmod_web_host_data::WebData;
use agentmod_web_host_dependency::{
    EnvironmentSecretDependency, MockSearchDocument, NetworkPolicy, ReqwestWebDependency,
    SearchProvider, WebDependencyConfig,
};
use agentmod_web_host_logic::{WebLogic, WebLogicConfig};
use agentmod_web_host_service::{WebHostService, WebHostServiceConfig};
use serde_json::{Value, json};
use uuid::Uuid;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the full-layer regression keeps composition and grant construction explicit"
)]
async fn service_to_dependency_search_uses_the_same_canonical_operation() {
    let root = tempfile::tempdir().expect("root");
    let dependency = ReqwestWebDependency::new(
        WebDependencyConfig {
            artifact_root: root.path().join("artifacts"),
            authorization_key_hex: "07".repeat(32),
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            maximum_replay_entries: 32,
            maximum_active_calls: 4,
            network_policy: NetworkPolicy {
                allowed_domains: vec!["example.com".to_owned()],
                denied_domains: Vec::new(),
                allow_private_network: false,
                allow_plain_http: false,
                allowed_methods: BTreeSet::from(["GET".to_owned()]),
            },
            maximum_redirects: 3,
            maximum_timeout: Duration::from_secs(5),
            maximum_response_bytes: 4096,
            maximum_inline_bytes: 1024,
            maximum_url_length: 2048,
            maximum_headers: 16,
            maximum_request_body_bytes: 1024,
            proxy_url: None,
            cache_entries: 0,
            search_provider: SearchProvider::Mock {
                documents: vec![MockSearchDocument {
                    title: "Rust language".to_owned(),
                    url: "https://example.com/rust".to_owned(),
                    snippet: "Safe systems programming".to_owned(),
                    published_at: None,
                }],
            },
        },
        EnvironmentSecretDependency,
    )
    .expect("dependency");
    let logic = WebLogic::new(
        WebData::new(dependency),
        WebLogicConfig {
            maximum_url_length: 2048,
            maximum_query_length: 1024,
            maximum_search_results: 10,
            maximum_headers: 16,
            maximum_request_body_bytes: 1024,
            maximum_timeout: Duration::from_secs(5),
            maximum_redirects: 3,
            maximum_response_bytes: 4096,
            maximum_inline_bytes: 1024,
        },
    )
    .expect("logic");
    let service = WebHostService::new(
        logic,
        WebHostServiceConfig {
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
        },
    )
    .expect("service");
    let cancellation = CancellationId::from_uuid(Uuid::now_v7());
    let arguments = json!({
        "query": "Rust systems",
        "count": 1,
        "freshness": null,
        "domain_allowlist": [],
        "domain_denylist": [],
        "language": null,
        "locale": null,
        "timeout_ms": 1000,
    });
    let canonical = serde_json::to_vec(&(
        "web.search",
        cancellation.to_string(),
        normalize_json(&arguments),
    ))
    .expect("canonical");
    let digest = ContentHash::digest(&canonical);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let now = i64::try_from(now).expect("timestamp");
    let grant = seal_authorization(
        &AuthorizationClaims {
            owner: "owner".to_owned(),
            session: "session".to_owned(),
            call_id: "search-call".to_owned(),
            action: "web.search".to_owned(),
            normalized_digest: digest,
            issued_at: TimestampMillis::new(now - 100),
            expires_at: TimestampMillis::new(now + 10_000),
            nonce: "search-nonce".to_owned(),
        },
        &AuthorizationKey::from_bytes([7; 32]),
    )
    .expect("grant");
    let events = service
        .handle(ToolHostCommand::Execute {
            call_id: "search-call".to_owned(),
            tool: "web.search".to_owned(),
            arguments,
            normalized_digest: digest.to_hex(),
            authorization_grant: grant,
            cancellation_id: cancellation,
        })
        .await
        .expect("search");
    assert!(events.iter().any(|event| matches!(
        event,
        ToolHostEvent::Completed { result, .. }
            if result["provider"] == "mock" && result["results"][0]["title"] == "Rust language"
    )));
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}
