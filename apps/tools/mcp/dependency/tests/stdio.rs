//! Real stdio MCP initialization, discovery, progress, invocation, and shutdown.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_mcp_host_dependency::{
    DependencyAuthorization, DependencyInvocationKind, DependencyInvokeRequest,
    DependencyServerConfig, DependencyTransportConfig, McpDependency, McpDependencyConfig,
    McpDependencyPort,
};
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use serde_json::Value;
use serde_json::json;

const KEY: [u8; 32] = [11; 32];

#[tokio::test]
async fn stdio_server_negotiates_discovers_and_invokes() {
    let root = tempfile::tempdir().expect("root");
    let executable = compile_fixture(root.path());
    let dependency = McpDependency::new(McpDependencyConfig {
        servers: vec![DependencyServerConfig {
            id: "fixture".to_owned(),
            display_name: "Fixture".to_owned(),
            active: true,
            transport: DependencyTransportConfig::Stdio {
                program: executable.to_string_lossy().into_owned(),
                arguments: Vec::new(),
                environment: BTreeMap::new(),
            },
        }],
        client_name: "agentmod-test".to_owned(),
        client_version: "1".to_owned(),
        request_timeout: Duration::from_secs(5),
        maximum_message_bytes: 64 * 1024,
        maximum_servers: 1,
        authorization_owner: "owner".to_owned(),
        authorization_session: "session".to_owned(),
        authorization_key_hex: encode_hex(&KEY),
        authorization_replay_root: root.path().join("authorization-replay"),
        http_state_root: root.path().join("http-state"),
    })
    .expect("dependency");
    let capabilities = dependency
        .capabilities(
            "fixture",
            authorization(
                "mcp.capabilities",
                json!({"server_id":"fixture"}),
                "capabilities-cancel",
                "capabilities-call",
                "capabilities-nonce",
            ),
        )
        .await
        .expect("capabilities");
    assert_eq!(capabilities.protocol_version, "2025-06-18");
    assert_eq!(capabilities.tools[0].name, "echo");
    let response = dependency
        .invoke(DependencyInvokeRequest {
            authorization: authorization(
                "mcp.invoke",
                json!({
                    "server_id":"fixture",
                    "kind":"tool",
                    "name":"echo",
                    "arguments":{"value":"hello"}
                }),
                "stdio-call",
                "invoke-call",
                "invoke-nonce",
            ),
            server_id: "fixture".to_owned(),
            kind: DependencyInvocationKind::Tool,
            name: "echo".to_owned(),
            arguments: json!({"value":"hello"}),
            cancellation_id: "stdio-call".to_owned(),
        })
        .await
        .expect("invoke");
    assert_eq!(response.result["content"][0]["text"], "echoed");
    assert_eq!(response.progress.len(), 1);
    dependency.shutdown().await;
}

fn authorization(
    action: &str,
    arguments: Value,
    cancellation_id: &str,
    call_id: &str,
    nonce: &str,
) -> DependencyAuthorization {
    let canonical = canonical_operation(action, &arguments, cancellation_id);
    let digest = ContentHash::digest(&canonical);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let grant = seal_authorization(
        &AuthorizationClaims {
            owner: "owner".to_owned(),
            session: "session".to_owned(),
            call_id: call_id.to_owned(),
            action: action.to_owned(),
            normalized_digest: digest,
            issued_at: TimestampMillis::new(i64::try_from(now).expect("time")),
            expires_at: TimestampMillis::new(i64::try_from(now + 30_000).expect("expiry")),
            nonce: nonce.to_owned(),
        },
        &AuthorizationKey::from_bytes(KEY),
    )
    .expect("grant");
    DependencyAuthorization {
        call_id: call_id.to_owned(),
        action: action.to_owned(),
        normalized_digest: digest.to_hex(),
        grant,
        arguments,
        cancellation_id: cancellation_id.to_owned(),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("write to string");
    }
    encoded
}

fn canonical_operation(action: &str, arguments: &Value, cancellation_id: &str) -> Vec<u8> {
    let normalized = normalize_json(arguments);
    serde_json::to_vec(&(action, cancellation_id, normalized)).expect("canonical operation")
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect();
            serde_json::to_value(sorted).expect("normalized JSON")
        }
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}

fn compile_fixture(root: &std::path::Path) -> PathBuf {
    let executable = root.join(if cfg!(windows) {
        "mcp-fixture.exe"
    } else {
        "mcp-fixture"
    });
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("stdio_server.rs");
    let serde = find_rlib("serde_json");
    let deps = serde.parent().expect("deps");
    let status = Command::new("rustc")
        .arg(source)
        .arg("--edition=2024")
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("--extern")
        .arg(format!("serde_json={}", serde.display()))
        .arg("-o")
        .arg(&executable)
        .status()
        .expect("rustc");
    assert!(status.success());
    executable
}

fn find_rlib(name: &str) -> PathBuf {
    let dependency_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..")
        .join("target")
        .join("debug")
        .join("deps");
    std::fs::read_dir(dependency_directory)
        .expect("deps")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name().is_some_and(|file| {
                let file = file.to_string_lossy();
                file.starts_with(&format!("lib{name}-")) && file.ends_with(".rlib")
            })
        })
        .expect("rlib")
}
