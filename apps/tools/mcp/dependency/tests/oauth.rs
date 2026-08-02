//! Real loopback MCP OAuth authorization, restart, refresh, and confidentiality tests.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agentmod_mcp_host_dependency::{
    DependencyAuthorization, DependencyOAuthStatusKind, DependencyServerConfig,
    DependencyTransportConfig, McpDependency, McpDependencyConfig, McpDependencyError,
    McpDependencyPort,
};
use agentmod_primitives::{ContentHash, TimestampMillis};
use agentmod_protocol_support::authorization::{
    AuthorizationClaims, AuthorizationKey, seal_authorization,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use url::Url;

const KEY: [u8; 32] = [23; 32];

#[derive(Default)]
struct FixtureState {
    challenge: Mutex<Option<String>>,
    saw_resource_on_authorize: AtomicBool,
    saw_resource_on_exchange: AtomicBool,
    saw_resource_on_refresh: AtomicBool,
    bearer_requests: AtomicUsize,
    refreshes: AtomicUsize,
    substituted_resource: AtomicBool,
}

#[tokio::test]
async fn loopback_authorization_refresh_restart_and_secret_non_disclosure() {
    let root = tempfile::tempdir().expect("root");
    let fixture = Arc::new(FixtureState::default());
    let server = OAuthFixture::start(Arc::clone(&fixture)).await;
    let callback_port = unused_loopback_port().await;
    let config = config(root.path(), &server.origin, callback_port);
    let dependency = McpDependency::new(config.clone()).expect("dependency");

    let start = dependency
        .begin_oauth(
            "protected",
            "begin-cancel",
            authorization(
                "mcp.oauth.begin",
                json!({"server_id":"protected"}),
                "begin-cancel",
                "begin-call",
                "begin-nonce",
            ),
        )
        .await
        .expect("begin");
    let authorization_url = Url::parse(&start.authorization_url).expect("authorization URL");
    let parameters = authorization_url.query_pairs().collect::<BTreeMap<_, _>>();
    assert_eq!(
        parameters.get("resource").map(std::convert::AsRef::as_ref),
        Some(format!("{}/mcp", server.origin).as_str())
    );
    assert_eq!(
        parameters
            .get("code_challenge_method")
            .map(std::convert::AsRef::as_ref),
        Some("S256")
    );
    fixture
        .challenge
        .lock()
        .await
        .replace(parameters["code_challenge"].to_string());
    fixture
        .saw_resource_on_authorize
        .store(parameters.contains_key("resource"), Ordering::SeqCst);

    dependency.shutdown().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let recovered = McpDependency::new(config).expect("recovered dependency");
    send_callback(
        callback_port,
        &format!(
            "/callback?code=authorization-code&state={}",
            parameters["state"]
        ),
    )
    .await;
    let status = wait_for_status(&recovered, "status-after-restart").await;
    assert_eq!(status.status, DependencyOAuthStatusKind::Authorized);
    assert_eq!(status.scopes, vec!["tools.read"]);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let capabilities = recovered
        .capabilities(
            "protected",
            authorization(
                "mcp.capabilities",
                json!({"server_id":"protected"}),
                "capabilities-cancel",
                "capabilities-call",
                "capabilities-nonce",
            ),
        )
        .await
        .expect("authorized capabilities");
    assert_eq!(capabilities.tools[0].name, "echo");
    assert!(fixture.saw_resource_on_authorize.load(Ordering::SeqCst));
    assert!(fixture.saw_resource_on_exchange.load(Ordering::SeqCst));
    assert!(fixture.saw_resource_on_refresh.load(Ordering::SeqCst));
    assert_eq!(fixture.refreshes.load(Ordering::SeqCst), 1);
    assert!(fixture.bearer_requests.load(Ordering::SeqCst) >= 2);

    let persisted = read_tree(&root.path().join("oauth-state"));
    for forbidden in [
        "authorization-code",
        "access-token-one",
        "access-token-two",
        "refresh-token-one",
        "refresh-token-two",
    ] {
        assert!(
            !persisted.contains(forbidden),
            "OAuth secret leaked in durable state"
        );
    }
    server.stop().await;
}

#[tokio::test]
async fn invalid_state_and_substituted_resource_fail_closed() {
    let root = tempfile::tempdir().expect("root");
    let fixture = Arc::new(FixtureState::default());
    let server = OAuthFixture::start(Arc::clone(&fixture)).await;
    let callback_port = unused_loopback_port().await;
    let dependency =
        McpDependency::new(config(root.path(), &server.origin, callback_port)).expect("dependency");
    let start = dependency
        .begin_oauth(
            "protected",
            "invalid-begin-cancel",
            authorization(
                "mcp.oauth.begin",
                json!({"server_id":"protected"}),
                "invalid-begin-cancel",
                "invalid-begin-call",
                "invalid-begin-nonce",
            ),
        )
        .await
        .expect("begin");
    send_callback(callback_port, "/callback?code=code&state=substituted").await;
    let status = wait_for_status(&dependency, "invalid-status").await;
    assert_eq!(status.status, DependencyOAuthStatusKind::Failed);
    assert!(status.transaction_id.is_none());
    assert!(!start.transaction_id.is_empty());

    dependency.shutdown().await;
    fixture.substituted_resource.store(true, Ordering::SeqCst);
    let second_root = tempfile::tempdir().expect("second root");
    let second_port = unused_loopback_port().await;
    let substituted = McpDependency::new(config(second_root.path(), &server.origin, second_port))
        .expect("substituted dependency");
    assert_eq!(
        substituted
            .begin_oauth(
                "protected",
                "substituted-cancel",
                authorization(
                    "mcp.oauth.begin",
                    json!({"server_id":"protected"}),
                    "substituted-cancel",
                    "substituted-call",
                    "substituted-nonce",
                ),
            )
            .await,
        Err(McpDependencyError::OAuthMetadata)
    );
    server.stop().await;
}

async fn wait_for_status(
    dependency: &McpDependency,
    nonce_prefix: &str,
) -> agentmod_mcp_host_dependency::DependencyOAuthStatus {
    for index in 0..100_u32 {
        let cancellation = format!("status-cancel-{index}");
        let status = dependency
            .oauth_status(
                "protected",
                authorization(
                    "mcp.oauth.status",
                    json!({"server_id":"protected"}),
                    &cancellation,
                    &format!("status-call-{index}"),
                    &format!("{nonce_prefix}-{index}"),
                ),
            )
            .await
            .expect("status");
        if status.status != DependencyOAuthStatusKind::Pending {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("OAuth callback did not complete");
}

fn config(root: &Path, origin: &str, callback_port: u16) -> McpDependencyConfig {
    McpDependencyConfig {
        servers: vec![DependencyServerConfig {
            id: "protected".into(),
            display_name: "Protected fixture".into(),
            active: true,
            transport: DependencyTransportConfig::StreamableHttpOAuth {
                url: format!("{origin}/mcp"),
                authorization_server: format!("{origin}/issuer"),
                client_id: "agentmod-test-client".into(),
                client_secret_environment: None,
                redirect_uri: format!("http://127.0.0.1:{callback_port}/callback"),
                scopes: vec!["tools.read".into()],
            },
        }],
        client_name: "agentmod-test".into(),
        client_version: "1".into(),
        request_timeout: Duration::from_secs(2),
        maximum_message_bytes: 64 * 1_024,
        maximum_servers: 1,
        authorization_owner: "owner".into(),
        authorization_session: "session".into(),
        authorization_key_hex: encode_hex(&KEY),
        authorization_replay_root: root.join("replay"),
        http_state_root: root.join("http"),
        oauth_state_root: root.join("oauth-state"),
        oauth_encryption_key_hex: Some(encode_hex(&[91; 32])),
    }
}

fn authorization(
    action: &str,
    arguments: Value,
    cancellation_id: &str,
    call_id: &str,
    nonce: &str,
) -> DependencyAuthorization {
    let canonical = canonical_operation(action, &arguments, cancellation_id);
    let normalized_digest = ContentHash::digest(&canonical);
    let issued = now_ms();
    let grant = seal_authorization(
        &AuthorizationClaims {
            owner: "owner".into(),
            session: "session".into(),
            call_id: call_id.into(),
            action: action.into(),
            normalized_digest,
            issued_at: TimestampMillis::new(issued),
            expires_at: TimestampMillis::new(issued + 60_000),
            nonce: nonce.into(),
        },
        &AuthorizationKey::from_bytes(KEY),
    )
    .expect("grant");
    DependencyAuthorization {
        call_id: call_id.into(),
        action: action.into(),
        normalized_digest: normalized_digest.to_hex(),
        grant,
        arguments,
        cancellation_id: cancellation_id.into(),
    }
}

fn canonical_operation(action: &str, arguments: &Value, cancellation_id: &str) -> Vec<u8> {
    let normalized = normalize_json(arguments);
    serde_json::to_vec(&(action, cancellation_id, normalized)).expect("canonical")
}

fn normalize_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => serde_json::to_value(
            map.iter()
                .map(|(key, value)| (key.clone(), normalize_json(value)))
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("normalized"),
        Value::Array(values) => Value::Array(values.iter().map(normalize_json).collect()),
        _ => value.clone(),
    }
}

async fn unused_loopback_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("port");
    listener.local_addr().expect("address").port()
}

async fn send_callback(port: u16, target: &str) {
    let mut connected = None;
    for _ in 0..100 {
        match TcpStream::connect(("127.0.0.1", port)).await {
            Ok(stream) => {
                connected = Some(stream);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let mut stream = connected.expect("callback listener");
    stream
        .write_all(
            format!("GET {target} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("callback");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.expect("response");
}

fn read_tree(root: &Path) -> String {
    let mut output = Vec::new();
    for entry in std::fs::read_dir(root).expect("state directory") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            output.extend_from_slice(read_tree(&path).as_bytes());
        } else {
            output.extend_from_slice(&std::fs::read(path).expect("state file"));
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("timestamp")
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("string formatting");
            encoded
        },
    )
}

struct OAuthFixture {
    origin: String,
    cancellation: tokio_util::sync::CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl OAuthFixture {
    async fn start(state: Arc<FixtureState>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("fixture");
        let address = listener.local_addr().expect("address");
        let origin = format!("http://{address}");
        let cancellation = tokio_util::sync::CancellationToken::new();
        let cancelled = cancellation.clone();
        let task_origin = origin.clone();
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    () = cancelled.cancelled() => break,
                    value = listener.accept() => value,
                };
                let Ok((mut stream, _)) = accepted else {
                    break;
                };
                let state = Arc::clone(&state);
                let origin = task_origin.clone();
                tokio::spawn(async move {
                    let request = read_http_request(&mut stream).await;
                    let response = fixture_response(&request, &origin, &state).await;
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            origin,
            cancellation,
            task,
        }
    }

    async fn stop(self) {
        self.cancellation.cancel();
        self.task.await.expect("fixture task");
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the test fixture keeps its complete HTTP protocol script visible in one deterministic dispatcher"
)]
async fn fixture_response(request: &str, origin: &str, state: &FixtureState) -> String {
    let first = request.lines().next().unwrap_or_default();
    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
    let (status, headers, value) = if first.starts_with("GET /mcp ") {
        (
            "401 Unauthorized",
            format!(
                "WWW-Authenticate: Bearer resource_metadata=\"{origin}/resource-metadata\", scope=\"tools.read\"\r\n"
            ),
            String::new(),
        )
    } else if first.starts_with("GET /resource-metadata ") {
        let resource = if state.substituted_resource.load(Ordering::SeqCst) {
            format!("{origin}/other")
        } else {
            format!("{origin}/mcp")
        };
        (
            "200 OK",
            "Content-Type: application/json\r\n".into(),
            json!({
                "resource": resource,
                "authorization_servers": [format!("{origin}/issuer")],
                "scopes_supported": ["tools.read"],
                "bearer_methods_supported": ["header"],
            })
            .to_string(),
        )
    } else if first.starts_with("GET /.well-known/oauth-authorization-server/issuer ") {
        (
            "200 OK",
            "Content-Type: application/json\r\n".into(),
            json!({
                "issuer": format!("{origin}/issuer"),
                "authorization_endpoint": format!("{origin}/authorize"),
                "token_endpoint": format!("{origin}/token"),
                "code_challenge_methods_supported": ["S256"],
                "grant_types_supported": ["authorization_code", "refresh_token"],
                "token_endpoint_auth_methods_supported": ["none"],
                "protected_resources": [format!("{origin}/mcp")],
            })
            .to_string(),
        )
    } else if first.starts_with("POST /token ") {
        let form = url::form_urlencoded::parse(body.as_bytes()).collect::<BTreeMap<_, _>>();
        if form.get("grant_type").map(std::convert::AsRef::as_ref) == Some("authorization_code") {
            state
                .saw_resource_on_exchange
                .store(form.contains_key("resource"), Ordering::SeqCst);
            let verifier = form.get("code_verifier").expect("verifier");
            let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
            assert_eq!(
                state.challenge.lock().await.as_deref(),
                Some(challenge.as_str())
            );
            (
                "200 OK",
                "Content-Type: application/json\r\nCache-Control: no-store\r\n".into(),
                json!({
                    "access_token":"access-token-one",
                    "token_type":"Bearer",
                    "expires_in":1,
                    "refresh_token":"refresh-token-one",
                    "scope":"tools.read",
                })
                .to_string(),
            )
        } else {
            state.refreshes.fetch_add(1, Ordering::SeqCst);
            state
                .saw_resource_on_refresh
                .store(form.contains_key("resource"), Ordering::SeqCst);
            (
                "200 OK",
                "Content-Type: application/json\r\nCache-Control: no-store\r\n".into(),
                json!({
                    "access_token":"access-token-two",
                    "token_type":"Bearer",
                    "expires_in":3600,
                    "refresh_token":"refresh-token-two",
                    "scope":"tools.read",
                })
                .to_string(),
            )
        }
    } else if first.starts_with("POST /mcp ") {
        assert!(
            request.contains("authorization: Bearer access-token-two")
                || request.contains("Authorization: Bearer access-token-two")
        );
        state.bearer_requests.fetch_add(1, Ordering::SeqCst);
        let request_json: Value = serde_json::from_str(body).expect("MCP request");
        let result = if request_json["method"] == "initialize" {
            json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{}}})
        } else {
            json!({"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object"}}]})
        };
        (
            "200 OK",
            "Content-Type: application/json\r\n".into(),
            json!({"jsonrpc":"2.0","id":request_json["id"],"result":result}).to_string(),
        )
    } else {
        ("404 Not Found", String::new(), String::new())
    };
    format!(
        "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{value}",
        value.len()
    )
}

async fn read_http_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1_024];
    loop {
        let count = stream.read(&mut buffer).await.expect("request");
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(split) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..split + 4]);
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if bytes.len() >= split + 4 + length {
                break;
            }
        }
        assert!(bytes.len() < 128 * 1_024);
    }
    String::from_utf8(bytes).expect("UTF-8 request")
}
