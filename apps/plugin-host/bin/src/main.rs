//! Plugin-host composition root and bounded JSONL protocol service.

use agentmod_plugin_host_data::PluginData;
use agentmod_plugin_host_dependency::{IsolatedPluginDependency, PluginDependencyConfig};
use agentmod_plugin_host_logic::PluginLogic;
use agentmod_plugin_host_service::PluginHostService;
use agentmod_plugin_protocol::{
    CURRENT_PROTOCOL_VERSION, PluginResponse, PluginResponseFrame, decode_bounded_request_frame,
};
use std::{collections::BTreeSet, error::Error, io, path::PathBuf, sync::Arc};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{Mutex, Semaphore, mpsc},
};

const MAX_FRAME: usize = 1024 * 1024;
type HostService = PluginHostService<PluginLogic<PluginData<IsolatedPluginDependency>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let owner_id = std::env::var("AGENTMOD_PLUGIN_OWNER")?;
    let session_id = std::env::var("AGENTMOD_PLUGIN_SESSION")?;
    let runtime_api_version = std::env::var("AGENTMOD_PLUGIN_RUNTIME_API_VERSION")
        .unwrap_or_else(|_| String::from("0.1.0"));
    let authorization_key_hex = std::env::var("AGENTMOD_PLUGIN_AUTH_KEY")?;
    let roots = std::env::var("AGENTMOD_PLUGIN_EXECUTABLE_ROOTS")?
        .split(';')
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .collect();
    let dependency = IsolatedPluginDependency::new(PluginDependencyConfig {
        runtime_api_version,
        protocol_version: CURRENT_PROTOCOL_VERSION,
        available_capabilities: BTreeSet::from([
            "events".into(),
            "tools".into(),
            "plugin_state".into(),
        ]),
        owner_id,
        session_id,
        authorization_key_hex,
        state_root: std::env::current_dir()?.join(".agentmod/plugin-state"),
        executable_roots: roots,
        observer_queue_capacity: 128,
        max_response_bytes: MAX_FRAME,
        rate_limit_per_minute: 120,
        max_restarts: 2,
        audit_capacity: 1024,
    })
    .await?;
    let service = Arc::new(PluginHostService::new(PluginLogic::new(PluginData::new(
        dependency,
    ))));
    let (sender, mut receiver) = mpsc::channel::<PluginResponseFrame>(128);
    let writer = tokio::spawn(async move {
        let mut out = tokio::io::stdout();
        while let Some(frame) = receiver.recv().await {
            frame
                .validate_contract()
                .map_err(|error| io::Error::other(error.to_string()))?;
            let mut bytes =
                serde_json::to_vec(&frame).map_err(|e| io::Error::other(e.to_string()))?;
            bytes.push(b'\n');
            out.write_all(&bytes).await?;
            out.flush().await?;
        }
        Ok::<(), io::Error>(())
    });
    serve_requests(service, sender.clone()).await?;
    drop(sender);
    writer.await??;
    Ok(())
}

async fn serve_requests(
    service: Arc<HostService>,
    sender: mpsc::Sender<PluginResponseFrame>,
) -> Result<(), Box<dyn Error>> {
    let limit = Arc::new(Semaphore::new(32));
    let active_correlations = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let mut input = BufReader::new(tokio::io::stdin());
    while let Some(frame) = read_bounded_frame(&mut input, MAX_FRAME).await? {
        let Ok(line) = frame else {
            sender
                .send(failed_frame("transport-error", "frame_too_large"))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
            break;
        };
        if line.len() > MAX_FRAME {
            sender
                .send(failed_frame("transport-error", "frame_too_large"))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
            break;
        }
        let Ok(request) = decode_bounded_request_frame(&line) else {
            sender
                .send(failed_frame(&untrusted_correlation(&line), "invalid_frame"))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
            continue;
        };
        let correlation_id = request.correlation_id;
        {
            let mut active = active_correlations.lock().await;
            if !active.insert(correlation_id.clone()) {
                // Exactly one response is permitted per correlation. The
                // original request retains ownership; a duplicate frame is
                // suppressed rather than creating an ambiguous second reply.
                continue;
            }
        }
        let Ok(permit) = Arc::clone(&limit).try_acquire_owned() else {
            active_correlations.lock().await.remove(&correlation_id);
            sender
                .send(failed_frame(&correlation_id, "host_overloaded"))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
            continue;
        };
        let service = Arc::clone(&service);
        let sender = sender.clone();
        let active_correlations = Arc::clone(&active_correlations);
        tokio::spawn(async move {
            let _permit = permit;
            let response = service.handle(request.command).await;
            let _ = sender
                .send(PluginResponseFrame {
                    correlation_id: correlation_id.clone(),
                    response,
                })
                .await;
            active_correlations.lock().await.remove(&correlation_id);
        });
    }
    Ok(())
}

async fn read_bounded_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> io::Result<Option<Result<Vec<u8>, ()>>> {
    let mut frame = Vec::with_capacity(maximum.min(8 * 1024).saturating_add(1));
    let mut oversized = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if frame.is_empty() && !oversized {
                return Ok(None);
            }
            return Ok(Some(if oversized { Err(()) } else { Ok(frame) }));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        let content = newline.map_or(available, |index| &available[..index]);
        if !oversized {
            let remaining = maximum.saturating_add(1).saturating_sub(frame.len());
            frame.extend_from_slice(&content[..content.len().min(remaining)]);
            oversized = frame.len() > maximum || content.len() > remaining;
        }
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(if oversized { Err(()) } else { Ok(frame) }));
        }
    }
}
fn failed(code: &str) -> PluginResponse {
    PluginResponse::Failed {
        code: code.into(),
        message: "plugin request was rejected".into(),
        retryable: false,
    }
}

fn failed_frame(correlation_id: &str, code: &str) -> PluginResponseFrame {
    PluginResponseFrame {
        correlation_id: correlation_id.to_owned(),
        response: failed(code),
    }
}

fn untrusted_correlation(bytes: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("correlation_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| {
                    !value.is_empty()
                        && value.len() <= 256
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte))
                })
                .map(str::to_owned)
        })
        .unwrap_or_else(|| String::from("transport-error"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_plugin_protocol as protocol;
    use agentmod_primitives::{ContentHash, TimestampMillis};
    use agentmod_protocol_support::authorization::{
        AuthorizationClaims, AuthorizationKey, seal_authorization,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn manifest(program: String) -> protocol::PluginManifest {
        protocol::PluginManifest {
            schema_version: 1,
            id: String::from("fixture.memory"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^0.1"),
            category: String::from("memory"),
            scope: String::from("session"),
            class: protocol::PluginClass::Extension,
            entrypoint: protocol::PluginEntrypoint {
                program,
                arguments: vec![String::from("--exact"), String::from("no-recursive-test")],
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
            timeout_ms: 100,
            failure_policy: String::from("reject"),
            max_attempts: 1,
            retry_backoff_ms: 0,
            state_migration_version: 1,
            configuration_schema: protocol::PluginConfigurationSchema {
                id: String::from("fixture.memory.config"),
                version: 1,
                required: false,
                inline_json: String::from(r#"{"type":"object","additionalProperties":false}"#),
            },
            node_executors: Vec::new(),
            context_transforms: Vec::new(),
            memory_providers: vec![protocol::PluginMemoryProviderDeclaration {
                provider_id: String::from("fixture.memory.provider"),
                version: String::from("1.0.0"),
                runtime_api: String::from("^0.1"),
                capabilities: Vec::new(),
                retrieve: protocol::PluginMemoryRetrieveDeclaration {
                    handler: String::from("retrieve"),
                    input_schema: String::from(r#"{"type":"object"}"#),
                    output_schema: String::from(r#"{"type":"object"}"#),
                    timeout_ms: 50,
                    failure_policy: protocol::PluginOperationFailurePolicy::Reject,
                    idempotency: protocol::PluginOperationIdempotency::Idempotent,
                    required_permissions: protocol::PluginOperationPermissions::default(),
                    state_scope: protocol::PluginOperationStateScope::Session,
                    external_effects: false,
                },
                write: None,
            }],
            compactors: Vec::new(),
        }
    }

    fn authorization(
        manifest: &protocol::PluginManifest,
        configuration: &serde_json::Value,
    ) -> protocol::PluginAuthorization {
        let digest = ContentHash::digest(
            &serde_json::to_vec(&(manifest, configuration)).expect("wire load tuple"),
        );
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("timestamp");
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: String::from("owner"),
                session: String::from("session-1"),
                call_id: String::from("load-memory"),
                action: String::from("plugin.load"),
                normalized_digest: digest,
                issued_at: TimestampMillis::new(now),
                expires_at: TimestampMillis::new(now + 30_000),
                nonce: String::from("load-memory-nonce"),
            },
            &AuthorizationKey::from_bytes([7; 32]),
        )
        .expect("grant");
        protocol::PluginAuthorization {
            owner_id: String::from("owner"),
            session_id: String::from("session-1"),
            call_id: String::from("load-memory"),
            normalized_digest: digest.to_hex(),
            grant,
            cancellation_id: String::from("load-memory-cancel"),
        }
    }

    #[tokio::test]
    async fn nonempty_wire_v6_manifest_load_authorization_survives_all_host_mappings() {
        let root = tempfile::tempdir().expect("root");
        let executable = std::env::current_exe().expect("test executable");
        let executable_root = executable.parent().expect("executable root").to_owned();
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
            rate_limit_per_minute: 10,
            max_restarts: 0,
            audit_capacity: 16,
        })
        .await
        .expect("dependency");
        let service = PluginHostService::new(PluginLogic::new(PluginData::new(dependency)));
        let manifest = manifest(executable.to_string_lossy().into_owned());
        let configuration = serde_json::json!({});
        let authorization = authorization(&manifest, &configuration);
        let response = service
            .handle(protocol::PluginCommand::Load {
                manifest: Box::new(manifest),
                configuration,
                authorization,
            })
            .await;
        assert!(
            !matches!(
                response,
                protocol::PluginResponse::Failed { ref code, .. }
                    if code == "authorization_denied"
            ),
            "the dependency-side load tuple drifted from the signed wire-v6 tuple"
        );
    }
}
