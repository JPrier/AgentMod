//! Plugin-host composition root and bounded JSONL protocol service.
//!
//! The host supervises plugin workers, persists plugin state and durable
//! observer deliveries, and shuts down gracefully when idle so that dormant
//! plugin-configured sessions retain no process.

use agentmod_plugin_host_data::PluginData;
use agentmod_plugin_host_dependency::{IsolatedPluginDependency, PluginDependencyConfig};
use agentmod_plugin_host_logic::PluginLogic;
use agentmod_plugin_host_service::PluginHostService;
use agentmod_plugin_protocol::{CURRENT_PROTOCOL_VERSION, PluginCommand, PluginResponse};
use std::{collections::BTreeSet, error::Error, io, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::{Semaphore, mpsc},
    time::timeout,
};

const MAX_FRAME: usize = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let owner_id = std::env::var("AGENTMOD_PLUGIN_OWNER")?;
    let session_id = std::env::var("AGENTMOD_PLUGIN_SESSION")?;
    let authorization_key_hex = std::env::var("AGENTMOD_PLUGIN_AUTH_KEY")?;
    let roots = std::env::var("AGENTMOD_PLUGIN_EXECUTABLE_ROOTS")?
        .split(';')
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .collect();
    let idle_timeout = Duration::from_millis(
        std::env::var("AGENTMOD_PLUGIN_IDLE_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_IDLE_TIMEOUT_MS),
    );
    let dependency = Arc::new(
        IsolatedPluginDependency::new(PluginDependencyConfig {
            runtime_api_version: "0.1.0".into(),
            protocol_version: CURRENT_PROTOCOL_VERSION,
            available_capabilities: BTreeSet::from([
                "events".into(),
                "tools".into(),
                "plugin_state".into(),
                "memory".into(),
                "compaction".into(),
                "context".into(),
                "graph_nodes".into(),
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
        .await?,
    );
    let restored_plugins = dependency.restore_loaded_plugins().await?;
    let recovered = dependency.recover_deliveries().await?;
    eprintln!(
        "{}",
        serde_json::json!({
            "event": "plugin_host.startup_recovery",
            "restored_plugins": restored_plugins,
            "requeued_deliveries": recovered,
        })
    );
    let service = Arc::new(PluginHostService::new(PluginLogic::new(PluginData::new(
        dependency.as_ref().clone(),
    ))));
    let (sender, mut receiver) = mpsc::channel::<PluginResponse>(128);
    let writer = tokio::spawn(async move {
        let mut out = tokio::io::stdout();
        while let Some(response) = receiver.recv().await {
            let mut bytes =
                serde_json::to_vec(&response).map_err(|e| io::Error::other(e.to_string()))?;
            bytes.push(b'\n');
            out.write_all(&bytes).await?;
            out.flush().await?;
        }
        Ok::<(), io::Error>(())
    });
    let limit = Arc::new(Semaphore::new(32));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        // Graceful idle teardown: exit when no command arrived for the idle
        // period and no invocation or durable delivery is pending.
        let line = match timeout(idle_timeout, lines.next_line()).await {
            Ok(Ok(Some(line))) => line,
            Ok(Ok(None)) => break,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                let running = service.active_invocations().await;
                let pending = service.pending_deliveries().await;
                if running == 0 && pending == 0 {
                    service.flush().await?;
                    eprintln!(
                        "{}",
                        serde_json::json!({
                            "event": "plugin_host.idle_teardown",
                            "running": running,
                            "pending_deliveries": pending,
                        })
                    );
                    break;
                }
                continue;
            }
        };
        if line.len() > MAX_FRAME {
            sender
                .send(failed("frame_too_large"))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
            continue;
        }
        let Ok(permit) = Arc::clone(&limit).try_acquire_owned() else {
            sender
                .send(failed("host_overloaded"))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped"))?;
            continue;
        };
        let service = Arc::clone(&service);
        let sender = sender.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let response = match serde_json::from_str::<PluginCommand>(&line) {
                Ok(command) => service.handle(command).await,
                Err(_) => failed("invalid_json"),
            };
            let _ = sender.send(response).await;
        });
    }
    drop(sender);
    writer.await??;
    Ok(())
}
fn failed(code: &str) -> PluginResponse {
    PluginResponse::Failed {
        code: code.into(),
        message: "plugin request was rejected".into(),
        retryable: false,
    }
}
