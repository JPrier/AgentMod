//! Browser-host composition root and bounded JSONL transport.

use std::{collections::BTreeSet, error::Error, io, sync::Arc, time::Duration};

use agentmod_browser_host_data::BrowserData;
use agentmod_browser_host_dependency::{
    BrowserDependencyConfig, BrowserDependencyPort, WebDriverBrowserDependency,
};
use agentmod_browser_host_logic::BrowserLogic;
use agentmod_browser_host_service::BrowserHostService;
use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};
use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    sync::{Semaphore, mpsc},
};

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_IN_FLIGHT: usize = 16;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let session = std::env::var("AGENTMOD_BROWSER_SESSION")?;
    let artifact_root = std::env::var_os("AGENTMOD_BROWSER_ARTIFACT_ROOT")
        .map(std::path::PathBuf::from)
        .ok_or("AGENTMOD_BROWSER_ARTIFACT_ROOT is required")?;
    let dependency = WebDriverBrowserDependency::new(BrowserDependencyConfig {
        webdriver_url: std::env::var("AGENTMOD_BROWSER_WEBDRIVER_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:4444/".to_owned()),
        browser_name: std::env::var("AGENTMOD_BROWSER_NAME")
            .unwrap_or_else(|_| "chrome".to_owned()),
        artifact_root,
        request_timeout: Duration::from_secs(120),
        maximum_inline_bytes: 128 * 1024,
        maximum_artifact_bytes: 32 * 1024 * 1024,
        maximum_url_length: 16 * 1024,
        allowed_domains: environment_list("AGENTMOD_BROWSER_ALLOWED_DOMAINS"),
        allow_loopback: environment_bool("AGENTMOD_BROWSER_ALLOW_LOOPBACK"),
        authorization_owner: std::env::var("AGENTMOD_BROWSER_OWNER")?,
        authorization_session: session,
        authorization_key_hex: std::env::var("AGENTMOD_BROWSER_AUTH_KEY")?,
    })?;
    let shutdown = dependency.clone();
    let logic = BrowserLogic::new(BrowserData::new(dependency), 128 * 1024, 32 * 1024 * 1024)?;
    let service = Arc::new(BrowserHostService::new(logic));
    serve(service, tokio::io::stdin(), tokio::io::stdout()).await?;
    shutdown.shutdown().await;
    Ok(())
}

async fn serve<L, W>(
    service: Arc<BrowserHostService<L>>,
    input: tokio::io::Stdin,
    mut output: W,
) -> Result<(), Box<dyn Error>>
where
    L: agentmod_browser_host_logic::BrowserLogicPort + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<ToolHostEvent>(128);
    let writer = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let mut bytes =
                serde_json::to_vec(&event).map_err(|error| io::Error::other(error.to_string()))?;
            bytes.push(b'\n');
            output.write_all(&bytes).await?;
            output.flush().await?;
        }
        Ok::<(), io::Error>(())
    });
    let limit = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let mut lines = BufReader::new(input).lines();
    while let Some(line) = lines.next_line().await? {
        if line.len() > MAX_FRAME_BYTES {
            sender.send(failed("frame_too_large")).await?;
            continue;
        }
        let Ok(command) = serde_json::from_str::<ToolHostCommand>(&line) else {
            sender.send(failed("invalid_json")).await?;
            continue;
        };
        let Ok(permit) = Arc::clone(&limit).try_acquire_owned() else {
            sender.send(failed("host_overloaded")).await?;
            continue;
        };
        let service = Arc::clone(&service);
        let sender = sender.clone();
        tokio::spawn(async move {
            let _permit = permit;
            for event in service.handle(command).await {
                if sender.send(event).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(sender);
    writer.await??;
    Ok(())
}

fn environment_list(name: &str) -> BTreeSet<String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn environment_bool(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
    })
}

fn failed(code: &str) -> ToolHostEvent {
    ToolHostEvent::Failed {
        call_id: "invalid".to_owned(),
        code: code.to_owned(),
        message: "browser host request was rejected".to_owned(),
        retryable: false,
    }
}
