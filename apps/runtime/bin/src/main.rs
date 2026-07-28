//! Runtime composition root.

use std::path::PathBuf;

use agentmod_event_pipeline::BlockingPipelineBuilder;
use agentmod_runtime_data::RuntimeData;
use agentmod_runtime_dependency::{
    LocalRuntimeDependencies,
    continuation::FileContinuationDependency,
    harness::{HarnessDependencyConfig, HarnessDependencyPort, ProcessHarnessDependency},
    local_rpc::{cleanup_local_endpoint, prepare_local_endpoint},
    process_tool::{ProcessCapabilityDependency, ProcessCapabilityDependencyConfig},
    receipt::ToolReceiptDependency,
    scheduler::{ProcessSchedulerDependency, ProcessSchedulerDependencyConfig},
    supervised::SupervisedRuntimeDependencies,
    tool::{ProcessToolHostDependency, ToolHostDependencyConfig, ToolHostKind},
};
use agentmod_runtime_logic::{
    RuntimeLogic,
    action::ActionProposal,
    harness::{ProviderExecutionLogic, ProviderExecutionPolicy},
    permission::{PermissionEffect, PermissionMatcher, PermissionPolicy, PermissionRule},
    turn::TurnLogic,
};
use agentmod_runtime_protocol::RuntimeRequest;
use agentmod_runtime_service::{
    RuntimeService, RuntimeServiceConfig,
    harness::{
        ProviderService, ProviderServicePort, ServiceExecuteProviderRequest, ServiceProviderEntry,
        ServiceProviderEvent,
    },
    local_rpc::{LocalRpcConfig, RuntimeWireEndpoint, run_local},
    turn::{RuntimeDaemonService, TurnService},
};

#[tokio::main]
#[allow(
    clippy::too_many_lines,
    reason = "the composition root explicitly assembles every isolated dependency boundary"
)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("harness-smoke") {
        return run_harness_smoke().await;
    }

    if std::env::args().nth(1).as_deref() == Some("serve") {
        let authorization_token = std::env::var("AGENTMOD_RUNTIME_AUTH_TOKEN").map_err(
            |_| "AGENTMOD_RUNTIME_AUTH_TOKEN must be set to at least 32 bytes when serving",
        )?;
        let endpoint =
            std::env::var("AGENTMOD_RUNTIME_ENDPOINT").unwrap_or_else(|_| default_endpoint());
        let configured_sessions_root = std::env::var_os("AGENTMOD_SESSION_ROOT")
            .map_or_else(|| PathBuf::from("sessions"), PathBuf::from);
        let sessions_root = if configured_sessions_root.is_absolute() {
            configured_sessions_root
        } else {
            std::env::current_dir()?.join(configured_sessions_root)
        };
        let harness = ProcessHarnessDependency::new(HarnessDependencyConfig {
            program: std::env::var("AGENTMOD_HARNESS_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-harness"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(120),
            frame_pacing: std::time::Duration::from_millis(
                std::env::var("AGENTMOD_HARNESS_FRAME_PACING_MS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0),
            ),
            authorization_key: ProcessHarnessDependency::generate_authorization_key(),
        })?;
        let filesystem = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Filesystem,
            program: std::env::var("AGENTMOD_FILESYSTEM_HOST_PROGRAM")
                .map_or_else(
                    |_| sibling_binary("agentmod-filesystem-host"),
                    PathBuf::from,
                )
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: "agentmod-runtime".into(),
            state_root: None,
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(120),
            authorization_key: ProcessToolHostDependency::generate_authorization_key(),
        })?;
        let browser = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Browser,
            program: std::env::var("AGENTMOD_BROWSER_HOST_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-browser-host"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: "agentmod-runtime".into(),
            state_root: Some(sessions_root.clone()),
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(120),
            authorization_key: ProcessToolHostDependency::generate_authorization_key(),
        })?;
        let git = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Git,
            program: std::env::var("AGENTMOD_GIT_HOST_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-git-host"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: "agentmod-runtime".into(),
            state_root: Some(sessions_root.clone()),
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(120),
            authorization_key: ProcessToolHostDependency::generate_authorization_key(),
        })?;
        let web = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Web,
            program: std::env::var("AGENTMOD_WEB_HOST_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-web-host"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: "agentmod-runtime".into(),
            state_root: Some(sessions_root.clone()),
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(120),
            authorization_key: ProcessToolHostDependency::generate_authorization_key(),
        })?;
        let lsp = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Lsp,
            program: std::env::var("AGENTMOD_LSP_HOST_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-lsp-host"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: "agentmod-runtime".into(),
            state_root: None,
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(120),
            authorization_key: ProcessToolHostDependency::generate_authorization_key(),
        })?;
        let mcp = ProcessToolHostDependency::new(ToolHostDependencyConfig {
            kind: ToolHostKind::Mcp,
            program: std::env::var("AGENTMOD_MCP_HOST_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-mcp-host"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: "agentmod-runtime".into(),
            state_root: Some(sessions_root.clone()),
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(120),
            authorization_key: ProcessToolHostDependency::generate_authorization_key(),
        })?;
        let processes = ProcessCapabilityDependency::new(ProcessCapabilityDependencyConfig {
            program: std::env::var("AGENTMOD_PROCESS_HOST_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-process-host"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner: "agentmod-runtime".into(),
            allowed_executables: std::env::var("AGENTMOD_PROCESS_ALLOWED_EXECUTABLES")
                .unwrap_or_default()
                .split(';')
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .collect(),
            endpoint_root: sessions_root.join(".process-hosts"),
            maximum_frame_bytes: 16 * 1024 * 1024,
            request_timeout: std::time::Duration::from_secs(24 * 60 * 60),
            authorization_key: ProcessCapabilityDependency::derive_authorization_key(
                authorization_token.as_bytes(),
            ),
        })?;
        let scheduler_root = std::env::var_os("AGENTMOD_SCHEDULER_ROOT").map_or_else(
            || {
                sessions_root
                    .parent()
                    .unwrap_or(&sessions_root)
                    .join("scheduler")
            },
            PathBuf::from,
        );
        let scheduler = ProcessSchedulerDependency::new(ProcessSchedulerDependencyConfig {
            program: std::env::var("AGENTMOD_SCHEDULER_PROGRAM")
                .map_or_else(|_| sibling_binary("agentmod-scheduler"), PathBuf::from)
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            state_root: scheduler_root,
            authentication_token: ProcessSchedulerDependency::generate_authentication_token(),
            maximum_frame_bytes: 1024 * 1024,
        })?;
        let data = RuntimeData::new(SupervisedRuntimeDependencies::new(
            harness,
            browser,
            filesystem,
            processes,
            git,
            web,
            lsp,
            mcp,
            ToolReceiptDependency::new(sessions_root.clone())?.with_post_persist_delay(
                std::time::Duration::from_millis(
                    std::env::var("AGENTMOD_TOOL_RECEIPT_DELAY_MS")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(0),
                ),
            )?,
            FileContinuationDependency::new(sessions_root.clone()),
            scheduler,
        ));
        let core = RuntimeService::new(
            RuntimeLogic::new(data.clone()),
            RuntimeServiceConfig {
                session_root: sessions_root.clone(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
        );
        let turns = TurnService::new(TurnLogic::new(data, provider_policy()), sessions_root);
        let recovery = turns.recover_startup_tools().await?;
        eprintln!(
            "{}",
            serde_json::json!({
                "event": "runtime.startup_tool_recovery",
                "receipt_count": recovery.receipt_count,
                "reconciled_count": recovery.reconciled_count,
                "already_terminal_count": recovery.already_terminal_count,
                "deferred_approval_count": recovery.deferred_approval_count,
                "orphaned_count": recovery.orphaned_count,
            })
        );
        let service = RuntimeDaemonService::new(core, turns);
        prepare_local_endpoint(&endpoint)?;
        let poll_interval_ms = std::env::var("AGENTMOD_SCHEDULER_POLL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_000);
        let poll_limit = std::env::var("AGENTMOD_SCHEDULER_POLL_LIMIT")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| (1..=1_000).contains(value))
            .unwrap_or(16);
        let scheduler_poller = if poll_interval_ms == 0 {
            None
        } else {
            let scheduler_service = service.clone();
            Some(tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_millis(poll_interval_ms));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if let Err(error) = scheduler_service
                        .handle_runtime_request(&RuntimeRequest::RunDueSchedules {
                            limit: poll_limit,
                        })
                        .await
                    {
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "event": "runtime.scheduler_poll_failed",
                                "message": error
                            })
                        );
                    }
                }
            }))
        };
        let result = run_local(
            service,
            LocalRpcConfig {
                endpoint: endpoint.clone(),
                authorization_token: authorization_token.into(),
                maximum_frame_bytes: agentmod_protocol_support::DEFAULT_MAX_FRAME_BYTES,
            },
        )
        .await;
        if let Some(poller) = scheduler_poller {
            poller.abort();
            let _ = poller.await;
        }
        let cleanup = cleanup_local_endpoint(&endpoint);
        result?;
        cleanup?;
        return Ok(());
    }

    let dependency = LocalRuntimeDependencies;
    let data = RuntimeData::new(dependency);
    let logic = RuntimeLogic::new(data);
    let service = RuntimeService::new(
        logic,
        RuntimeServiceConfig {
            session_root: PathBuf::from("sessions"),
            version: env!("CARGO_PKG_VERSION").into(),
        },
    );
    let response = service.handle_wire(&RuntimeRequest::Health)?;
    println!("{}", serde_json::to_string(&response)?);
    Ok(())
}

fn provider_policy() -> ProviderExecutionPolicy {
    let empty_pipeline = || {
        BlockingPipelineBuilder::<ActionProposal>::new()
            .compile()
            .expect("empty built-in pipeline is valid")
    };
    let allow_sensitive = std::env::var("AGENTMOD_PERMISSION_MODE")
        .is_ok_and(|value| value.eq_ignore_ascii_case("allow"));
    ProviderExecutionPolicy {
        style_pipeline: std::sync::Arc::new(empty_pipeline()),
        plugin_pipeline: std::sync::Arc::new(empty_pipeline()),
        user_policy: PermissionPolicy::new(
            "default-interactive",
            [
                "filesystem.write",
                "filesystem.edit",
                "filesystem.apply_patch",
                "process.run",
                "process.start",
                "process.run_pty",
                "process.start_pty",
                "process.input",
                "process.resize",
                "process.interrupt",
                "process.kill",
                "git.worktree_create",
                "git.worktree_cleanup",
                "git.checkpoint_create",
                "git.checkpoint_restore",
                "http.request",
                "web.fetch",
                "web.search",
                "mcp.invoke",
                "browser.start",
                "browser.navigate",
                "browser.screenshot",
                "browser.click",
                "browser.type",
                "browser.submit",
                "browser.download",
                "browser.close",
            ]
            .into_iter()
            .enumerate()
            .filter(|_| !allow_sensitive)
            .map(|(index, tool)| PermissionRule {
                id: format!("ask-sensitive-action-{}", index + 1),
                priority: 100,
                matcher: PermissionMatcher {
                    tool: Some(tool.into()),
                    ..PermissionMatcher::default()
                },
                effect: PermissionEffect::Ask,
                reason: "sensitive action requires explicit approval".into(),
            })
            .collect(),
            PermissionEffect::Allow,
            "read-only model and tool actions may continue",
        ),
        mandatory_policy: PermissionPolicy::new(
            "runtime-mandatory",
            Vec::new(),
            PermissionEffect::Allow,
            "provider request passed mandatory runtime checks",
        ),
    }
}

async fn run_harness_smoke() -> Result<(), Box<dyn std::error::Error>> {
    let program = std::env::var("AGENTMOD_HARNESS_PROGRAM")
        .map_or_else(|_| sibling_binary("agentmod-harness"), PathBuf::from);
    let dependency = ProcessHarnessDependency::new(HarnessDependencyConfig {
        program: program.to_string_lossy().into_owned(),
        arguments: Vec::new(),
        maximum_frame_bytes: 16 * 1024 * 1024,
        request_timeout: std::time::Duration::from_secs(10),
        frame_pacing: std::time::Duration::ZERO,
        authorization_key: ProcessHarnessDependency::generate_authorization_key(),
    })?;
    let data = agentmod_runtime_data::harness::HarnessData::new(dependency.clone());
    let logic = ProviderExecutionLogic::new(data, provider_policy());
    let service = ProviderService::new(logic);
    let events = service
        .execute(ServiceExecuteProviderRequest {
            session_id: uuid::Uuid::now_v7().to_string(),
            provider: "deterministic-mock".into(),
            model: "mock-model".into(),
            entries: vec![ServiceProviderEntry::User("runtime harness smoke".into())],
            options: serde_json::json!({
                "mock_scenario": "streaming_text",
                "mock_text": "runtime-harness-ok"
            }),
            cancellation_id: uuid::Uuid::now_v7().to_string(),
            style: "persistent-chat".into(),
            workspace: "smoke".into(),
        })
        .await?;
    dependency.shutdown().await;
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            ServiceProviderEvent::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    let completed = events
        .iter()
        .any(|event| matches!(event, ServiceProviderEvent::Completed { .. }));
    if !completed || text != "alpha beta runtime-harness-ok" {
        return Err("runtime/harness smoke response was incomplete".into());
    }
    println!(
        "{}",
        serde_json::json!({
            "status": "ok",
            "boundary": "runtime_service_to_harness_process",
            "event_count": events.len(),
            "text": text
        })
    );
    Ok(())
}

fn sibling_binary(name: &str) -> PathBuf {
    let executable = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_owned()
    };
    std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.join(&executable)))
        .unwrap_or_else(|| PathBuf::from(executable))
}

#[cfg(windows)]
fn default_endpoint() -> String {
    String::from(r"\\.\pipe\agentmod-runtime")
}

#[cfg(unix)]
fn default_endpoint() -> String {
    String::from("/tmp/agentmod-runtime.sock")
}
