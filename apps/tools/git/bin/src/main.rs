//! Git-host composition root and JSONL stdio transport.

use std::{error::Error, time::Duration};

use agentmod_git_host_data::GitData;
use agentmod_git_host_dependency::{GitDependencyConfig, TokioGitDependency};
use agentmod_git_host_logic::{GitLogic, GitLogicConfig};
use agentmod_git_host_service::{GitHostService, GitHostServiceConfig};
use agentmod_tool_protocol::ToolHostCommand;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let workspace_root = std::env::current_dir()?.canonicalize()?;
    let owner_id = std::env::var("AGENTMOD_GIT_OWNER")?;
    let session_id = std::env::var("AGENTMOD_GIT_SESSION")?;
    let authorization_key_hex = std::env::var("AGENTMOD_GIT_AUTH_KEY")?;
    let artifact_root = std::env::var_os("AGENTMOD_GIT_ARTIFACT_ROOT").map_or_else(
        || {
            std::env::temp_dir()
                .join("agentmod")
                .join("git-artifacts")
                .join(safe_scope(&session_id))
        },
        std::path::PathBuf::from,
    );
    let dependency = TokioGitDependency::new(GitDependencyConfig {
        workspace_root: workspace_root.clone(),
        artifact_root,
        output_limit_bytes: 1024 * 1024,
        checkpoint_limit_bytes: 256 * 1024 * 1024,
        command_timeout: Duration::from_secs(120),
        authorization_key_hex,
    })?;
    let data = GitData::new(dependency);
    let logic = GitLogic::new(data, GitLogicConfig { workspace_root });
    let service = GitHostService::new(
        logic,
        GitHostServiceConfig {
            owner_id,
            session_id,
        },
    )?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        let command: ToolHostCommand = serde_json::from_str(&line)?;
        let events = service.handle(command).await?;
        for event in events {
            let mut encoded = serde_json::to_vec(&event)?;
            encoded.push(b'\n');
            stdout.write_all(&encoded).await?;
        }
        stdout.flush().await?;
    }
    Ok(())
}

fn safe_scope(session_id: &str) -> String {
    let scope: String = session_id
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .take(128)
        .collect();
    if scope.is_empty() {
        String::from("standalone")
    } else {
        scope
    }
}
