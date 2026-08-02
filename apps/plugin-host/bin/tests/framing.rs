//! Process-level framing bounds for the plugin host.

use std::{process::Stdio, time::Duration};

use agentmod_plugin_protocol::{PluginResponse, PluginResponseFrame};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};

const MAX_FRAME: usize = 1024 * 1024;

#[tokio::test]
async fn oversized_unterminated_stdin_frame_is_bounded_rejected_and_terminates_host() {
    let root = tempfile::tempdir().expect("host root");
    let executable = std::path::PathBuf::from(env!("CARGO_BIN_EXE_agentmod-plugin-host"));
    let executable_root = executable.parent().expect("executable root");
    let mut child = Command::new(&executable)
        .current_dir(root.path())
        .env("AGENTMOD_PLUGIN_OWNER", "owner")
        .env("AGENTMOD_PLUGIN_SESSION", "session-1")
        .env("AGENTMOD_PLUGIN_RUNTIME_API_VERSION", "0.1.0")
        .env("AGENTMOD_PLUGIN_AUTH_KEY", "07".repeat(32))
        .env(
            "AGENTMOD_PLUGIN_EXECUTABLE_ROOTS",
            executable_root.to_string_lossy().as_ref(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn plugin host");
    let mut stdin = child.stdin.take().expect("host stdin");
    stdin
        .write_all(&vec![b'x'; MAX_FRAME + 64 * 1024])
        .await
        .expect("write oversized frame");
    drop(stdin);
    let mut stdout = child.stdout.take().expect("host stdout");
    let mut output = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stdout.read_to_end(&mut output))
        .await
        .expect("host did not terminate")
        .expect("read host response");
    let status = child.wait().await.expect("host status");
    assert!(status.success());
    assert!(
        output.len() < 1024,
        "the bounded rejection response unexpectedly retained input"
    );
    let response: PluginResponseFrame =
        serde_json::from_slice(&output).expect("bounded protocol response");
    assert!(matches!(
        response.response,
        PluginResponse::Failed { ref code, .. } if code == "frame_too_large"
    ));
    assert_eq!(response.correlation_id, "transport-error");
}
