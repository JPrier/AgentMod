//! Plugin processes: spawn, line-framed stdio I/O, and supervision signals.

use std::path::Path;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};

/// Messages delivered to the host actor.
#[derive(Debug)]
pub enum Msg {
    /// One protocol line from a plugin's stdout.
    Line { proc: u64, line: String },
    /// The process exited (crash, kill, or clean shutdown).
    Exited { proc: u64, status: String },
    /// Periodic supervision tick.
    Tick,
    /// Graceful shutdown request.
    Shutdown,
}

/// Handle to a running plugin process.
pub struct ProcHandle {
    pub tx: mpsc::UnboundedSender<String>,
    pub pid: Option<u32>,
    kill: Option<oneshot::Sender<()>>,
}

impl ProcHandle {
    /// Send one JSON line.
    pub fn send(&self, line: String) {
        let _ = self.tx.send(line);
    }

    /// Kill the process (supervisor escalation).
    pub fn kill(&mut self) {
        if let Some(k) = self.kill.take() {
            let _ = k.send(());
        }
    }
}

/// Spawn a plugin process speaking newline-delimited JSON-RPC on stdio.
///
/// # Errors
/// When the command is empty or cannot be started.
pub fn spawn(id: u64, name: &str, command: &[String], base_dir: &Path, data_dir: &Path, out: mpsc::UnboundedSender<Msg>) -> Result<ProcHandle, String> {
    let (program, args) = command.split_first().ok_or_else(|| format!("plugin `{name}` has an empty command"))?;
    let mut child = Command::new(program)
        .args(args)
        .current_dir(base_dir)
        .env("AGENTMOD_PLUGIN", name)
        .env("AGENTMOD_PLUGIN_DATA", data_dir.join("plugin-data"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawning plugin `{name}` ({program}): {e}"))?;
    let pid = child.id();
    let mut stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            if stdin.write_all(line.as_bytes()).await.is_err() || stdin.write_all(b"\n").await.is_err() {
                break;
            }
            let _ = stdin.flush().await;
        }
    });
    let out2 = out.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if out2.send(Msg::Line { proc: id, line }).is_err() {
                break;
            }
        }
    });
    let tag = name.to_owned();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("  [{tag}] {line}");
        }
    });
    let (kill_tx, kill_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let status = tokio::select! {
            s = child.wait() => s.map_or_else(|e| e.to_string(), |s| s.to_string()),
            _ = kill_rx => {
                let _ = child.kill().await;
                "killed by supervisor".to_owned()
            }
        };
        let _ = out.send(Msg::Exited { proc: id, status });
    });
    Ok(ProcHandle { tx, pid, kill: Some(kill_tx) })
}
