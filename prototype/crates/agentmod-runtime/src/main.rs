//! `agentmod` — the native prototype runtime.
//!
//! ```text
//! agentmod serve   [--config agentmod.toml] [--data .agentmod]
//! agentmod compile [--config agentmod.toml] [--json]
//! agentmod inspect [--data .agentmod] [SESSION] [--json]
//! agentmod verify  [--data .agentmod]
//! agentmod config  [--config agentmod.toml]
//! ```

mod config;
mod host;
mod inspect;
mod proc;
mod store;

use std::path::PathBuf;
use std::process::ExitCode;

use agentmod_core::compiler::Severity;

struct Args {
    cmd: String,
    config: PathBuf,
    data: PathBuf,
    json: bool,
    positional: Vec<String>,
}

fn parse() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let cmd = it.next().unwrap_or_else(|| "help".into());
    let mut a = Args {
        cmd,
        config: PathBuf::from("agentmod.toml"),
        data: PathBuf::from(".agentmod"),
        json: false,
        positional: Vec::new(),
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" | "-c" => a.config = it.next().ok_or("--config needs a path")?.into(),
            "--data" | "-d" => a.data = it.next().ok_or("--data needs a path")?.into(),
            "--json" => a.json = true,
            s if s.starts_with('-') => return Err(format!("unknown flag {s}")),
            s => a.positional.push(s.to_owned()),
        }
    }
    Ok(a)
}

const HELP: &str = "agentmod — AgentMod High-Level Design prototype runtime

USAGE:
  agentmod serve   [--config agentmod.toml] [--data .agentmod]   run the runtime and its plugins
  agentmod compile [--config agentmod.toml] [--json]             handshake plugins and validate the graph
  agentmod inspect [--data .agentmod] [SESSION] [--json]         read logs (replay-as-reading; runs no plugins)
  agentmod verify  [--data .agentmod]                            replay every log through a fresh kernel
  agentmod config  [--config agentmod.toml]                      print the config as JSON (used by the browser runtime)
";

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: {e}\n\n{HELP}");
            return ExitCode::from(2);
        }
    };
    let result = match args.cmd.as_str() {
        "serve" => serve(&args, false).await,
        "compile" => serve(&args, true).await,
        "inspect" => inspect::inspect(
            &args.data,
            args.positional.first().map(String::as_str),
            args.json,
        ),
        "verify" => inspect::verify(&args.data),
        "config" => export_config(&args),
        _ => {
            print!("{HELP}");
            Ok(())
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(args: &Args, compile_only: bool) -> Result<(), String> {
    let loaded = config::load(&args.config)?;
    let (host, compilation) = host::Host::boot(host::Options {
        config: loaded.config,
        base_dir: loaded.base_dir,
        data_dir: args.data.clone(),
        compile_only,
    })
    .await?;
    if compile_only && args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&compilation).map_err(|e| e.to_string())?
        );
    } else {
        for d in &compilation.diagnostics {
            let sev = match d.severity {
                Severity::Error => "error",
                Severity::Warning => "warning",
                Severity::Info => "info",
            };
            let loc = [
                d.definition.as_deref(),
                d.plugin.as_deref(),
                d.event.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" / ");
            eprintln!("{sev}[{}] {loc}: {}", d.code, d.message);
        }
        if compile_only {
            for (name, def) in &compilation.definitions {
                println!("definition `{name}` — {} plugin(s)", def.plugins.len());
                for (event, p) in &def.pipelines {
                    let b: Vec<&str> = p.blocking.iter().map(|s| s.plugin.as_str()).collect();
                    let a: Vec<&str> = p.asyncs.iter().map(|s| s.plugin.as_str()).collect();
                    println!(
                        "  {event:<20} blocking [{}]  async [{}]",
                        b.join(" → "),
                        a.join(", ")
                    );
                }
            }
            println!(
                "config {} — {}",
                compilation.hash,
                if compilation.ok { "ok" } else { "REJECTED" }
            );
        }
    }
    if !compilation.ok {
        return Err("configuration failed to compile".into());
    }
    if compile_only {
        return Ok(());
    }
    eprintln!(
        "agentmod: runtime ready — config {} — data in {}",
        compilation.hash,
        args.data.display()
    );
    let tx = host.sender();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("agentmod: shutting down");
        let _ = tx.send(proc::Msg::Shutdown);
    });
    host.run().await;
    Ok(())
}

fn export_config(args: &Args) -> Result<(), String> {
    let mut loaded = config::load(&args.config)?;
    for p in loaded.config.plugins.values_mut() {
        p.binary_hash = None;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&loaded.config).map_err(|e| e.to_string())?
    );
    Ok(())
}
