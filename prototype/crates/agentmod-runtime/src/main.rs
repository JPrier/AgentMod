//! `agentmod` — the native prototype runtime.
//!
//! ```text
//! agentmod serve   [--config agentmod.toml] [--data .agentmod]
//! agentmod compile [--config agentmod.toml] [--json]
//! agentmod inspect [--data .agentmod] [SESSION] [--json]
//! agentmod verify  [--data .agentmod]
//! agentmod metrics [--data .agentmod] [SESSION] [--json] [--turns]
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
    turns: bool,
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
        turns: false,
        positional: Vec::new(),
    };
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" | "-c" => a.config = it.next().ok_or("--config needs a path")?.into(),
            "--data" | "-d" => a.data = it.next().ok_or("--data needs a path")?.into(),
            "--json" => a.json = true,
            "--turns" => a.turns = true,
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
  agentmod metrics [--data .agentmod] [SESSION] [--json] [--turns]
                                                                 model efficiency + control-plane amplification, from the logs
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
        "metrics" => inspect::metrics(
            &args.data,
            args.positional.first().map(String::as_str),
            args.json,
            args.turns,
        ),
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
    let mut loaded = config::load(&args.config)?;
    config::disable_unrunnable(&mut loaded.config);
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
            if compile_only || d.severity != Severity::Info {
                eprintln!("{sev}[{}] {}: {}", d.code, d.location(), d.message);
            }
        }
        if compile_only {
            for (name, rev) in &compilation.definitions {
                print_revision(name, rev);
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

/// Print one compiled definition: its graph (as routes), and the report sections.
fn print_revision(name: &str, rev: &agentmod_core::compiler::Revision) {
    let g = &rev.graph;
    let members = rev.plugins.values().filter(|m| m.member).count();
    println!(
        "definition `{name}` — revision {} — runtime {} — {members} plugin(s){}",
        rev.revision,
        rev.runtime,
        if rev.ok { "" } else { " — REJECTED" }
    );
    if !rev.definition.phases.is_empty() {
        println!("  phases: {}", rev.definition.phases.join(" → "));
    }
    println!("  graph ({} edges):", g.edges.len());
    for r in &g.routes {
        let chain: Vec<String> = r
            .blocking
            .iter()
            .map(|i| {
                let e = g.edge(*i);
                let ph = e.position.as_ref().map(|p| p.phase.as_str()).unwrap_or("?");
                format!("{ph}:{}", e.to)
            })
            .collect();
        let asyncs: Vec<&str> = r.asyncs.iter().map(|i| g.edge(*i).to.as_str()).collect();
        let mut line = format!("    {} --{}-->", r.from, r.class);
        if !chain.is_empty() {
            line.push_str(&format!(" blocking [{}]", chain.join(" → ")));
        }
        if !asyncs.is_empty() {
            line.push_str(&format!(" async [{}]", asyncs.join(", ")));
        }
        match (r.handler, r.exactly_one_handler) {
            (Some(i), _) => line.push_str(&format!(" handler {}", g.edge(i).to)),
            (None, true) => line.push_str(" handler (none: dispatch-failed)"),
            _ => {}
        }
        println!("{line}");
    }
    let rp = &rev.report;
    let section = |title: &str, items: Vec<String>| {
        if !items.is_empty() {
            println!("  {title}:");
            for i in items {
                println!("    {i}");
            }
        }
    };
    section("cycles", rp.cycles.iter().map(|c| c.join(" → ")).collect());
    section(
        "families",
        rp.families
            .iter()
            .flat_map(|(f, h)| {
                h.members
                    .iter()
                    .chain(&h.subfamilies)
                    .map(move |(m, p)| format!("{f} {m} → {p}"))
            })
            .collect(),
    );
    section(
        "capabilities",
        rp.capabilities
            .iter()
            .map(|(c, p)| format!("{c}: {}", p.join(", ")))
            .collect(),
    );
    section("interrupt", rp.interrupt.clone());
    section("unlisted keys", rp.unlisted_keys.clone());
    section(
        "external surface",
        rp.external_surface
            .iter()
            .map(|(k, v)| format!("{k}: {}", v.join("; ")))
            .collect(),
    );
    section("fixed orders", rp.fixed_orders.clone());
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
