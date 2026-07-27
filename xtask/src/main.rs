use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        eprintln!("usage: cargo run -p xtask -- architecture [--manifest-path <path>]");
        return ExitCode::from(2);
    };

    if command != "architecture" {
        eprintln!("unknown xtask command `{command}`");
        return ExitCode::from(2);
    }

    let mut manifest_path = PathBuf::from("Cargo.toml");
    while let Some(argument) = args.next() {
        if argument == "--manifest-path" {
            let Some(value) = args.next() else {
                eprintln!("--manifest-path requires a path");
                return ExitCode::from(2);
            };
            manifest_path = PathBuf::from(value);
        } else {
            eprintln!("unknown architecture option `{argument}`");
            return ExitCode::from(2);
        }
    }

    match xtask::architecture::validate_manifest(&manifest_path) {
        Ok(report) if report.is_clean() => {
            println!(
                "architecture: checked {} packages; no violations",
                report.package_count
            );
            ExitCode::SUCCESS
        }
        Ok(report) => {
            for diagnostic in &report.diagnostics {
                eprintln!("{diagnostic}");
            }
            eprintln!(
                "architecture: {} violation(s) across {} packages",
                report.diagnostics.len(),
                report.package_count
            );
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("architecture: unable to inspect workspace: {error}");
            ExitCode::from(2)
        }
    }
}
