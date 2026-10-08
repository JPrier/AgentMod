//! Loading the TOML deployment config and stamping plugin code hashes.

use std::path::{Path, PathBuf};

use agentmod_core::manifest::DeploymentConfig;
use sha2::{Digest, Sha256};

/// A loaded config plus the directory plugin commands run from (the config's directory).
#[derive(Debug, Clone)]
pub struct Loaded {
    pub config: DeploymentConfig,
    pub base_dir: PathBuf,
}

/// Parse a TOML config file.
///
/// # Errors
/// I/O or parse failures.
pub fn load(path: &Path) -> Result<Loaded, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    let mut config: DeploymentConfig =
        toml::from_str(&text).map_err(|e| format!("parsing {}: {e}", path.display()))?;
    let base_dir = path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let base_dir = if base_dir.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        base_dir
    };
    stamp_binaries(&mut config, &base_dir);
    Ok(Loaded { config, base_dir })
}

/// Relative module specifiers a JavaScript file imports statically
/// (`import … from './x.js'`, `export … from '../y.js'`, `import('./z.js')`).
fn js_imports(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for marker in [
        "from '",
        "from \"",
        "import('",
        "import(\"",
        "import '",
        "import \"",
    ] {
        let quote = marker.chars().last().unwrap_or('\'');
        let mut rest = text;
        while let Some(i) = rest.find(marker) {
            rest = &rest[i + marker.len()..];
            if let Some(end) = rest.find(quote) {
                let spec = &rest[..end];
                if spec.starts_with("./") || spec.starts_with("../") {
                    out.push(spec.to_owned());
                }
                rest = &rest[end..];
            }
        }
    }
    out
}

/// The entry file plus every file it imports relatively, transitively (sorted).
/// A plugin's identity must change when any of its code changes, including
/// shared SDK modules it imports.
fn code_closure(entry: &Path) -> Vec<PathBuf> {
    let mut seen = std::collections::BTreeSet::new();
    let mut stack = vec![entry.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(canon) = p.canonicalize() else {
            continue;
        };
        if !seen.insert(canon.clone()) {
            continue;
        }
        let is_js = canon.extension().is_some_and(|e| e == "js" || e == "mjs");
        if !is_js {
            continue;
        }
        if let Ok(text) = std::fs::read_to_string(&canon) {
            let dir = canon.parent().map(Path::to_path_buf).unwrap_or_default();
            for spec in js_imports(&text) {
                stack.push(dir.join(spec));
            }
        }
    }
    seen.into_iter().collect()
}

/// Fill `binary_hash` for every plugin from the files its command references
/// (and, for JavaScript, the modules they import).
pub fn stamp_binaries(config: &mut DeploymentConfig, base_dir: &Path) {
    for plugin in config.plugins.values_mut() {
        let mut h = Sha256::new();
        let mut any = false;
        for arg in plugin.command.iter().skip(1) {
            let p = base_dir.join(arg);
            if !p.is_file() {
                continue;
            }
            let root = base_dir
                .canonicalize()
                .unwrap_or_else(|_| base_dir.to_path_buf());
            for file in code_closure(&p) {
                if let Ok(bytes) = std::fs::read(&file) {
                    let rel = file.strip_prefix(&root).unwrap_or(&file);
                    h.update(rel.to_string_lossy().as_bytes());
                    h.update(&bytes);
                    any = true;
                }
            }
        }
        if !any {
            h.update(plugin.command.join(" ").as_bytes());
        }
        plugin.binary_hash = Some(hex::encode(h.finalize())[..16].to_owned());
    }
}

/// Disable plugins this host cannot run. A plugin with a `module` but no
/// `command` is a worker-only plugin for the browser runtime (for example
/// `linux-sandbox`, which needs a browser for CheerpX). This mirrors the
/// browser host, which disables command-only plugins; the compiler reports
/// each skipped subscriber as `disabled-plugin` info.
pub fn disable_unrunnable(config: &mut DeploymentConfig) {
    for plugin in config.plugins.values_mut() {
        if plugin.command.is_empty() && plugin.module.is_some() {
            plugin.disabled = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_only_plugins_are_disabled_natively() {
        let mut config: DeploymentConfig = toml::from_str(
            r#"
            [plugins.both]
            command = ["node", "both.js"]
            module = "both.js"
            [plugins.native]
            command = ["python3", "native.py"]
            [plugins.worker]
            module = "worker.js"
            "#,
        )
        .unwrap();
        disable_unrunnable(&mut config);
        assert!(!config.plugins["both"].disabled);
        assert!(!config.plugins["native"].disabled);
        assert!(config.plugins["worker"].disabled);
    }

    #[test]
    fn stamps_cover_imported_modules() {
        let dir = std::env::temp_dir().join(format!("agentmod-stamp-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("p")).unwrap();
        std::fs::create_dir_all(dir.join("sdk")).unwrap();
        std::fs::write(dir.join("p/main.js"), "import { x } from '../sdk/a.js';\n").unwrap();
        std::fs::write(
            dir.join("sdk/a.js"),
            "export const x = 1; import('./b.js');\n",
        )
        .unwrap();
        std::fs::write(dir.join("sdk/b.js"), "export const y = 1;\n").unwrap();
        let mk = || -> DeploymentConfig {
            toml::from_str("[plugins.p]\ncommand = [\"node\", \"p/main.js\"]\n").unwrap()
        };
        let mut a = mk();
        stamp_binaries(&mut a, &dir);
        std::fs::write(dir.join("sdk/b.js"), "export const y = 2;\n").unwrap();
        let mut b = mk();
        stamp_binaries(&mut b, &dir);
        assert_ne!(
            a.plugins["p"].binary_hash, b.plugins["p"].binary_hash,
            "a transitive import changed the stamp"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
