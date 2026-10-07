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

/// Fill `binary_hash` for every plugin from the files its command references.
pub fn stamp_binaries(config: &mut DeploymentConfig, base_dir: &Path) {
    for plugin in config.plugins.values_mut() {
        let mut h = Sha256::new();
        let mut any = false;
        for arg in plugin.command.iter().skip(1) {
            let p = base_dir.join(arg);
            if let Ok(bytes) = std::fs::read(&p) {
                h.update(arg.as_bytes());
                h.update(&bytes);
                any = true;
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
}
