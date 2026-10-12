//! Embedded workflows (compiler.md §4): a definition with a declared
//! receive/emit interface used as a plugin inside another definition.

use std::collections::BTreeMap;

use crate::compiler::EmbedCx;
use crate::manifest::{Definition, DeploymentConfig, Manifest};

/// Inline every embedded workflow of `def` (none yet: see below).
pub(crate) fn inline_workflows(
    cx: &mut EmbedCx<'_, '_>,
    _name: &str,
    def: &Definition,
    _config: &DeploymentConfig,
    _manifests: &BTreeMap<String, Manifest>,
) -> Definition {
    let mut out = def.clone();
    let embedded: Vec<String> = def
        .plugins
        .iter()
        .filter(|(_, p)| p.workflow.is_some())
        .map(|(n, _)| n.clone())
        .collect();
    for n in embedded {
        cx.error(
            "unsupported-workflow",
            &format!("plugins.{n}"),
            None,
            &[&n],
            format!("`{n}` embeds a workflow; embedded workflows are not supported by this build"),
        );
        out.plugins.remove(&n);
    }
    out
}
