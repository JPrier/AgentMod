use cargo_metadata::{Metadata, MetadataCommand, Package, PackageId};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

pub const MISSING_LAYER_METADATA: &str = "AMOD001";
pub const MISSING_PROCESS_METADATA: &str = "AMOD002";
pub const SKIPPED_LAYER: &str = "AMOD003";
pub const UPWARD_LAYER: &str = "AMOD004";
pub const CROSS_PROCESS_INTERNAL: &str = "AMOD005";
pub const PROTOCOL_IN_DOMAIN_LAYER: &str = "AMOD006";
pub const EXTERNAL_API_ABOVE_DEPENDENCY: &str = "AMOD007";
pub const CROSS_LAYER_TYPE_ALIAS: &str = "AMOD008";
pub const CROSS_LAYER_REEXPORT: &str = "AMOD009";
pub const UPWARD_CALLBACK: &str = "AMOD010";
pub const PATH_METADATA_MISMATCH: &str = "AMOD011";
pub const INCOMPLETE_PROCESS_LAYERS: &str = "AMOD012";

const LAYERS: [&str; 4] = ["service", "logic", "data", "dependency"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    pub code: &'static str,
    pub package: String,
    pub path: Option<PathBuf>,
    pub message: String,
    pub help: String,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "error[{}] {}", self.code, self.package)?;
        if let Some(path) = &self.path {
            write!(formatter, " ({})", path.display())?;
        }
        write!(formatter, ": {}\n  help: {}", self.message, self.help)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ValidationReport {
    pub package_count: usize,
    pub diagnostics: Vec<Diagnostic>,
}

impl ValidationReport {
    pub fn is_clean(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

#[derive(Clone, Debug, Default)]
struct ArchitectureMetadata {
    kind: Option<String>,
    process: Option<String>,
    layer: Option<String>,
}

impl ArchitectureMetadata {
    fn from_package(package: &Package) -> Self {
        let metadata = package.metadata.get("agentmod");
        Self {
            kind: metadata
                .and_then(|value| value.get("kind"))
                .and_then(|value| value.as_str())
                .map(str::to_owned),
            process: metadata
                .and_then(|value| value.get("process"))
                .and_then(|value| value.as_str())
                .map(str::to_owned),
            layer: metadata
                .and_then(|value| value.get("layer"))
                .and_then(|value| value.as_str())
                .map(str::to_owned),
        }
    }

    fn is_protocol(&self) -> bool {
        self.kind.as_deref() == Some("protocol")
    }

    fn is_composition_root(&self) -> bool {
        self.kind.as_deref() == Some("composition-root")
    }
}

pub fn validate_manifest(manifest_path: &Path) -> Result<ValidationReport, String> {
    let canonical_manifest = manifest_path
        .canonicalize()
        .map_err(|error| format!("{}: {error}", manifest_path.display()))?;
    let metadata = MetadataCommand::new()
        .manifest_path(&canonical_manifest)
        .no_deps()
        .exec()
        .map_err(|error| error.to_string())?;
    Ok(validate_metadata(&metadata))
}

fn validate_metadata(metadata: &Metadata) -> ValidationReport {
    let workspace_members: BTreeSet<_> = metadata.workspace_members.iter().cloned().collect();
    let packages: Vec<_> = metadata
        .packages
        .iter()
        .filter(|package| workspace_members.contains(&package.id))
        .collect();
    let package_by_name: HashMap<_, _> = packages
        .iter()
        .map(|package| (package.name.as_str(), *package))
        .collect();
    let package_by_id: HashMap<_, _> = packages
        .iter()
        .map(|package| (package.id.clone(), *package))
        .collect();
    let metadata_by_id: HashMap<_, _> = packages
        .iter()
        .map(|package| {
            (
                package.id.clone(),
                ArchitectureMetadata::from_package(package),
            )
        })
        .collect();
    let mut diagnostics = Vec::new();

    validate_metadata_declarations(&packages, &metadata_by_id, &mut diagnostics);
    validate_process_completeness(&packages, &metadata_by_id, &mut diagnostics);

    if let Some(resolve) = &metadata.resolve {
        for node in &resolve.nodes {
            let Some(source_package) = package_by_id.get(&node.id) else {
                continue;
            };
            let source_architecture = &metadata_by_id[&node.id];
            for dependency in &node.deps {
                let Some(target_package) = package_by_id.get(&dependency.pkg) else {
                    continue;
                };
                let target_architecture = &metadata_by_id[&dependency.pkg];
                validate_dependency_edge(
                    source_package,
                    source_architecture,
                    target_package,
                    target_architecture,
                    &mut diagnostics,
                );
            }
        }
    } else {
        // `cargo metadata --no-deps` normally retains resolve information. This
        // fallback also handles synthetic metadata used by downstream tooling.
        for source_package in &packages {
            let source_architecture = &metadata_by_id[&source_package.id];
            for dependency in &source_package.dependencies {
                let Some(target_package) = package_by_name.get(dependency.name.as_str()) else {
                    continue;
                };
                let target_architecture = &metadata_by_id[&target_package.id];
                validate_dependency_edge(
                    source_package,
                    source_architecture,
                    target_package,
                    target_architecture,
                    &mut diagnostics,
                );
            }
        }
    }

    for package in &packages {
        let architecture = &metadata_by_id[&package.id];
        validate_sources(package, architecture, &mut diagnostics);
    }

    diagnostics.sort_by(|left, right| {
        left.code
            .cmp(right.code)
            .then_with(|| left.package.cmp(&right.package))
            .then_with(|| left.path.cmp(&right.path))
            .then_with(|| left.message.cmp(&right.message))
    });
    diagnostics.dedup();

    ValidationReport {
        package_count: packages.len(),
        diagnostics,
    }
}

fn validate_metadata_declarations(
    packages: &[&Package],
    metadata_by_id: &HashMap<PackageId, ArchitectureMetadata>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for package in packages {
        let architecture = &metadata_by_id[&package.id];
        let manifest_path = package.manifest_path.as_std_path();
        let inferred_layer = infer_layer_from_path(manifest_path);
        let inferred_process = infer_process_from_path(manifest_path);

        if inferred_layer.is_some() && architecture.layer.is_none() {
            diagnostics.push(diagnostic(
                MISSING_LAYER_METADATA,
                package,
                Some(manifest_path),
                "a package stored in a layer directory has no `package.metadata.agentmod.layer`",
                "declare `layer` and `process` under `[package.metadata.agentmod]`",
            ));
        }

        if let Some(layer) = architecture.layer.as_deref() {
            if !LAYERS.contains(&layer) {
                diagnostics.push(diagnostic(
                    MISSING_LAYER_METADATA,
                    package,
                    Some(manifest_path),
                    format!("declared unknown architecture layer `{layer}`"),
                    "use exactly one of: service, logic, data, dependency",
                ));
            }
            if architecture.process.as_deref().is_none_or(str::is_empty) {
                diagnostics.push(diagnostic(
                    MISSING_PROCESS_METADATA,
                    package,
                    Some(manifest_path),
                    "a process-layer package has no process owner",
                    "set `package.metadata.agentmod.process` to the owning executable process",
                ));
            }
        }

        if architecture.is_composition_root()
            && architecture.process.as_deref().is_none_or(str::is_empty)
        {
            diagnostics.push(diagnostic(
                MISSING_PROCESS_METADATA,
                package,
                Some(manifest_path),
                "a composition root has no process owner",
                "set `package.metadata.agentmod.process` to the process being assembled",
            ));
        }

        if let (Some(inferred), Some(declared)) = (inferred_layer, architecture.layer.as_deref()) {
            if inferred != declared {
                diagnostics.push(diagnostic(
                    PATH_METADATA_MISMATCH,
                    package,
                    Some(manifest_path),
                    format!(
                        "manifest path implies layer `{inferred}`, but metadata declares `{declared}`"
                    ),
                    "move the crate or correct its declared layer so path and metadata agree",
                ));
            }
        }

        if let (Some(inferred), Some(declared)) =
            (inferred_process, architecture.process.as_deref())
        {
            if inferred != declared {
                diagnostics.push(diagnostic(
                    PATH_METADATA_MISMATCH,
                    package,
                    Some(manifest_path),
                    format!(
                        "manifest path implies process `{inferred}`, but metadata declares `{declared}`"
                    ),
                    "move the crate or correct its process owner so path and metadata agree",
                ));
            }
        }
    }
}

fn validate_process_completeness(
    packages: &[&Package],
    metadata_by_id: &HashMap<PackageId, ArchitectureMetadata>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut process_layers: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for package in packages {
        let architecture = &metadata_by_id[&package.id];
        if let (Some(process), Some(layer)) = (
            architecture.process.as_deref(),
            architecture.layer.as_deref(),
        ) {
            if LAYERS.contains(&layer) {
                process_layers.entry(process).or_default().insert(layer);
            }
        }
    }

    for (process, present) in process_layers {
        let missing: Vec<_> = LAYERS
            .iter()
            .copied()
            .filter(|layer| !present.contains(layer))
            .collect();
        if !missing.is_empty() {
            diagnostics.push(Diagnostic {
                code: INCOMPLETE_PROCESS_LAYERS,
                package: process.to_owned(),
                path: None,
                message: format!(
                    "deployable process is missing required layer crate(s): {}",
                    missing.join(", ")
                ),
                help: "provide service, logic, data, and dependency crates for every process"
                    .to_owned(),
            });
        }
    }
}

fn validate_dependency_edge(
    source_package: &Package,
    source: &ArchitectureMetadata,
    target_package: &Package,
    target: &ArchitectureMetadata,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let manifest_path = source_package.manifest_path.as_std_path();

    if let (Some(source_process), Some(target_process)) =
        (source.process.as_deref(), target.process.as_deref())
    {
        if source_process != target_process
            && source.layer.is_some()
            && target.layer.is_some()
            && !source.is_composition_root()
        {
            diagnostics.push(diagnostic(
                CROSS_PROCESS_INTERNAL,
                source_package,
                Some(manifest_path),
                format!(
                    "process `{source_process}` imports internal crate `{}` from process `{target_process}`",
                    target_package.name
                ),
                "communicate through a versioned protocol instead of importing process internals",
            ));
        }
    }

    if matches!(source.layer.as_deref(), Some("logic" | "data")) && target.is_protocol() {
        diagnostics.push(diagnostic(
            PROTOCOL_IN_DOMAIN_LAYER,
            source_package,
            Some(manifest_path),
            format!(
                "{} layer depends on wire-contract crate `{}`",
                source.layer.as_deref().unwrap_or_default(),
                target_package.name
            ),
            "map protocol DTOs in service or dependency; expose layer-owned types inward",
        ));
    }

    let (Some(source_layer), Some(target_layer)) =
        (source.layer.as_deref(), target.layer.as_deref())
    else {
        return;
    };
    if source.process != target.process {
        return;
    }

    let source_rank = layer_rank(source_layer);
    let target_rank = layer_rank(target_layer);
    let (Some(source_rank), Some(target_rank)) = (source_rank, target_rank) else {
        return;
    };

    if target_rank < source_rank {
        diagnostics.push(diagnostic(
            UPWARD_LAYER,
            source_package,
            Some(manifest_path),
            format!(
                "{source_layer} layer depends upward on {} layer crate `{}`",
                target_layer, target_package.name
            ),
            "remove the upward dependency and translate results at the existing boundary",
        ));
    } else if target_rank > source_rank + 1 {
        diagnostics.push(diagnostic(
            SKIPPED_LAYER,
            source_package,
            Some(manifest_path),
            format!(
                "{source_layer} layer skips directly to {} layer crate `{}`",
                target_layer, target_package.name
            ),
            "route the call through the layer directly beneath the caller",
        ));
    }
}

fn validate_sources(
    package: &Package,
    architecture: &ArchitectureMetadata,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let Some(layer) = architecture.layer.as_deref() else {
        return;
    };
    let package_root = package
        .manifest_path
        .parent()
        .map(|path| path.as_std_path().to_owned())
        .unwrap_or_default();

    for entry in WalkDir::new(&package_root)
        .into_iter()
        .filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            name != "target" && name != ".git"
        })
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.path().extension().and_then(|value| value.to_str()) == Some("rs")
        })
    {
        let path = entry.path();
        let Ok(source) = fs::read_to_string(path) else {
            continue;
        };
        validate_protocol_source(package, layer, path, &source, diagnostics);
        validate_external_api_source(package, layer, path, &source, diagnostics);
        validate_aliases_and_reexports(package, path, &source, diagnostics);
        validate_callbacks(package, layer, path, &source, diagnostics);
    }
}

fn validate_protocol_source(
    package: &Package,
    layer: &str,
    path: &Path,
    source: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if !matches!(layer, "logic" | "data") {
        return;
    }
    let protocol_path = Regex::new(r"\b[A-Za-z][A-Za-z0-9_]*_protocol::").expect("valid regex");
    if protocol_path.is_match(&strip_line_comments(source)) {
        diagnostics.push(diagnostic(
            PROTOCOL_IN_DOMAIN_LAYER,
            package,
            Some(path),
            format!("{layer} source refers directly to a protocol DTO"),
            "translate wire DTOs in service/dependency and use a layer-owned type here",
        ));
    }
}

fn validate_external_api_source(
    package: &Package,
    layer: &str,
    path: &Path,
    source: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if layer == "dependency" {
        return;
    }
    let dependency_only_paths = [
        "std::fs::",
        "std::process::",
        "tokio::fs::",
        "tokio::process::",
        "reqwest::",
        "hyper::client::",
        "sqlx::",
        "rusqlite::",
        "git2::",
        "gix::",
        "rmcp::",
        "lsp_types::",
        "tower_lsp::",
        "keyring::",
        "secret_service::",
        "wasmtime::",
        "chromiumoxide::",
        "headless_chrome::",
    ];
    let inspected = strip_line_comments(source);
    if let Some(external_path) = dependency_only_paths
        .iter()
        .find(|candidate| inspected.contains(**candidate))
    {
        diagnostics.push(diagnostic(
            EXTERNAL_API_ABOVE_DEPENDENCY,
            package,
            Some(path),
            format!("{layer} source uses dependency-only API `{external_path}`"),
            "define a layer-owned request and move the external API interaction to dependency",
        ));
    }
}

fn validate_aliases_and_reexports(
    package: &Package,
    path: &Path,
    source: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let inspected = strip_line_comments(source);
    let alias =
        Regex::new(r"(?m)^\s*pub\s+type\s+[A-Za-z_][A-Za-z0-9_]*(?:\s*<[^;=]+>)?\s*=\s*([^;]+);")
            .expect("valid regex");
    for captures in alias.captures_iter(&inspected) {
        let target = &captures[1];
        if contains_cross_layer_reference(target) {
            diagnostics.push(diagnostic(
                CROSS_LAYER_TYPE_ALIAS,
                package,
                Some(path),
                format!(
                    "public type alias exposes another boundary: `{}`",
                    target.trim()
                ),
                "define a layer-owned newtype or struct and map explicitly at the boundary",
            ));
        }
    }

    let reexport =
        Regex::new(r"(?m)^\s*pub\s+use\s+([^;]+);").expect("public-use regex must compile");
    for captures in reexport.captures_iter(&inspected) {
        let target = &captures[1];
        if contains_cross_layer_reference(target) {
            diagnostics.push(diagnostic(
                CROSS_LAYER_REEXPORT,
                package,
                Some(path),
                format!(
                    "public re-export exposes another boundary: `{}`",
                    target.trim()
                ),
                "expose a layer-owned type and keep the mapping in the calling layer",
            ));
        }
    }
}

fn validate_callbacks(
    package: &Package,
    layer: &str,
    path: &Path,
    source: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let upper_markers: &[&str] = match layer {
        "logic" => &["Service"],
        "data" => &["Service", "Logic"],
        "dependency" => &["Service", "Logic", "Data"],
        _ => return,
    };
    let callbacks =
        Regex::new(r"(?s)(?:dyn\s+)?Fn(?:Mut|Once)?\s*\([^)]{0,512}\)").expect("valid regex");
    let upper_type = Regex::new(&format!(
        r"\b(?:{})(?:[A-Z][A-Za-z0-9_]*)?\b",
        upper_markers.join("|")
    ))
    .expect("upper-layer type regex must compile");
    if callbacks
        .find_iter(&strip_line_comments(source))
        .any(|found| upper_type.is_match(found.as_str()))
    {
        diagnostics.push(diagnostic(
            UPWARD_CALLBACK,
            package,
            Some(path),
            format!("{layer} source declares a callback carrying an upper-layer type"),
            "return dependency/data records upward; do not call an upper layer through callbacks",
        ));
    }
}

fn infer_layer_from_path(manifest_path: &Path) -> Option<&str> {
    manifest_path
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .find(|component| LAYERS.contains(component))
}

fn infer_process_from_path(manifest_path: &Path) -> Option<&str> {
    let components: Vec<_> = manifest_path
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect();
    components
        .windows(2)
        .find(|pair| LAYERS.contains(&pair[1]) || pair[1] == "bin")
        .map(|pair| pair[0])
}

fn layer_rank(layer: &str) -> Option<u8> {
    LAYERS
        .iter()
        .position(|candidate| *candidate == layer)
        .map(|position| position as u8)
}

fn contains_cross_layer_reference(value: &str) -> bool {
    Regex::new(
        r"(?i)(?:^|[^a-z0-9])(?:[a-z0-9]+_)?(?:service|logic|data|dependency|[a-z0-9]+_protocol)::",
    )
    .expect("cross-layer reference regex must compile")
    .is_match(value)
}

fn strip_line_comments(source: &str) -> String {
    source
        .lines()
        .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
}

fn diagnostic(
    code: &'static str,
    package: &Package,
    path: Option<&Path>,
    message: impl Into<String>,
    help: impl Into<String>,
) -> Diagnostic {
    Diagnostic {
        code,
        package: package.name.clone(),
        path: path.map(Path::to_owned),
        message: message.into(),
        help: help.into(),
    }
}

pub fn cargo_architecture_command(manifest_path: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .arg("run")
        .arg("-p")
        .arg("xtask")
        .arg("--")
        .arg("architecture")
        .arg("--manifest-path")
        .arg(manifest_path);
    command
}
