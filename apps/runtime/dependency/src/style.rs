//! Filesystem discovery and persistent cache support for session styles.

use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use thiserror::Error;
use uuid::Uuid;

const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_CACHE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_DISCOVERED_FILES: usize = 1024;

/// Source root that supplied a style manifest.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DependencyStyleSourceKind {
    /// Per-user style root.
    User,
    /// Project-local style root.
    Project,
    /// One activated plugin style root.
    Plugin,
}

/// On-disk manifest encoding.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DependencyStyleManifestFormat {
    /// TOML manifest.
    Toml,
    /// JSON manifest.
    Json,
}

/// Explicit optional roots used for a bounded discovery pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DependencyStyleDiscoveryRequest {
    /// Optional user styles directory.
    pub user_root: Option<PathBuf>,
    /// Optional project styles directory.
    pub project_root: Option<PathBuf>,
    /// Activated plugin style directories.
    pub plugin_roots: Vec<PathBuf>,
}

/// Uninterpreted manifest content found below an explicit root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleManifestRecord {
    /// Stable source location.
    pub source_locator: String,
    /// Root category that supplied the file.
    pub source_kind: DependencyStyleSourceKind,
    /// Input encoding.
    pub format: DependencyStyleManifestFormat,
    /// Exact input byte count.
    pub bytes: u64,
    /// UTF-8 manifest text. No manifest interpretation is performed here.
    pub contents: String,
}

/// A style-id disable marker found without parsing any manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleDisabledMarker {
    /// Marker filename without the `.disabled` suffix.
    pub style_id: String,
    /// Root category that supplied the marker.
    pub source_kind: DependencyStyleSourceKind,
    /// Stable marker location.
    pub source_locator: String,
}

/// Non-fatal discovery failure for one root entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleDiscoveryError {
    /// Stable location where available.
    pub source_locator: String,
    /// Stable machine-readable reason.
    pub code: &'static str,
    /// Safe diagnostic text.
    pub message: String,
}

/// Stable result of a bounded discovery pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DependencyStyleDiscovery {
    /// Eligible manifest files, sorted by kind and locator.
    pub manifests: Vec<DependencyStyleManifestRecord>,
    /// Disable marker names, sorted by kind and locator.
    pub disabled_markers: Vec<DependencyStyleDisabledMarker>,
    /// Rejected entries and unreadable roots, sorted by locator.
    pub errors: Vec<DependencyStyleDiscoveryError>,
}

/// Dependency boundary for style root discovery and compiled-cache I/O.
pub trait SessionStyleDependencyPort {
    /// Reads eligible manifest files directly below explicitly configured roots.
    ///
    /// # Errors
    ///
    /// Returns an adapter failure if discovery cannot be performed.
    fn discover_session_styles(
        &self,
        request: DependencyStyleDiscoveryRequest,
    ) -> Result<DependencyStyleDiscovery, SessionStyleDependencyError>;

    /// Loads one dependency-owned persistent compiled cache entry.
    ///
    /// # Errors
    ///
    /// Returns an adapter failure for unsafe paths or unreadable cache files.
    fn load_session_style_cache(
        &self,
        request: DependencyStyleCacheLoadRequest,
    ) -> Result<Option<DependencyStyleCacheRecord>, SessionStyleDependencyError>;

    /// Atomically stores one dependency-owned persistent compiled cache entry.
    ///
    /// # Errors
    ///
    /// Returns an adapter failure for unsafe paths or failed filesystem writes.
    fn store_session_style_cache(
        &self,
        request: DependencyStyleCacheStoreRequest,
    ) -> Result<(), SessionStyleDependencyError>;
}

/// Explicit cache lookup request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleCacheLoadRequest {
    /// Caller-owned cache root.
    pub cache_root: PathBuf,
    /// Exact 64-character hexadecimal cache key.
    pub cache_key: String,
}

/// Persisted opaque cache entry returned to runtime data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleCacheRecord {
    /// Exact cache key used to load this entry.
    pub cache_key: String,
    /// Exact bytes loaded from disk.
    pub bytes: u64,
    /// Opaque UTF-8 JSON cache document.
    pub contents: String,
}

/// Explicit cache write request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyStyleCacheStoreRequest {
    /// Caller-owned cache root.
    pub cache_root: PathBuf,
    /// Exact 64-character hexadecimal cache key.
    pub cache_key: String,
    /// Opaque JSON cache document; capped before persistence.
    pub contents: String,
}

/// Filesystem discovery or cache failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SessionStyleDependencyError {
    /// A configured cache root was empty.
    #[error("session-style cache root is empty")]
    EmptyCacheRoot,
    /// The provided cache key was not a BLAKE3-style hexadecimal name.
    #[error("session-style cache key is invalid")]
    InvalidCacheKey,
    /// A cache document exceeded the fixed bounded limit.
    #[error("session-style cache document exceeds the fixed size limit")]
    CacheTooLarge,
    /// Cache data was not UTF-8 JSON text.
    #[error("session-style cache document is invalid UTF-8")]
    CacheNotUtf8,
    /// Filesystem operation failed.
    #[error("session-style filesystem operation failed: {0}")]
    Io(String),
}

impl SessionStyleDependencyPort for crate::LocalRuntimeDependencies {
    fn discover_session_styles(
        &self,
        request: DependencyStyleDiscoveryRequest,
    ) -> Result<DependencyStyleDiscovery, SessionStyleDependencyError> {
        Ok(discover(request))
    }

    fn load_session_style_cache(
        &self,
        request: DependencyStyleCacheLoadRequest,
    ) -> Result<Option<DependencyStyleCacheRecord>, SessionStyleDependencyError> {
        load_cache(request)
    }

    fn store_session_style_cache(
        &self,
        request: DependencyStyleCacheStoreRequest,
    ) -> Result<(), SessionStyleDependencyError> {
        store_cache(request)
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the bounded discovery pass keeps all root-entry validation in one audit-friendly filesystem boundary"
)]
fn discover(request: DependencyStyleDiscoveryRequest) -> DependencyStyleDiscovery {
    let mut roots = Vec::new();
    if let Some(root) = request.user_root {
        roots.push((DependencyStyleSourceKind::User, root));
    }
    if let Some(root) = request.project_root {
        roots.push((DependencyStyleSourceKind::Project, root));
    }
    roots.extend(
        request
            .plugin_roots
            .into_iter()
            .map(|root| (DependencyStyleSourceKind::Plugin, root)),
    );
    roots.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut result = DependencyStyleDiscovery::default();
    for (kind, root) in roots {
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                result
                    .errors
                    .push(discovery_error(&root, "root_unreadable", &error));
                continue;
            }
        };
        let mut entries = entries.filter_map(Result::ok).collect::<Vec<_>>();
        entries.sort_by_key(fs::DirEntry::path);
        for entry in entries.into_iter().take(MAX_DISCOVERED_FILES) {
            let path = entry.path();
            let locator = locator(&path);
            let metadata = match entry.metadata() {
                Ok(metadata) if metadata.is_file() => metadata,
                Ok(_) => continue,
                Err(error) => {
                    result
                        .errors
                        .push(discovery_error(&path, "metadata_unreadable", &error));
                    continue;
                }
            };
            if path.extension().and_then(|value| value.to_str()) == Some("disabled") {
                if let Some(style_id) = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .filter(|id| !id.is_empty())
                {
                    result.disabled_markers.push(DependencyStyleDisabledMarker {
                        style_id: style_id.to_owned(),
                        source_kind: kind,
                        source_locator: locator,
                    });
                }
                continue;
            }
            let Some(format) = manifest_format(&path) else {
                continue;
            };
            if metadata.len() > MAX_MANIFEST_BYTES {
                result.errors.push(DependencyStyleDiscoveryError {
                    source_locator: locator,
                    code: "manifest_too_large",
                    message: format!("manifest exceeds {MAX_MANIFEST_BYTES} bytes"),
                });
                continue;
            }
            match read_manifest(&path) {
                Ok((contents, bytes)) => result.manifests.push(DependencyStyleManifestRecord {
                    source_locator: locator,
                    source_kind: kind,
                    format,
                    bytes,
                    contents,
                }),
                Err(ManifestReadError::TooLarge) => {
                    result.errors.push(DependencyStyleDiscoveryError {
                        source_locator: locator,
                        code: "manifest_too_large",
                        message: format!("manifest exceeds {MAX_MANIFEST_BYTES} bytes"),
                    });
                }
                Err(ManifestReadError::Io(error)) => {
                    result
                        .errors
                        .push(discovery_error(&path, "manifest_unreadable", &error));
                }
                Err(ManifestReadError::NotUtf8) => {
                    result.errors.push(DependencyStyleDiscoveryError {
                        source_locator: locator,
                        code: "manifest_not_utf8",
                        message: "manifest is not valid UTF-8".into(),
                    });
                }
            }
        }
    }
    result.manifests.sort_by(|left, right| {
        left.source_kind
            .cmp(&right.source_kind)
            .then_with(|| left.source_locator.cmp(&right.source_locator))
    });
    result.disabled_markers.sort_by(|left, right| {
        left.source_kind
            .cmp(&right.source_kind)
            .then_with(|| left.source_locator.cmp(&right.source_locator))
    });
    result.errors.sort_by(|left, right| {
        left.source_locator
            .cmp(&right.source_locator)
            .then_with(|| left.code.cmp(right.code))
    });
    result
}

fn load_cache(
    request: DependencyStyleCacheLoadRequest,
) -> Result<Option<DependencyStyleCacheRecord>, SessionStyleDependencyError> {
    let path = cache_path(&request.cache_root, &request.cache_key)?;
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(SessionStyleDependencyError::Io(error.to_string())),
    };
    if !metadata.is_file() || metadata.len() > MAX_CACHE_BYTES {
        return Err(SessionStyleDependencyError::CacheTooLarge);
    }
    let contents = fs::read_to_string(path).map_err(|error| match error.kind() {
        std::io::ErrorKind::InvalidData => SessionStyleDependencyError::CacheNotUtf8,
        _ => SessionStyleDependencyError::Io(error.to_string()),
    })?;
    Ok(Some(DependencyStyleCacheRecord {
        cache_key: request.cache_key,
        bytes: metadata.len(),
        contents,
    }))
}

fn store_cache(
    request: DependencyStyleCacheStoreRequest,
) -> Result<(), SessionStyleDependencyError> {
    let DependencyStyleCacheStoreRequest {
        cache_root,
        cache_key,
        contents,
    } = request;
    if contents.len() as u64 > MAX_CACHE_BYTES {
        return Err(SessionStyleDependencyError::CacheTooLarge);
    }
    let destination = cache_path(&cache_root, &cache_key)?;
    fs::create_dir_all(&cache_root)
        .map_err(|error| SessionStyleDependencyError::Io(error.to_string()))?;
    let temporary = cache_root.join(format!(".{}.{}.tmp", cache_key, Uuid::now_v7()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|error| SessionStyleDependencyError::Io(error.to_string()))?;
    file.write_all(contents.as_bytes())
        .map_err(|error| SessionStyleDependencyError::Io(error.to_string()))?;
    file.sync_all()
        .map_err(|error| SessionStyleDependencyError::Io(error.to_string()))?;
    drop(file);
    fs::rename(&temporary, &destination)
        .map_err(|error| SessionStyleDependencyError::Io(error.to_string()))
}

fn cache_path(root: &Path, key: &str) -> Result<PathBuf, SessionStyleDependencyError> {
    if root.as_os_str().is_empty() {
        return Err(SessionStyleDependencyError::EmptyCacheRoot);
    }
    if key.len() != 64 || !key.bytes().all(|value| value.is_ascii_hexdigit()) {
        return Err(SessionStyleDependencyError::InvalidCacheKey);
    }
    Ok(root.join(format!("{key}.json")))
}

fn manifest_format(path: &Path) -> Option<DependencyStyleManifestFormat> {
    match path.extension().and_then(|value| value.to_str()) {
        Some("toml") => Some(DependencyStyleManifestFormat::Toml),
        Some("json") => Some(DependencyStyleManifestFormat::Json),
        _ => None,
    }
}

enum ManifestReadError {
    TooLarge,
    NotUtf8,
    Io(std::io::Error),
}

fn read_manifest(path: &Path) -> Result<(String, u64), ManifestReadError> {
    let file = fs::File::open(path).map_err(ManifestReadError::Io)?;
    let mut bytes = Vec::new();
    file.take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(ManifestReadError::Io)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(ManifestReadError::TooLarge);
    }
    let byte_count = bytes.len() as u64;
    let contents = String::from_utf8(bytes).map_err(|_| ManifestReadError::NotUtf8)?;
    Ok((contents, byte_count))
}

fn locator(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn discovery_error(
    path: &Path,
    code: &'static str,
    error: &std::io::Error,
) -> DependencyStyleDiscoveryError {
    DependencyStyleDiscoveryError {
        source_locator: locator(path),
        code,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_filters_and_sorts_without_parsing() {
        let root = tempfile::tempdir().expect("root");
        fs::write(root.path().join("z.toml"), "z").expect("toml");
        fs::write(root.path().join("a.json"), "a").expect("json");
        fs::write(root.path().join("skip.txt"), "skip").expect("text");
        fs::write(root.path().join("custom.disabled"), "marker").expect("marker");
        let found = crate::LocalRuntimeDependencies
            .discover_session_styles(DependencyStyleDiscoveryRequest {
                user_root: Some(root.path().to_owned()),
                ..Default::default()
            })
            .expect("discover");
        assert_eq!(
            found
                .manifests
                .iter()
                .map(|record| record
                    .source_locator
                    .rsplit(['\\', '/'])
                    .next()
                    .expect("name"))
                .collect::<Vec<_>>(),
            vec!["a.json", "z.toml"]
        );
        assert_eq!(found.disabled_markers[0].style_id, "custom");
    }

    #[test]
    fn discovery_rejects_oversized_manifest() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("large.json");
        let file = fs::File::create(&path).expect("file");
        file.set_len(MAX_MANIFEST_BYTES + 1).expect("large file");
        let found = crate::LocalRuntimeDependencies
            .discover_session_styles(DependencyStyleDiscoveryRequest {
                user_root: Some(root.path().to_owned()),
                ..Default::default()
            })
            .expect("discover");
        assert!(found.manifests.is_empty());
        assert_eq!(found.errors[0].code, "manifest_too_large");
    }

    #[test]
    fn cache_round_trip_and_corruption_are_bounded() {
        let root = tempfile::tempdir().expect("root");
        let key = "a".repeat(64);
        crate::LocalRuntimeDependencies
            .store_session_style_cache(DependencyStyleCacheStoreRequest {
                cache_root: root.path().to_owned(),
                cache_key: key.clone(),
                contents: "{\"value\":1}".into(),
            })
            .expect("store");
        assert_eq!(
            crate::LocalRuntimeDependencies
                .load_session_style_cache(DependencyStyleCacheLoadRequest {
                    cache_root: root.path().to_owned(),
                    cache_key: key.clone()
                })
                .expect("load")
                .expect("entry")
                .contents,
            "{\"value\":1}"
        );
        fs::write(root.path().join(format!("{key}.json")), vec![0xff]).expect("corrupt");
        assert_eq!(
            crate::LocalRuntimeDependencies.load_session_style_cache(
                DependencyStyleCacheLoadRequest {
                    cache_root: root.path().to_owned(),
                    cache_key: key
                }
            ),
            Err(SessionStyleDependencyError::CacheNotUtf8)
        );
    }
}
