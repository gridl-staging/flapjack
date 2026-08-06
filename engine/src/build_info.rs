// The collector and digest policy below are imported directly by build.rs; they are
// intentionally unused by non-test runtime library builds.
#![cfg_attr(not(test), allow(dead_code))]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

pub const BUILD_INFO_SCHEMA_VERSION: u8 = 1;
const EMBEDDED_BUILD_INFO_JSON_CAPACITY: usize = 16 * 1024;
const BUILD_INFO_JSON_BEGIN_MARKER: &[u8] = b"FLAPJACK_BUILD_INFO_JSON_BEGIN\n";
const BUILD_INFO_JSON_END_MARKER: &[u8] = b"\nFLAPJACK_BUILD_INFO_JSON_END\n";

#[repr(C)]
struct EmbeddedBuildInfoJson {
    bytes: [u8; EMBEDDED_BUILD_INFO_JSON_CAPACITY],
}

#[used]
static FLAPJACK_BUILD_INFO_JSON_EMBED: EmbeddedBuildInfoJson =
    embedded_build_info_json(option_env!("FLAPJACK_INTERNAL_BUILD_INFO_JSON"));

pub(crate) const WORKSPACE_DIGEST_FILES: &[&str] = &[
    "Cargo.lock",
    "Cargo.toml",
    "build.rs",
    "flapjack-http/Cargo.toml",
    "flapjack-http/build.rs",
    "flapjack-replication/Cargo.toml",
    "flapjack-server/Cargo.toml",
    "flapjack-ssl/Cargo.toml",
];

pub(crate) const WORKSPACE_DIGEST_RUST_DIRS: &[&str] = &[
    "src",
    "flapjack-http/src",
    "flapjack-replication/src",
    "flapjack-server/src",
    "flapjack-ssl/src",
];

pub(crate) const CORE_FEATURES: &[(&str, &str)] = &[
    ("CARGO_FEATURE_ANALYTICS", "analytics"),
    ("CARGO_FEATURE_AXUM_SUPPORT", "axum-support"),
    ("CARGO_FEATURE_DECOMPOUND", "decompound"),
    ("CARGO_FEATURE_MEMORY_STATS", "memory-stats"),
    ("CARGO_FEATURE_OPENAPI", "openapi"),
    ("CARGO_FEATURE_S3_SNAPSHOTS", "s3-snapshots"),
    ("CARGO_FEATURE_VECTOR_SEARCH", "vector-search"),
    ("CARGO_FEATURE_VECTOR_SEARCH_LOCAL", "vector-search-local"),
];

const WORKSPACE_DIGEST_EXCLUDED_PATH_PREFIXES: &[&str] = &[
    ".cache",
    ".git",
    ".idea",
    ".vscode",
    "dashboard/dist",
    "node_modules",
    "src/__pycache__",
    "target",
];

const WORKSPACE_DIGEST_EXCLUDED_FILE_NAMES: &[&str] = &[".DS_Store"];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "camelCase")]
pub struct BuildCapabilities {
    pub vector_search: bool,
    pub vector_search_local: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "camelCase")]
pub struct BuildInfo {
    pub schema_version: u8,
    pub version: String,
    #[cfg_attr(feature = "openapi", schema(required))]
    pub revision: Option<String>,
    pub revision_known: bool,
    #[cfg_attr(feature = "openapi", schema(required))]
    pub dirty: Option<bool>,
    pub dirty_known: bool,
    pub workspace_digest: String,
    pub profile: String,
    pub target: String,
    pub features: Vec<String>,
    pub capabilities: BuildCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawBuildInputs {
    pub revision: Option<String>,
    pub dirty: Option<bool>,
    pub workspace_digest: String,
    pub profile: String,
    pub target: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct VcsState {
    pub revision: Option<String>,
    pub dirty: Option<bool>,
}

/// Returns the canonical build identity for this compiled core crate.
pub fn build_info() -> &'static BuildInfo {
    static BUILD_INFO: OnceLock<BuildInfo> = OnceLock::new();
    BUILD_INFO.get_or_init(|| {
        build_info_from_inputs(
            env!("CARGO_PKG_VERSION"),
            RawBuildInputs {
                revision: non_empty(option_env!("FLAPJACK_INTERNAL_BUILD_REVISION")),
                dirty: parse_dirty(option_env!("FLAPJACK_INTERNAL_BUILD_DIRTY")),
                workspace_digest: option_env!("FLAPJACK_INTERNAL_WORKSPACE_DIGEST")
                    .unwrap_or_default()
                    .to_owned(),
                profile: option_env!("FLAPJACK_INTERNAL_BUILD_PROFILE")
                    .unwrap_or_default()
                    .to_owned(),
                target: option_env!("FLAPJACK_INTERNAL_BUILD_TARGET")
                    .unwrap_or_default()
                    .to_owned(),
            },
            enabled_core_features(),
        )
    })
}

pub fn canonical_build_info_json(info: &BuildInfo) -> serde_json::Result<String> {
    serde_json::to_string(info)
}

pub fn embedded_build_info_json_from_bytes(bytes: &[u8]) -> Result<String, String> {
    let (json_start, json_end) = embedded_build_info_json_range(bytes)?;
    let json_bytes = &bytes[json_start..json_end];
    let json = std::str::from_utf8(json_bytes)
        .map_err(|error| format!("embedded build-info JSON is not UTF-8: {error}"))?;
    serde_json::from_str::<BuildInfo>(json)
        .map_err(|error| format!("embedded build-info JSON is malformed: {error}"))?;
    Ok(json.to_owned())
}

pub(crate) fn build_info_from_inputs<I, S>(
    version: &str,
    raw: RawBuildInputs,
    features: I,
) -> BuildInfo
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let features = normalized_features(features);
    let vector_search_local = features
        .binary_search_by_key(&"vector-search-local", String::as_str)
        .is_ok();
    let vector_search = vector_search_local
        || features
            .binary_search_by_key(&"vector-search", String::as_str)
            .is_ok();

    BuildInfo {
        schema_version: BUILD_INFO_SCHEMA_VERSION,
        version: version.to_owned(),
        revision_known: raw.revision.is_some(),
        revision: raw.revision,
        dirty_known: raw.dirty.is_some(),
        dirty: raw.dirty,
        workspace_digest: raw.workspace_digest,
        profile: raw.profile,
        target: raw.target,
        features,
        capabilities: BuildCapabilities {
            vector_search,
            vector_search_local,
        },
    }
}

pub(crate) fn collect_vcs_inputs<F>(
    revision_override: Option<&str>,
    discover: F,
) -> Result<VcsState, String>
where
    F: FnOnce() -> VcsState,
{
    match revision_override {
        Some(revision) => Ok(VcsState {
            revision: Some(validate_revision_override(revision)?),
            dirty: None,
        }),
        None => Ok(discover()),
    }
}

pub(crate) fn discover_vcs<F>(mut run_git: F) -> VcsState
where
    F: FnMut(&[&str]) -> Result<String, String>,
{
    let revision = run_git(&["rev-parse", "HEAD"])
        .ok()
        .and_then(|output| normalize_revision(&output));
    let dirty = run_git(&["status", "--porcelain"])
        .ok()
        .map(|output| !output.trim().is_empty());
    VcsState { revision, dirty }
}

pub(crate) fn vcs_invalidation_paths<F>(mut run_git: F) -> Vec<PathBuf>
where
    F: FnMut(&[&str]) -> Result<String, String>,
{
    let mut paths = Vec::new();
    if let Ok(head) = run_git(&["rev-parse", "--git-path", "HEAD"]) {
        push_non_empty_path(&mut paths, head);
    }
    if let Ok(symbolic_ref) = run_git(&["symbolic-ref", "-q", "HEAD"]) {
        let symbolic_ref = symbolic_ref.trim();
        if !symbolic_ref.is_empty() {
            if let Ok(ref_path) = run_git(&["rev-parse", "--git-path", symbolic_ref]) {
                push_non_empty_path(&mut paths, ref_path);
            }
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

fn push_non_empty_path(paths: &mut Vec<PathBuf>, path: String) {
    let trimmed = path.trim();
    if !trimmed.is_empty() {
        paths.push(PathBuf::from(trimmed));
    }
}

pub(crate) fn workspace_digest(workspace_root: &Path) -> io::Result<String> {
    let included_paths = workspace_digest_paths(workspace_root)?;
    let mut hasher = Sha256::new();
    for relative_path in included_paths {
        let bytes = fs::read(workspace_root.join(&relative_path))?;
        let normalized_path = relative_path_to_slashes(&relative_path)?;
        update_length_prefixed(&mut hasher, normalized_path.as_bytes());
        update_length_prefixed(&mut hasher, &bytes);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn workspace_digest_paths(workspace_root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for relative_path in WORKSPACE_DIGEST_FILES {
        let absolute_path = workspace_root.join(relative_path);
        if absolute_path.is_file() {
            paths.push(normalized_relative_path(workspace_root, &absolute_path)?);
        }
    }
    for relative_dir in WORKSPACE_DIGEST_RUST_DIRS {
        collect_rust_sources(
            workspace_root,
            &workspace_root.join(relative_dir),
            &mut paths,
        )?;
    }
    let mut normalized_paths = paths
        .into_iter()
        .map(|path| relative_path_to_slashes(&path).map(|normalized| (normalized, path)))
        .collect::<io::Result<Vec<_>>>()?;
    normalized_paths.sort_by(|left, right| left.0.cmp(&right.0));
    normalized_paths.dedup_by(|left, right| left.0 == right.0);
    Ok(normalized_paths.into_iter().map(|(_, path)| path).collect())
}

fn collect_rust_sources(
    workspace_root: &Path,
    directory: &Path,
    paths: &mut Vec<PathBuf>,
) -> io::Result<()> {
    if !directory.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_symlink() {
            continue;
        }
        let relative_path = normalized_relative_path(workspace_root, &path)?;
        if is_excluded(&relative_path) {
            continue;
        }
        if file_type.is_dir() {
            collect_rust_sources(workspace_root, &path, paths)?;
        } else if file_type.is_file() && path.extension().is_some_and(|extension| extension == "rs")
        {
            paths.push(relative_path);
        }
    }
    Ok(())
}

fn normalized_relative_path(workspace_root: &Path, path: &Path) -> io::Result<PathBuf> {
    let relative = path.strip_prefix(workspace_root).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("digest input is outside workspace root: {}", path.display()),
        )
    })?;
    if relative
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        Ok(relative.to_path_buf())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "digest input is not a normalized relative path: {}",
                relative.display()
            ),
        ))
    }
}

fn relative_path_to_slashes(path: &Path) -> io::Result<String> {
    let components = path
        .components()
        .map(|component| match component {
            Component::Normal(value) => value.to_str().map(str::to_owned).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "digest path is not valid UTF-8")
            }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "digest path must be normalized and repo-relative",
            )),
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(components.join("/"))
}

fn is_excluded(path: &Path) -> bool {
    path_is_prefixed_by_excluded_path(path) || file_name_is_excluded(path)
}

fn path_is_prefixed_by_excluded_path(path: &Path) -> bool {
    WORKSPACE_DIGEST_EXCLUDED_PATH_PREFIXES
        .iter()
        .any(|excluded_path| path.starts_with(excluded_path))
}

fn file_name_is_excluded(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| WORKSPACE_DIGEST_EXCLUDED_FILE_NAMES.contains(&name))
}

fn update_length_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn normalized_features<I, S>(features: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    features
        .into_iter()
        .map(|feature| feature.as_ref().to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(crate) fn enabled_core_features_from_env<F>(mut env_present: F) -> Vec<&'static str>
where
    F: FnMut(&str) -> bool,
{
    CORE_FEATURES
        .iter()
        .filter_map(|(env_name, feature_name)| env_present(env_name).then_some(*feature_name))
        .collect()
}

const fn embedded_build_info_json(json: Option<&str>) -> EmbeddedBuildInfoJson {
    let json = match json {
        Some(value) => value.as_bytes(),
        None => b"",
    };
    let required_len =
        BUILD_INFO_JSON_BEGIN_MARKER.len() + json.len() + BUILD_INFO_JSON_END_MARKER.len();
    if required_len > EMBEDDED_BUILD_INFO_JSON_CAPACITY {
        panic!("embedded build-info JSON exceeds static capacity");
    }

    let mut bytes = [0_u8; EMBEDDED_BUILD_INFO_JSON_CAPACITY];
    let mut offset = 0;
    offset = copy_const_bytes(&mut bytes, offset, BUILD_INFO_JSON_BEGIN_MARKER);
    offset = copy_const_bytes(&mut bytes, offset, json);
    let _ = copy_const_bytes(&mut bytes, offset, BUILD_INFO_JSON_END_MARKER);
    EmbeddedBuildInfoJson { bytes }
}

const fn copy_const_bytes(
    target: &mut [u8; EMBEDDED_BUILD_INFO_JSON_CAPACITY],
    offset: usize,
    source: &[u8],
) -> usize {
    let mut index = 0;
    while index < source.len() {
        target[offset + index] = source[index];
        index += 1;
    }
    offset + source.len()
}

fn embedded_build_info_json_range(bytes: &[u8]) -> Result<(usize, usize), String> {
    let starts = find_all_bytes(bytes, BUILD_INFO_JSON_BEGIN_MARKER);
    if starts.len() != 1 {
        return Err(format!(
            "embedded build-info JSON begin marker must appear exactly once, found {}",
            starts.len()
        ));
    }
    let ends = find_all_bytes(bytes, BUILD_INFO_JSON_END_MARKER);
    if ends.len() != 1 {
        return Err(format!(
            "embedded build-info JSON end marker must appear exactly once, found {}",
            ends.len()
        ));
    }

    let json_start = starts[0] + BUILD_INFO_JSON_BEGIN_MARKER.len();
    let json_end = ends[0];
    if json_end < json_start {
        return Err("embedded build-info JSON end marker precedes begin marker".to_owned());
    }
    Ok((json_start, json_end))
}

fn find_all_bytes(haystack: &[u8], needle: &[u8]) -> Vec<usize> {
    haystack
        .windows(needle.len())
        .enumerate()
        .filter_map(|(index, candidate)| (candidate == needle).then_some(index))
        .collect()
}

fn validate_revision_override(revision: &str) -> Result<String, String> {
    (revision.len() == 40
        && revision
            .chars()
            .all(|character| character.is_ascii_hexdigit()))
    .then(|| revision.to_ascii_lowercase())
    .ok_or_else(|| {
        format!(
            "FLAPJACK_BUILD_REVISION must be exactly 40 hexadecimal characters; received {revision:?}"
        )
    })
}

fn normalize_revision(revision: &str) -> Option<String> {
    let revision = revision.trim();
    (revision.len() == 40
        && revision
            .chars()
            .all(|character| character.is_ascii_hexdigit()))
    .then(|| revision.to_ascii_lowercase())
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value.filter(|value| !value.is_empty()).map(str::to_owned)
}

fn parse_dirty(value: Option<&str>) -> Option<bool> {
    match value {
        Some("true") => Some(true),
        Some("false") => Some(false),
        _ => None,
    }
}

fn enabled_core_features() -> Vec<&'static str> {
    enabled_core_features_from_env(|env_name| match env_name {
        "CARGO_FEATURE_ANALYTICS" => cfg!(feature = "analytics"),
        "CARGO_FEATURE_AXUM_SUPPORT" => cfg!(feature = "axum-support"),
        "CARGO_FEATURE_DECOMPOUND" => cfg!(feature = "decompound"),
        "CARGO_FEATURE_MEMORY_STATS" => cfg!(feature = "memory-stats"),
        "CARGO_FEATURE_OPENAPI" => cfg!(feature = "openapi"),
        "CARGO_FEATURE_S3_SNAPSHOTS" => cfg!(feature = "s3-snapshots"),
        "CARGO_FEATURE_VECTOR_SEARCH" => cfg!(feature = "vector-search"),
        "CARGO_FEATURE_VECTOR_SEARCH_LOCAL" => cfg!(feature = "vector-search-local"),
        _ => false,
    })
}

#[cfg(test)]
#[path = "build_info/tests.rs"]
mod tests;
