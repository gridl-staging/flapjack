use std::path::Path;

pub(crate) fn visible_tenant_dir_name(entry: &std::fs::DirEntry) -> Option<String> {
    let is_directory = entry
        .file_type()
        .map(|file_type| file_type.is_dir())
        .unwrap_or(false);
    visible_tenant_name_if_visible_directory(entry.file_name(), is_directory)
}

/// TODO: Document visible_tenant_name_if_visible_directory.
fn visible_tenant_name_if_visible_directory(
    name: std::ffi::OsString,
    is_directory: bool,
) -> Option<String> {
    if !is_directory {
        return None;
    }

    let name = name.into_string().ok()?;
    // Skip hidden dirs (`.`-prefix), underscore-prefixed infrastructure dirs
    // (`_`-prefix convention for internal storage like `_usage`), and the
    // legacy `analytics` dir (Parquet files, not a search index).
    // Probing any of these as a tenant index would break the readiness probe.
    if name.starts_with('.') || name.starts_with('_') || name == "analytics" {
        None
    } else {
        Some(name)
    }
}

pub(crate) fn visible_tenant_dir_names(data_path: &Path) -> Result<Vec<String>, std::io::Error> {
    Ok(data_path
        .read_dir()?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| visible_tenant_dir_name(&entry))
        .collect())
}

pub(crate) fn has_visible_tenant_dirs(data_path: &Path) -> Result<bool, std::io::Error> {
    Ok(data_path
        .read_dir()?
        .filter_map(|entry| entry.ok())
        .any(|entry| visible_tenant_dir_name(&entry).is_some()))
}

#[cfg(test)]
mod tests {
    use super::{
        has_visible_tenant_dirs, visible_tenant_dir_name, visible_tenant_dir_names,
        visible_tenant_name_if_visible_directory,
    };
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use tempfile::TempDir;

    #[test]
    fn visible_tenant_dir_name_returns_only_visible_directories() {
        let temp_dir = TempDir::new().unwrap();
        fs::create_dir(temp_dir.path().join("products")).unwrap();
        fs::create_dir(temp_dir.path().join(".hidden")).unwrap();
        fs::write(temp_dir.path().join("notes.txt"), "not a directory").unwrap();

        let mut names: Vec<Option<String>> = fs::read_dir(temp_dir.path())
            .unwrap()
            .map(|entry| visible_tenant_dir_name(&entry.unwrap()))
            .collect();
        names.sort();

        assert_eq!(names, vec![None, None, Some("products".to_string())]);
    }

    #[test]
    fn visible_tenant_dir_names_skip_hidden_dirs_and_files() {
        let temp_dir = TempDir::new().unwrap();
        fs::create_dir(temp_dir.path().join("products")).unwrap();
        fs::create_dir(temp_dir.path().join(".internal")).unwrap();
        fs::write(temp_dir.path().join("README.txt"), "not a tenant").unwrap();

        let tenant_dirs = visible_tenant_dir_names(temp_dir.path()).unwrap();

        assert_eq!(tenant_dirs, vec!["products".to_string()]);
    }

    #[test]
    fn visible_tenant_dir_names_skip_underscore_infrastructure_dirs() {
        // Dirs like `_usage` are created by internal server components on startup
        // and must not be probed as tenant indexes by the readiness handler.
        let temp_dir = TempDir::new().unwrap();
        fs::create_dir(temp_dir.path().join("products")).unwrap();
        fs::create_dir(temp_dir.path().join("_usage")).unwrap();
        fs::create_dir(temp_dir.path().join("analytics")).unwrap();

        let mut tenant_dirs = visible_tenant_dir_names(temp_dir.path()).unwrap();
        tenant_dirs.sort();

        assert_eq!(tenant_dirs, vec!["products".to_string()]);
    }

    #[test]
    fn has_visible_tenant_dirs_ignores_hidden_dirs() {
        let temp_dir = TempDir::new().unwrap();
        fs::create_dir(temp_dir.path().join(".internal")).unwrap();

        assert!(
            !has_visible_tenant_dirs(temp_dir.path()).unwrap(),
            "hidden directories should not suppress startup restore"
        );

        fs::create_dir(temp_dir.path().join("products")).unwrap();

        assert!(has_visible_tenant_dirs(temp_dir.path()).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn visible_tenant_dir_name_skips_non_utf8_directories() {
        let invalid_name = std::ffi::OsString::from_vec(vec![0x66, 0x80, 0x6f]);

        assert_eq!(
            visible_tenant_name_if_visible_directory(invalid_name, true),
            None
        );
    }
}
