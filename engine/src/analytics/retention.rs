use std::path::Path;

const PARTITION_PREFIX: &str = "date=";
const PARTITION_DATE_FORMAT: &str = "%Y-%m-%d";
const RETENTION_INTERVAL_SECONDS: u64 = 86_400;

/// Delete Parquet partition directories older than the configured retention period.
///
/// Walks the analytics directory looking for `date=YYYY-MM-DD/` directories
/// and removes any that are older than `retention_days`.
pub fn cleanup_old_partitions(analytics_dir: &Path, retention_days: u32) -> Result<usize, String> {
    if !analytics_dir.exists() {
        return Ok(0);
    }

    let cutoff = cutoff_date(retention_days);
    let mut removed = 0;

    for index_dir in read_root_subdirectories(analytics_dir)? {
        for event_type_dir in read_child_subdirectories(&index_dir) {
            for partition_dir in read_child_subdirectories(&event_type_dir) {
                if remove_partition_if_expired(&partition_dir, cutoff) {
                    removed += 1;
                }
            }
        }
    }

    Ok(removed)
}

fn cutoff_date(retention_days: u32) -> chrono::NaiveDate {
    chrono::Utc::now().date_naive() - chrono::Duration::days(retention_days as i64)
}

fn read_root_subdirectories(path: &Path) -> Result<Vec<std::path::PathBuf>, String> {
    let entries = std::fs::read_dir(path).map_err(|e| format!("read_dir error: {}", e))?;
    Ok(filter_directory_entries(entries))
}

fn read_child_subdirectories(path: &Path) -> Vec<std::path::PathBuf> {
    match std::fs::read_dir(path) {
        Ok(entries) => filter_directory_entries(entries),
        Err(_) => Vec::new(),
    }
}

fn filter_directory_entries(entries: std::fs::ReadDir) -> Vec<std::path::PathBuf> {
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect()
}

fn partition_date_from_name(name: &str) -> Option<chrono::NaiveDate> {
    let date_str = name.strip_prefix(PARTITION_PREFIX)?;
    chrono::NaiveDate::parse_from_str(date_str, PARTITION_DATE_FORMAT).ok()
}

/// TODO: Document remove_partition_if_expired.
fn remove_partition_if_expired(partition_dir: &Path, cutoff: chrono::NaiveDate) -> bool {
    let partition_name = partition_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let Some(partition_date) = partition_date_from_name(partition_name) else {
        return false;
    };
    if partition_date >= cutoff {
        return false;
    }

    match std::fs::remove_dir_all(partition_dir) {
        Ok(()) => {
            tracing::info!(
                "[analytics] Removed old partition: {}",
                partition_dir.display()
            );
            true
        }
        Err(error) => {
            tracing::warn!(
                "[analytics] Failed to remove old partition {}: {}",
                partition_dir.display(),
                error
            );
            false
        }
    }
}

fn log_cleanup_result(phase: &str, cleanup_result: Result<usize, String>) {
    match cleanup_result {
        Ok(removed_partitions) if removed_partitions > 0 => {
            tracing::info!(
                "[analytics] {} cleanup: removed {} old partitions",
                phase,
                removed_partitions
            );
        }
        Ok(_) => {}
        Err(error) => tracing::warn!("[analytics] {} cleanup error: {}", phase, error),
    }
}

fn run_cleanup_phase(analytics_dir: &Path, retention_days: u32, phase: &str) {
    let cleanup_result = cleanup_old_partitions(analytics_dir, retention_days);
    log_cleanup_result(phase, cleanup_result);
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn partition_date_from_name_parses_valid_partition_name() {
        assert_eq!(
            partition_date_from_name("date=2020-01-01"),
            Some(chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap())
        );
    }

    #[test]
    fn partition_date_from_name_rejects_non_partition_names() {
        assert_eq!(partition_date_from_name("2020-01-01"), None);
        assert_eq!(partition_date_from_name("date=not-a-date"), None);
    }

    #[test]
    fn cleanup_nonexistent_dir_returns_zero() {
        let dir = std::env::temp_dir().join("fj_retention_test_nonexistent");
        let _ = fs::remove_dir_all(&dir); // ensure it doesn't exist
        assert_eq!(cleanup_old_partitions(&dir, 30).unwrap(), 0);
    }

    #[test]
    fn cleanup_removes_old_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        // Create: base/myindex/searches/date=2020-01-01/  (very old)
        let old_part = base.join("myindex/searches/date=2020-01-01");
        fs::create_dir_all(&old_part).unwrap();
        fs::write(old_part.join("data.parquet"), b"fake").unwrap();

        let removed = cleanup_old_partitions(base, 30).unwrap();
        assert_eq!(removed, 1);
        assert!(!old_part.exists());
    }

    #[test]
    fn cleanup_keeps_recent_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let recent_part = base.join(format!("myindex/searches/date={}", today));
        fs::create_dir_all(&recent_part).unwrap();
        fs::write(recent_part.join("data.parquet"), b"fake").unwrap();

        let removed = cleanup_old_partitions(base, 30).unwrap();
        assert_eq!(removed, 0);
        assert!(recent_part.exists());
    }

    #[test]
    fn cleanup_skips_non_date_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        let non_date = base.join("myindex/searches/not_a_date_dir");
        fs::create_dir_all(&non_date).unwrap();

        let removed = cleanup_old_partitions(base, 30).unwrap();
        assert_eq!(removed, 0);
        assert!(non_date.exists());
    }

    #[test]
    fn cleanup_handles_multiple_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        let old1 = base.join("idx_a/searches/date=2020-01-01");
        let old2 = base.join("idx_b/events/date=2020-06-15");
        fs::create_dir_all(&old1).unwrap();
        fs::create_dir_all(&old2).unwrap();

        let removed = cleanup_old_partitions(base, 30).unwrap();
        assert_eq!(removed, 2);
    }
}

/// Run retention cleanup as a background task (daily).
pub async fn run_retention_loop(analytics_dir: std::path::PathBuf, retention_days: u32) {
    run_cleanup_phase(&analytics_dir, retention_days, "Startup");

    // Then every 24 hours
    let mut interval =
        tokio::time::interval(tokio::time::Duration::from_secs(RETENTION_INTERVAL_SECONDS));
    interval.tick().await; // skip first immediate tick
    loop {
        interval.tick().await;
        run_cleanup_phase(&analytics_dir, retention_days, "Retention");
    }
}
