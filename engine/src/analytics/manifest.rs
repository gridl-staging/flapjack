use super::schema::rollup_schema_version_u32;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

const TIER_PREFERENCE: &[&str] = &["1day", "1hour", "5min"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowStatus {
    Closed,
    Pending,
    Recomputing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowEntry {
    pub start_ms: i64,
    pub end_ms: i64,
    pub status: WindowStatus,
    pub event_count: i64,
    pub file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateState {
    pub windows: Vec<WindowEntry>,
    pub complete: bool,
    pub total_event_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierState {
    pub dates: HashMap<String, DateState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollupManifest {
    pub schema_version: u32,
    pub index: String,
    pub tiers: HashMap<String, TierState>,
}

impl RollupManifest {
    pub fn new(index_name: &str) -> Self {
        Self {
            schema_version: rollup_schema_version_u32(),
            index: index_name.to_string(),
            tiers: HashMap::new(),
        }
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        match fs::read_to_string(path) {
            Ok(content) => serde_json::from_str(&content)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let index_name = path
                    .parent()
                    .and_then(|p| p.parent())
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");
                Ok(Self::new(index_name))
            }
            Err(e) => Err(e),
        }
    }

    pub fn save(&self, path: &Path) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = path.with_extension("json.tmp");
        fs::write(&tmp_path, json)?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    pub fn has_complete_coverage(&self, date: &str, tier: &str) -> bool {
        let Some(tier_state) = self.tiers.get(tier) else {
            return false;
        };
        let Some(date_state) = tier_state.dates.get(date) else {
            return false;
        };
        if !date_state.complete || date_state.windows.is_empty() {
            return false;
        }
        date_state
            .windows
            .iter()
            .all(|w| w.status == WindowStatus::Closed)
    }

    pub fn has_certified_coverage(&self, date: &str, tier: &str) -> bool {
        if !self.has_complete_coverage(date, tier) {
            return false;
        }
        let date_state = &self.tiers[tier].dates[date];
        let window_sum: i64 = date_state.windows.iter().map(|w| w.event_count).sum();
        date_state.total_event_count == window_sum
    }

    pub fn best_tier_for_date(&self, date: &str) -> &str {
        for tier in TIER_PREFERENCE {
            if self.has_complete_coverage(date, tier) {
                return tier;
            }
        }
        "raw"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::schema::rollup_schema_version_u32;

    fn sample_manifest() -> RollupManifest {
        let mut m = RollupManifest::new("products");
        let mut tier = TierState {
            dates: HashMap::new(),
        };
        tier.dates.insert(
            "2026-04-10".to_string(),
            DateState {
                windows: vec![
                    WindowEntry {
                        start_ms: 1712707200000,
                        end_ms: 1712710800000,
                        status: WindowStatus::Closed,
                        event_count: 100,
                        file: "rollup_1hour_1712707200000_0.parquet".to_string(),
                    },
                    WindowEntry {
                        start_ms: 1712710800000,
                        end_ms: 1712714400000,
                        status: WindowStatus::Closed,
                        event_count: 150,
                        file: "rollup_1hour_1712710800000_0.parquet".to_string(),
                    },
                ],
                complete: true,
                total_event_count: 250,
            },
        );
        m.tiers.insert("1hour".to_string(), tier);
        m
    }

    // ── Constructor ─────────────────────────────────────────────────────

    #[test]
    fn new_uses_shared_rollup_schema_version() {
        let m = RollupManifest::new("idx");
        assert_eq!(m.schema_version, rollup_schema_version_u32());
    }

    #[test]
    fn new_sets_index_name() {
        let m = RollupManifest::new("my_index");
        assert_eq!(m.index, "my_index");
    }

    #[test]
    fn new_has_empty_tiers() {
        let m = RollupManifest::new("idx");
        assert!(m.tiers.is_empty());
    }

    // ── Round-trip persistence ──────────────────────────────────────────

    #[test]
    fn round_trip_save_load() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir
            .path()
            .join("products")
            .join("rollups")
            .join("manifest.json");
        let original = sample_manifest();
        original.save(&path).unwrap();

        let loaded = RollupManifest::load(&path).unwrap();
        assert_eq!(loaded.schema_version, original.schema_version);
        assert_eq!(loaded.index, original.index);
        assert_eq!(loaded.tiers.len(), original.tiers.len());

        let orig_date = &original.tiers["1hour"].dates["2026-04-10"];
        let loaded_date = &loaded.tiers["1hour"].dates["2026-04-10"];
        assert_eq!(loaded_date.windows.len(), orig_date.windows.len());
        assert_eq!(loaded_date.total_event_count, orig_date.total_event_count);
        assert_eq!(loaded_date.complete, orig_date.complete);
        assert_eq!(
            loaded_date.windows[0].start_ms,
            orig_date.windows[0].start_ms
        );
        assert_eq!(loaded_date.windows[0].status, WindowStatus::Closed);
        assert_eq!(loaded_date.windows[0].file, orig_date.windows[0].file);
    }

    #[test]
    fn load_returns_default_for_missing_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir
            .path()
            .join("test_idx")
            .join("rollups")
            .join("manifest.json");
        let m = RollupManifest::load(&path).unwrap();
        assert_eq!(m.schema_version, 1);
        assert_eq!(m.index, "test_idx");
        assert!(m.tiers.is_empty());
    }

    #[test]
    fn atomic_save_creates_final_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        let m = sample_manifest();
        m.save(&path).unwrap();
        assert!(path.exists());
        let tmp = path.with_extension("json.tmp");
        assert!(!tmp.exists());
    }

    // ── JSON format ─────────────────────────────────────────────────────

    #[test]
    fn window_status_serializes_lowercase() {
        let json = serde_json::to_string(&WindowStatus::Closed).unwrap();
        assert_eq!(json, "\"closed\"");
        let json = serde_json::to_string(&WindowStatus::Pending).unwrap();
        assert_eq!(json, "\"pending\"");
        let json = serde_json::to_string(&WindowStatus::Recomputing).unwrap();
        assert_eq!(json, "\"recomputing\"");
    }

    #[test]
    fn window_status_deserializes_lowercase() {
        let s: WindowStatus = serde_json::from_str("\"closed\"").unwrap();
        assert_eq!(s, WindowStatus::Closed);
        let s: WindowStatus = serde_json::from_str("\"pending\"").unwrap();
        assert_eq!(s, WindowStatus::Pending);
        let s: WindowStatus = serde_json::from_str("\"recomputing\"").unwrap();
        assert_eq!(s, WindowStatus::Recomputing);
    }

    // ── has_complete_coverage ───────────────────────────────────────────

    #[test]
    fn complete_coverage_true_when_all_closed() {
        let m = sample_manifest();
        assert!(m.has_complete_coverage("2026-04-10", "1hour"));
    }

    #[test]
    fn complete_coverage_false_when_any_pending() {
        let mut m = sample_manifest();
        m.tiers
            .get_mut("1hour")
            .unwrap()
            .dates
            .get_mut("2026-04-10")
            .unwrap()
            .windows[1]
            .status = WindowStatus::Pending;
        assert!(!m.has_complete_coverage("2026-04-10", "1hour"));
    }

    #[test]
    fn complete_coverage_false_when_any_recomputing() {
        let mut m = sample_manifest();
        m.tiers
            .get_mut("1hour")
            .unwrap()
            .dates
            .get_mut("2026-04-10")
            .unwrap()
            .windows[0]
            .status = WindowStatus::Recomputing;
        assert!(!m.has_complete_coverage("2026-04-10", "1hour"));
    }

    #[test]
    fn complete_coverage_false_for_missing_tier() {
        let m = sample_manifest();
        assert!(!m.has_complete_coverage("2026-04-10", "1day"));
    }

    #[test]
    fn complete_coverage_false_for_missing_date() {
        let m = sample_manifest();
        assert!(!m.has_complete_coverage("2026-04-11", "1hour"));
    }

    #[test]
    fn complete_coverage_false_for_empty_windows() {
        let mut m = sample_manifest();
        m.tiers
            .get_mut("1hour")
            .unwrap()
            .dates
            .get_mut("2026-04-10")
            .unwrap()
            .windows
            .clear();
        assert!(!m.has_complete_coverage("2026-04-10", "1hour"));
    }

    #[test]
    fn complete_coverage_false_when_date_marked_incomplete() {
        let mut m = sample_manifest();
        m.tiers
            .get_mut("1hour")
            .unwrap()
            .dates
            .get_mut("2026-04-10")
            .unwrap()
            .complete = false;
        assert!(!m.has_complete_coverage("2026-04-10", "1hour"));
    }

    // ── has_certified_coverage ──────────────────────────────────────────

    #[test]
    fn certified_coverage_true_when_complete_and_counts_match() {
        let m = sample_manifest();
        assert!(m.has_certified_coverage("2026-04-10", "1hour"));
    }

    #[test]
    fn certified_coverage_false_when_counts_dont_sum() {
        let mut m = sample_manifest();
        m.tiers
            .get_mut("1hour")
            .unwrap()
            .dates
            .get_mut("2026-04-10")
            .unwrap()
            .total_event_count = 999;
        assert!(!m.has_certified_coverage("2026-04-10", "1hour"));
    }

    #[test]
    fn certified_coverage_false_when_not_complete() {
        let mut m = sample_manifest();
        m.tiers
            .get_mut("1hour")
            .unwrap()
            .dates
            .get_mut("2026-04-10")
            .unwrap()
            .windows[0]
            .status = WindowStatus::Pending;
        assert!(!m.has_certified_coverage("2026-04-10", "1hour"));
    }

    #[test]
    fn certified_coverage_false_when_date_marked_incomplete() {
        let mut m = sample_manifest();
        m.tiers
            .get_mut("1hour")
            .unwrap()
            .dates
            .get_mut("2026-04-10")
            .unwrap()
            .complete = false;
        assert!(!m.has_certified_coverage("2026-04-10", "1hour"));
    }

    // ── best_tier_for_date ──────────────────────────────────────────────

    #[test]
    fn best_tier_returns_raw_when_no_coverage() {
        let m = RollupManifest::new("idx");
        assert_eq!(m.best_tier_for_date("2026-04-10"), "raw");
    }

    #[test]
    fn best_tier_returns_1day_over_1hour() {
        let mut m = sample_manifest();
        let mut day_tier = TierState {
            dates: HashMap::new(),
        };
        day_tier.dates.insert(
            "2026-04-10".to_string(),
            DateState {
                windows: vec![WindowEntry {
                    start_ms: 1712707200000,
                    end_ms: 1712793600000,
                    status: WindowStatus::Closed,
                    event_count: 250,
                    file: "rollup_1day_1712707200000_0.parquet".to_string(),
                }],
                complete: true,
                total_event_count: 250,
            },
        );
        m.tiers.insert("1day".to_string(), day_tier);
        assert_eq!(m.best_tier_for_date("2026-04-10"), "1day");
    }

    #[test]
    fn best_tier_returns_1hour_when_no_1day() {
        let m = sample_manifest();
        assert_eq!(m.best_tier_for_date("2026-04-10"), "1hour");
    }

    #[test]
    fn best_tier_returns_5min_when_no_higher() {
        let mut m = RollupManifest::new("idx");
        let mut tier = TierState {
            dates: HashMap::new(),
        };
        tier.dates.insert(
            "2026-04-10".to_string(),
            DateState {
                windows: vec![WindowEntry {
                    start_ms: 1712707200000,
                    end_ms: 1712707500000,
                    status: WindowStatus::Closed,
                    event_count: 10,
                    file: "rollup_5min_1712707200000_0.parquet".to_string(),
                }],
                complete: true,
                total_event_count: 10,
            },
        );
        m.tiers.insert("5min".to_string(), tier);
        assert_eq!(m.best_tier_for_date("2026-04-10"), "5min");
    }

    #[test]
    fn best_tier_skips_incomplete_higher_tier() {
        let mut m = sample_manifest();
        let mut day_tier = TierState {
            dates: HashMap::new(),
        };
        day_tier.dates.insert(
            "2026-04-10".to_string(),
            DateState {
                windows: vec![WindowEntry {
                    start_ms: 1712707200000,
                    end_ms: 1712793600000,
                    status: WindowStatus::Pending,
                    event_count: 0,
                    file: "".to_string(),
                }],
                complete: false,
                total_event_count: 0,
            },
        );
        m.tiers.insert("1day".to_string(), day_tier);
        assert_eq!(m.best_tier_for_date("2026-04-10"), "1hour");
    }

    #[test]
    fn best_tier_skips_closed_tier_when_date_marked_incomplete() {
        let mut m = sample_manifest();
        let mut day_tier = TierState {
            dates: HashMap::new(),
        };
        day_tier.dates.insert(
            "2026-04-10".to_string(),
            DateState {
                windows: vec![WindowEntry {
                    start_ms: 1712707200000,
                    end_ms: 1712793600000,
                    status: WindowStatus::Closed,
                    event_count: 250,
                    file: "rollup_1day_1712707200000_0.parquet".to_string(),
                }],
                complete: false,
                total_event_count: 250,
            },
        );
        m.tiers.insert("1day".to_string(), day_tier);
        assert_eq!(m.best_tier_for_date("2026-04-10"), "1hour");
    }
}
