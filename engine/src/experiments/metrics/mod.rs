//! Per-user metrics aggregation for A/B testing experiments, reading search and click events from Parquet files and producing arm-level statistics for delta method z-tests, Welch's t-tests, and interleaving preference scoring.

mod aggregation;
mod interleaving;
#[cfg(feature = "analytics")]
mod io;
mod types;

pub use types::{ArmMetrics, ExperimentMetrics, InterleavingMetrics};

#[cfg(feature = "analytics")]
pub use io::{get_experiment_metrics, get_interleaving_metrics, get_pre_experiment_covariates};

#[cfg(test)]
use aggregation::{aggregate_experiment_metrics, compute_pre_experiment_covariates, safe_div};
#[cfg(test)]
use interleaving::{aggregate_interleaving_clicks, compute_interleaving_metrics};
#[cfg(all(test, feature = "analytics"))]
use std::path::Path;
#[cfg(test)]
use types::{EventRow, PreSearchRow, SearchRow};

#[cfg(test)]
#[path = "../metrics_tests.rs"]
mod tests;
