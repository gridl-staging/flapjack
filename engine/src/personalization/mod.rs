//! Personalization profile computation.
//!
//! Design notes:
//! - Build per-user affinities from insight events and indexed document facets.
//! - Use the strategy weights with raw score formula:
//!   raw_score = interaction_count * event_score * facet_score.
//! - Only include events from the last 90 days.
//! - Normalize per user so the strongest affinity is exactly 20 and all others
//!   are scaled relative to that strongest signal.

pub mod profile;

pub use profile::{
    extract_facet_values, ComputedProfile, EventScoring, FacetScoring, PersonalizationProfile,
    PersonalizationProfileStore, PersonalizationStrategy, ResolvedInsightEvent, STRATEGY_FILENAME,
};
