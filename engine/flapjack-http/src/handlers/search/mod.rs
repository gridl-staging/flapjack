mod batch;
mod experiments;
mod geo;
mod highlight;
mod hybrid;
mod personalization;
mod pipeline;
pub mod request;
mod reranking;
mod response;
mod single;
mod single_support;
mod synonyms;
mod transforms;

// Re-export public API
pub use batch::batch_search;
pub(crate) use request::build_params_echo;
pub use request::resolve_search_mode;
pub use single::{search, search_get, search_single};

// Re-export utoipa-generated path structs for openapi.rs
#[allow(unused_imports)]
pub use batch::__path_batch_search;
#[allow(unused_imports)]
pub use single::__path_search;
#[allow(unused_imports)]
pub use single::__path_search_get;

// Private re-imports so that `super::*` from the #[cfg(test)] block below
// continues to resolve after functions moved to sub-modules.
#[cfg(test)]
use experiments::*;
#[cfg(test)]
use geo::*;
#[cfg(test)]
#[allow(unused_imports)]
use highlight::*;
#[cfg(test)]
use personalization::*;
#[cfg(test)]
use pipeline::*;
#[cfg(test)]
use request::*;
#[cfg(test)]
use reranking::*;
#[cfg(test)]
#[allow(unused_imports)]
use single::*;
#[cfg(test)]
#[allow(unused_imports)]
use synonyms::*;
// Types needed by test code that were previously imported at module scope.
#[cfg(test)]
use super::AppState;
#[cfg(test)]
use crate::dto::SearchRequest;
#[cfg(test)]
use flapjack::types::ScoredDocument;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "stage5_integration_tests.rs"]
mod stage5_integration_tests;
