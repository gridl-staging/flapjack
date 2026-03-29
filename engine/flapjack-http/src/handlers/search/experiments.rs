use std::collections::HashMap;

use flapjack::experiments::{assignment, assignment::AssignmentMethod, config::QueryOverrides};

use crate::dto::SearchRequest;
use crate::handlers::AppState;

#[derive(Debug, Clone)]
pub(super) struct ExperimentContext {
    pub(super) experiment_id: String,
    pub(super) variant_id: String,
    pub(super) assignment_method: String,
    pub(super) interleaving_variant_index: Option<String>,
    pub(super) interleaved_teams: Option<HashMap<String, String>>,
}

pub(super) fn assignment_method_str(method: &AssignmentMethod) -> &'static str {
    match method {
        AssignmentMethod::UserToken => "user_token",
        AssignmentMethod::SessionId => "session_id",
        AssignmentMethod::QueryId => "query_id",
    }
}

/// Applies A/B test query overrides (typo tolerance, synonyms, rules) to a search request.
pub(super) fn apply_query_overrides(req: &mut SearchRequest, overrides: &QueryOverrides) {
    if let Some(ref typo_tolerance) = overrides.typo_tolerance {
        req.typo_tolerance = Some(typo_tolerance.clone());
    }
    if let Some(enable_synonyms) = overrides.enable_synonyms {
        req.enable_synonyms = Some(enable_synonyms);
    }
    if let Some(enable_rules) = overrides.enable_rules {
        req.enable_rules = Some(enable_rules);
    }
    if let Some(ref rule_contexts) = overrides.rule_contexts {
        req.rule_contexts = Some(rule_contexts.clone());
    }
    if let Some(ref filters) = overrides.filters {
        // AND-merge with existing filters (e.g. secured API key restrictions)
        // to avoid bypassing access controls.
        req.filters = Some(match &req.filters {
            Some(existing) => format!("({}) AND ({})", existing, filters),
            None => filters.clone(),
        });
    }
    if let Some(ref optional_filters) = overrides.optional_filters {
        req.optional_filters = Some(serde_json::Value::Array(
            optional_filters
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ));
    }
    if let Some(ref remove_words_if_no_results) = overrides.remove_words_if_no_results {
        req.remove_words_if_no_results = Some(remove_words_if_no_results.clone());
    }

    if overrides.custom_ranking.is_some() {
        tracing::debug!("skipping custom_ranking query override (index-level only)");
    }
    if overrides.attribute_weights.is_some() {
        tracing::debug!("skipping attribute_weights query override (index-level only)");
    }
}

/// Resolves the active A/B test experiment for an index: assigns the user to a variant
/// via user-token/session/query-id hashing, applies query overrides, and returns
/// the effective index name and experiment context.
pub(super) fn resolve_experiment_context(
    state: &AppState,
    index_name: &str,
    req: &mut SearchRequest,
    assignment_query_id: &str,
) -> (String, Option<ExperimentContext>) {
    let mut effective_index = index_name.to_string();
    let Some(store) = state.experiment_store.as_ref() else {
        return (effective_index, None);
    };
    let Some(experiment) = store.get_active_for_index(index_name) else {
        return (effective_index, None);
    };
    // get_active_for_index already filters for Running status
    if req.enable_ab_test == Some(false) {
        return (effective_index, None);
    }

    if experiment.interleaving == Some(true) {
        if let Some(variant_index_name) = experiment.variant.index_name {
            return (
                effective_index,
                Some(ExperimentContext {
                    experiment_id: experiment.id,
                    variant_id: "interleaved".to_string(),
                    assignment_method: "interleaved".to_string(),
                    interleaving_variant_index: Some(variant_index_name),
                    interleaved_teams: None,
                }),
            );
        }
        return (effective_index, None);
    }

    let assignment = assignment::assign_variant(
        &experiment,
        req.user_token.as_deref(),
        req.session_id.as_deref(),
        assignment_query_id,
    );
    let (variant_id, arm) = if assignment.arm == "variant" {
        ("variant", &experiment.variant)
    } else {
        ("control", &experiment.control)
    };

    if let Some(ref overrides) = arm.query_overrides {
        apply_query_overrides(req, overrides);
    }
    if let Some(ref routed_index) = arm.index_name {
        effective_index = routed_index.clone();
    }

    (
        effective_index,
        Some(ExperimentContext {
            experiment_id: experiment.id,
            variant_id: variant_id.to_string(),
            assignment_method: assignment_method_str(&assignment.method).to_string(),
            interleaving_variant_index: None,
            interleaved_teams: None,
        }),
    )
}
