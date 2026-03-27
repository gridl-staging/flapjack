use super::*;

/// Helper to build a SearchRow for tests.
fn search(user: &str, variant: &str, qid: Option<&str>, nb_hits: u32, method: &str) -> SearchRow {
    SearchRow {
        user_token: user.to_string(),
        variant_id: variant.to_string(),
        query_id: qid.map(|s| s.to_string()),
        nb_hits,
        has_results: nb_hits > 0,
        assignment_method: method.to_string(),
    }
}

/// Helper to build a click EventRow (no positions).
fn click(qid: &str) -> EventRow {
    EventRow {
        query_id: qid.to_string(),
        event_type: "click".to_string(),
        value: None,
        positions: None,
        interleaving_team: None,
    }
}

/// Helper to build a click EventRow with positions.
fn click_at(qid: &str, positions: &[u32]) -> EventRow {
    EventRow {
        query_id: qid.to_string(),
        event_type: "click".to_string(),
        value: None,
        positions: Some(serde_json::to_string(positions).unwrap()),
        interleaving_team: None,
    }
}

/// Helper to build an interleaving click EventRow with team attribution.
fn interleaving_click(qid: &str, team: &str) -> EventRow {
    EventRow {
        query_id: qid.to_string(),
        event_type: "click".to_string(),
        value: None,
        positions: None,
        interleaving_team: Some(team.to_string()),
    }
}

/// Helper to build a conversion EventRow with revenue.
fn conversion(qid: &str, value: f64) -> EventRow {
    EventRow {
        query_id: qid.to_string(),
        event_type: "conversion".to_string(),
        value: Some(value),
        positions: None,
        interleaving_team: None,
    }
}

// ── per_user_ids alignment ────────────────────────────────────

/// Verify that `per_user_ids` is index-aligned with `per_user_ctrs` so CUPED covariate matching can zip them.
#[test]
fn arm_metrics_contains_per_user_ids_aligned_with_tuples() {
    // Two users in control: alice (3 searches, 1 click), bob (2 searches, 0 clicks)
    let mut searches = Vec::new();
    let mut events = Vec::new();

    for j in 0..3 {
        let qid = format!("alice_{j}");
        searches.push(search("alice", "control", Some(&qid), 5, "user_token"));
        if j == 0 {
            events.push(click(&qid));
        }
    }
    for j in 0..2 {
        let qid = format!("bob_{j}");
        searches.push(search("bob", "control", Some(&qid), 5, "user_token"));
    }

    // Add a variant user so aggregate doesn't fail
    searches.push(search("carol", "variant", Some("carol_0"), 5, "user_token"));

    let m = aggregate_experiment_metrics(&searches, &events, None);

    // per_user_ids should have exactly 2 entries matching per_user_ctrs length
    assert_eq!(m.control.per_user_ids.len(), m.control.per_user_ctrs.len());
    assert_eq!(m.control.per_user_ids.len(), 2);

    // Find each user and verify their tuple aligns
    for (i, uid) in m.control.per_user_ids.iter().enumerate() {
        let (clicks, searches_count) = m.control.per_user_ctrs[i];
        match uid.as_str() {
            "alice" => {
                assert_eq!(clicks, 1.0);
                assert_eq!(searches_count, 3.0);
            }
            "bob" => {
                assert_eq!(clicks, 0.0);
                assert_eq!(searches_count, 2.0);
            }
            other => panic!("unexpected user_id: {}", other),
        }
    }
}

// ── CTR per arm ─────────────────────────────────────────────────

/// Verify CTR computation for uniform per-user click rates (control 1.0, variant 2.0) across 5-user arms.
#[test]
fn metrics_returns_correct_ctr_per_arm() {
    // Control: 5 users, each does 10 searches, each gets 1 click = CTR ~0.10
    // Variant: 5 users, each does 10 searches, each gets 2 clicks = CTR ~0.20
    let mut searches = Vec::new();
    let mut events = Vec::new();

    for i in 0..5 {
        for j in 0..10 {
            let qid = format!("ctrl_{i}_{j}");
            searches.push(search(
                &format!("user_ctrl_{i}"),
                "control",
                Some(&qid),
                5,
                "user_token",
            ));
            // 1 click per search for control
            events.push(click(&qid));
        }
    }

    for i in 0..5 {
        for j in 0..10 {
            let qid = format!("var_{i}_{j}");
            searches.push(search(
                &format!("user_var_{i}"),
                "variant",
                Some(&qid),
                5,
                "user_token",
            ));
            // 2 clicks per search for variant
            events.push(click(&qid));
            events.push(click(&qid));
        }
    }

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.control.searches, 50);
    assert_eq!(m.control.clicks, 50);
    assert_eq!(m.control.users, 5);
    assert!((m.control.ctr - 1.0).abs() < 0.001); // 50 clicks / 50 searches = 1.0
                                                  // Wait — each search gets 1 click, so CTR = clicks/searches = 50/50 = 1.0 raw
                                                  // But per-user: each user has 10 clicks / 10 searches = 1.0

    assert_eq!(m.variant.searches, 50);
    assert_eq!(m.variant.clicks, 100); // 2 clicks per search * 50 searches
    assert_eq!(m.variant.users, 5);
    assert!((m.variant.ctr - 2.0).abs() < 0.001); // 100/50 = 2.0
}

/// Verify CTR computation with heterogeneous per-user click counts across 10-user arms (control 0.125, variant 0.175).
#[test]
fn metrics_with_realistic_ctrs() {
    // Control: 10 users. 5 users do 20 searches each with 2 clicks, 5 do 20 with 3 clicks
    // Control total: 200 searches, 25 clicks, CTR = 25/200 = 0.125
    // Variant: 10 users. 5 do 20 searches each with 3 clicks, 5 do 20 with 4 clicks
    // Variant total: 200 searches, 35 clicks, CTR = 35/200 = 0.175
    let mut searches = Vec::new();
    let mut events = Vec::new();
    let mut qid_counter = 0u64;

    // Control arm
    for i in 0..10 {
        let clicks_per_user = if i < 5 { 2 } else { 3 };
        for j in 0..20 {
            let qid = format!("q{qid_counter}");
            qid_counter += 1;
            searches.push(search(
                &format!("ctrl_u{i}"),
                "control",
                Some(&qid),
                10,
                "user_token",
            ));
            if j < clicks_per_user {
                events.push(click(&qid));
            }
        }
    }

    // Variant arm
    for i in 0..10 {
        let clicks_per_user = if i < 5 { 3 } else { 4 };
        for j in 0..20 {
            let qid = format!("q{qid_counter}");
            qid_counter += 1;
            searches.push(search(
                &format!("var_u{i}"),
                "variant",
                Some(&qid),
                10,
                "user_token",
            ));
            if j < clicks_per_user {
                events.push(click(&qid));
            }
        }
    }

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.control.searches, 200);
    assert_eq!(m.control.clicks, 25); // 5*2 + 5*3
    assert_eq!(m.control.users, 10);
    assert!((m.control.ctr - 0.125).abs() < 0.001);

    assert_eq!(m.variant.searches, 200);
    assert_eq!(m.variant.clicks, 35); // 5*3 + 5*4
    assert_eq!(m.variant.users, 10);
    assert!((m.variant.ctr - 0.175).abs() < 0.001);
}

// ── Excludes query_id assignments ───────────────────────────────

/// Verify that searches assigned via `query_id` fallback are excluded from arm metrics and counted in `no_stable_id_queries`.
#[test]
fn metrics_excludes_query_id_assignments() {
    let searches = vec![
        search("u1", "control", Some("q1"), 5, "user_token"),
        search("u2", "variant", Some("q2"), 5, "user_token"),
        search("u3", "control", Some("q3"), 5, "user_token"),
        // These should be excluded from arm stats
        search("anon1", "control", Some("q4"), 5, "query_id"),
        search("anon2", "variant", Some("q5"), 5, "query_id"),
        search("anon3", "control", Some("q6"), 5, "query_id"),
    ];
    let events = vec![
        click("q1"),
        click("q2"),
        click("q4"), // click from excluded user
    ];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.no_stable_id_queries, 3);
    assert_eq!(m.control.searches + m.variant.searches, 3); // only stable-id
    assert_eq!(m.control.clicks, 1); // q1
    assert_eq!(m.variant.clicks, 1); // q2
}

// ── Zero division safety ────────────────────────────────────────

#[test]
fn metrics_handles_zero_division_safely() {
    let m = aggregate_experiment_metrics(&[], &[], None);

    assert_eq!(m.control.ctr, 0.0);
    assert_eq!(m.control.conversion_rate, 0.0);
    assert_eq!(m.control.revenue_per_search, 0.0);
    assert_eq!(m.control.zero_result_rate, 0.0);
    assert_eq!(m.control.abandonment_rate, 0.0);
    assert_eq!(m.variant.ctr, 0.0);
    assert!(!m.control.ctr.is_nan());
    assert!(!m.variant.abandonment_rate.is_nan());
}

// ── Abandonment rate ────────────────────────────────────────────

/// Verify abandonment rate equals abandoned searches divided by searches-with-results, excluding zero-result searches from the denominator.
#[test]
fn abandonment_rate_computed_correctly() {
    // 10 searches: 3 have nb_hits=0 (zero result), 7 have results
    // Of the 7 with results: 4 get clicks, 3 don't (abandoned)
    // AbandonmentRate = 3 / 7 ≈ 0.4286
    let searches = vec![
        search("u1", "control", Some("q1"), 0, "user_token"), // zero result
        search("u1", "control", Some("q2"), 0, "user_token"), // zero result
        search("u1", "control", Some("q3"), 0, "user_token"), // zero result
        search("u1", "control", Some("q4"), 5, "user_token"), // has results, gets click
        search("u1", "control", Some("q5"), 5, "user_token"), // has results, gets click
        search("u1", "control", Some("q6"), 5, "user_token"), // has results, gets click
        search("u1", "control", Some("q7"), 5, "user_token"), // has results, gets click
        search("u1", "control", Some("q8"), 5, "user_token"), // has results, no click = abandoned
        search("u1", "control", Some("q9"), 5, "user_token"), // has results, no click = abandoned
        search("u1", "control", Some("q10"), 5, "user_token"), // has results, no click = abandoned
    ];
    let events = vec![click("q4"), click("q5"), click("q6"), click("q7")];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.control.zero_result_searches, 3);
    assert_eq!(m.control.abandoned_searches, 3);
    assert!((m.control.zero_result_rate - 0.3).abs() < 0.001); // 3/10
    assert!((m.control.abandonment_rate - 3.0 / 7.0).abs() < 0.001);
}

// ── Per-user CTRs for delta method ──────────────────────────────

/// Verify `per_user_ctrs` contains (clicks, searches) tuples per user for the delta method z-test.
#[test]
fn per_user_ctrs_returned_for_delta_method() {
    let searches = vec![
        search("u1", "control", Some("q1"), 5, "user_token"),
        search("u1", "control", Some("q2"), 5, "user_token"),
        search("u2", "control", Some("q3"), 5, "user_token"),
    ];
    let events = vec![click("q1")]; // u1 gets 1 click out of 2 searches

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.control.per_user_ctrs.len(), 2); // 2 users
                                                  // Find u1's entry: (1.0, 2.0) and u2's entry: (0.0, 1.0)
    let mut ctrs_sorted: Vec<(f64, f64)> = m.control.per_user_ctrs.clone();
    ctrs_sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    assert_eq!(ctrs_sorted[0], (0.0, 1.0)); // u2: 0 clicks, 1 search
    assert_eq!(ctrs_sorted[1], (1.0, 2.0)); // u1: 1 click, 2 searches
}

/// Verify arm CTR is the mean of per-user CTRs, not the ratio of aggregate clicks to aggregate searches.
#[test]
fn ctr_uses_mean_of_per_user_ctrs() {
    // u1: 1/1 = 1.0 CTR, u2: 1/9 ≈ 0.1111 CTR, mean ≈ 0.5556
    let mut searches = Vec::new();
    let mut events = Vec::new();

    searches.push(search("u1", "control", Some("q1"), 5, "user_token"));
    events.push(click("q1"));

    for i in 0..9 {
        let qid = format!("q2_{i}");
        searches.push(search("u2", "control", Some(&qid), 5, "user_token"));
        if i == 0 {
            events.push(click(&qid));
        }
    }

    let m = aggregate_experiment_metrics(&searches, &events, None);

    let expected_mean_ctr = (1.0 + (1.0 / 9.0)) / 2.0;
    assert!((m.control.ctr - expected_mean_ctr).abs() < 0.0001);
}

// ── Per-user revenues for Welch's t-test ────────────────────────

/// Verify `per_user_revenues` contains per-user total revenue values suitable for Welch's t-test.
#[test]
fn per_user_revenues_returned_for_welch_test() {
    let searches = vec![
        search("u1", "control", Some("q1"), 5, "user_token"),
        search("u2", "control", Some("q2"), 5, "user_token"),
    ];
    let events = vec![
        conversion("q1", 25.0),
        conversion("q1", 10.0), // u1 gets 2 conversions = $35
        conversion("q2", 50.0), // u2 gets 1 conversion = $50
    ];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    let mut revs = m.control.per_user_revenues.clone();
    revs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert!((revs[0] - 35.0).abs() < 0.001);
    assert!((revs[1] - 50.0).abs() < 0.001);
}

// ── Conversions and revenue ─────────────────────────────────────

/// Verify conversion counts and revenue accumulate correctly, including multiple conversions on a single search.
#[test]
fn conversions_and_revenue_tracked() {
    let searches = vec![
        search("u1", "control", Some("q1"), 5, "user_token"),
        search("u1", "control", Some("q2"), 5, "user_token"),
        search("u2", "variant", Some("q3"), 5, "user_token"),
    ];
    let events = vec![
        conversion("q1", 10.0),
        conversion("q3", 25.0),
        conversion("q3", 15.0), // two conversions on one search
    ];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.control.conversions, 1);
    assert!((m.control.revenue - 10.0).abs() < 0.001);
    assert_eq!(m.variant.conversions, 2);
    assert!((m.variant.revenue - 40.0).abs() < 0.001);
}

// ── Session ID assignment is included ───────────────────────────

#[test]
fn session_id_assignment_included_in_arm_stats() {
    let searches = vec![
        search("u1", "control", Some("q1"), 5, "session_id"),
        search("u2", "variant", Some("q2"), 5, "session_id"),
    ];
    let events = vec![click("q1")];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.no_stable_id_queries, 0);
    assert_eq!(m.control.searches, 1);
    assert_eq!(m.variant.searches, 1);
    assert_eq!(m.control.clicks, 1);
}

// ── Winsorization ───────────────────────────────────────────────

/// Verify that winsorization clamps extreme per-user CTRs to the specified cap while leaving normal values unchanged.
#[test]
fn winsorization_caps_extreme_per_user_ctrs() {
    // User u1: 10 clicks / 10 searches = CTR 1.0 (extreme)
    // User u2: 1 click / 10 searches = CTR 0.1 (normal)
    // With cap = 0.5, u1's CTR should be capped to 0.5
    let mut searches = Vec::new();
    let mut events = Vec::new();
    for j in 0..10 {
        let qid = format!("q1_{j}");
        searches.push(search("u1", "control", Some(&qid), 5, "user_token"));
        events.push(click(&qid)); // u1 clicks everything
    }
    for j in 0..10 {
        let qid = format!("q2_{j}");
        searches.push(search("u2", "control", Some(&qid), 5, "user_token"));
        if j == 0 {
            events.push(click(&qid)); // u2 clicks once
        }
    }

    let m = aggregate_experiment_metrics(&searches, &events, Some(0.5));

    // After winsorization with cap=0.5:
    // u1 raw CTR=1.0 → capped to 0.5, so clicks become 0.5 * 10 = 5.0
    // u2 raw CTR=0.1 → below cap, unchanged
    let mut ctrs: Vec<f64> = m
        .control
        .per_user_ctrs
        .iter()
        .map(|(c, s)| if *s > 0.0 { c / s } else { 0.0 })
        .collect();
    ctrs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert!((ctrs[0] - 0.1).abs() < 0.001); // u2 unchanged
    assert!((ctrs[1] - 0.5).abs() < 0.001); // u1 capped
}

// ── Searches without query_id still counted ─────────────────────

#[test]
fn searches_without_query_id_counted_but_no_click_join() {
    let searches = vec![
        search("u1", "control", None, 5, "user_token"), // no query_id
        search("u1", "control", Some("q1"), 5, "user_token"),
    ];
    let events = vec![click("q1")];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.control.searches, 2);
    assert_eq!(m.control.clicks, 1); // only the one with query_id
                                     // The search without query_id and with results counts as abandoned
    assert_eq!(m.control.abandoned_searches, 1);
}

// ── MeanClickRank diagnostic metric ────────────────────────────

/// Verify single-user mean click rank equals the arithmetic mean of min click positions across searches.
#[test]
fn mean_click_rank_basic() {
    // Single user clicks at positions [1], [3], [5] across 3 searches.
    // Per-user mean = (1+3+5)/3 = 3.0, arm mean = 3.0
    let searches = vec![
        search("u1", "control", Some("q1"), 5, "user_token"),
        search("u1", "control", Some("q2"), 5, "user_token"),
        search("u1", "control", Some("q3"), 5, "user_token"),
    ];
    let events = vec![
        click_at("q1", &[1]),
        click_at("q2", &[3]),
        click_at("q3", &[5]),
    ];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert!(
        (m.control.mean_click_rank - 3.0).abs() < 0.001,
        "expected 3.0, got {}",
        m.control.mean_click_rank
    );
}

/// Verify mean click rank uses per-user averaging (not naive event-level mean) to avoid heavy-user bias.
#[test]
fn mean_click_rank_per_user_averaging() {
    // User A: clicks at [1], [2] → user mean = 1.5
    // User B: clicks at [5] → user mean = 5.0
    // Arm mean = (1.5 + 5.0) / 2 = 3.25 (not naive event-level mean 2.67)
    let searches = vec![
        search("uA", "control", Some("q1"), 5, "user_token"),
        search("uA", "control", Some("q2"), 5, "user_token"),
        search("uB", "control", Some("q3"), 5, "user_token"),
    ];
    let events = vec![
        click_at("q1", &[1]),
        click_at("q2", &[2]),
        click_at("q3", &[5]),
    ];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert!(
        (m.control.mean_click_rank - 3.25).abs() < 0.001,
        "expected 3.25 (per-user avg), got {}",
        m.control.mean_click_rank
    );
}

#[test]
fn mean_click_rank_uses_min_position() {
    // Multi-object click with positions [5, 2] → min is 2 (highest ranked)
    let searches = vec![search("u1", "control", Some("q1"), 5, "user_token")];
    let events = vec![click_at("q1", &[5, 2])];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert!(
        (m.control.mean_click_rank - 2.0).abs() < 0.001,
        "expected 2.0 (min of [5,2]), got {}",
        m.control.mean_click_rank
    );
}

#[test]
fn mean_click_rank_ignores_non_positive_positions() {
    // Positions are 1-indexed. Ignore malformed 0 values and use min valid position.
    let searches = vec![search("u1", "control", Some("q1"), 5, "user_token")];
    let events = vec![click_at("q1", &[0, 4, 2])];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert!(
        (m.control.mean_click_rank - 2.0).abs() < 0.001,
        "expected 2.0 (min valid position), got {}",
        m.control.mean_click_rank
    );
}

/// Verify that negative position values in click payloads are filtered out, keeping only valid 1-indexed positions.
#[test]
fn mean_click_rank_ignores_negative_positions() {
    // Malformed payload may contain negatives; ignore them and keep valid 1-indexed values.
    let searches = vec![search("u1", "control", Some("q1"), 5, "user_token")];
    let events = vec![EventRow {
        query_id: "q1".to_string(),
        event_type: "click".to_string(),
        value: None,
        positions: Some("[-3, 4, 2]".to_string()),
        interleaving_team: None,
    }];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert!(
        (m.control.mean_click_rank - 2.0).abs() < 0.001,
        "expected 2.0 (min valid positive position), got {}",
        m.control.mean_click_rank
    );
}

#[test]
fn mean_click_rank_zero_clicks_returns_zero() {
    // No clicks → 0.0
    let searches = vec![search("u1", "control", Some("q1"), 5, "user_token")];
    let events: Vec<EventRow> = vec![];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert_eq!(m.control.mean_click_rank, 0.0);
}

/// Verify mean click rank is computed independently per arm and that lower rank indicates better result relevance.
#[test]
fn mean_click_rank_per_arm() {
    // Control: clicks at [1], [2] → mean 1.5
    // Variant: clicks at [1], [1] → mean 1.0 (better)
    let searches = vec![
        search("u1", "control", Some("q1"), 5, "user_token"),
        search("u1", "control", Some("q2"), 5, "user_token"),
        search("u2", "variant", Some("q3"), 5, "user_token"),
        search("u2", "variant", Some("q4"), 5, "user_token"),
    ];
    let events = vec![
        click_at("q1", &[1]),
        click_at("q2", &[2]),
        click_at("q3", &[1]),
        click_at("q4", &[1]),
    ];

    let m = aggregate_experiment_metrics(&searches, &events, None);

    assert!(
        (m.control.mean_click_rank - 1.5).abs() < 0.001,
        "control expected 1.5, got {}",
        m.control.mean_click_rank
    );
    assert!(
        (m.variant.mean_click_rank - 1.0).abs() < 0.001,
        "variant expected 1.0, got {}",
        m.variant.mean_click_rank
    );
    // Variant has lower (better) rank
    assert!(m.variant.mean_click_rank < m.control.mean_click_rank);
}

// ── CUPED Pre-Experiment Covariates ─────────────────────────────

fn pre_search(user: &str, qid: Option<&str>, nb_hits: u32) -> PreSearchRow {
    PreSearchRow {
        user_token: user.to_string(),
        query_id: qid.map(|s| s.to_string()),
        nb_hits,
        has_results: nb_hits > 0,
    }
}

/// Verify pre-experiment covariate computation returns per-user CTR values for CUPED variance reduction.
#[test]
fn pre_experiment_covariate_returns_per_user_ctr() {
    use crate::experiments::config::PrimaryMetric;

    // u1: 2 searches, 1 click → CTR 0.5
    // u2: 3 searches, 0 clicks → CTR 0.0
    let searches = vec![
        pre_search("u1", Some("q1"), 5),
        pre_search("u1", Some("q2"), 5),
        pre_search("u2", Some("q3"), 5),
        pre_search("u2", Some("q4"), 5),
        pre_search("u2", Some("q5"), 5),
    ];
    let events = vec![click("q1")];

    let covariates = compute_pre_experiment_covariates(&searches, &events, &PrimaryMetric::Ctr);

    assert_eq!(covariates.len(), 2);
    assert!(
        (covariates["u1"] - 0.5).abs() < 0.001,
        "u1 CTR should be 0.5, got {}",
        covariates["u1"]
    );
    assert!(
        (covariates["u2"] - 0.0).abs() < 0.001,
        "u2 CTR should be 0.0, got {}",
        covariates["u2"]
    );
}

#[test]
fn pre_experiment_covariate_empty_searches_returns_empty() {
    use crate::experiments::config::PrimaryMetric;

    let covariates = compute_pre_experiment_covariates(&[], &[], &PrimaryMetric::Ctr);
    assert!(covariates.is_empty());
}

// ── Parquet I/O integration tests ───────────────────────────────

#[cfg(feature = "analytics")]
mod parquet_tests {
    use super::*;
    use crate::analytics::schema::{InsightEvent, SearchEvent};
    use crate::analytics::writer;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Build a `SearchEvent` with the given experiment assignment fields for parquet integration tests.
    ///
    /// # Arguments
    ///
    /// * `user_token` - Simulated user identifier.
    /// * `variant_id` - Arm label ("control" or "variant").
    /// * `experiment_id` - Experiment identifier for row filtering.
    /// * `query_id` - Unique query identifier for click join.
    /// * `nb_hits` - Number of hits returned by the search.
    /// * `assignment_method` - How the user was assigned ("user_token", "session_id", or "query_id").
    fn make_search_event(
        user_token: &str,
        variant_id: &str,
        experiment_id: &str,
        query_id: &str,
        nb_hits: u32,
        assignment_method: &str,
    ) -> SearchEvent {
        SearchEvent {
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            query: "test query".to_string(),
            query_id: Some(query_id.to_string()),
            index_name: "products".to_string(),
            nb_hits,
            processing_time_ms: 5,
            user_token: Some(user_token.to_string()),
            user_ip: None,
            filters: None,
            facets: None,
            analytics_tags: None,
            page: 0,
            hits_per_page: 20,
            has_results: nb_hits > 0,
            country: None,
            region: None,
            experiment_id: Some(experiment_id.to_string()),
            variant_id: Some(variant_id.to_string()),
            assignment_method: Some(assignment_method.to_string()),
        }
    }

    /// Build an `InsightEvent` of type "click" at position 1 for parquet integration tests.
    ///
    /// # Arguments
    ///
    /// * `query_id` - Query to associate the click with for join.
    /// * `user_token` - Simulated user identifier.
    fn make_click_event(query_id: &str, user_token: &str) -> InsightEvent {
        InsightEvent {
            event_type: "click".to_string(),
            event_subtype: None,
            event_name: "Click".to_string(),
            index: "products".to_string(),
            user_token: user_token.to_string(),
            authenticated_user_token: None,
            query_id: Some(query_id.to_string()),
            object_ids: vec!["obj1".to_string()],
            object_ids_alt: vec![],
            positions: Some(vec![1]),
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
            value: None,
            currency: None,
            interleaving_team: None,
        }
    }

    /// Seed search events into the analytics directory structure.
    fn seed_search_events(data_dir: &Path, index_name: &str, events: &[SearchEvent]) {
        let dir = data_dir.join(index_name).join("searches");
        writer::flush_search_events(events, &dir).unwrap();
    }

    /// Seed insight events into the analytics directory structure.
    fn seed_insight_events(data_dir: &Path, index_name: &str, events: &[InsightEvent]) {
        let dir = data_dir.join(index_name).join("events");
        writer::flush_insight_events(events, &dir).unwrap();
    }

    /// Seed legacy insight parquet rows that predate the `positions` column.
    fn seed_legacy_insight_events_without_positions(
        data_dir: &Path,
        index_name: &str,
        rows: &[(&str, &str, Option<f64>)],
    ) {
        let dir = data_dir.join(index_name).join("events");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy_events.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::Utf8, true),
            Field::new("event_type", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));

        let query_ids = StringArray::from(
            rows.iter()
                .map(|(qid, _, _)| Some((*qid).to_string()))
                .collect::<Vec<Option<String>>>(),
        );
        let event_types = StringArray::from(
            rows.iter()
                .map(|(_, event_type, _)| Some((*event_type).to_string()))
                .collect::<Vec<Option<String>>>(),
        );
        let values = Float64Array::from(
            rows.iter()
                .map(|(_, _, value)| *value)
                .collect::<Vec<Option<f64>>>(),
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(query_ids), Arc::new(event_types), Arc::new(values)],
        )
        .unwrap();

        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Verify end-to-end parquet read path returns correct click counts, search counts, and mean click rank for seeded data.
    #[tokio::test]
    async fn parquet_metrics_returns_correct_ctr() {
        let tmp = TempDir::new().unwrap();

        let search_events: Vec<SearchEvent> = (0..20)
            .map(|i| {
                make_search_event(
                    &format!("user_{}", i % 4),
                    if i < 10 { "control" } else { "variant" },
                    "exp-1",
                    &format!("qid_{i}"),
                    5,
                    "user_token",
                )
            })
            .collect();

        // 6 clicks on control (qid_0..qid_5), 8 clicks on variant (qid_10..qid_17)
        let mut click_events = Vec::new();
        for i in 0..6 {
            click_events.push(make_click_event(
                &format!("qid_{i}"),
                &format!("user_{}", i % 4),
            ));
        }
        for i in 10..18 {
            click_events.push(make_click_event(
                &format!("qid_{i}"),
                &format!("user_{}", i % 4),
            ));
        }

        seed_search_events(tmp.path(), "products", &search_events);
        seed_insight_events(tmp.path(), "products", &click_events);

        let m = get_experiment_metrics("exp-1", &["products"], tmp.path(), None)
            .await
            .unwrap();

        assert_eq!(m.control.searches, 10);
        assert_eq!(m.control.clicks, 6);
        assert_eq!(m.variant.searches, 10);
        assert_eq!(m.variant.clicks, 8);
        // All clicks have positions=[1], so mean_click_rank should be 1.0 for both arms
        assert!(
            (m.control.mean_click_rank - 1.0).abs() < 0.001,
            "parquet control mean_click_rank expected 1.0, got {}",
            m.control.mean_click_rank
        );
        assert!(
            (m.variant.mean_click_rank - 1.0).abs() < 0.001,
            "parquet variant mean_click_rank expected 1.0, got {}",
            m.variant.mean_click_rank
        );
    }

    /// Verify the parquet I/O path excludes `query_id`-assigned searches from arm statistics and increments `no_stable_id_queries`.
    #[tokio::test]
    async fn parquet_metrics_excludes_query_id_assignment() {
        let tmp = TempDir::new().unwrap();

        let search_events = vec![
            make_search_event("u1", "control", "exp-1", "q1", 5, "user_token"),
            make_search_event("u2", "variant", "exp-1", "q2", 5, "user_token"),
            make_search_event("anon", "control", "exp-1", "q3", 5, "query_id"),
        ];
        let click_events = vec![make_click_event("q1", "u1"), make_click_event("q3", "anon")];

        seed_search_events(tmp.path(), "products", &search_events);
        seed_insight_events(tmp.path(), "products", &click_events);

        let m = get_experiment_metrics("exp-1", &["products"], tmp.path(), None)
            .await
            .unwrap();

        assert_eq!(m.no_stable_id_queries, 1);
        assert_eq!(m.control.searches, 1);
        assert_eq!(m.variant.searches, 1);
    }

    #[tokio::test]
    async fn parquet_metrics_empty_dir_returns_zeros() {
        let tmp = TempDir::new().unwrap();

        let m = get_experiment_metrics("exp-1", &["products"], tmp.path(), None)
            .await
            .unwrap();

        assert_eq!(m.control.searches, 0);
        assert_eq!(m.control.ctr, 0.0);
        assert!(!m.control.ctr.is_nan());
    }

    /// Verify that legacy parquet files missing the `positions` column are read without error and produce zero mean click rank.
    #[tokio::test]
    async fn parquet_metrics_supports_legacy_events_without_positions_column() {
        let tmp = TempDir::new().unwrap();

        let search_events = vec![
            make_search_event("u1", "control", "exp-legacy", "q1", 5, "user_token"),
            make_search_event("u2", "variant", "exp-legacy", "q2", 5, "user_token"),
        ];
        seed_search_events(tmp.path(), "products", &search_events);
        seed_legacy_insight_events_without_positions(
            tmp.path(),
            "products",
            &[("q1", "click", None), ("q2", "click", None)],
        );

        let m = get_experiment_metrics("exp-legacy", &["products"], tmp.path(), None)
            .await
            .unwrap();

        assert_eq!(m.control.clicks, 1);
        assert_eq!(m.variant.clicks, 1);
        // Legacy events have no positions column → mean_click_rank should be 0.0
        assert_eq!(
            m.control.mean_click_rank, 0.0,
            "legacy events should have zero mean_click_rank"
        );
        assert_eq!(
            m.variant.mean_click_rank, 0.0,
            "legacy events should have zero mean_click_rank"
        );
    }
}

// ── Interleaving click aggregation ────────────────────────────

/// Verify interleaving click aggregation groups clicks by query and returns correct (control, variant) count tuples.
#[test]
fn aggregate_interleaving_clicks_per_query() {
    let events = vec![
        interleaving_click("q1", "control"),
        interleaving_click("q1", "control"),
        interleaving_click("q1", "variant"),
        interleaving_click("q2", "variant"),
        interleaving_click("q2", "variant"),
        interleaving_click("q3", "control"),
        interleaving_click("q3", "control"),
        interleaving_click("q3", "variant"),
        interleaving_click("q3", "variant"),
    ];

    let result = aggregate_interleaving_clicks(&events);

    assert_eq!(result.total_queries, 3);

    // Sort for deterministic assertion (HashMap iteration order is random)
    let mut per_query = result.per_query.clone();
    per_query.sort();

    // q1: (2,1), q2: (0,2), q3: (2,2) — sorted: (0,2), (2,1), (2,2)
    assert_eq!(per_query, vec![(0, 2), (2, 1), (2, 2)]);
}

#[test]
fn aggregate_interleaving_empty_clicks() {
    let events: Vec<EventRow> = vec![];
    let result = aggregate_interleaving_clicks(&events);
    assert_eq!(result.total_queries, 0);
    assert!(result.per_query.is_empty());
}

/// Verify that non-click events (e.g. conversions) with `interleaving_team` set are excluded from interleaving aggregation.
#[test]
fn aggregate_interleaving_ignores_non_click_events() {
    let events = vec![
        interleaving_click("q1", "control"),
        // conversion event with interleaving_team — should be ignored
        EventRow {
            query_id: "q1".to_string(),
            event_type: "conversion".to_string(),
            value: Some(10.0),
            positions: None,
            interleaving_team: Some("variant".to_string()),
        },
    ];

    let result = aggregate_interleaving_clicks(&events);
    assert_eq!(result.total_queries, 1);
    assert_eq!(result.per_query[0], (1, 0)); // only the click counted
}

#[test]
fn aggregate_interleaving_ignores_clicks_without_team() {
    let events = vec![
        interleaving_click("q1", "control"),
        click("q1"), // no interleaving_team — should be ignored
        click("q2"), // no interleaving_team — should be ignored
    ];

    let result = aggregate_interleaving_clicks(&events);
    assert_eq!(result.total_queries, 1); // only q1 has interleaving click
    assert_eq!(result.per_query[0], (1, 0));
}

#[test]
fn aggregate_interleaving_ignores_invalid_team_values() {
    let events = vec![
        interleaving_click("q1", "control"),
        interleaving_click("q2", "garbage"), // invalid — should be ignored
    ];

    let result = aggregate_interleaving_clicks(&events);
    assert_eq!(result.total_queries, 1);
    assert_eq!(result.per_query[0], (1, 0));
}

// ── Interleaving data quality (first-team distribution) ─────

/// Verify `first_team_a_ratio` is close to 0.5 for a well-distributed hash over 100 deterministic query IDs.
#[test]
fn compute_interleaving_metrics_includes_first_team_ratio() {
    // Generate 100 queries with deterministic first-team via murmurhash3.
    // With a well-distributed hash, the ratio should be close to 0.50.
    // Use ±0.10 tolerance (2σ for binomial n=100 p=0.5) — tight enough to
    // catch real distribution bugs while avoiding flaky false failures.
    let mut events = Vec::new();
    for i in 0..100 {
        let qid = format!("q{}", i);
        events.push(interleaving_click(&qid, "control"));
        events.push(interleaving_click(&qid, "variant"));
    }

    let result = compute_interleaving_metrics(&events, "exp-quality-test");

    assert_eq!(result.total_queries, 100);
    assert!(
        result.first_team_a_ratio >= 0.40 && result.first_team_a_ratio <= 0.60,
        "first_team_a_ratio {} should be within ±0.10 of 0.50 (2σ for n=100)",
        result.first_team_a_ratio
    );
}

#[test]
fn compute_interleaving_metrics_zero_queries_gives_half_ratio() {
    let events: Vec<EventRow> = vec![];
    let result = compute_interleaving_metrics(&events, "exp-empty");
    assert_eq!(result.total_queries, 0);
    assert!((result.first_team_a_ratio - 0.5).abs() < f64::EPSILON);
}

// --- safe_div coverage (s40 test-audit, pre-split) ---

#[test]
fn safe_div_zero_denominator_returns_zero() {
    assert_eq!(safe_div(42.0, 0.0), 0.0);
    assert_eq!(safe_div(0.0, 0.0), 0.0);
}

#[test]
fn safe_div_normal_division() {
    assert!((safe_div(10.0, 4.0) - 2.5).abs() < f64::EPSILON);
}
