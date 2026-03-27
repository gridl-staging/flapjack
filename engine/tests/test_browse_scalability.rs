use axum::http::{Method, StatusCode};
use serde_json::json;

mod common;

const ADMIN_KEY: &str = "test-admin-key-scalability";

/// Read doc count from env with default 100000.
fn browse_docs_ci_count() -> usize {
    common::env_usize_or_default("BROWSE_CI_DOC_COUNT", 100_000)
}

/// Read stress doc count from env with default 1000000.
fn browse_docs_stress_count() -> usize {
    common::env_usize_or_default("BROWSE_STRESS_DOC_COUNT", 1_000_000)
}

#[tokio::test]
async fn browse_scalability_ci_returns_all_docs_exactly_once() {
    let doc_count = browse_docs_ci_count();
    const HITS_PER_PAGE: usize = 1_000;

    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    eprintln!("[CI-browse] Indexing {doc_count} docs...");
    let t_index_start = std::time::Instant::now();
    common::seed_docs_bulk(&app, "scalability_ci", ADMIN_KEY, doc_count).await;
    eprintln!(
        "[CI-browse] Indexing complete in {:.1}s",
        t_index_start.elapsed().as_secs_f64()
    );

    // First page assertions
    let (status, first_page) = common::send_json(
        &app,
        Method::POST,
        "/1/indexes/scalability_ci/browse",
        ADMIN_KEY,
        Some(json!({"hitsPerPage": HITS_PER_PAGE})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // First page reports nbHits == doc_count and configured hitsPerPage
    assert_eq!(
        first_page["nbHits"],
        json!(doc_count),
        "nbHits should equal doc_count"
    );
    assert_eq!(
        first_page["hitsPerPage"],
        json!(HITS_PER_PAGE),
        "hitsPerPage should match configured value"
    );

    // Browse all pages
    eprintln!("[CI-browse] Browsing all pages...");
    let t_browse_start = std::time::Instant::now();
    let (visited, last_body, page_count) =
        common::browse_all_cursor_pages(&app, "scalability_ci", ADMIN_KEY, HITS_PER_PAGE).await;
    eprintln!(
        "[CI-browse] Browse complete: {} pages, {} unique docs in {:.1}s",
        page_count,
        visited.len(),
        t_browse_start.elapsed().as_secs_f64()
    );

    // Assert exactly-once invariants
    common::assert_browse_exactly_once_invariants(
        &visited,
        &last_body,
        page_count,
        doc_count,
        HITS_PER_PAGE,
    );
}

#[tokio::test]
#[ignore = "stress test — run manually with cargo test -- --ignored"]
async fn browse_scalability_stress_very_slow() {
    let doc_count = browse_docs_stress_count();
    const HITS_PER_PAGE: usize = 1_000;

    let (app, _tmp) = common::build_test_app_for_local_requests(Some(ADMIN_KEY));

    let baseline_rss_kb = common::sample_rss_kb();
    let mut peak_rss_kb = baseline_rss_kb;

    eprintln!("[STRESS-browse] Indexing {doc_count} docs...");
    let t_index_start = std::time::Instant::now();
    common::seed_docs_bulk(&app, "scalability_stress", ADMIN_KEY, doc_count).await;
    let index_secs = t_index_start.elapsed().as_secs_f64();
    eprintln!("[STRESS-browse] Indexing complete in {:.1}s", index_secs);

    // Memory checkpoint after indexing
    let rss_after_index = common::sample_rss_kb();
    if rss_after_index > peak_rss_kb {
        peak_rss_kb = rss_after_index;
    }

    // Browse all pages
    eprintln!("[STRESS-browse] Browsing all pages...");
    let t_browse_start = std::time::Instant::now();
    let (visited, last_body, page_count) =
        common::browse_all_cursor_pages(&app, "scalability_stress", ADMIN_KEY, HITS_PER_PAGE).await;
    let browse_secs = t_browse_start.elapsed().as_secs_f64();

    // Final memory sample
    let final_rss = common::sample_rss_kb();
    if final_rss > peak_rss_kb {
        peak_rss_kb = final_rss;
    }

    eprintln!(
        "[STRESS-browse] Complete: {page_count} pages, {} unique docs in {:.1}s",
        visited.len(),
        browse_secs
    );
    eprintln!(
        "[STRESS-browse] Memory (KB): baseline={}, peak={}, final={}",
        baseline_rss_kb, peak_rss_kb, final_rss
    );

    // Assert exactly-once invariants (same as CI test)
    common::assert_browse_exactly_once_invariants(
        &visited,
        &last_body,
        page_count,
        doc_count,
        HITS_PER_PAGE,
    );

    // Memory telemetry is logged as diagnostics only — no RSS threshold assertions
    // in the stress test either, to keep it environment-agnostic
}
