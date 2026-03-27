/// Integration tests for the Dictionaries API (HTTP layer).
///
/// Covers all 5 dictionary endpoints through the HTTP router,
/// validating wire format, error codes, and end-to-end behavior.
/// Also tests search-pipeline integration: custom dictionaries affecting search results.
use serde_json::json;
use serde_json::Value;

mod common;

// ── Helpers ───────────────────────────────────────────────────────────

async fn server() -> (String, common::TempDir) {
    common::spawn_server().await
}

async fn post_json(
    client: &reqwest::Client,
    url: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    post_json_with_app_id(client, url, None, body).await
}

async fn post_json_with_app_id(
    client: &reqwest::Client,
    url: &str,
    app_id: Option<&str>,
    body: serde_json::Value,
) -> reqwest::Response {
    let mut request = client.post(url).header("Content-Type", "application/json");
    if let Some(app_id) = app_id {
        request = request.header("x-algolia-application-id", app_id);
    }
    request
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap()
}

async fn put_json(
    client: &reqwest::Client,
    url: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    put_json_with_app_id(client, url, None, body).await
}

async fn put_json_with_app_id(
    client: &reqwest::Client,
    url: &str,
    app_id: Option<&str>,
    body: serde_json::Value,
) -> reqwest::Response {
    let mut request = client.put(url).header("Content-Type", "application/json");
    if let Some(app_id) = app_id {
        request = request.header("x-algolia-application-id", app_id);
    }
    request
        .body(serde_json::to_string(&body).unwrap())
        .send()
        .await
        .unwrap()
}

async fn get_json(client: &reqwest::Client, url: &str) -> reqwest::Response {
    client.get(url).send().await.unwrap()
}

// ── H.1: Batch add stopword entries, then search returns them ─────────

#[tokio::test]
async fn test_batch_add_stopwords_then_search() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Batch add
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                {
                    "action": "addEntry",
                    "body": {
                        "objectID": "sw-1",
                        "language": "en",
                        "word": "the",
                        "state": "enabled",
                        "type": "custom"
                    }
                },
                {
                    "action": "addEntry",
                    "body": {
                        "objectID": "sw-2",
                        "language": "en",
                        "word": "and",
                        "state": "enabled",
                        "type": "custom"
                    }
                }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["taskID"].is_number(), "batch should return taskID");
    assert!(
        body["updatedAt"].is_string(),
        "batch should return updatedAt"
    );

    // Search
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/search", base),
        json!({ "query": "" }),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["nbHits"], 2);
    assert_eq!(body["hits"].as_array().unwrap().len(), 2);
}

// ── H.2: Batch add then delete, verify entry removed ──────────────────

#[tokio::test]
async fn test_batch_add_then_delete() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Add two entries
    post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "sw-1", "language": "en", "word": "the", "state": "enabled", "type": "custom" } },
                { "action": "addEntry", "body": { "objectID": "sw-2", "language": "en", "word": "and", "state": "enabled", "type": "custom" } }
            ]
        }),
    )
    .await;

    // Delete one existing entry plus one missing entry (missing should be tolerated)
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "deleteEntry", "body": { "objectID": "sw-1" } },
                { "action": "deleteEntry", "body": { "objectID": "does-not-exist" } }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), 200);

    // Search should only return sw-2
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/search", base),
        json!({ "query": "" }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["nbHits"], 1);
    assert_eq!(body["hits"][0]["objectID"], "sw-2");
}

// ── H.3: clearExistingDictionaryEntries clears before adding ──────────

#[tokio::test]
async fn test_clear_existing_entries() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Add initial entries
    post_json(
        &client,
        &format!("{}/1/dictionaries/plurals/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "pl-1", "language": "en", "words": ["mouse", "mice"], "type": "custom" } },
                { "action": "addEntry", "body": { "objectID": "pl-2", "language": "en", "words": ["ox", "oxen"], "type": "custom" } }
            ]
        }),
    )
    .await;

    // Clear and add new
    post_json(
        &client,
        &format!("{}/1/dictionaries/plurals/batch", base),
        json!({
            "clearExistingDictionaryEntries": true,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "pl-3", "language": "fr", "words": ["oeil", "yeux"], "type": "custom" } }
            ]
        }),
    )
    .await;

    // Search should only have pl-3
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/plurals/search", base),
        json!({ "query": "" }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["nbHits"], 1);
    assert_eq!(body["hits"][0]["objectID"], "pl-3");
}

// ── H.8: Invalid dictionary name returns 400 ─────────────────────────

#[tokio::test]
async fn test_invalid_dictionary_name_returns_400() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/synonyms/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": []
        }),
    )
    .await;
    assert_eq!(
        resp.status(),
        400,
        "invalid dictionary name should return 400"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("invalid dictionary name"),
        "error message should indicate invalid dictionary name: {:?}",
        body
    );

    // Also test search with invalid name
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/foobar/search", base),
        json!({ "query": "test" }),
    )
    .await;
    assert_eq!(resp.status(), 400);
}

// ── H.9: Settings round-trip ──────────────────────────────────────────

#[tokio::test]
async fn test_settings_roundtrip() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Get default settings
    let resp = get_json(&client, &format!("{}/1/dictionaries/*/settings", base)).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["disableStandardEntries"]
            .as_object()
            .map(|m| m.is_empty())
            .unwrap_or(true),
        "default settings should have empty disableStandardEntries"
    );

    // Set settings
    let settings = json!({
        "disableStandardEntries": {
            "stopwords": { "fr": true },
            "plurals": { "de": true }
        }
    });
    let resp = put_json(
        &client,
        &format!("{}/1/dictionaries/*/settings", base),
        settings.clone(),
    )
    .await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["taskID"].is_number());

    // Get again and verify
    let resp = get_json(&client, &format!("{}/1/dictionaries/*/settings", base)).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["disableStandardEntries"]["stopwords"]["fr"], true);
    assert_eq!(body["disableStandardEntries"]["plurals"]["de"], true);
}

#[tokio::test]
async fn test_settings_accepts_null_dictionary_type_map() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    let resp = put_json(
        &client,
        &format!("{}/1/dictionaries/*/settings", base),
        json!({
            "disableStandardEntries": {
                "stopwords": { "fr": true },
                "compounds": null
            }
        }),
    )
    .await;
    assert_eq!(
        resp.status(),
        200,
        "settings endpoint should accept null dictionary maps"
    );

    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["taskID"].is_number());

    let resp = get_json(&client, &format!("{}/1/dictionaries/*/settings", base)).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["disableStandardEntries"]["stopwords"]["fr"], true);
}

// ── H.10: Languages endpoint returns correct nbCustomEntries ──────────

#[tokio::test]
async fn test_languages_endpoint_counts() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Add entries across types and languages
    post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "sw-1", "language": "en", "word": "the", "state": "enabled", "type": "custom" } },
                { "action": "addEntry", "body": { "objectID": "sw-2", "language": "en", "word": "a", "state": "enabled", "type": "custom" } },
                { "action": "addEntry", "body": { "objectID": "sw-3", "language": "fr", "word": "le", "state": "enabled", "type": "custom" } }
            ]
        }),
    )
    .await;

    post_json(
        &client,
        &format!("{}/1/dictionaries/plurals/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "pl-1", "language": "en", "words": ["cat", "cats"], "type": "custom" } }
            ]
        }),
    )
    .await;

    // List languages
    let resp = get_json(&client, &format!("{}/1/dictionaries/*/languages", base)).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["en"]["stopwords"]["nbCustomEntries"], 2);
    assert_eq!(body["en"]["plurals"]["nbCustomEntries"], 1);
    assert!(
        body["en"]["compounds"].is_null() || body["en"].get("compounds").is_none(),
        "en should have no compounds"
    );
    assert_eq!(body["fr"]["stopwords"]["nbCustomEntries"], 1);
}

#[tokio::test]
async fn test_languages_endpoint_returns_null_for_unsupported_dictionary_types() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "sw-1", "language": "en", "word": "the", "state": "enabled", "type": "custom" } }
            ]
        }),
    )
    .await;

    let resp = get_json(&client, &format!("{}/1/dictionaries/*/languages", base)).await;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    let en = body["en"]
        .as_object()
        .expect("expected language object for en");
    assert!(en.contains_key("stopwords"));
    assert!(en.contains_key("plurals"));
    assert!(en.contains_key("compounds"));
    assert!(
        en["plurals"].is_null(),
        "missing per-language plural entries should be encoded as null"
    );
    assert!(
        en["compounds"].is_null(),
        "missing per-language compound entries should be encoded as null"
    );
}

// ── G.9: Malformed batch actions return 400 ───────────────────────────

#[tokio::test]
async fn test_malformed_batch_missing_object_id_returns_400() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                {
                    "action": "addEntry",
                    "body": {
                        "language": "en",
                        "word": "the"
                    }
                }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), 400, "missing objectID should return 400");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["message"].as_str().unwrap().contains("objectID"),
        "error should mention objectID: {:?}",
        body
    );
}

#[tokio::test]
async fn test_malformed_batch_invalid_entry_returns_400() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Stopword entry missing required "word" field
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                {
                    "action": "addEntry",
                    "body": {
                        "objectID": "sw-bad",
                        "language": "en"
                    }
                }
            ]
        }),
    )
    .await;
    assert_eq!(
        resp.status(),
        400,
        "malformed entry should return 400, not 500"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("word"),
        "error should explain malformed stopword entry: {:?}",
        body
    );
}

#[tokio::test]
async fn test_malformed_batch_invalid_action_returns_400() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Invalid action value
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                {
                    "action": "updateEntry",
                    "body": { "objectID": "sw-1" }
                }
            ]
        }),
    )
    .await;
    assert_eq!(resp.status(), 400, "invalid action should return 400");
    let body: serde_json::Value = resp.json().await.unwrap();
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("addEntry") && message.contains("deleteEntry"),
        "error should describe valid batch actions: {:?}",
        body
    );
}

// ── H.7: Dictionary changes take effect without restart ───────────────

#[tokio::test]
async fn test_dictionary_changes_without_restart() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Initially no entries
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/compounds/search", base),
        json!({ "query": "" }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["nbHits"], 0);

    // Add an entry
    post_json(
        &client,
        &format!("{}/1/dictionaries/compounds/batch", base),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "cp-1", "language": "de", "word": "Haustür", "decomposition": ["Haus", "Tür"], "type": "custom" } }
            ]
        }),
    )
    .await;

    // Search immediately sees the new entry (no restart needed)
    let resp = post_json(
        &client,
        &format!("{}/1/dictionaries/compounds/search", base),
        json!({ "query": "Haus" }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["nbHits"], 1,
        "new entry should be immediately searchable"
    );
    assert_eq!(body["hits"][0]["objectID"], "cp-1");
}

// ── Search pipeline helpers ───────────────────────────────────────────

/// Index documents via batch API and wait for task completion.
async fn index_docs(client: &reqwest::Client, base: &str, index: &str, docs: Vec<Value>) {
    let requests: Vec<Value> = docs
        .into_iter()
        .map(|d| json!({ "action": "addObject", "body": d }))
        .collect();
    let resp = post_json_with_app_id(
        client,
        &format!("{}/1/indexes/{}/batch", base, index),
        Some(index),
        json!({ "requests": requests }),
    )
    .await;
    assert_eq!(resp.status(), 200, "batch upload failed");
    let body: Value = resp.json().await.unwrap();
    let task_id = body["taskID"].as_i64().expect("missing taskID from batch");
    // Extract addr from base URL for wait_for_task
    let addr = base.trim_start_matches("http://");
    common::wait_for_task(client, addr, task_id).await;
}

/// Search an index with given params.
async fn search_index(client: &reqwest::Client, base: &str, index: &str, params: Value) -> Value {
    let resp = post_json_with_app_id(
        client,
        &format!("{}/1/indexes/{}/query", base, index),
        Some(index),
        params,
    )
    .await;
    assert_eq!(resp.status(), 200, "search failed");
    resp.json().await.unwrap()
}

// ── H.4: Custom French stopword blocks term when built-in disabled ────

#[tokio::test]
async fn test_custom_stopword_blocks_term_in_search() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Index documents with a French word that IS a custom stopword
    index_docs(
        &client,
        &base,
        "test_sw",
        vec![
            json!({ "objectID": "1", "title": "bonjour monde" }),
            json!({ "objectID": "2", "title": "monde entier" }),
        ],
    )
    .await;

    // Add a custom French stopword for "monde"
    post_json_with_app_id(
        &client,
        &format!("{}/1/dictionaries/stopwords/batch", base),
        Some("test_sw"),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "sw-monde", "language": "fr", "word": "monde", "state": "enabled", "type": "custom" } }
            ]
        }),
    )
    .await;

    // Disable standard French stopwords so only our custom one applies
    put_json_with_app_id(
        &client,
        &format!("{}/1/dictionaries/*/settings", base),
        Some("test_sw"),
        json!({
            "disableStandardEntries": {
                "stopwords": { "fr": true }
            }
        }),
    )
    .await;

    // Search with removeStopWords for French — "monde" should be treated as stopword
    // and removed from query, returning broader results
    let result = search_index(
        &client,
        &base,
        "test_sw",
        json!({
            "query": "bonjour monde",
            "removeStopWords": ["fr"],
            "queryLanguages": ["fr"]
        }),
    )
    .await;

    // With "monde" removed as stopword, we search just "bonjour"
    // so doc 2 ("monde entier") should NOT match since "bonjour" isn't in it
    let hits = result["hits"].as_array().unwrap();
    let hit_ids: Vec<&str> = hits
        .iter()
        .map(|h| h["objectID"].as_str().unwrap())
        .collect();
    assert!(
        hit_ids.contains(&"1"),
        "doc 1 should match 'bonjour': hit_ids={:?}",
        hit_ids
    );
    assert!(
        !hit_ids.contains(&"2"),
        "doc 2 should NOT match when 'monde' is stopped (only 'bonjour' remains): hit_ids={:?}",
        hit_ids
    );
}

// ── H.5: Custom plural pair expands query ─────────────────────────────

#[tokio::test]
async fn test_custom_plural_expands_query() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Index documents with singular and plural forms of a custom pair
    index_docs(
        &client,
        &base,
        "test_pl",
        vec![
            json!({ "objectID": "1", "title": "cactus garden" }),
            json!({ "objectID": "2", "title": "many cacti here" }),
        ],
    )
    .await;

    // Add custom plural equivalence (cactus <-> cacti, not in any built-in dictionary)
    post_json_with_app_id(
        &client,
        &format!("{}/1/dictionaries/plurals/batch", base),
        Some("test_pl"),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "pl-cactus", "language": "en", "words": ["cactus", "cacti"], "type": "custom" } }
            ]
        }),
    )
    .await;

    // Search for "cactus" with ignorePlurals — should also find "cacti"
    let result = search_index(
        &client,
        &base,
        "test_pl",
        json!({
            "query": "cactus",
            "ignorePlurals": true,
            "queryLanguages": ["en"]
        }),
    )
    .await;

    let hits = result["hits"].as_array().unwrap();
    let hit_ids: Vec<&str> = hits
        .iter()
        .map(|h| h["objectID"].as_str().unwrap())
        .collect();
    assert!(
        hit_ids.contains(&"1"),
        "doc 1 (cactus garden) should match: {:?}",
        hit_ids
    );
    assert!(
        hit_ids.contains(&"2"),
        "doc 2 (many cacti) should match via custom plural expansion: {:?}",
        hit_ids
    );
}

// ── H.6: Custom compound decomposition affects search ─────────────────

#[tokio::test]
async fn test_custom_compound_decomposition_affects_search() {
    let (addr, _tmp) = server().await;
    let client = reqwest::Client::new();
    let base = format!("http://{}", addr);

    // Index documents with decomposed parts of a compound word
    index_docs(
        &client,
        &base,
        "test_cp",
        vec![
            json!({ "objectID": "1", "title": "Haus verkaufen" }),
            json!({ "objectID": "2", "title": "Tür reparieren" }),
        ],
    )
    .await;

    // Add custom compound decomposition
    post_json_with_app_id(
        &client,
        &format!("{}/1/dictionaries/compounds/batch", base),
        Some("test_cp"),
        json!({
            "clearExistingDictionaryEntries": false,
            "requests": [
                { "action": "addEntry", "body": { "objectID": "cp-haustuer", "language": "de", "word": "Haustür", "decomposition": ["Haus", "Tür"], "type": "custom" } }
            ]
        }),
    )
    .await;

    // Search for the compound word — decompound should find documents with parts
    let result = search_index(
        &client,
        &base,
        "test_cp",
        json!({
            "query": "Haustür",
            "decompoundQuery": true,
            "queryLanguages": ["de"]
        }),
    )
    .await;

    let hits = result["hits"].as_array().unwrap();
    let hit_ids: Vec<&str> = hits
        .iter()
        .map(|h| h["objectID"].as_str().unwrap())
        .collect();
    // With decompounding, "Haustür" -> "Haus" + "Tür", so both docs should match
    assert!(
        hit_ids.contains(&"1"),
        "doc 1 (Haus verkaufen) should match via decompound: {:?}",
        hit_ids
    );
    assert!(
        hit_ids.contains(&"2"),
        "doc 2 (Tür reparieren) should match via decompound: {:?}",
        hit_ids
    );
}
