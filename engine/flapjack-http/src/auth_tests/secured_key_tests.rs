use super::*;

// ── SecuredKeyRestrictions::from_params ──

#[test]
fn secured_key_restrictions_filters() {
    let r = SecuredKeyRestrictions::from_params("filters=brand%3ANike");
    assert_eq!(r.filters, Some("brand:Nike".to_string()));
}

#[test]
fn secured_key_restrictions_valid_until() {
    let r = SecuredKeyRestrictions::from_params("validUntil=1700000000");
    assert_eq!(r.valid_until, Some(1700000000));
}

#[test]
fn secured_key_restrictions_restrict_indices_csv() {
    let r = SecuredKeyRestrictions::from_params("restrictIndices=prod,staging");
    let indices = r.restrict_indices.unwrap();
    assert_eq!(indices, vec!["prod", "staging"]);
}

#[test]
fn secured_key_restrictions_restrict_indices_json() {
    let r = SecuredKeyRestrictions::from_params("restrictIndices=%5B%22prod%22%2C%22staging%22%5D");
    let indices = r.restrict_indices.unwrap();
    assert_eq!(indices, vec!["prod", "staging"]);
}

#[test]
fn secured_key_restrictions_user_token() {
    let r = SecuredKeyRestrictions::from_params("userToken=user123");
    assert_eq!(r.user_token, Some("user123".to_string()));
}

#[test]
fn secured_key_restrictions_hits_per_page() {
    let r = SecuredKeyRestrictions::from_params("hitsPerPage=5");
    assert_eq!(r.hits_per_page, Some(5));
}

#[test]
fn secured_key_restrictions_restrict_sources() {
    let r = SecuredKeyRestrictions::from_params("restrictSources=127.0.0.0/8,10.0.0.0/8");
    assert_eq!(
        r.restrict_sources,
        Some("127.0.0.0/8,10.0.0.0/8".to_string())
    );
}

#[test]
fn secured_key_restrictions_empty() {
    let r = SecuredKeyRestrictions::from_params("");
    assert!(r.filters.is_none());
    assert!(r.valid_until.is_none());
    assert!(r.restrict_indices.is_none());
    assert!(r.user_token.is_none());
    assert!(r.hits_per_page.is_none());
}

// ── generate_secured_api_key ──

#[test]
fn generate_secured_api_key_produces_base64() {
    let key = generate_secured_api_key("parent_key", "filters=brand:Nike");
    // Should be valid base64
    assert!(BASE64.decode(key.as_bytes()).is_ok());
}

#[test]
fn generate_secured_api_key_deterministic() {
    let k1 = generate_secured_api_key("key", "params");
    let k2 = generate_secured_api_key("key", "params");
    assert_eq!(k1, k2);
}

#[test]
fn generate_secured_api_key_different_params_differ() {
    let k1 = generate_secured_api_key("key", "filters=a");
    let k2 = generate_secured_api_key("key", "filters=b");
    assert_ne!(k1, k2);
}
