//! Stub summary for error.rs.
use http::StatusCode;
use thiserror::Error;

/// Unified error type for the Flapjack engine.
///
/// Each variant carries a human-readable message and maps to a specific HTTP status code
/// via [`status_code`](Self::status_code). When the `axum-support` feature is enabled,
/// implements `IntoResponse` to produce Algolia-compatible JSON error bodies
/// (`{ "message": "...", "status": N }`) while sanitizing internal errors to prevent
/// leaking file paths, bucket names, or engine internals.
#[derive(Error, Debug, Clone)]
pub enum FlapjackError {
    #[error("Tenant not found: {0}")]
    TenantNotFound(String),

    #[error("Index already exists for tenant: {0}")]
    IndexAlreadyExists(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Query too complex: {0}")]
    QueryTooComplex(String),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Invalid document: {0}")]
    InvalidDocument(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Type mismatch for field {field}: expected {expected}, got {actual}")]
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },

    #[error("Field not found in schema: {0}")]
    FieldNotFound(String),

    #[error("Too many concurrent writes: {current} active, max {max}")]
    TooManyConcurrentWrites { current: usize, max: usize },

    #[error("Buffer size {requested} exceeds max {max} bytes")]
    BufferSizeExceeded { requested: usize, max: usize },

    #[error("Document size {size} exceeds max {max} bytes")]
    DocumentTooLarge { size: usize, max: usize },

    #[error("Batch size {size} exceeds max {max} documents")]
    BatchTooLarge { size: usize, max: usize },

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("ObjectID does not exist")]
    ObjectNotFound,

    #[error("Write queue full (1000 operations pending)")]
    QueueFull,

    #[error("IO error: {0}")]
    Io(String),

    #[error("Tantivy error: {0}")]
    Tantivy(String),

    #[error("Query parse error: {0}")]
    QueryParse(String),

    #[error("JSON error: {0}")]
    Json(String),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("SSL error: {0}")]
    Ssl(String),

    #[error("ACME error: {0}")]
    Acme(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Memory pressure: {allocated_mb} MB allocated of {limit_mb} MB limit ({level})")]
    MemoryPressure {
        allocated_mb: usize,
        limit_mb: usize,
        level: String,
    },

    #[error("Index paused for migration: {0}")]
    IndexPaused(String),

    #[error("{0}")]
    Forbidden(String),
}

pub type Result<T> = std::result::Result<T, FlapjackError>;

impl From<std::io::Error> for FlapjackError {
    fn from(e: std::io::Error) -> Self {
        FlapjackError::Io(e.to_string())
    }
}

impl From<tantivy::TantivyError> for FlapjackError {
    fn from(e: tantivy::TantivyError) -> Self {
        FlapjackError::Tantivy(e.to_string())
    }
}

impl From<tantivy::query::QueryParserError> for FlapjackError {
    fn from(e: tantivy::query::QueryParserError) -> Self {
        FlapjackError::QueryParse(e.to_string())
    }
}

impl From<serde_json::Error> for FlapjackError {
    fn from(e: serde_json::Error) -> Self {
        FlapjackError::Json(e.to_string())
    }
}

impl From<flapjack_ssl::FlapjackError> for FlapjackError {
    fn from(e: flapjack_ssl::FlapjackError) -> Self {
        // Map SSL crate errors to main crate errors
        match e {
            flapjack_ssl::FlapjackError::Config(msg) => FlapjackError::Config(msg),
            flapjack_ssl::FlapjackError::Ssl(msg) => FlapjackError::Ssl(msg),
            flapjack_ssl::FlapjackError::Acme(msg) => FlapjackError::Acme(msg),
            _ => FlapjackError::Ssl(e.to_string()),
        }
    }
}

#[cfg(feature = "axum-support")]
impl From<axum::extract::rejection::JsonRejection> for FlapjackError {
    fn from(rejection: axum::extract::rejection::JsonRejection) -> Self {
        FlapjackError::Json(rejection.body_text())
    }
}

impl FlapjackError {
    // Status-code mapping policy (Algolia parity):
    // Variant(s)                                               | HTTP | Rationale
    // ---------------------------------------------------------|------|--------------------------------------------
    // TenantNotFound, TaskNotFound, ObjectNotFound            | 404  | Missing resource lookup
    // IndexAlreadyExists                                      | 409  | Conflict with existing resource
    // InvalidQuery, QueryTooComplex, InvalidSchema,           | 400  | Client payload/query contract violation
    // MissingField, TypeMismatch, FieldNotFound,              |      |
    // BufferSizeExceeded, DocumentTooLarge, BatchTooLarge,    |      |
    // InvalidDocument, QueryParse, Json                       |      |
    // QueueFull                                               | 429  | Backpressure/rate limiting
    // Forbidden                                               | 403  | Authenticated but not authorized
    // TooManyConcurrentWrites, MemoryPressure, IndexPaused    | 503  | Temporary service unavailability/retryable
    // Io, Tantivy, S3, Ssl, Acme, Config                      | 500  | Internal server/runtime dependency failure
    /// Map this error variant to the appropriate HTTP status code.
    ///
    /// Follows Algolia parity conventions:
    /// - **404** — missing resource (`TenantNotFound`, `TaskNotFound`, `ObjectNotFound`)
    /// - **409** — conflict (`IndexAlreadyExists`)
    /// - **400** — client contract violations (invalid query, schema, document, parse errors)
    /// - **429** — backpressure (`QueueFull`)
    /// - **403** — authorization failure (`Forbidden`)
    /// - **503** — temporary unavailability (`TooManyConcurrentWrites`, `MemoryPressure`, `IndexPaused`)
    /// - **500** — internal failures (`Io`, `Tantivy`, `S3`, `Ssl`, `Acme`, `Config`)
    ///
    /// # Returns
    ///
    /// The `http::StatusCode` corresponding to this error variant.
    pub fn status_code(&self) -> StatusCode {
        match self {
            FlapjackError::TenantNotFound(_) => StatusCode::NOT_FOUND,
            FlapjackError::IndexAlreadyExists(_) => StatusCode::CONFLICT,
            FlapjackError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
            FlapjackError::QueryTooComplex(_) => StatusCode::BAD_REQUEST,
            FlapjackError::InvalidSchema(_) => StatusCode::BAD_REQUEST,
            FlapjackError::MissingField(_) => StatusCode::BAD_REQUEST,
            FlapjackError::TypeMismatch { .. } => StatusCode::BAD_REQUEST,
            FlapjackError::FieldNotFound(_) => StatusCode::BAD_REQUEST,
            FlapjackError::TooManyConcurrentWrites { .. } => StatusCode::SERVICE_UNAVAILABLE,
            FlapjackError::BufferSizeExceeded { .. } => StatusCode::BAD_REQUEST,
            FlapjackError::DocumentTooLarge { .. } => StatusCode::BAD_REQUEST,
            FlapjackError::BatchTooLarge { .. } => StatusCode::BAD_REQUEST,
            FlapjackError::TaskNotFound(_) => StatusCode::NOT_FOUND,
            FlapjackError::ObjectNotFound => StatusCode::NOT_FOUND,
            FlapjackError::QueueFull => StatusCode::TOO_MANY_REQUESTS,
            FlapjackError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FlapjackError::Tantivy(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FlapjackError::QueryParse(_) => StatusCode::BAD_REQUEST,
            FlapjackError::Json(_) => StatusCode::BAD_REQUEST,
            FlapjackError::InvalidDocument(_) => StatusCode::BAD_REQUEST,
            FlapjackError::S3(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FlapjackError::Ssl(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FlapjackError::Acme(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FlapjackError::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            FlapjackError::MemoryPressure { .. } => StatusCode::SERVICE_UNAVAILABLE,
            FlapjackError::IndexPaused(_) => StatusCode::SERVICE_UNAVAILABLE,
            FlapjackError::Forbidden(_) => StatusCode::FORBIDDEN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── status_code mapping ─────────────────────────────────────────────

    #[test]
    fn tenant_not_found_is_404() {
        let e = FlapjackError::TenantNotFound("test".into());
        assert_eq!(e.status_code(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn index_already_exists_is_409() {
        let e = FlapjackError::IndexAlreadyExists("test".into());
        assert_eq!(e.status_code(), StatusCode::CONFLICT);
    }

    #[test]
    fn forbidden_is_403() {
        let e = FlapjackError::Forbidden("not allowed".into());
        assert_eq!(e.status_code(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn invalid_query_is_400() {
        let e = FlapjackError::InvalidQuery("bad".into());
        assert_eq!(e.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn query_too_complex_is_400() {
        let e = FlapjackError::QueryTooComplex("complex".into());
        assert_eq!(e.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn missing_field_is_400() {
        let e = FlapjackError::MissingField("id".into());
        assert_eq!(e.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn type_mismatch_is_400() {
        let e = FlapjackError::TypeMismatch {
            field: "price".into(),
            expected: "integer".into(),
            actual: "string".into(),
        };
        assert_eq!(e.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn too_many_writes_is_503() {
        let e = FlapjackError::TooManyConcurrentWrites {
            current: 41,
            max: 40,
        };
        assert_eq!(e.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn document_too_large_is_400() {
        let e = FlapjackError::DocumentTooLarge {
            size: 4_000_000,
            max: 3_145_728,
        };
        assert_eq!(e.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn queue_full_is_429() {
        assert_eq!(
            FlapjackError::QueueFull.status_code(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn too_many_writes_and_queue_full_have_distinct_status_codes() {
        // Individual status codes are verified by too_many_writes_is_503 and queue_full_is_429.
        // This test guards against accidental convergence.
        assert_ne!(
            FlapjackError::TooManyConcurrentWrites {
                current: 41,
                max: 40,
            }
            .status_code(),
            FlapjackError::QueueFull.status_code()
        );
    }

    #[test]
    fn io_error_is_500() {
        let e = FlapjackError::Io("disk full".into());
        assert_eq!(e.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn tantivy_error_is_500() {
        let e = FlapjackError::Tantivy("corrupt index".into());
        assert_eq!(e.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn query_parse_is_400() {
        let e = FlapjackError::QueryParse("unexpected token".into());
        assert_eq!(e.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn json_error_is_400() {
        let e = FlapjackError::Json("invalid json".into());
        assert_eq!(e.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn s3_error_is_500() {
        let e = FlapjackError::S3("access denied".into());
        assert_eq!(e.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn ssl_error_is_500() {
        let e = FlapjackError::Ssl("cert expired".into());
        assert_eq!(e.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn memory_pressure_is_503() {
        let e = FlapjackError::MemoryPressure {
            allocated_mb: 900,
            limit_mb: 1000,
            level: "warning".into(),
        };
        assert_eq!(e.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn task_not_found_is_404() {
        let e = FlapjackError::TaskNotFound("abc123".into());
        assert_eq!(e.status_code(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn object_not_found_is_404() {
        let e = FlapjackError::ObjectNotFound;
        assert_eq!(e.status_code(), StatusCode::NOT_FOUND);
    }

    // ── Display / Error trait ───────────────────────────────────────────

    #[test]
    fn error_display_includes_message() {
        let e = FlapjackError::TenantNotFound("my_index".into());
        let msg = format!("{}", e);
        assert!(msg.contains("my_index"));
    }

    #[test]
    fn error_display_type_mismatch() {
        let e = FlapjackError::TypeMismatch {
            field: "price".into(),
            expected: "integer".into(),
            actual: "string".into(),
        };
        let msg = format!("{}", e);
        assert!(msg.contains("price"));
        assert!(msg.contains("integer"));
        assert!(msg.contains("string"));
    }

    // ── From conversions ────────────────────────────────────────────────

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let fj_err: FlapjackError = io_err.into();
        assert!(matches!(fj_err, FlapjackError::Io(_)));
        assert!(fj_err.to_string().contains("file not found"));
    }

    #[test]
    fn from_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let fj_err: FlapjackError = json_err.into();
        assert!(matches!(fj_err, FlapjackError::Json(_)));
    }

    // ── IndexPaused ─────────────────────────────────────────────────────

    #[test]
    fn test_index_paused_is_503() {
        let e = FlapjackError::IndexPaused("foo".into());
        assert_eq!(e.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn test_index_paused_display_message() {
        let e = FlapjackError::IndexPaused("foo".into());
        let msg = e.to_string();
        assert!(
            msg.contains("paused"),
            "message should contain 'paused': {}",
            msg
        );
        assert!(
            msg.contains("foo"),
            "message should contain index name 'foo': {}",
            msg
        );
    }

    // ── into_response() HTTP status correctness ──────────────────────────
    // These tests verify the ACTUAL HTTP response status code, not just status_code().
    // Both must agree — divergence means clients see different codes than logging/metrics.

    #[cfg(feature = "axum-support")]
    mod into_response_tests {
        use super::*;
        use axum::response::IntoResponse;

        fn status_from_response(e: FlapjackError) -> http::StatusCode {
            e.into_response().status()
        }

        #[test]
        fn too_many_concurrent_writes_http_response_is_503() {
            let e = FlapjackError::TooManyConcurrentWrites {
                current: 41,
                max: 40,
            };
            assert_eq!(
                status_from_response(e),
                StatusCode::SERVICE_UNAVAILABLE,
                "TooManyConcurrentWrites HTTP response must be 503 (matches status_code())"
            );
        }

        #[test]
        fn queue_full_http_response_is_429() {
            assert_eq!(
                status_from_response(FlapjackError::QueueFull),
                StatusCode::TOO_MANY_REQUESTS,
                "QueueFull HTTP response must be 429 (matches status_code())"
            );
        }

        #[test]
        fn forbidden_http_response_is_403() {
            assert_eq!(
                status_from_response(FlapjackError::Forbidden("not allowed".into())),
                StatusCode::FORBIDDEN,
                "Forbidden HTTP response must be 403 (matches status_code())"
            );
        }

        #[test]
        fn index_paused_http_response_is_503_with_retry_after() {
            let response = FlapjackError::IndexPaused("my_index".into()).into_response();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                response
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok()),
                Some("1"),
                "IndexPaused response must include Retry-After: 1"
            );
        }

        /// Exhaustively verify that every `FlapjackError` variant produces an HTTP response
        /// whose status code matches `status_code()`, guarding against divergence between
        /// the status mapping and the `IntoResponse` implementation.
        #[test]
        fn into_response_status_matches_status_code_for_all_variants() {
            // Exhaustive check: every variant's HTTP response status equals status_code()
            let errors: Vec<FlapjackError> = vec![
                FlapjackError::TenantNotFound("t".into()),
                FlapjackError::IndexAlreadyExists("t".into()),
                FlapjackError::InvalidQuery("q".into()),
                FlapjackError::QueryTooComplex("q".into()),
                FlapjackError::InvalidSchema("s".into()),
                FlapjackError::InvalidDocument("d".into()),
                FlapjackError::MissingField("f".into()),
                FlapjackError::TypeMismatch {
                    field: "f".into(),
                    expected: "int".into(),
                    actual: "str".into(),
                },
                FlapjackError::FieldNotFound("f".into()),
                FlapjackError::TooManyConcurrentWrites { current: 5, max: 4 },
                FlapjackError::BufferSizeExceeded {
                    requested: 100,
                    max: 50,
                },
                FlapjackError::DocumentTooLarge { size: 100, max: 50 },
                FlapjackError::BatchTooLarge { size: 100, max: 50 },
                FlapjackError::TaskNotFound("id".into()),
                FlapjackError::ObjectNotFound,
                FlapjackError::QueueFull,
                FlapjackError::Io("err".into()),
                FlapjackError::Tantivy("err".into()),
                FlapjackError::QueryParse("err".into()),
                FlapjackError::Json("err".into()),
                FlapjackError::S3("err".into()),
                FlapjackError::Ssl("err".into()),
                FlapjackError::Acme("err".into()),
                FlapjackError::Config("err".into()),
                FlapjackError::MemoryPressure {
                    allocated_mb: 900,
                    limit_mb: 1000,
                    level: "warn".into(),
                },
                FlapjackError::IndexPaused("idx".into()),
                FlapjackError::Forbidden("not allowed".into()),
            ];
            for e in errors {
                let expected = e.status_code();
                let actual = status_from_response(e.clone());
                assert_eq!(
                    actual, expected,
                    "into_response() status ({}) != status_code() ({}) for {:?}",
                    actual, expected, e
                );
            }
        }

        // ── Algolia-compatible response body shape ──────────────────────
        // Error responses must be exactly { "message": "...", "status": N }
        // No extra fields (error, request_id, suggestion, docs).

        async fn body_json(e: FlapjackError) -> serde_json::Value {
            let resp = e.into_response();
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice(&body).unwrap()
        }

        /// Verify that error response bodies contain exactly `message` and `status` fields
        /// with no legacy fields (`error`, `request_id`, `suggestion`, `docs`), ensuring
        /// Algolia-compatible response shape.
        #[tokio::test]
        async fn error_response_has_message_and_status_only() {
            let json = body_json(FlapjackError::TenantNotFound("test".into())).await;
            assert!(json.get("message").is_some(), "must have 'message'");
            assert_eq!(json["status"], 404, "status must be HTTP code as u16");
            // Must NOT have old fields
            assert!(json.get("error").is_none(), "must not have 'error'");
            assert!(
                json.get("request_id").is_none(),
                "must not have 'request_id'"
            );
            assert!(
                json.get("suggestion").is_none(),
                "must not have 'suggestion'"
            );
            assert!(json.get("docs").is_none(), "must not have 'docs'");
        }

        /// Verify that the `status` field in the JSON error body matches the actual HTTP
        /// status code for a representative set of error variants.
        #[tokio::test]
        async fn error_response_status_field_matches_http_status() {
            let cases: Vec<(FlapjackError, u16)> = vec![
                (FlapjackError::TenantNotFound("x".into()), 404),
                (FlapjackError::InvalidQuery("x".into()), 400),
                (FlapjackError::IndexAlreadyExists("x".into()), 409),
                (FlapjackError::TaskNotFound("x".into()), 404),
                (FlapjackError::ObjectNotFound, 404),
                (FlapjackError::QueueFull, 429),
                (FlapjackError::Io("x".into()), 500),
            ];
            for (err, expected_status) in cases {
                let json = body_json(err).await;
                assert_eq!(
                    json["status"], expected_status,
                    "status field must match HTTP status code"
                );
            }
        }
        /// TODO: Document internal_errors_dont_leak_details.
        #[tokio::test]
        async fn internal_errors_dont_leak_details() {
            // Tantivy errors must not leak engine internals
            let json = body_json(FlapjackError::Tantivy(
                "segment corruption at /var/data/index/segments".into(),
            ))
            .await;
            let msg = json["message"].as_str().unwrap();
            assert!(
                !msg.contains("/var"),
                "internal error must not leak file paths: {}",
                msg
            );
            assert!(
                !msg.contains("segment"),
                "internal error must not leak Tantivy details: {}",
                msg
            );

            // IO errors must not leak paths
            let json = body_json(FlapjackError::Io(
                "permission denied: /tmp/data/index".into(),
            ))
            .await;
            let msg = json["message"].as_str().unwrap();
            assert!(
                !msg.contains("/Users"),
                "IO error must not leak file paths: {}",
                msg
            );

            // S3 errors must not leak credentials/config
            let json = body_json(FlapjackError::S3(
                "access denied for bucket my-secret-bucket".into(),
            ))
            .await;
            let msg = json["message"].as_str().unwrap();
            assert!(
                !msg.contains("my-secret-bucket"),
                "S3 error must not leak bucket names: {}",
                msg
            );
        }
    }
}

// Axum IntoResponse implementation (feature-gated)
#[cfg(feature = "axum-support")]
use axum::response::{IntoResponse, Json, Response};
#[cfg(feature = "axum-support")]
use serde::Serialize;

#[cfg(feature = "axum-support")]
#[derive(Serialize)]
pub struct ErrorResponse {
    pub message: String,
    pub status: u16,
}

#[cfg(feature = "axum-support")]
impl FlapjackError {
    /// User-facing error message. Internal errors are sanitized to avoid leaking
    /// file paths, stack traces, bucket names, or other server internals.
    fn api_message(&self) -> String {
        match self {
            FlapjackError::TenantNotFound(t) => format!("Index '{}' does not exist", t),
            FlapjackError::IndexAlreadyExists(t) => format!("Index '{}' already exists", t),
            FlapjackError::InvalidQuery(msg) => msg.clone(),
            FlapjackError::QueryTooComplex(msg) => msg.clone(),
            FlapjackError::InvalidSchema(msg) => msg.clone(),
            FlapjackError::MissingField(f) => format!("Required field '{}' is missing", f),
            FlapjackError::TypeMismatch {
                field,
                expected,
                actual,
            } => format!("Field '{}' expected {}, got {}", field, expected, actual),
            FlapjackError::FieldNotFound(f) => format!("Field '{}' not found in schema", f),
            FlapjackError::TooManyConcurrentWrites { current, max } => {
                format!(
                    "Too many concurrent writes: {} active, max {}",
                    current, max
                )
            }
            FlapjackError::BufferSizeExceeded { requested, max } => {
                format!("Buffer size {} exceeds max {} bytes", requested, max)
            }
            FlapjackError::DocumentTooLarge { size, max } => {
                format!("Document size {} exceeds max {} bytes", size, max)
            }
            FlapjackError::BatchTooLarge { size, max } => {
                format!("Batch size {} exceeds max {} documents", size, max)
            }
            FlapjackError::TaskNotFound(id) => format!("Task '{}' not found", id),
            FlapjackError::ObjectNotFound => "ObjectID does not exist".to_string(),
            FlapjackError::QueueFull => "Write queue full".to_string(),
            FlapjackError::InvalidDocument(msg) => msg.clone(),
            FlapjackError::QueryParse(e) => format!("Query parse error: {}", e),
            FlapjackError::Json(e) => format!("JSON error: {}", e),
            FlapjackError::MemoryPressure { .. } => {
                "Server under memory pressure, retry later".to_string()
            }
            FlapjackError::IndexPaused(_) => "Index is temporarily unavailable".to_string(),
            FlapjackError::Forbidden(msg) => msg.clone(),
            // Internal errors — sanitized, no details leaked
            FlapjackError::Io(_)
            | FlapjackError::Tantivy(_)
            | FlapjackError::S3(_)
            | FlapjackError::Ssl(_)
            | FlapjackError::Acme(_)
            | FlapjackError::Config(_) => "Internal server error".to_string(),
        }
    }
}

#[cfg(feature = "axum-support")]
impl IntoResponse for FlapjackError {
    /// Convert this error into an Axum HTTP response with an Algolia-compatible JSON body.
    ///
    /// Produces a response with:
    /// - HTTP status from [`status_code`](Self::status_code)
    /// - JSON body containing only `message` and `status` fields
    /// - `Retry-After: 5` header for `MemoryPressure` variants
    /// - `Retry-After: 1` header for `IndexPaused` variants
    ///
    /// Internal errors (`Io`, `Tantivy`, `S3`, `Ssl`, `Acme`, `Config`) are logged at
    /// error level server-side, then sanitized to a generic "Internal server error" message
    /// so that file paths, stack traces, and infrastructure details are never exposed to clients.
    fn into_response(self) -> Response {
        let status = self.status_code();
        let message = self.api_message();

        // Log internal details server-side before sanitizing the response
        if matches!(
            &self,
            FlapjackError::Io(_)
                | FlapjackError::Tantivy(_)
                | FlapjackError::S3(_)
                | FlapjackError::Ssl(_)
                | FlapjackError::Acme(_)
                | FlapjackError::Config(_)
        ) {
            tracing::error!("{}", self);
        }

        let error_response = ErrorResponse {
            message,
            status: status.as_u16(),
        };

        let mut response = (status, Json(error_response)).into_response();
        if matches!(&self, FlapjackError::MemoryPressure { .. }) {
            response
                .headers_mut()
                .insert("Retry-After", "5".parse().unwrap());
        }
        if matches!(&self, FlapjackError::IndexPaused(_)) {
            response
                .headers_mut()
                .insert("Retry-After", "1".parse().unwrap());
        }
        response
    }
}
