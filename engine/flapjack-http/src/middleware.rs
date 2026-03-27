use axum::{
    extract::Request,
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue},
    middleware::Next,
    response::Response,
};
use ipnet::IpNet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tracing::Instrument;

use crate::error_response::json_error;

pub const DEFAULT_TRUSTED_PROXY_CIDRS: &str = "127.0.0.0/8,::1/128";
pub const REQUEST_ID_HEADER_NAME: &str = "x-request-id";

#[derive(Clone, Debug)]
pub struct RequestId(pub String);

fn canonical_request_id(headers: &HeaderMap) -> String {
    headers
        .get(REQUEST_ID_HEADER_NAME)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

#[derive(Debug, Clone, Default)]
pub struct TrustedProxyMatcher {
    networks: Vec<IpNet>,
}

impl TrustedProxyMatcher {
    pub fn from_csv(raw: &str) -> Result<Self, String> {
        let mut networks = Vec::new();
        for entry in raw
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let parsed = entry
                .parse::<IpNet>()
                .map_err(|_| format!("Invalid trusted proxy CIDR: {entry}"))?;
            networks.push(parsed.trunc());
        }
        Ok(Self { networks })
    }

    pub fn from_env_var(var_name: &str) -> Result<Self, String> {
        match std::env::var(var_name) {
            Ok(raw) => Self::from_optional_csv(Some(&raw)),
            Err(std::env::VarError::NotPresent) => Self::from_optional_csv(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(format!("{var_name} must be valid UTF-8"))
            }
        }
    }

    pub fn from_optional_csv(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(str::trim).filter(|value| !value.is_empty()) {
            None => Self::from_csv(DEFAULT_TRUSTED_PROXY_CIDRS),
            Some(value)
                if value.eq_ignore_ascii_case("off") || value.eq_ignore_ascii_case("none") =>
            {
                Ok(Self::default())
            }
            Some(value) => Self::from_csv(value),
        }
    }

    pub fn is_trusted(&self, ip: IpAddr) -> bool {
        self.networks.iter().any(|network| network.contains(&ip))
    }

    pub fn len(&self) -> usize {
        self.networks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.networks.is_empty()
    }
}

fn parse_x_forwarded_for(headers: &HeaderMap) -> Vec<IpAddr> {
    headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|raw| raw.split(','))
        .filter_map(|item| item.trim().parse().ok())
        .collect()
}

fn parse_x_real_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get("x-real-ip")
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| raw.trim().parse().ok())
}

/// Resolve the originating client IP from `X-Forwarded-For` or `X-Real-IP` headers.
///
/// Walks the XFF chain from rightmost (nearest) to leftmost (furthest) hop and returns the first IP not matched by `trusted_proxy_matcher`. If every hop is trusted, returns the leftmost entry. Falls back to `X-Real-IP`, then to `peer_ip`.
///
/// # Arguments
///
/// * `headers` - Request headers potentially containing `X-Forwarded-For` and `X-Real-IP`.
/// * `peer_ip` - The socket-level peer address (always trusted).
/// * `trusted_proxy_matcher` - CIDR matcher identifying known proxy addresses.
fn trusted_forwarded_client_ip(
    headers: &HeaderMap,
    peer_ip: IpAddr,
    trusted_proxy_matcher: &TrustedProxyMatcher,
) -> Option<IpAddr> {
    let forwarded_chain = parse_x_forwarded_for(headers);
    if !forwarded_chain.is_empty() {
        // Walk from nearest hop to furthest hop. The first untrusted hop is the
        // boundary client address; this avoids trusting spoofed left-most values.
        for candidate in forwarded_chain.iter().rev().copied() {
            if !trusted_proxy_matcher.is_trusted(candidate) {
                return Some(candidate);
            }
        }
        // If every hop is trusted, use the furthest forwarded value.
        return forwarded_chain.first().copied();
    }

    parse_x_real_ip(headers).or(Some(peer_ip))
}

/// Best-effort client IP extraction.
///
/// Trust model:
/// - Always trust socket peer (`ConnectInfo`).
/// - Trust `X-Forwarded-For`/`X-Real-IP` only when the socket peer is in
///   `TrustedProxyMatcher` attached to request extensions.
pub fn extract_client_ip_opt(request: &Request) -> Option<IpAddr> {
    let peer_ip = request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip());
    let trusted_proxy_matcher = request.extensions().get::<Arc<TrustedProxyMatcher>>();

    match (peer_ip, trusted_proxy_matcher) {
        (Some(peer_ip), Some(matcher)) if matcher.is_trusted(peer_ip) => {
            trusted_forwarded_client_ip(request.headers(), peer_ip, matcher.as_ref())
        }
        (Some(peer_ip), _) => Some(peer_ip),
        _ => None,
    }
}

/// Extract client IP with precedence:
/// trusted `X-Forwarded-For` -> trusted `X-Real-IP` -> socket peer (`ConnectInfo`) -> `127.0.0.1`.
pub fn extract_client_ip(request: &Request) -> IpAddr {
    extract_client_ip_opt(request).unwrap_or_else(|| "127.0.0.1".parse().unwrap())
}

/// Extract client IP for rate limiting purposes.
///
/// Uses the same resolution as `extract_client_ip`: when the peer is a
/// trusted proxy, resolves the rightmost untrusted XFF IP; otherwise uses
/// the peer IP directly.  XFF headers from untrusted peers are ignored to
/// prevent spoof-based rate-limit evasion.
pub fn extract_rate_limit_ip(request: &Request) -> IpAddr {
    extract_client_ip(request)
}

pub async fn normalize_content_type(mut request: Request, next: Next) -> Response {
    if request.method() == axum::http::Method::POST || request.method() == axum::http::Method::PUT {
        request
            .headers_mut()
            .insert(CONTENT_TYPE, "application/json".parse().unwrap());
    }
    next.run(request).await
}

/// Ensures all error responses (4xx, 5xx) are JSON `{ "message": "...", "status": N }`.
/// Catches Axum's built-in plain-text rejections (e.g. JSON parse failures) and wraps
/// them in the Algolia-compatible error format.
pub async fn ensure_json_errors(request: Request, next: Next) -> Response {
    let response = next.run(request).await;

    if !response.status().is_client_error() && !response.status().is_server_error() {
        return response;
    }

    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("application/json"));

    if is_json {
        return response;
    }

    let status = response.status();
    let body_bytes = axum::body::to_bytes(response.into_body(), 10_000)
        .await
        .unwrap_or_default();
    let message =
        String::from_utf8(body_bytes.to_vec()).unwrap_or_else(|_| "Unknown error".to_string());

    json_error(status, message)
}

/// Chrome 142+ Private Network Access: when a public HTTPS site fetches localhost,
/// the preflight includes `Access-Control-Request-Private-Network: true`.
/// The server must respond with `Access-Control-Allow-Private-Network: true`.
pub async fn allow_private_network(request: Request, next: Next) -> Response {
    let needs_pna = request
        .headers()
        .get("access-control-request-private-network")
        .is_some();
    let mut response = next.run(request).await;
    if needs_pna {
        response.headers_mut().insert(
            "access-control-allow-private-network",
            "true".parse().unwrap(),
        );
    }
    response
}

pub async fn request_id_middleware(mut request: Request, next: Next) -> Response {
    let request_id = canonical_request_id(request.headers());
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));

    let span = tracing::info_span!("http_request", request_id = tracing::field::Empty);
    span.record("request_id", tracing::field::display(&request_id));
    let mut response = next.run(request).instrument(span).await;
    response.headers_mut().insert(
        REQUEST_ID_HEADER_NAME,
        HeaderValue::from_str(&request_id).expect("request ID should be valid header value"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
        Json, Router,
    };
    use serde_json::Value;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;
    use tracing::{field::Field, Event, Subscriber};
    use tracing_subscriber::{
        layer::{Context, Layer},
        prelude::*,
        registry::LookupSpan,
    };

    #[derive(Clone, Default)]
    struct CapturedLogLines {
        lines: Arc<Mutex<Vec<String>>>,
    }

    impl CapturedLogLines {
        fn lines(&self) -> Vec<String> {
            self.lines.lock().unwrap().clone()
        }
    }

    #[derive(Clone)]
    struct TestWriter(Arc<Mutex<Vec<u8>>>);

    impl TestWriter {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Vec::new())))
        }

        fn output(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TestWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[derive(Clone, Debug)]
    struct SpanRequestId(String);

    #[derive(Default)]
    struct SpanRequestIdVisitor {
        request_id: Option<String>,
    }

    impl tracing::field::Visit for SpanRequestIdVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "request_id" {
                self.request_id = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "request_id" {
                self.request_id = Some(format!("{value:?}"));
            }
        }
    }

    #[derive(Default)]
    struct EventMessageVisitor {
        message: Option<String>,
    }

    impl tracing::field::Visit for EventMessageVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "message" {
                self.message = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = Some(format!("{value:?}"));
            }
        }
    }

    impl<S> Layer<S> for CapturedLogLines
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            ctx: Context<'_, S>,
        ) {
            let mut visitor = SpanRequestIdVisitor::default();
            attrs.record(&mut visitor);
            if let (Some(request_id), Some(span_ref)) = (visitor.request_id, ctx.span(id)) {
                span_ref.extensions_mut().insert(SpanRequestId(request_id));
            }
        }

        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            ctx: Context<'_, S>,
        ) {
            let mut visitor = SpanRequestIdVisitor::default();
            values.record(&mut visitor);
            if let (Some(request_id), Some(span_ref)) = (visitor.request_id, ctx.span(id)) {
                span_ref.extensions_mut().insert(SpanRequestId(request_id));
            }
        }

        /// TODO: Document CapturedLogLines.on_event.
        fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
            let mut message_visitor = EventMessageVisitor::default();
            event.record(&mut message_visitor);
            let Some(message) = message_visitor.message else {
                return;
            };

            let request_id = ctx.event_scope(event).and_then(|scope| {
                scope.from_root().find_map(|span| {
                    span.extensions()
                        .get::<SpanRequestId>()
                        .map(|value| value.0.clone())
                })
            });

            self.lines.lock().unwrap().push(format!(
                "message={message} request_id={}",
                request_id.unwrap_or_default()
            ));
        }
    }

    fn request_id_test_router() -> Router {
        Router::new()
            .route(
                "/request-id",
                get(|| async {
                    tracing::info!("request_id test handler");
                    tracing::error!("request_id test handler error");
                    StatusCode::OK
                }),
            )
            .layer(axum::middleware::from_fn(request_id_middleware))
    }

    /// TODO: Document request_id_generates_parseable_uuid_v4_when_missing.
    #[tokio::test]
    async fn request_id_generates_parseable_uuid_v4_when_missing() {
        let app = request_id_test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/request-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            .expect("response should include x-request-id");
        let parsed = uuid::Uuid::parse_str(request_id).expect("request ID should be a UUID");
        assert_eq!(
            parsed.get_version(),
            Some(uuid::Version::Random),
            "request ID should be UUID v4"
        );
    }

    /// TODO: Document request_id_echoes_client_header_when_present.
    #[tokio::test]
    async fn request_id_echoes_client_header_when_present() {
        let app = request_id_test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/request-id")
                    .header("x-request-id", "client-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            .expect("response should include x-request-id");
        assert_eq!(request_id, "client-123");
    }

    /// TODO: Document request_id_generates_parseable_uuid_v4_when_header_is_blank.
    #[tokio::test]
    async fn request_id_generates_parseable_uuid_v4_when_header_is_blank() {
        let app = request_id_test_router();
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/request-id")
                    .header(REQUEST_ID_HEADER_NAME, "   ")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            .expect("response should include x-request-id");
        let parsed = uuid::Uuid::parse_str(request_id).expect("request ID should be a UUID");
        assert_eq!(
            parsed.get_version(),
            Some(uuid::Version::Random),
            "blank request IDs should fall back to UUID v4"
        );
    }

    /// TODO: Document log_line_matches_response_header_value.
    #[test]
    fn log_line_matches_response_header_value() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let captured_logs = CapturedLogLines::default();
        let subscriber = tracing_subscriber::registry().with(captured_logs.clone());
        let app = request_id_test_router();
        let response = tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                app.oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/request-id")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
            })
        });

        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            .expect("response should include x-request-id");
        let log_contents = captured_logs.lines();
        assert!(
            log_contents
                .iter()
                .any(|line| line.contains("request_id test handler")
                    && line.contains(&format!("request_id={request_id}"))),
            "expected handler log line to include request_id {request_id}, got:\n{:?}",
            log_contents
        );
    }

    /// TODO: Document json_logs_include_request_id_from_middleware_span.
    #[test]
    fn json_logs_include_request_id_from_middleware_span() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");
        let writer = TestWriter::new();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone()),
        );
        let app = request_id_test_router();
        let response = tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                app.oneshot(
                    Request::builder()
                        .method("GET")
                        .uri("/request-id")
                        .header(REQUEST_ID_HEADER_NAME, "client-json-123")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
            })
        });

        let request_id = response
            .headers()
            .get(REQUEST_ID_HEADER_NAME)
            .and_then(|value| value.to_str().ok())
            .expect("response should include x-request-id");
        let lines = writer.output();
        let matching_log = lines
            .trim()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("json log line must parse"))
            .find(|value| value["fields"]["message"] == "request_id test handler")
            .expect("request handler log line should be present");

        let http_span = matching_log["spans"]
            .as_array()
            .expect("json log line should include spans")
            .iter()
            .find(|span| span["name"] == "http_request")
            .expect("json log line should include http_request span");

        assert_eq!(
            http_span["request_id"], request_id,
            "JSON logs must carry the same request ID emitted in the response header"
        );
    }

    /// Verify that plain text error responses (e.g., from Axum rejections) are wrapped into the Algolia-compatible JSON error format.
    #[tokio::test]
    async fn wraps_plain_text_errors_into_algolia_json_shape() {
        let app = Router::new()
            .route(
                "/fail",
                post(|| async { (StatusCode::BAD_REQUEST, "bad payload") }),
            )
            .layer(axum::middleware::from_fn(ensure_json_errors));

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/fail")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.contains("application/json"),
            "expected JSON content-type, got: {}",
            ct
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "message": "bad payload", "status": 400 })
        );
    }

    /// Verify that responses already formatted as JSON errors pass through the middleware unchanged.
    #[tokio::test]
    async fn keeps_existing_json_error_response_unchanged() {
        let app = Router::new()
            .route(
                "/json-error",
                post(|| async {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "message": "already json", "status": 400 })),
                    )
                }),
            )
            .layer(axum::middleware::from_fn(ensure_json_errors));

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/json-error")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "message": "already json", "status": 400 })
        );
    }

    #[test]
    fn extract_client_ip_ignores_forwarded_headers_without_trusted_proxy() {
        let mut req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "198.51.100.10, 10.0.0.1")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.77:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));

        let ip = extract_client_ip(&req);
        assert_eq!(ip, "127.0.0.77".parse::<IpAddr>().unwrap());
    }

    /// Verify that the X-Real-IP header is preferred for client IP extraction when the socket peer is within the trusted proxy CIDR.
    #[test]
    fn extract_client_ip_uses_x_real_ip_when_peer_is_trusted_proxy() {
        let mut req = Request::builder()
            .uri("/1/indexes")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.77:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8").expect("valid trusted proxy CIDR"),
        ));

        let ip = extract_client_ip(&req);
        assert_eq!(ip, "10.0.0.2".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn extract_client_ip_falls_back_to_connect_info() {
        let mut req = Request::builder()
            .uri("/1/indexes")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.77:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));

        let ip = extract_client_ip(&req);
        assert_eq!(ip, "127.0.0.77".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn extract_client_ip_rejects_forwarded_headers_without_peer_info() {
        let req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "198.51.100.10")
            .header("x-real-ip", "198.51.100.11")
            .body(Body::empty())
            .unwrap();

        let ip = extract_client_ip(&req);
        assert_eq!(ip, "127.0.0.1".parse::<IpAddr>().unwrap());
    }

    /// Verify that when the peer is a trusted proxy, the first untrusted IP walking rightward through X-Forwarded-For is selected.
    #[test]
    fn extract_client_ip_uses_first_untrusted_from_right_when_peer_is_trusted_proxy() {
        let mut req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "198.51.100.10, 10.0.0.1")
            .header("x-real-ip", "10.0.0.2")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.77:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8").expect("valid trusted proxy CIDR"),
        ));

        let ip = extract_client_ip(&req);
        assert_eq!(ip, "10.0.0.1".parse::<IpAddr>().unwrap());
    }

    /// Verify that when all X-Forwarded-For entries are trusted proxies, the leftmost (furthest) IP is returned.
    #[test]
    fn extract_client_ip_uses_leftmost_after_skipping_trusted_forward_chain() {
        let mut req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "198.51.100.10, 10.0.0.1")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.77:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8,10.0.0.0/8")
                .expect("valid trusted proxy CIDR"),
        ));

        let ip = extract_client_ip(&req);
        assert_eq!(ip, "198.51.100.10".parse::<IpAddr>().unwrap());
    }

    /// Verify that multiple X-Forwarded-For header values are combined into a single chain for consistent IP resolution.
    #[test]
    fn extract_client_ip_combines_multiple_x_forwarded_for_headers() {
        let mut req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "198.51.100.10")
            .header("x-forwarded-for", "10.0.0.1")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.77:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8").expect("valid trusted proxy CIDR"),
        ));

        let ip = extract_client_ip(&req);
        assert_eq!(ip, "10.0.0.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn trusted_proxy_matcher_rejects_invalid_cidr() {
        let err = TrustedProxyMatcher::from_csv("not-a-cidr").expect_err("invalid CIDR");
        assert!(err.contains("Invalid trusted proxy CIDR"));
    }

    #[test]
    fn trusted_proxy_matcher_defaults_to_loopback_when_not_configured() {
        let matcher = TrustedProxyMatcher::from_optional_csv(None).expect("default matcher");
        assert!(matcher.is_trusted("127.0.0.1".parse().unwrap()));
        assert!(matcher.is_trusted("::1".parse().unwrap()));
        assert!(!matcher.is_trusted("203.0.113.7".parse().unwrap()));
    }

    #[test]
    fn trusted_proxy_matcher_supports_explicit_off_keyword() {
        let matcher = TrustedProxyMatcher::from_optional_csv(Some("off")).expect("off matcher");
        assert!(matcher.is_empty());
    }

    // === extract_rate_limit_ip tests ===

    /// Verify that rate limiting IP extraction uses trusted-proxy-aware XFF resolution when the peer is trusted.
    #[test]
    fn rate_limit_ip_uses_trusted_path_when_peer_is_trusted_and_xff_present() {
        // Trusted proxy: XFF rightmost-untrusted should be used (same as extract_client_ip)
        let mut req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "203.0.113.7, 10.0.0.1")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.1:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8").expect("valid"),
        ));

        let ip = extract_rate_limit_ip(&req);
        // 10.0.0.1 is the first untrusted from right (only loopback is trusted)
        assert_eq!(ip, "10.0.0.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn rate_limit_ip_uses_peer_when_peer_is_not_trusted_ignoring_xff() {
        // Untrusted peer: XFF is attacker-controlled, use peer IP to prevent spoof bypass
        let mut req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "203.0.113.7, 198.51.100.2")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "192.168.1.100:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));

        let ip = extract_rate_limit_ip(&req);
        // Must use actual peer IP, not spoofable XFF
        assert_eq!(ip, "192.168.1.100".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn rate_limit_ip_uses_peer_when_no_xff_headers() {
        let mut req = Request::builder()
            .uri("/1/indexes")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "192.168.1.100:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));

        let ip = extract_rate_limit_ip(&req);
        assert_eq!(ip, "192.168.1.100".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn rate_limit_ip_ignores_xff_when_no_connect_info() {
        // Without ConnectInfo (can't happen in production), falls back to loopback
        let req = Request::builder()
            .uri("/1/indexes")
            .header("x-forwarded-for", "203.0.113.7, 198.51.100.2")
            .body(Body::empty())
            .unwrap();

        let ip = extract_rate_limit_ip(&req);
        assert_eq!(ip, "127.0.0.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn rate_limit_ip_falls_back_to_loopback_with_no_info() {
        let req = Request::builder()
            .uri("/1/indexes")
            .body(Body::empty())
            .unwrap();

        let ip = extract_rate_limit_ip(&req);
        assert_eq!(ip, "127.0.0.1".parse::<IpAddr>().unwrap());
    }

    // ── Stage C: Trusted proxy / GeoIP resolution matrix ──

    /// When peer is NOT in the trusted proxy list, XFF is ignored and peer IP is used.
    /// This is the same path that `aroundLatLngViaIP` and analytics country enrichment
    /// would use — both call `extract_client_ip_opt`.
    #[test]
    fn geoip_uses_peer_ip_when_proxy_not_trusted() {
        let mut req = Request::builder()
            .uri("/1/indexes/products/query")
            .header("x-forwarded-for", "203.0.113.7")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "192.168.1.50:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        // No TrustedProxyMatcher in extensions → peer is not trusted

        let ip = extract_client_ip(&req);
        assert_eq!(
            ip,
            "192.168.1.50".parse::<IpAddr>().unwrap(),
            "should use peer IP when proxy is not trusted"
        );
    }

    /// When peer IS a trusted proxy, the first untrusted IP in XFF chain is returned.
    #[test]
    fn geoip_uses_forwarded_chain_when_peer_is_trusted() {
        let mut req = Request::builder()
            .uri("/1/indexes/products/query")
            .header("x-forwarded-for", "203.0.113.7, 10.0.0.1")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.1:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8").unwrap(),
        ));

        let ip = extract_client_ip(&req);
        assert_eq!(
            ip,
            "10.0.0.1".parse::<IpAddr>().unwrap(),
            "should use first untrusted IP from right in XFF chain"
        );
    }

    /// With a multi-hop trusted chain, the rightmost untrusted IP is selected.
    #[test]
    fn geoip_takes_first_untrusted_from_right_in_xff_chain() {
        // XFF: spoofed_client, real_client, trusted_proxy
        // Trusted: 10.0.0.0/8 and 127.0.0.0/8
        let mut req = Request::builder()
            .uri("/1/indexes/products/query")
            .header("x-forwarded-for", "198.51.100.1, 203.0.113.50, 10.0.0.5")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.1:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8,10.0.0.0/8").unwrap(),
        ));

        let ip = extract_client_ip(&req);
        assert_eq!(
            ip,
            "203.0.113.50".parse::<IpAddr>().unwrap(),
            "should pick 203.0.113.50 as the first untrusted from right"
        );
    }

    /// Multiple X-Forwarded-For headers are combined into a single chain.
    #[test]
    fn geoip_handles_multiple_x_forwarded_for_headers_consistently() {
        let mut req = Request::builder()
            .uri("/1/indexes/products/query")
            .header("x-forwarded-for", "198.51.100.1")
            .header("x-forwarded-for", "203.0.113.7")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.1:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8").unwrap(),
        ));

        let ip = extract_client_ip(&req);
        // Multiple headers combined: [198.51.100.1, 203.0.113.7]
        // Walking from right: 203.0.113.7 is untrusted → selected
        assert_eq!(
            ip,
            "203.0.113.7".parse::<IpAddr>().unwrap(),
            "multiple XFF headers should be combined and resolved consistently"
        );
    }

    /// Both `extract_client_ip` and `extract_client_ip_opt` use the same resolution
    /// path, so analytics country enrichment and aroundLatLngViaIP see the same IP.
    #[test]
    fn analytics_country_enrichment_uses_same_client_ip_path() {
        // Trusted proxy scenario
        let mut req = Request::builder()
            .uri("/1/indexes/products/query")
            .header("x-forwarded-for", "203.0.113.7, 10.0.0.1")
            .body(Body::empty())
            .unwrap();
        let socket_addr: SocketAddr = "127.0.0.1:7700".parse().unwrap();
        req.extensions_mut()
            .insert(axum::extract::ConnectInfo(socket_addr));
        req.extensions_mut().insert(Arc::new(
            TrustedProxyMatcher::from_csv("127.0.0.0/8").unwrap(),
        ));

        let ip_for_geo = extract_client_ip_opt(&req);
        let ip_for_country = extract_client_ip_opt(&req);

        assert_eq!(
            ip_for_geo, ip_for_country,
            "geo search and analytics country enrichment must resolve to the same client IP"
        );
        assert_eq!(
            ip_for_geo,
            Some("10.0.0.1".parse::<IpAddr>().unwrap()),
            "both should resolve to the first untrusted IP"
        );
    }
}
