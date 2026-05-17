use axum::{
    extract::Request,
    http::{header, HeaderValue},
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

/// Middleware that extracts or generates a unique request ID and
/// echoes it back on the response. Reads `X-Request-Id` from the
/// inbound request (Nginx sets it when present) or generates a
/// UUID v4. Records the id onto the current `http.request` span so
/// generated ids — not just inbound ones — are correlatable in
/// traces.
pub async fn request_id_middleware(req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    tracing::Span::current().record("request_id", request_id.as_str());

    let mut response = next.run(req).await;

    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(header::HeaderName::from_static("x-request-id"), val);
    }

    response
}
