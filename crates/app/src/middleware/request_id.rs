use axum::{
    extract::Request,
    http::{header, HeaderValue},
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

/// Middleware that extracts or generates a unique request ID and
/// echoes it back on the response. Reads `X-Request-ID` from the
/// inbound request (set by Nginx when present) or generates a
/// UUID v4. The legacy v1 handlers used to read it from request
/// extensions; the v2 modules don't, so the extension insert was
/// dropped at the v1 cull.
pub async fn request_id_middleware(req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let mut response = next.run(req).await;

    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(header::HeaderName::from_static("x-request-id"), val);
    }

    response
}
