use axum::{
    extract::Request,
    http::{HeaderValue, header},
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

/// Request ID key stored in request extensions.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// Middleware that extracts or generates a unique request ID.
/// - Extracts `X-Request-ID` from Nginx (if present)
/// - Otherwise generates a UUID v4
/// - Stores in request extensions for handler access
/// - Adds to response headers for client correlation
pub async fn request_id_middleware(mut req: Request, next: Next) -> Response {
    // Extract from Nginx or generate new
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Store in extensions for handlers
    req.extensions_mut().insert(RequestId(request_id.clone()));

    let mut response = next.run(req).await;

    // Add to response headers
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(
            header::HeaderName::from_static("x-request-id"),
            val,
        );
    }

    response
}
