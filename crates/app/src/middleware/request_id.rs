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

// ─── Integration tests for request_id_middleware (T-4) ───────
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{middleware::from_fn, routing::get, Router};
    use axum_test::TestServer;

    fn router_under_request_id() -> Router {
        Router::new()
            .route("/x", get(|| async { "ok" }))
            .layer(from_fn(request_id_middleware))
    }

    #[tokio::test]
    async fn echoes_inbound_request_id_unchanged() {
        let server = TestServer::new(router_under_request_id());
        let res = server
            .get("/x")
            .add_header("x-request-id", "caller-supplied-123")
            .await;
        assert_eq!(res.status_code(), 200);
        // Echo must be byte-identical so caller-side log correlation
        // works against the value the caller already wrote.
        assert_eq!(res.header("x-request-id"), "caller-supplied-123");
    }

    #[tokio::test]
    async fn generates_uuid_when_inbound_header_absent() {
        let server = TestServer::new(router_under_request_id());
        let res = server.get("/x").await;
        assert_eq!(res.status_code(), 200);
        let header_val = res.header("x-request-id");
        let echoed = header_val.to_str().unwrap();
        // UUID v4 canonical hex/dash format is 36 chars; the
        // exact parse is the strongest property to assert.
        assert_eq!(echoed.len(), 36, "got {echoed:?}");
        Uuid::parse_str(echoed).expect("generated value must parse as UUID");
    }
}
