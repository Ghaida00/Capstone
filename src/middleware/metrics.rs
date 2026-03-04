use axum::{
    extract::Request,
    middleware::Next,
    response::Response,
};

/// Prometheus metrics middleware — automatically captures request count
/// and latency for every route. Eliminates per-handler instrumentation.
pub async fn metrics_middleware(req: Request, next: Next) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let start = std::time::Instant::now();

    let response = next.run(req).await;

    let status = response.status().as_u16().to_string();
    let duration = start.elapsed().as_secs_f64();

    // Record request count
    metrics::counter!(
        "http_requests_total",
        "method" => method.clone(),
        "path" => path.clone(),
        "status" => status
    )
    .increment(1);

    // Record latency histogram
    metrics::histogram!(
        "http_request_duration_seconds",
        "method" => method,
        "path" => path
    )
    .record(duration);

    response
}
