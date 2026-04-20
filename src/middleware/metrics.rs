use axum::{extract::MatchedPath, extract::Request, middleware::Next, response::Response};

/// Prometheus metrics middleware — automatically captures request count
/// and latency for every route.
///
/// Fix #19: Uses `MatchedPath` instead of `req.uri().path()` to normalize
/// route templates. This prevents unbounded metric cardinality from paths
/// like `/api/v1/transactions/{uuid}` creating a unique time series per
/// request, which would OOM Prometheus.
pub async fn metrics_middleware(req: Request, next: Next) -> Response {
    let method = req.method().to_string();

    // Fix #19: Use the matched route template (e.g. "/api/v1/transactions/:id")
    // instead of the actual URI path (e.g. "/api/v1/transactions/550e8400-...")
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|mp| mp.as_str().to_string())
        .unwrap_or_else(|| req.uri().path().to_string());

    let start = std::time::Instant::now();

    let response = next.run(req).await;

    let status = response.status().as_u16().to_string();
    let duration = start.elapsed().as_secs_f64();

    // Record request count (OTel semantic convention keys)
    metrics::counter!(
        "http_requests_total",
        "http.request.method" => method.clone(),
        "url.path" => path.clone(),
        "http.response.status_code" => status
    )
    .increment(1);

    // Record latency histogram
    metrics::histogram!(
        "http_request_duration_seconds",
        "http.request.method" => method,
        "url.path" => path
    )
    .record(duration);

    response
}
