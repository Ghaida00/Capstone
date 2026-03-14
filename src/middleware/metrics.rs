use axum::{extract::Request, middleware::Next, response::Response};

/// Prometheus metrics middleware — automatically captures request count
/// and latency for every route.
///
/// Uses OpenTelemetry semantic convention attribute keys for
/// cross-service compatibility with tools like Grafana Tempo and Jaeger.
pub async fn metrics_middleware(req: Request, next: Next) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
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
