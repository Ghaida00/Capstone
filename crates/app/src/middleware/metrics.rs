use axum::{extract::MatchedPath, extract::Request, middleware::Next, response::Response};

/// Prometheus metrics middleware — automatically captures request count
/// and latency for every route.
///
/// Fix #19: Uses `MatchedPath` instead of `req.uri().path()` to normalize
/// route templates. This prevents unbounded metric cardinality from paths
/// like `/api/v2/transactions/{uuid}` creating a unique time series per
/// request, which would OOM Prometheus.
pub async fn metrics_middleware(req: Request, next: Next) -> Response {
    let method = req.method().to_string();

    // Fix #19: Use the matched route template (e.g. "/api/v2/transactions/:id")
    // instead of the actual URI path (e.g. "/api/v2/transactions/550e8400-...")
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

#[cfg(test)]
mod tests {
    use super::metrics_middleware;
    use axum::{routing::get, Router};
    use metrics_exporter_prometheus::PrometheusBuilder;
    use tower::ServiceExt;

    /// Guards `metrics_middleware`'s body and the inner-router topology:
    /// when the middleware runs on a sub-router under `nest_service`,
    /// `MatchedPath` resolves to the full prefixed template and the
    /// rendered `url_path` label is bounded.
    ///
    /// Note: this does NOT guard the *placement* of the layer in
    /// `bootstrap.rs::apply_protection_stack` — a regression that
    /// moves the layer back to the outer router would compile clean
    /// and this test would still pass. Live `/metrics` per-account
    /// series count is the placement guard (and is verified on the
    /// real stack during WP2 verification).
    #[tokio::test]
    async fn inner_layer_yields_bounded_template_label() {
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .expect("recorder");

        let inner = Router::new()
            .route("/{account_number}/balance", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(metrics_middleware));
        let app: Router = Router::new().nest_service("/api/v2/accounts", inner);

        for acc in ["ACC_0000060", "ACC_0000055", "ACC_0000091"] {
            let uri = format!("/api/v2/accounts/{acc}/balance");
            let resp = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(&uri)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
        }

        let out = handle.render();
        assert!(
            out.contains(r#"url_path="/api/v2/accounts/{account_number}/balance""#),
            "expected bounded template label; got:\n{out}"
        );
        for acc in ["ACC_0000060", "ACC_0000055", "ACC_0000091"] {
            assert!(
                !out.contains(&format!("url_path=\"/api/v2/accounts/{acc}/balance\"")),
                "cardinality bomb: per-account series present for {acc}:\n{out}"
            );
        }
    }
}
