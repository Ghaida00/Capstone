//! O-4: `http_request_duration_seconds` must render as a Prometheus
//! histogram (with `_bucket` series), not a summary. Library-behavior
//! regression guard: locks the `set_buckets_for_metric` + `Matcher::Full`
//! mechanism for the SLO-relevant metric. (Behavioural live-stack proof
//! is in WP2 verification; this is a cheap binary-level guard.)

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

#[tokio::test]
async fn http_duration_renders_as_histogram_with_buckets() {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("http_request_duration_seconds".to_string()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )
        .expect("set_buckets_for_metric accepts the SLO bucket list")
        .install_recorder()
        .expect("install_recorder");

    metrics::histogram!(
        "http_request_duration_seconds",
        "http.request.method" => "GET",
        "url.path" => "/api/v2/accounts/{account_number}/balance"
    )
    .record(0.012_f64);

    let out = handle.render();
    assert!(
        out.contains("# TYPE http_request_duration_seconds histogram"),
        "must be histogram (not summary); render:\n{out}"
    );
    assert!(
        out.contains("http_request_duration_seconds_bucket"),
        "must emit _bucket series; render:\n{out}"
    );
    assert!(out.contains("http_request_duration_seconds_sum"));
    assert!(out.contains("http_request_duration_seconds_count"));
    assert!(
        out.contains(r#"le="+Inf""#),
        "histograms must include le=+Inf"
    );
}
