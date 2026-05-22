//! W3C trace-context propagation across the AMQP boundary.
//!
//! `traceparent` rides the `serde_json` outbox payload across the
//! Redis/outbox storage hop; the producer lifts it into AMQP message
//! headers here, and the consumer reconstructs the remote parent
//! context so one trace spans HTTP -> storage -> worker -> AMQP ->
//! consumer.

use amqprs::{FieldTable, FieldValue};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// W3C `traceparent` of the current tracing span's OTel context.
/// `None` when there is no active OTel span context.
pub fn current_traceparent() -> Option<String> {
    let cx = tracing::Span::current().context();
    let mut carrier = std::collections::HashMap::new();
    TraceContextPropagator::new().inject_context(&cx, &mut carrier);
    carrier.remove("traceparent")
}

/// Set a known W3C `traceparent` onto AMQP message headers.
pub fn inject_traceparent(headers: &mut FieldTable, traceparent: &str) {
    if let (Ok(k), Ok(v)) = ("traceparent".try_into(), traceparent.try_into()) {
        headers.insert(k, FieldValue::S(v));
    }
}

struct FieldTableExtractor<'a>(&'a FieldTable);

impl Extractor for FieldTableExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        let k = key.try_into().ok()?;
        match self.0.get(&k) {
            Some(FieldValue::S(s)) => Some(s.as_ref().as_str()),
            _ => None,
        }
    }
    fn keys(&self) -> Vec<&str> {
        Vec::new()
    }
}

/// Reconstruct a remote parent `Context` from AMQP message headers.
pub fn extract_parent_context(headers: &FieldTable) -> Context {
    TraceContextPropagator::new().extract(&FieldTableExtractor(headers))
}
