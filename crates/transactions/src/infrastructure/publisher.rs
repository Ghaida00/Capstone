//! Adapter from the domain's `TransactionPublisher` port to the
//! concrete `shared_kernel::queue::producer::QueueProducer`.
//!
//! Lives in `infrastructure/` because it imports the queue
//! crate (`amqprs`-backed via shared_kernel). The application
//! layer never sees either type — only the trait.

use async_trait::async_trait;
use serde_json::json;

use shared_kernel::queue::producer::QueueProducer;

use super::super::domain::{PublishedTransaction, TransactionPublisher};

pub(crate) struct QueueProducerAdapter {
    inner: QueueProducer,
}

impl QueueProducerAdapter {
    pub(crate) fn new(inner: QueueProducer) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl TransactionPublisher for QueueProducerAdapter {
    async fn publish_created(&self, payload: PublishedTransaction) -> Result<(), String> {
        // Serialise to the exact JSON shape the legacy producer
        // sent. Matters because the consumer (still un-rewired
        // — see Phase 2 follow-up) expects these field names.
        let value = json!({
            "from_account":    payload.from_account,
            "to_account":      payload.to_account,
            "amount":          payload.amount_str,
            "currency":        payload.currency,
            "reference_id":    payload.reference_id,
            "description":     payload.description,
            "request_id":      payload.request_id,
            "shard":           payload.shard,
            "idempotency_key": payload.idempotency_key,
            "request_hash":    payload.request_hash,
        });

        self.inner.publish(&value).await.map_err(|e| e.to_string())
    }
}
