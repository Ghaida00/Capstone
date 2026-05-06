//! Custom amqprs `ChannelCallback` with metric/logging hooks
//! for broker-side events the default impl silently swallowed
//! (basic.return, basic.cancel, channel.close).
//!
//! Also tracks publisher confirms when confirm-mode is enabled
//! on the channel — each `publish_ack` / `publish_nack` resolves
//! a oneshot keyed by the broker-assigned delivery tag.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use amqprs::callbacks::ChannelCallback;
use amqprs::channel::Channel;
use amqprs::{Ack, BasicProperties, Cancel, CloseChannel, Nack, Return};
use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub enum ConfirmResult {
    Ack,
    Nack,
}

/// Per-channel pending-confirm registry. Broker assigns sequential
/// delivery_tags starting at 1 after `confirm_select`; the publisher
/// serialises both the publish AND the confirm-wait under
/// `producer::ConfirmingChannel::publish_lock`, so at any moment a
/// channel has at most one pending tag — which lets `note_returned`
/// capture the singular pending tag and `resolve` downgrade only
/// the matching ack.
#[derive(Default)]
pub struct ConfirmRegistry {
    pending: Mutex<BTreeMap<u64, oneshot::Sender<ConfirmResult>>>,
    /// Tag of the in-flight mandatory publish that the broker
    /// rejected as unrouteable (`basic.return`). 0 means "no return
    /// pending". Captured by `note_returned` from the singular
    /// pending tag at the time the return arrived. Cleared by
    /// `register` at the start of every new publish so a return
    /// that arrived after its publisher abandoned the tag (timeout,
    /// local publish error) cannot bleed into the next, unrelated
    /// publish. Consumed by `resolve` only on the matching tag, so
    /// a stale value cannot affect ack handling for any other tag.
    returned_tag: AtomicU64,
}

impl ConfirmRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, tag: u64, tx: oneshot::Sender<ConfirmResult>) {
        // Drop any stale return left by a previous publisher that
        // abandoned its tag (timeout / `basic_publish` error) — the
        // return belongs to that publish and is meaningless now.
        self.returned_tag.store(0, Ordering::Release);
        self.pending.lock().await.insert(tag, tx);
    }

    /// Records that the broker returned the in-flight mandatory
    /// publish as unrouteable. Reads the singular pending tag (the
    /// publish_lock invariant guarantees at most one) and stores
    /// it; the matching `resolve` consumes it via per-tag CAS.
    pub async fn note_returned(&self) {
        let pending = self.pending.lock().await;
        if let Some((&tag, _)) = pending.iter().next() {
            self.returned_tag.store(tag, Ordering::Release);
        }
        // Empty `pending` means the publisher already abandoned its
        // tag before this return arrived; the return is intentionally
        // dropped on the floor.
    }

    pub async fn resolve(&self, tag: u64, multiple: bool, result: ConfirmResult) {
        let mut p = self.pending.lock().await;
        let to_resolve: Vec<u64> = if multiple {
            p.range(..=tag).map(|(k, _)| *k).collect()
        } else {
            vec![tag]
        };
        for k in to_resolve {
            if let Some(s) = p.remove(&k) {
                // Per-tag CAS: an Ack is downgraded to Nack only if
                // `returned_tag` matches THIS tag, atomically clearing
                // the slot. Other tags in a multi-ack batch see the
                // unmodified result.
                let downgrade = matches!(result, ConfirmResult::Ack)
                    && self
                        .returned_tag
                        .compare_exchange(k, 0, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok();
                let actual = if downgrade {
                    ConfirmResult::Nack
                } else {
                    result
                };
                let _ = s.send(actual);
            }
        }
    }

    pub async fn drop_tag(&self, tag: u64) {
        self.pending.lock().await.remove(&tag);
    }
}

pub struct LoggingChannelCallback {
    pub registry: Arc<ConfirmRegistry>,
}

impl LoggingChannelCallback {
    pub fn new(registry: Arc<ConfirmRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl ChannelCallback for LoggingChannelCallback {
    async fn close(
        &mut self,
        _channel: &Channel,
        close: CloseChannel,
    ) -> std::result::Result<(), amqprs::error::Error> {
        tracing::error!(cause = %close, "AMQP channel closed by broker");
        metrics::counter!("amqp_channel_close_total").increment(1);
        Ok(())
    }
    async fn cancel(
        &mut self,
        _channel: &Channel,
        cancel: Cancel,
    ) -> std::result::Result<(), amqprs::error::Error> {
        tracing::warn!(consumer_tag = %cancel.consumer_tag(), "AMQP consumer cancelled by broker");
        metrics::counter!("amqp_consumer_cancel_total").increment(1);
        Ok(())
    }
    async fn flow(
        &mut self,
        _channel: &Channel,
        active: bool,
    ) -> std::result::Result<bool, amqprs::error::Error> {
        tracing::info!(active, "AMQP flow request from broker");
        metrics::counter!("amqp_flow_total").increment(1);
        Ok(true)
    }
    async fn publish_ack(&mut self, _channel: &Channel, ack: Ack) {
        self.registry
            .resolve(ack.delivery_tag(), ack.mutiple(), ConfirmResult::Ack)
            .await;
    }
    async fn publish_nack(&mut self, _channel: &Channel, nack: Nack) {
        tracing::warn!(tag = nack.delivery_tag(), "AMQP publish NACKed by broker");
        metrics::counter!("amqp_publish_nack_total").increment(1);
        self.registry
            .resolve(nack.delivery_tag(), nack.multiple(), ConfirmResult::Nack)
            .await;
    }
    async fn publish_return(
        &mut self,
        _channel: &Channel,
        ret: Return,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        tracing::warn!(
            reply = %ret,
            content_size = content.len(),
            "AMQP publish returned (unrouteable, mandatory=true)"
        );
        metrics::counter!("amqp_publish_return_total").increment(1);
        // Flag the in-flight publish as returned so the matching
        // `publish_ack` resolves as Nack instead of Ack — without
        // this, unrouteable messages report a successful publish.
        self.registry.note_returned().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn register(reg: &ConfirmRegistry, tag: u64) -> oneshot::Receiver<ConfirmResult> {
        let (tx, rx) = oneshot::channel();
        reg.register(tag, tx).await;
        rx
    }

    #[tokio::test]
    async fn ack_resolves_to_ack() {
        let reg = ConfirmRegistry::new();
        let rx = register(&reg, 1).await;
        reg.resolve(1, false, ConfirmResult::Ack).await;
        assert!(matches!(rx.await.unwrap(), ConfirmResult::Ack));
    }

    #[tokio::test]
    async fn nack_resolves_to_nack() {
        let reg = ConfirmRegistry::new();
        let rx = register(&reg, 1).await;
        reg.resolve(1, false, ConfirmResult::Nack).await;
        assert!(matches!(rx.await.unwrap(), ConfirmResult::Nack));
    }

    #[tokio::test]
    async fn return_then_ack_downgrades_to_nack() {
        let reg = ConfirmRegistry::new();
        let rx = register(&reg, 1).await;
        reg.note_returned().await;
        reg.resolve(1, false, ConfirmResult::Ack).await;
        assert!(matches!(rx.await.unwrap(), ConfirmResult::Nack));
    }

    #[tokio::test]
    async fn return_after_drop_does_not_poison_next_publish() {
        // Reproduces the bleed-through bug fixed by tag-keying:
        // publisher A times out, drops its tag, late basic.return
        // arrives, publisher B publishes — B's ack must NOT be
        // downgraded.
        let reg = ConfirmRegistry::new();
        let _rx_a = register(&reg, 1).await;
        reg.drop_tag(1).await;
        // Late return for the abandoned tag — pending is empty so
        // returned_tag stays 0.
        reg.note_returned().await;
        let rx_b = register(&reg, 2).await;
        reg.resolve(2, false, ConfirmResult::Ack).await;
        assert!(matches!(rx_b.await.unwrap(), ConfirmResult::Ack));
    }

    #[tokio::test]
    async fn register_clears_stale_returned_flag() {
        // If a return for the previous publish raced into
        // returned_tag with a non-empty `pending`, the next
        // register() must reset the slot before its tag goes in.
        let reg = ConfirmRegistry::new();
        let _rx_a = register(&reg, 1).await;
        reg.note_returned().await; // captures tag=1
        reg.drop_tag(1).await;
        let rx_b = register(&reg, 2).await; // must clear returned_tag
        reg.resolve(2, false, ConfirmResult::Ack).await;
        assert!(matches!(rx_b.await.unwrap(), ConfirmResult::Ack));
    }

    #[tokio::test]
    async fn multi_ack_downgrades_only_returned_tag() {
        // Synthetic scenario: two pending tags (publish_lock would
        // normally prevent this, but the resolve path must handle
        // it correctly anyway — multiple=true ack covers both).
        // Only the returned tag downgrades to Nack.
        let reg = ConfirmRegistry::new();
        let rx1 = register(&reg, 1).await;
        let rx2 = register(&reg, 2).await;
        // Manually set returned_tag without going through note_returned
        // so we can stage two pending tags simultaneously.
        reg.returned_tag.store(1, Ordering::Release);
        reg.resolve(2, true, ConfirmResult::Ack).await;
        assert!(matches!(rx1.await.unwrap(), ConfirmResult::Nack));
        assert!(matches!(rx2.await.unwrap(), ConfirmResult::Ack));
    }

    #[tokio::test]
    async fn drop_tag_removes_pending() {
        let reg = ConfirmRegistry::new();
        let rx = register(&reg, 1).await;
        reg.drop_tag(1).await;
        // resolve on a dropped tag is a no-op — sender side is gone.
        reg.resolve(1, false, ConfirmResult::Ack).await;
        // The receiver sees the sender drop, not a value.
        assert!(rx.await.is_err());
    }
}
