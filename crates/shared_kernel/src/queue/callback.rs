//! Custom amqprs `ChannelCallback` with metric/logging hooks
//! for broker-side events the default impl silently swallowed
//! (basic.return, basic.cancel, channel.close).
//!
//! Also tracks publisher confirms when confirm-mode is enabled
//! on the channel — each `publish_ack` / `publish_nack` resolves
//! a oneshot keyed by the broker-assigned delivery tag.

use std::collections::BTreeMap;
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
/// must match its tag counter to broker's by serialising publishes
/// per channel (see `producer::publish_with_confirm`).
#[derive(Default)]
pub struct ConfirmRegistry {
    pending: Mutex<BTreeMap<u64, oneshot::Sender<ConfirmResult>>>,
}

impl ConfirmRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, tag: u64, tx: oneshot::Sender<ConfirmResult>) {
        self.pending.lock().await.insert(tag, tx);
    }

    pub async fn resolve(&self, tag: u64, multiple: bool, result: ConfirmResult) {
        let mut p = self.pending.lock().await;
        if multiple {
            let keys: Vec<u64> = p.range(..=tag).map(|(k, _)| *k).collect();
            for k in keys {
                if let Some(s) = p.remove(&k) {
                    let _ = s.send(result);
                }
            }
        } else if let Some(s) = p.remove(&tag) {
            let _ = s.send(result);
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
    }
}
