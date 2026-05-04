//! Event-driven cache invalidator.
//!
//! Subscribes to `transactions.committed` and DELs the cache keys
//! whose backing rows just changed. Bounds the staleness window
//! at "next flush" rather than the cache TTL.
//!
//! Best-effort: failures to delete are logged + counted but never
//! propagate. The cache TTL still bounds worst-case staleness.

use std::sync::Arc;

use serde::Deserialize;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use shared_kernel::cache::redis::RedisCache;
use shared_kernel::events::{EventSubscriber, EVENT_TRANSACTIONS_COMMITTED};

#[derive(Deserialize)]
struct CommittedKeys {
    /// Transaction id — used to invalidate the per-id cache
    /// entry populated by the `get_by_id` handler. Optional
    /// for forwards-compat with subscribers reading older event
    /// payloads.
    #[serde(default)]
    id: Option<Uuid>,
    from_account: String,
    to_account: String,
    reference_id: Option<String>,
}

pub fn spawn_cache_invalidator(
    subscriber: Arc<dyn EventSubscriber>,
    cache: RedisCache,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let mut rx = subscriber.subscribe();
    tokio::spawn(async move {
        tracing::info!("transactions: cache invalidator started");
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Ok(event) => {
                        if event.name != EVENT_TRANSACTIONS_COMMITTED {
                            continue;
                        }
                        let keys: CommittedKeys = match event.decode_payload() {
                            Ok(k) => k,
                            Err(e) => {
                                tracing::warn!(error = %e, "cache invalidator: payload decode failed");
                                continue;
                            }
                        };
                        invalidate(&cache, &keys).await;
                    }
                    Err(RecvError::Lagged(n)) => {
                        // Bus dropped events — stale keys may linger
                        // until TTL. Counter surfaces the gap.
                        metrics::counter!("cache_invalidator_lagged_total").increment(n);
                        tracing::warn!(skipped = n, "cache invalidator lagged");
                    }
                    Err(RecvError::Closed) => break,
                },
                _ = cancel.cancelled() => break,
            }
        }
        tracing::info!("transactions: cache invalidator exiting");
    })
}

async fn invalidate(cache: &RedisCache, keys: &CommittedKeys) {
    let v = shared_kernel::cache::redis::CACHE_KEY_VERSION;
    if let Some(id) = keys.id {
        // The `get_by_id` handler caches per-uuid; without this
        // del the 30s TTL was the only bound on stale status.
        del(cache, &format!("{}:txn:{}", v, id)).await;
    }
    if let Some(ref_id) = keys.reference_id.as_deref() {
        del(cache, &format!("{}:tx_status:{}", v, ref_id)).await;
    }
    // Both balance read paths (v2 service-level + v1-style handler)
    // need flushing so neither path serves a stale value post-credit.
    for acc in [&keys.from_account, &keys.to_account] {
        del(cache, &format!("{}:acc:{}", v, acc)).await;
        del(cache, &format!("{}:balance:{}", v, acc)).await;
    }
}

async fn del(cache: &RedisCache, key: &str) {
    if let Err(e) = cache.delete(key).await {
        tracing::warn!(error = %e, key = %key, "cache invalidate del failed");
        metrics::counter!("cache_invalidator_errors_total").increment(1);
    }
}
