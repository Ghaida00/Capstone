//! In-memory ring buffer that satisfies the `NotificationStore`
//! trait.
//!
//! Bounded by capacity passed at construction. Oldest entries are
//! evicted first. This is the Phase 3 placeholder for the future
//! `notification_log` table — when that lands, a sibling
//! `repository.rs` will hold a sqlx-backed implementation and
//! `infrastructure/mod.rs::init` will select between them by
//! config.

use async_trait::async_trait;
use std::collections::VecDeque;
use tokio::sync::RwLock;

use super::super::domain::NotificationStore;
use super::super::ports::{NotificationEntry, NotificationError};

pub(crate) struct InMemoryNotificationStore {
    capacity: usize,
    entries: RwLock<VecDeque<NotificationEntry>>,
}

impl InMemoryNotificationStore {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            entries: RwLock::new(VecDeque::with_capacity(capacity)),
        }
    }
}

#[async_trait]
impl NotificationStore for InMemoryNotificationStore {
    async fn append(&self, entry: NotificationEntry) -> Result<(), NotificationError> {
        let mut guard = self.entries.write().await;
        if guard.len() == self.capacity {
            guard.pop_front();
        }
        guard.push_back(entry);
        // Surface basic visibility: total notifications appended
        // since process start. Useful as a "is the bridge alive?"
        // signal in Grafana.
        metrics::counter!("notifications_appended_total").increment(1);
        Ok(())
    }

    async fn recent(&self, limit: usize) -> Result<Vec<NotificationEntry>, NotificationError> {
        let guard = self.entries.read().await;
        // Iterate newest-first by reversing the deque.
        let out: Vec<NotificationEntry> = guard.iter().rev().take(limit).cloned().collect();
        Ok(out)
    }
}
