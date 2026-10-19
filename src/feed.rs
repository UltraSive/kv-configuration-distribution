//! In-process feed of applied log entries, for watchers on this node.
//!
//! The applier publishes every committed batch here after it is visible to
//! reads. Watchers catch up from the store's log (or, on proxies, which keep
//! no log, from a short in-memory tail) and then follow the live broadcast.

use crate::types::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Batches a watcher may fall behind by before it has to catch up again.
const BROADCAST_CAPACITY: usize = 1024;
/// Recent entries a proxy keeps in memory for watchers that reconnect.
const RECENT_ENTRIES: usize = 10_000;
const RECENT_BYTES: usize = 16 << 20;

#[derive(Clone, Debug)]
pub enum FeedEvent {
    Entries(Arc<[LogEntry]>),
    /// The store was replaced by a snapshot (or a proxy reset) ending at
    /// `index`; individual changes up to it are not available.
    /// `seq` counts resets since startup.
    Reset {
        index: Index,
        seq: u64,
    },
}

#[derive(Default)]
struct Recent {
    entries: VecDeque<LogEntry>,
    bytes: usize,
}

pub struct Feed {
    tx: broadcast::Sender<FeedEvent>,
    /// `Some` on nodes without a log.
    recent: Option<Mutex<Recent>>,
    resets: AtomicU64,
}

fn entry_bytes(e: &LogEntry) -> usize {
    e.ops
        .iter()
        .map(|op| op.key().len() + op.value().map_or(0, <[u8]>::len) + 16)
        .sum()
}

impl Feed {
    pub fn new(keep_recent: bool) -> Self {
        Self {
            tx: broadcast::channel(BROADCAST_CAPACITY).0,
            recent: keep_recent.then(Mutex::default),
            resets: AtomicU64::new(0),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<FeedEvent> {
        self.tx.subscribe()
    }

    pub fn publish(&self, entries: &[LogEntry]) {
        if entries.is_empty() {
            return;
        }
        let entries: Arc<[LogEntry]> = entries.into();
        // The tail lock is held across the send so `recent_from` and the
        // broadcast always agree on what has been published.
        let _tail = self.recent.as_ref().map(|recent| {
            let mut r = recent.lock().unwrap();
            for e in entries.iter() {
                r.bytes += entry_bytes(e);
                r.entries.push_back(e.clone());
            }
            while r.entries.len() > RECENT_ENTRIES
                || (r.bytes > RECENT_BYTES && r.entries.len() > 1)
            {
                let e = r.entries.pop_front().unwrap();
                r.bytes -= entry_bytes(&e);
            }
            r
        });
        let _ = self.tx.send(FeedEvent::Entries(entries));
    }

    pub fn reset(&self, index: Index) {
        let _tail = self.recent.as_ref().map(|recent| {
            let mut r = recent.lock().unwrap();
            *r = Recent::default();
            r
        });
        let seq = self.resets.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.tx.send(FeedEvent::Reset { index, seq });
    }

    /// Resets so far; compare with `FeedEvent::Reset::seq` to spot one
    /// missed while lagging.
    pub fn resets(&self) -> u64 {
        self.resets.load(Ordering::SeqCst)
    }

    /// Proxies only: up to `max` in-memory entries starting at `from`, or
    /// `Err(oldest)` if `from` has already been dropped.
    pub fn recent_from(&self, from: Index, max: usize) -> Option<Result<Vec<LogEntry>, Index>> {
        let r = self.recent.as_ref()?.lock().unwrap();
        let Some(first) = r.entries.front().map(|e| e.index) else {
            return Some(Ok(vec![]));
        };
        if from < first {
            return Some(Err(first));
        }
        let skip = (from - first) as usize;
        Some(Ok(r.entries.iter().skip(skip).take(max).cloned().collect()))
    }
}
