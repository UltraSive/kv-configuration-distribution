//! The single writer. Every mutation of the store goes through one thread,
//! which makes ordering, conflict checks and cache-fill races trivial.

use crate::cache::{CacheLimits, SharedCache, entry_size};
use crate::config::Role;
use crate::error::{Error, Result, WriteError};
use crate::feed::Feed;
use crate::store::{ApplyMode, Store};
use crate::types::*;
use fjall::PersistMode;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot, watch};

type Reply<T> = oneshot::Sender<T>;

enum Cmd {
    Write {
        txn: Txn,
        reply: Reply<Result<Index, WriteError>>,
    },
    Apply {
        entries: Vec<LogEntry>,
        reply: Reply<Result<()>>,
    },
    BeginBootstrap {
        reply: Reply<Result<()>>,
    },
    SnapshotChunk {
        items: Vec<(Vec<u8>, Vec<u8>)>,
        reply: Reply<Result<()>>,
    },
    FinishBootstrap {
        db_id: DbId,
        index: Index,
        hash: Hash,
        reply: Reply<Result<()>>,
    },
    CacheFill {
        items: Vec<(Vec<u8>, Record)>,
    },
    Maintain,
}

#[derive(Clone, Debug)]
pub struct ApplierConfig {
    pub role: Role,
    pub max_batch: usize,
    pub log_retention: u64,
    pub history_window: u64,
    pub proxy_window: u64,
    pub cache_limits: CacheLimits,
    pub max_value_bytes: usize,
    pub max_txn_ops: usize,
}

#[derive(Clone)]
pub struct Applier {
    tx: mpsc::Sender<Cmd>,
}

impl Applier {
    pub fn spawn(
        store: Arc<Store>,
        cfg: ApplierConfig,
        cache: Option<SharedCache>,
        feed: Arc<Feed>,
    ) -> std::io::Result<(Applier, watch::Receiver<Index>, JoinHandle<()>)> {
        let (tx, rx) = mpsc::channel(4096);
        let (applied_tx, applied_rx) = watch::channel(store.state().applied);
        let worker = Worker {
            store,
            cfg,
            cache,
            applied_tx,
            feed,
        };
        let handle = std::thread::Builder::new()
            .name("kv-applier".into())
            .spawn(move || worker.run(rx))?;
        Ok((Applier { tx }, applied_rx, handle))
    }

    async fn call<T>(&self, make: impl FnOnce(Reply<T>) -> Cmd) -> Option<T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(make(tx)).await.ok()?;
        rx.await.ok()
    }

    pub async fn write(&self, txn: Txn) -> Result<Index, WriteError> {
        self.call(|reply| Cmd::Write { txn, reply })
            .await
            .unwrap_or_else(|| Err(WriteError::Unavailable("applier stopped".into())))
    }

    pub async fn apply(&self, entries: Vec<LogEntry>) -> Result<()> {
        self.call(|reply| Cmd::Apply { entries, reply })
            .await
            .unwrap_or(Err(Error::ApplierStopped))
    }

    pub async fn begin_bootstrap(&self) -> Result<()> {
        self.call(|reply| Cmd::BeginBootstrap { reply })
            .await
            .unwrap_or(Err(Error::ApplierStopped))
    }

    pub async fn snapshot_chunk(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        self.call(|reply| Cmd::SnapshotChunk { items, reply })
            .await
            .unwrap_or(Err(Error::ApplierStopped))
    }

    pub async fn finish_bootstrap(&self, db_id: DbId, index: Index, hash: Hash) -> Result<()> {
        self.call(|reply| Cmd::FinishBootstrap {
            db_id,
            index,
            hash,
            reply,
        })
        .await
        .unwrap_or(Err(Error::ApplierStopped))
    }

    /// Best effort; dropped if the applier is saturated.
    pub fn cache_fill(&self, items: Vec<(Vec<u8>, Record)>) {
        let _ = self.tx.try_send(Cmd::CacheFill { items });
    }

    pub fn maintain(&self) {
        let _ = self.tx.try_send(Cmd::Maintain);
    }
}

struct Worker {
    store: Arc<Store>,
    cfg: ApplierConfig,
    cache: Option<SharedCache>,
    applied_tx: watch::Sender<Index>,
    feed: Arc<Feed>,
}

impl Worker {
    fn run(self, mut rx: mpsc::Receiver<Cmd>) {
        let mut pending = None;
        loop {
            let cmd = match pending.take() {
                Some(c) => c,
                None => match rx.blocking_recv() {
                    Some(c) => c,
                    None => break,
                },
            };
            match cmd {
                Cmd::Write { txn, reply } => {
                    // Group commit: everything already queued shares one fsync.
                    let mut batch = vec![(txn, reply)];
                    while batch.len() < self.cfg.max_batch {
                        match rx.try_recv() {
                            Ok(Cmd::Write { txn, reply }) => batch.push((txn, reply)),
                            Ok(other) => {
                                pending = Some(other);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    self.process_writes(batch);
                }
                Cmd::Apply { entries, reply } => {
                    let _ = reply.send(self.apply(&entries));
                }
                Cmd::BeginBootstrap { reply } => {
                    if let Some(cache) = &self.cache {
                        cache.lock().unwrap().clear();
                    }
                    let _ = reply.send(self.store.begin_bootstrap());
                    self.publish();
                }
                Cmd::SnapshotChunk { items, reply } => {
                    let _ = reply.send(self.store.ingest_snapshot(&items));
                }
                Cmd::FinishBootstrap {
                    db_id,
                    index,
                    hash,
                    reply,
                } => {
                    let res = self.store.finish_bootstrap(db_id, index, hash);
                    self.publish();
                    if res.is_ok() {
                        self.feed.reset(index);
                    }
                    let _ = reply.send(res);
                }
                Cmd::CacheFill { items } => self.cache_fill(&items),
                Cmd::Maintain => self.maintain(),
            }
        }
        if let Err(e) = self.store.persist() {
            tracing::error!("final persist failed: {e}");
        }
    }

    fn publish(&self) {
        self.applied_tx.send_replace(self.store.state().applied);
    }

    fn apply(&self, entries: &[LogEntry]) -> Result<()> {
        let mode = match self.cfg.role {
            Role::Proxy => ApplyMode {
                history: false,
                log: false,
            },
            _ => ApplyMode {
                history: true,
                log: true,
            },
        };
        // Followers don't need to fsync: fjall's journal recovers to a
        // consistent prefix and the missing tail is re-fetched upstream.
        self.store
            .apply_entries(entries, mode, PersistMode::Buffer)?;
        if let Some(cache) = &self.cache {
            // Logged writes are always cached, whatever the limits: the
            // recent-writes window depends on them.
            let mut cache = cache.lock().unwrap();
            for op in entries.iter().flat_map(|e| &e.ops) {
                let rec = Record {
                    index: 0,
                    value: op.value().map(<[u8]>::to_vec),
                };
                cache.upsert(op.key().to_vec(), entry_size(op.key(), &rec));
            }
        }
        self.publish();
        self.feed.publish(entries);
        Ok(())
    }

    fn validate(&self, txn: &Txn) -> Result<(), WriteError> {
        let invalid = |m: String| Err(WriteError::Invalid(m));
        if txn.ops.is_empty() {
            return invalid("transaction has no operations".into());
        }
        if txn.ops.len() > self.cfg.max_txn_ops {
            return invalid(format!("more than {} operations", self.cfg.max_txn_ops));
        }
        let mut seen = HashSet::new();
        for op in &txn.ops {
            let k = op.key();
            if k.is_empty() || k.len() > 4096 {
                return invalid("keys must be 1..=4096 bytes".into());
            }
            if !seen.insert(k) {
                return invalid(format!("duplicate key {:?}", String::from_utf8_lossy(k)));
            }
            if op
                .value()
                .is_some_and(|v| v.len() > self.cfg.max_value_bytes)
            {
                return invalid(format!("value exceeds {} bytes", self.cfg.max_value_bytes));
            }
        }
        Ok(())
    }

    fn process_writes(&self, batch: Vec<(Txn, Reply<Result<Index, WriteError>>)>) {
        if self.cfg.role != Role::Root {
            for (_, reply) in batch {
                let _ = reply.send(Err(WriteError::Unavailable("not the root".into())));
            }
            return;
        }
        let st = self.store.state();
        let mut next = st.applied + 1;
        let mut prev = st.hash;
        let ts = now_ms();
        // Key -> version as of the transactions accepted so far in this batch.
        let mut overlay: HashMap<Vec<u8>, Index> = HashMap::new();
        let mut entries = Vec::new();
        let mut replies = Vec::new();

        for (txn, reply) in batch {
            if let Err(e) = self.validate(&txn) {
                let _ = reply.send(Err(e));
                continue;
            }
            let mut failed = None;
            for c in &txn.checks {
                let actual = match overlay.get(&c.key) {
                    Some(v) => *v,
                    None => match self.store.version_of(&c.key) {
                        Ok(v) => v,
                        Err(e) => {
                            failed = Some(WriteError::Storage(e.to_string()));
                            break;
                        }
                    },
                };
                if actual != c.index {
                    failed = Some(WriteError::Conflict {
                        key: String::from_utf8_lossy(&c.key).into_owned(),
                        expected: c.index,
                        actual,
                    });
                    break;
                }
            }
            if let Some(e) = failed {
                let _ = reply.send(Err(e));
                continue;
            }
            for op in &txn.ops {
                let v = if op.value().is_some() { next } else { 0 };
                overlay.insert(op.key().to_vec(), v);
            }
            let entry = LogEntry::new(&prev, next, ts, txn.ops);
            prev = entry.hash;
            next += 1;
            entries.push(entry);
            replies.push(reply);
        }
        if entries.is_empty() {
            return;
        }
        let mode = ApplyMode {
            history: true,
            log: true,
        };
        match self
            .store
            .apply_entries(&entries, mode, PersistMode::SyncAll)
        {
            Ok(()) => {
                self.publish();
                self.feed.publish(&entries);
                for (reply, e) in replies.into_iter().zip(&entries) {
                    let _ = reply.send(Ok(e.index));
                }
            }
            Err(err) => {
                tracing::error!("commit failed: {err}");
                for reply in replies {
                    let _ = reply.send(Err(WriteError::Storage(err.to_string())));
                }
            }
        }
    }

    /// Caches values fetched on a miss, unless over the hard limit.
    fn cache_fill(&self, items: &[(Vec<u8>, Record)]) {
        let Some(cache) = &self.cache else { return };
        {
            let mut c = cache.lock().unwrap();
            if !self.cfg.cache_limits.admits(c.bytes()) {
                c.rejected_fills += items.len() as u64;
                return;
            }
        }
        match self.store.fill_absent(items) {
            Ok(inserted) => {
                let mut c = cache.lock().unwrap();
                for i in inserted {
                    let (k, r) = &items[i];
                    c.upsert(k.clone(), entry_size(k, r));
                }
            }
            Err(e) => tracing::warn!("cache fill failed: {e}"),
        }
    }

    fn maintain(&self) {
        match &self.cache {
            Some(cache) => self.evict(cache),
            None => {
                for _ in 0..8 {
                    match self
                        .store
                        .prune(self.cfg.log_retention, self.cfg.history_window, 10_000)
                    {
                        Ok(true) => continue,
                        Ok(false) => break,
                        Err(e) => {
                            tracing::error!("prune failed: {e}");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Evicts least-recently-used keys down to the byte target, but never
    /// ones written inside the recent-writes window: a proxy relies on
    /// "absent locally" meaning "not written within the window" to fetch
    /// misses consistently.
    fn evict(&self, cache: &SharedCache) {
        const MAX_PER_PASS: usize = 100_000;
        let floor = self
            .store
            .state()
            .applied
            .saturating_sub(self.cfg.proxy_window);
        let mut c = cache.lock().unwrap();
        let Some(target) = self.cfg.cache_limits.eviction_target(c.bytes()) else {
            return;
        };
        let mut victims = Vec::new();
        let mut protected = Vec::new();
        while c.bytes() > target && victims.len() + protected.len() < MAX_PER_PASS {
            let Some((k, size)) = c.pop_lru() else { break };
            match self.store.get_record(&k) {
                Ok(Some(r)) if r.index > floor => protected.push((k, size)),
                Ok(None) => {}
                _ => victims.push(k),
            }
        }
        for (k, size) in protected {
            c.upsert(k, size);
        }
        c.evictions += victims.len() as u64;
        drop(c);
        if let Err(e) = self.store.remove_data(&victims) {
            tracing::warn!("eviction failed: {e}");
        }
    }
}
