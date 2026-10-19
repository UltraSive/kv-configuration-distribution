//! A running node: storage, the single writer, and the read/write paths
//! for each role.

use crate::applier::{Applier, ApplierConfig};
use crate::cache::{CacheIndex, CacheLimits, SharedCache, free_disk_bytes};
use crate::config::{Config, Role};
use crate::error::WriteError;
use crate::protocol::{RpcReq, RpcResp};
use crate::rpc::{CallError, RpcPool};
use crate::store::Store;
use crate::types::*;
use serde::Serialize;
use serde_bytes::ByteBuf;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("not ready: {0}")]
    NotReady(String),
    #[error("timed out waiting for index {wanted} (at {index})")]
    Timeout { index: Index, wanted: Index },
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("log entries before {floor} are no longer retained")]
    Gone { floor: Index },
    #[error("internal: {0}")]
    Internal(String),
}

impl From<crate::error::Error> for NodeError {
    fn from(e: crate::error::Error) -> Self {
        NodeError::Internal(e.to_string())
    }
}

pub type NodeResult<T> = Result<T, NodeError>;

#[derive(Default)]
pub struct Metrics {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub write_conflicts: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub upstream_fetch_errors: AtomicU64,
    pub snapshots_installed: AtomicU64,
    pub snapshots_served: AtomicU64,
    pub entries_applied: AtomicU64,
    pub replication_errors: AtomicU64,
}

impl Metrics {
    pub fn inc(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Default)]
pub struct Heartbeat {
    pub root_index: Index,
    pub root_time_ms: u64,
    pub received: Option<Instant>,
}

pub struct ProxyState {
    pub cache: SharedCache,
    pub replicas: RpcPool,
}

#[derive(Debug, Serialize)]
pub struct Status {
    pub role: Role,
    pub node_id: String,
    pub db_id: Option<String>,
    pub applied_index: Index,
    pub applied_hash: String,
    pub log_floor: Index,
    pub history_floor: Index,
    pub origin: Index,
    pub root_index: Option<Index>,
    pub lag_entries: Option<u64>,
    pub heartbeat_age_ms: Option<u64>,
    pub upstream: Option<String>,
    pub cached_keys: Option<usize>,
    pub cache_bytes: Option<u64>,
    pub cache_soft_limit: Option<u64>,
    pub cache_hard_limit: Option<u64>,
    pub cache_evictions: Option<u64>,
    pub cache_rejected_fills: Option<u64>,
    pub disk_free_bytes: Option<u64>,
}

pub struct Node {
    pub cfg: Config,
    pub store: Arc<Store>,
    pub applier: Applier,
    applied: watch::Receiver<Index>,
    heartbeat: Mutex<Heartbeat>,
    pub upstream: Mutex<Option<String>>,
    pub proxy: Option<ProxyState>,
    /// Peers writes are forwarded to on non-root nodes.
    writes: Option<RpcPool>,
    pub metrics: Metrics,
}

pub struct ReadOpts {
    pub min_index: Index,
    pub wait: Duration,
}

impl Node {
    pub fn open(cfg: Config) -> anyhow::Result<(Arc<Node>, JoinHandle<()>)> {
        let store = Arc::new(Store::open(&cfg.data_dir)?);
        if cfg.role == Role::Root {
            let id = store.init_root()?;
            tracing::info!("root database {id}");
        }
        let cache: Option<SharedCache> = (cfg.role == Role::Proxy).then(|| {
            let mut cache = CacheIndex::new();
            for (k, size) in store.data_sizes().flatten() {
                cache.upsert(k, size);
            }
            Arc::new(Mutex::new(cache))
        });
        let acfg = ApplierConfig {
            role: cfg.role,
            max_batch: NonZeroUsize::new(cfg.max_batch).map_or(1, NonZeroUsize::get),
            log_retention: cfg.log_retention,
            history_window: cfg.history_window,
            proxy_window: cfg.proxy_window,
            cache_limits: CacheLimits {
                data_dir: cfg.data_dir.clone(),
                soft_bytes: cfg.proxy_soft_limit,
                hard_bytes: cfg.proxy_hard_limit,
                disk_soft_free: cfg.proxy_disk_soft_free,
                disk_hard_free: cfg.proxy_disk_hard_free,
            },
            max_value_bytes: cfg.max_value_bytes,
            max_txn_ops: cfg.max_txn_ops,
        };
        let (applier, applied, join) = Applier::spawn(store.clone(), acfg, cache.clone())?;
        let node_id = store.node_id();
        let proxy = cache.map(|cache| ProxyState {
            cache,
            replicas: RpcPool::new(cfg.replicas.clone(), node_id),
        });
        let writes = (cfg.role != Role::Root).then(|| RpcPool::new(cfg.upstreams.clone(), node_id));
        let node = Node {
            cfg,
            store,
            applier,
            applied,
            heartbeat: Mutex::new(Heartbeat::default()),
            upstream: Mutex::new(None),
            proxy,
            writes,
            metrics: Metrics::default(),
        };
        Ok((Arc::new(node), join))
    }

    pub fn role(&self) -> Role {
        self.cfg.role
    }

    pub fn applied(&self) -> Index {
        *self.applied.borrow()
    }

    pub fn applied_watch(&self) -> watch::Receiver<Index> {
        self.applied.clone()
    }

    pub fn default_wait(&self) -> Duration {
        Duration::from_millis(self.cfg.read_wait_ms)
    }

    /// Waits until this node has applied `index`.
    pub async fn wait_for(&self, index: Index, wait: Duration) -> bool {
        if self.applied() >= index {
            return true;
        }
        let mut rx = self.applied.clone();
        matches!(
            tokio::time::timeout(wait, rx.wait_for(|i| *i >= index)).await,
            Ok(Ok(_))
        )
    }

    async fn ensure_ready(&self, opts: &ReadOpts) -> NodeResult<()> {
        if !self.wait_for(opts.min_index, opts.wait).await {
            return Err(NodeError::Timeout {
                index: self.applied(),
                wanted: opts.min_index,
            });
        }
        if self.store.state().db_id.is_none() {
            return Err(NodeError::NotReady("no data replicated yet".into()));
        }
        Ok(())
    }

    pub fn record_heartbeat(&self, root_index: Index, root_time_ms: u64) {
        *self.heartbeat.lock().unwrap() = Heartbeat {
            root_index,
            root_time_ms,
            received: Some(Instant::now()),
        };
    }

    /// Heartbeat to relay downstream: our own head on the root, otherwise
    /// the latest one received from upstream.
    pub fn heartbeat(&self) -> Heartbeat {
        if self.role() == Role::Root {
            return Heartbeat {
                root_index: self.applied(),
                root_time_ms: now_ms(),
                received: Some(Instant::now()),
            };
        }
        self.heartbeat.lock().unwrap().clone()
    }

    /// Reads keys at a single consistent index. Absent keys are `None`.
    pub async fn get(
        &self,
        keys: Vec<Vec<u8>>,
        opts: ReadOpts,
    ) -> NodeResult<(Index, Vec<Option<Record>>)> {
        self.ensure_ready(&opts).await?;
        Metrics::inc(&self.metrics.reads, keys.len() as u64);
        let (index, mut records) = self.store.read_latest(&keys)?;
        if let Some(proxy) = &self.proxy {
            self.fill_misses(proxy, index, &keys, &mut records).await?;
        }
        let records = records
            .into_iter()
            .map(|r| r.filter(|r| !r.is_tombstone()))
            .collect();
        Ok((index, records))
    }

    /// Proxy read path. Every log entry is applied locally and keys written
    /// within the last `proxy_window` indexes are never evicted, so a key
    /// absent here was last written at or before `low`. Any replica that has
    /// reached `low` therefore holds the right value; replicas ahead of us
    /// answer from MVCC history at our index so reads never jump forward.
    async fn fill_misses(
        &self,
        proxy: &ProxyState,
        index: Index,
        keys: &[Vec<u8>],
        records: &mut [Option<Record>],
    ) -> NodeResult<()> {
        let missing: Vec<usize> = (0..keys.len()).filter(|&i| records[i].is_none()).collect();
        let hits = (keys.len() - missing.len()) as u64;
        Metrics::inc(&self.metrics.cache_hits, hits);
        {
            let mut cache = proxy.cache.lock().unwrap();
            for (k, r) in keys.iter().zip(records.iter()) {
                if r.is_some() {
                    cache.touch(k);
                }
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        Metrics::inc(&self.metrics.cache_misses, missing.len() as u64);
        let origin = self.store.state().origin;
        let low = origin.max(index.saturating_sub(self.cfg.proxy_window));
        let req = RpcReq::GetAt {
            keys: missing
                .iter()
                .map(|&i| ByteBuf::from(keys[i].clone()))
                .collect(),
            at: index,
            min: low,
        };
        let start = proxy.replicas.next_start();
        let mut last_err = String::from("no replicas configured");
        for attempt in 0..proxy.replicas.len() {
            let resp = proxy.replicas.call(start + attempt, &req, true).await;
            let raw = match resp {
                Ok(RpcResp::Values { records: raw, .. }) if raw.len() == missing.len() => raw,
                Ok(other) => {
                    last_err = format!("replica answered {other:?}");
                    continue;
                }
                Err(CallError::NotSent(e) | CallError::Lost(e)) => {
                    last_err = e.to_string();
                    continue;
                }
            };
            let mut fill = Vec::with_capacity(missing.len());
            for (&i, raw) in missing.iter().zip(raw) {
                let rec = match raw {
                    Some(raw) => match Record::decode(&keys[i], &raw) {
                        Ok(r) => r,
                        Err(e) => {
                            return Err(NodeError::Internal(format!(
                                "corrupt record from replica: {e}"
                            )));
                        }
                    },
                    // Negative entry: absent as of `index`.
                    None => Record {
                        index: 0,
                        value: None,
                    },
                };
                fill.push((keys[i].clone(), rec.clone()));
                records[i] = Some(rec);
            }
            self.applier.cache_fill(fill);
            return Ok(());
        }
        Metrics::inc(&self.metrics.upstream_fetch_errors, 1);
        Err(NodeError::Unavailable(format!(
            "no replica could serve index {index}: {last_err}"
        )))
    }

    pub async fn scan(
        &self,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
        opts: ReadOpts,
    ) -> NodeResult<(Index, Vec<(Vec<u8>, Record)>)> {
        self.ensure_ready(&opts).await?;
        let Some(proxy) = &self.proxy else {
            return Ok(self.store.scan(&prefix, after.as_deref(), limit)?);
        };
        // Proxies don't hold every key, so scans go to a replica that is at
        // least as new as this proxy.
        let req = RpcReq::Scan {
            prefix: ByteBuf::from(prefix),
            after: after.map(ByteBuf::from),
            limit: limit as u32,
            min: self.applied(),
        };
        let start = proxy.replicas.next_start();
        let mut last_err = String::from("no replicas configured");
        for attempt in 0..proxy.replicas.len() {
            match proxy.replicas.call(start + attempt, &req, true).await {
                Ok(RpcResp::Scanned { index, items }) => {
                    let items = items
                        .into_iter()
                        .map(|(k, raw)| {
                            let r = Record::decode(&k, &raw).map_err(|e| {
                                NodeError::Internal(format!("corrupt record from replica: {e}"))
                            })?;
                            Ok((k.into_vec(), r))
                        })
                        .collect::<NodeResult<_>>()?;
                    return Ok((index, items));
                }
                Ok(other) => last_err = format!("replica answered {other:?}"),
                Err(CallError::NotSent(e) | CallError::Lost(e)) => last_err = e.to_string(),
            }
        }
        Err(NodeError::Unavailable(last_err))
    }

    /// Commits a transaction. Non-root nodes forward it up the tree, and if
    /// `wait_local` is set, wait until the commit has replicated back here
    /// (read-your-writes on this node).
    pub async fn write(&self, txn: Txn, wait_local: bool) -> NodeResult<Index> {
        let res = match &self.writes {
            None => self.applier.write(txn).await,
            Some(pool) => forward_write(pool, txn).await,
        };
        match &res {
            Ok(_) => Metrics::inc(&self.metrics.writes, 1),
            Err(WriteError::Conflict { .. }) => Metrics::inc(&self.metrics.write_conflicts, 1),
            Err(_) => {}
        }
        let index = res?;
        if wait_local && self.writes.is_some() {
            self.wait_for(index, self.default_wait()).await;
        }
        Ok(index)
    }

    pub fn status(&self) -> Status {
        let st = self.store.state();
        let hb = self.heartbeat();
        let root_index = hb.received.map(|_| hb.root_index);
        let cache = self.proxy.as_ref().map(|p| {
            let c = p.cache.lock().unwrap();
            (c.len(), c.bytes(), c.evictions, c.rejected_fills)
        });
        let is_proxy = cache.is_some();
        Status {
            role: self.role(),
            node_id: self.store.node_id().to_string(),
            db_id: st.db_id.map(|d| d.to_string()),
            applied_index: st.applied,
            applied_hash: hash_hex(&st.hash),
            log_floor: st.log_floor,
            history_floor: st.history_floor,
            origin: st.origin,
            root_index,
            lag_entries: root_index.map(|r| r.saturating_sub(st.applied)),
            heartbeat_age_ms: hb.received.map(|t| t.elapsed().as_millis() as u64),
            upstream: self.upstream.lock().unwrap().clone(),
            cached_keys: cache.map(|c| c.0),
            cache_bytes: cache.map(|c| c.1),
            cache_soft_limit: is_proxy.then_some(self.cfg.proxy_soft_limit),
            cache_hard_limit: is_proxy.then_some(self.cfg.proxy_hard_limit),
            cache_evictions: cache.map(|c| c.2),
            cache_rejected_fills: cache.map(|c| c.3),
            disk_free_bytes: free_disk_bytes(&self.cfg.data_dir),
        }
    }
}

async fn forward_write(pool: &RpcPool, txn: Txn) -> Result<Index, WriteError> {
    let req = RpcReq::Write { txn };
    let mut last = String::from("no upstreams configured");
    for i in 0..pool.len() {
        // Fresh connection per write: retrying is only safe if the request
        // provably never left this node.
        match pool.call(i, &req, false).await {
            Ok(RpcResp::Committed { index }) => return Ok(index),
            Ok(RpcResp::WriteFailed { error }) => return Err(error),
            Ok(other) => {
                return Err(WriteError::Unavailable(format!(
                    "unexpected reply {other:?}"
                )));
            }
            Err(CallError::NotSent(e)) => last = format!("{}: {e}", pool.addrs[i]),
            Err(CallError::Lost(e)) => return Err(WriteError::Unknown(e.to_string())),
        }
    }
    Err(WriteError::Unavailable(last))
}
