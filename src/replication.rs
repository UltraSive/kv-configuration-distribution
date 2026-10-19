//! Pull-based log replication, modelled on Quicksilver:
//!
//! - downstream connects and says `{node_id, db_id, last_index, last_hash}`
//! - upstream checks identity, verifies the hash at `last_index` and streams
//!   from `last_index + 1`, or sends a consistent snapshot if the log no
//!   longer reaches back that far
//! - every entry is checked for contiguity and hash-chain continuity before
//!   it is applied
//! - heartbeats carry the root's head index and clock down the tree

use crate::config::Role;
use crate::error::{Error, Result};
use crate::node::{Metrics, Node, ReadOpts};
use crate::protocol::{self, Conn, Down, Hello, PROTOCOL_VERSION, RejectReason, RpcReq, RpcResp};
use crate::store::AtRead;
use crate::types::*;
use futures::FutureExt;
use futures::StreamExt;
use serde_bytes::ByteBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{Instant, interval, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const HEARTBEAT: Duration = Duration::from_secs(1);
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ENTRIES_PER_FRAME: usize = 1024;
const MAX_FRAME_BYTES: usize = 4 << 20;
const SNAPSHOT_CHUNK_BYTES: usize = 1 << 20;
const MAX_APPLY_BATCH: usize = 8192;

// ---- upstream side -----------------------------------------------------------

pub async fn serve(
    node: Arc<Node>,
    listener: TcpListener,
    cancel: CancellationToken,
    tracker: TaskTracker,
) {
    loop {
        let stream = tokio::select! {
            _ = cancel.cancelled() => return,
            r = listener.accept() => match r {
                Ok((s, _)) => s,
                Err(e) => {
                    tracing::warn!("peer accept failed: {e}");
                    continue;
                }
            },
        };
        let node = node.clone();
        let cancel = cancel.clone();
        tracker.spawn(async move {
            let peer = stream
                .peer_addr()
                .map(|a| a.to_string())
                .unwrap_or_default();
            tokio::select! {
                _ = cancel.cancelled() => {}
                r = handle_conn(node, stream) => if let Err(e) = r {
                    tracing::debug!("peer {peer}: {e}");
                },
            }
        });
    }
}

async fn handle_conn(node: Arc<Node>, stream: TcpStream) -> Result<()> {
    let (mut conn, names) = protocol::accept(stream, node.tls.peer_acceptor.as_ref()).await?;
    let hello: Hello = timeout(IDLE_TIMEOUT, protocol::recv(&mut conn))
        .await
        .map_err(|_| Error::Protocol("no hello".into()))??
        .ok_or_else(|| Error::Protocol("closed before hello".into()))?;
    match hello {
        Hello::Rpc { proto, .. } if proto == PROTOCOL_VERSION => {
            let writer = node
                .peer_may_write(names.as_deref())
                .then_some(())
                .ok_or_else(|| names.unwrap_or_default().join(", "));
            serve_rpc(&node, conn, writer).await
        }
        Hello::Replicate {
            proto,
            node_id,
            db_id,
            last_index,
            last_hash,
            allow_snapshot,
            newest_only,
        } if proto == PROTOCOL_VERSION => {
            let req = Downstream {
                node_id,
                db_id,
                last_index,
                last_hash,
                allow_snapshot,
                newest_only,
            };
            serve_replication(&node, conn, req).await
        }
        _ => {
            reject(
                &mut conn,
                RejectReason::Protocol,
                "unsupported protocol version",
            )
            .await
        }
    }
}

async fn reject(conn: &mut Conn, reason: RejectReason, message: impl Into<String>) -> Result<()> {
    let message = message.into();
    protocol::send(
        conn,
        &Down::Rejected {
            reason,
            message: message.clone(),
        },
    )
    .await?;
    Err(Error::Protocol(format!(
        "rejected downstream ({reason:?}): {message}"
    )))
}

struct Downstream {
    node_id: NodeId,
    db_id: Option<DbId>,
    last_index: Index,
    last_hash: Hash,
    allow_snapshot: bool,
    newest_only: bool,
}

async fn serve_replication(node: &Arc<Node>, mut conn: Conn, req: Downstream) -> Result<()> {
    if node.role() == Role::Proxy {
        return reject(
            &mut conn,
            RejectReason::NotReady,
            "proxies do not serve replication",
        )
        .await;
    }
    let st = node.store.state();
    let Some(db_id) = st.db_id else {
        return reject(
            &mut conn,
            RejectReason::NotReady,
            "upstream has no data yet",
        )
        .await;
    };
    if req.node_id == node.store.node_id() {
        return reject(
            &mut conn,
            RejectReason::SelfReplication,
            "refusing to replicate to myself",
        )
        .await;
    }
    if let Some(theirs) = req.db_id
        && theirs != db_id
    {
        return reject(
            &mut conn,
            RejectReason::DbMismatch,
            format!("upstream db is {db_id}, downstream has {theirs}"),
        )
        .await;
    }
    protocol::send(
        &mut conn,
        &Down::Welcome {
            db_id,
            node_id: node.store.node_id(),
            head: st.applied,
        },
    )
    .await?;

    let next = if req.newest_only || (req.db_id.is_none() && !req.allow_snapshot) {
        let st = node.store.state();
        protocol::send(
            &mut conn,
            &Down::StartAt {
                index: st.applied,
                hash: st.hash,
            },
        )
        .await?;
        st.applied + 1
    } else if req.db_id.is_none() {
        send_snapshot(node, &mut conn).await? + 1
    } else {
        if req.last_index > node.applied()
            && !node.wait_for(req.last_index, Duration::from_secs(5)).await
        {
            return reject(
                &mut conn,
                RejectReason::Behind,
                format!(
                    "upstream is at {}, downstream at {}",
                    node.applied(),
                    req.last_index
                ),
            )
            .await;
        }
        match node.store.hash_at(req.last_index)? {
            Some(h) if h == req.last_hash => req.last_index + 1,
            Some(_) => {
                return reject(
                    &mut conn,
                    RejectReason::Diverged,
                    format!("log diverges at index {}", req.last_index),
                )
                .await;
            }
            None if req.allow_snapshot => send_snapshot(node, &mut conn).await? + 1,
            None => {
                return reject(
                    &mut conn,
                    RejectReason::Gap,
                    format!("index {} is older than the retained log", req.last_index),
                )
                .await;
            }
        }
    };
    stream_log(node, conn, next).await
}

/// Streams a consistent snapshot; returns the index it represents.
async fn send_snapshot(node: &Arc<Node>, conn: &mut Conn) -> Result<Index> {
    let (tx, mut rx) = mpsc::channel::<Down>(4);
    let store = node.store.clone();
    let producer = tokio::task::spawn_blocking(move || {
        let begin = tx.clone();
        store.export_snapshot(
            SNAPSHOT_CHUNK_BYTES,
            |index, hash| {
                begin
                    .blocking_send(Down::SnapshotBegin { index, hash })
                    .is_ok()
            },
            |items| {
                let items = items
                    .into_iter()
                    .map(|(k, v)| (ByteBuf::from(k), ByteBuf::from(v)))
                    .collect();
                tx.blocking_send(Down::SnapshotChunk { items }).is_ok()
            },
        )
    });
    let mut index = None;
    while let Some(msg) = rx.recv().await {
        if let Down::SnapshotBegin { index: i, .. } = &msg {
            index = Some(*i);
        }
        protocol::send(conn, &msg).await?;
    }
    producer
        .await
        .map_err(|e| Error::Protocol(e.to_string()))??;
    let index = index.ok_or_else(|| Error::Protocol("snapshot produced nothing".into()))?;
    protocol::send(conn, &Down::SnapshotEnd).await?;
    Metrics::inc(&node.metrics.snapshots_served, 1);
    tracing::info!("served snapshot at index {index}");
    Ok(index)
}

async fn stream_log(node: &Arc<Node>, mut conn: Conn, mut next: Index) -> Result<()> {
    let mut applied = node.applied_watch();
    let mut ticker = interval(HEARTBEAT);
    let mut last_hb = Instant::now();
    let db_id = node.store.state().db_id;
    loop {
        let st = node.store.state();
        if st.db_id != db_id {
            // This node took a snapshot of another database (--resync): the
            // downstream reconnects and finds out.
            return Err(Error::Protocol("database replaced; closing stream".into()));
        }
        if next < st.log_floor {
            return reject(
                &mut conn,
                RejectReason::Gap,
                "downstream fell behind the retained log",
            )
            .await;
        }
        if next <= st.applied {
            let entries = node
                .store
                .log_range(next, MAX_ENTRIES_PER_FRAME, MAX_FRAME_BYTES)?;
            if entries.is_empty() {
                return Err(Error::Protocol(format!("log entry {next} missing")));
            }
            next += entries.len() as u64;
            protocol::send(&mut conn, &Down::Entries { entries }).await?;
            if last_hb.elapsed() >= HEARTBEAT {
                send_heartbeat(node, &mut conn).await?;
                last_hb = Instant::now();
            }
            continue;
        }
        tokio::select! {
            r = applied.changed() => if r.is_err() { return Ok(()) },
            _ = ticker.tick() => {
                send_heartbeat(node, &mut conn).await?;
                last_hb = Instant::now();
            }
        }
    }
}

async fn send_heartbeat(node: &Node, conn: &mut Conn) -> Result<()> {
    let hb = node.heartbeat();
    if hb.received.is_none() {
        return Ok(());
    }
    protocol::send(
        conn,
        &Down::Heartbeat {
            root_index: hb.root_index,
            root_time_ms: hb.root_time_ms,
        },
    )
    .await
}

/// Serves a peer's RPCs. `writer` is `Err(its names)` if it may not write.
async fn serve_rpc(
    node: &Arc<Node>,
    mut conn: Conn,
    writer: std::result::Result<(), String>,
) -> Result<()> {
    while let Some(req) = protocol::recv::<_, RpcReq>(&mut conn).await? {
        let resp = match (&req, &writer) {
            (RpcReq::Write { .. }, Err(names)) => {
                tracing::warn!("refused a write from peer [{names}]: not a --peer-writer");
                RpcResp::WriteFailed {
                    error: crate::error::WriteError::Forbidden(format!(
                        "peer [{names}] may not write here"
                    )),
                }
            }
            _ => handle_rpc(node, req).await,
        };
        protocol::send(&mut conn, &resp).await?;
    }
    Ok(())
}

async fn handle_rpc(node: &Arc<Node>, req: RpcReq) -> RpcResp {
    let wait = Duration::from_secs(2);
    match req {
        RpcReq::Write { txn } => match node.write(txn, false).await {
            Ok(index) => RpcResp::Committed { index },
            Err(crate::node::NodeError::Write(error)) => RpcResp::WriteFailed { error },
            Err(e) => RpcResp::Error {
                message: e.to_string(),
            },
        },
        _ if node.role() == Role::Proxy => RpcResp::Error {
            message: "proxies do not serve reads to peers".into(),
        },
        RpcReq::GetAt { keys, at, min } => {
            if node.store.state().db_id.is_none() || !node.wait_for(min, wait).await {
                return RpcResp::Behind {
                    index: node.applied(),
                };
            }
            let keys: Vec<Vec<u8>> = keys.into_iter().map(ByteBuf::into_vec).collect();
            match node.store.get_at(&keys, at) {
                Ok(AtRead::Ok { index, records }) => RpcResp::Values {
                    index,
                    records: records
                        .into_iter()
                        .zip(&keys)
                        .map(|(r, k)| r.map(|r| ByteBuf::from(r.encode(k))))
                        .collect(),
                },
                Ok(AtRead::HistoryUnavailable { floor }) => RpcResp::HistoryUnavailable { floor },
                Err(e) => RpcResp::Error {
                    message: e.to_string(),
                },
            }
        }
        RpcReq::Scan {
            prefix,
            after,
            limit,
            min,
        } => {
            let opts = ReadOpts {
                min_index: min,
                wait,
            };
            match node
                .scan(
                    prefix.into_vec(),
                    after.map(ByteBuf::into_vec),
                    limit as usize,
                    opts,
                )
                .await
            {
                Ok((index, items)) => RpcResp::Scanned {
                    index,
                    items: items
                        .into_iter()
                        .map(|(k, r)| {
                            let raw = r.encode(&k);
                            (ByteBuf::from(k), ByteBuf::from(raw))
                        })
                        .collect(),
                },
                Err(crate::node::NodeError::Timeout { index, .. }) => RpcResp::Behind { index },
                Err(e) => RpcResp::Error {
                    message: e.to_string(),
                },
            }
        }
    }
}

// ---- downstream side ---------------------------------------------------------

/// Follows the configured upstreams forever, failing over in order.
pub async fn follow(node: Arc<Node>, cancel: CancellationToken) {
    let ups = node.cfg.upstreams.clone();
    let mut i = 0usize;
    let mut backoff = Duration::from_millis(100);
    loop {
        let addr = &ups[i % ups.len()];
        let started = Instant::now();
        let res = tokio::select! {
            _ = cancel.cancelled() => return,
            r = follow_once(&node, addr) => r,
        };
        *node.upstream.lock().unwrap() = None;
        if let Err(e) = res {
            Metrics::inc(&node.metrics.replication_errors, 1);
            tracing::warn!("replication from {addr} stopped: {e}");
        }
        if started.elapsed() > Duration::from_secs(30) {
            backoff = Duration::from_millis(100);
        } else {
            i += 1;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}

async fn follow_once(node: &Arc<Node>, addr: &str) -> Result<()> {
    let tls = node.tls.peer_connector.as_ref();
    let mut conn = protocol::connect(addr, tls, Duration::from_secs(3)).await?;
    let st = node.store.state();
    let is_proxy = node.role() == Role::Proxy;
    // Resyncing: present as a new node, so the upstream sends a snapshot (or,
    // to a proxy, its head).
    let resync = node.resync.load(Ordering::Relaxed);
    let (db, last_index, last_hash) = if resync {
        (None, 0, GENESIS_HASH)
    } else {
        (st.db_id, st.applied, st.hash)
    };
    protocol::send(
        &mut conn,
        &Hello::Replicate {
            proto: PROTOCOL_VERSION,
            node_id: node.store.node_id(),
            db_id: db,
            last_index,
            last_hash,
            allow_snapshot: !is_proxy,
            newest_only: is_proxy && db.is_none(),
        },
    )
    .await?;

    let mut db_id = None;
    let mut snapshot: Option<(Index, Hash)> = None;
    let mut queued: Option<Down> = None;
    loop {
        let msg = match queued.take() {
            Some(m) => m,
            None => match timeout(IDLE_TIMEOUT, protocol::recv::<_, Down>(&mut conn)).await {
                Err(_) => return Err(Error::Protocol("upstream idle; no heartbeat".into())),
                Ok(r) => r?.ok_or_else(|| Error::Protocol("upstream closed".into()))?,
            },
        };
        match msg {
            Down::Welcome {
                db_id: id,
                node_id,
                head,
            } => {
                tracing::info!(
                    "replicating from {addr} (node {node_id}, db {id}, head {head}, local {})",
                    node.applied()
                );
                db_id = Some(id);
                *node.upstream.lock().unwrap() = Some(addr.to_string());
            }
            Down::Rejected { reason, message } => {
                if node.cfg.resync
                    && matches!(reason, RejectReason::DbMismatch | RejectReason::Diverged)
                {
                    tracing::warn!(
                        "upstream {addr} has another history ({message}); \
                         replacing local data with its snapshot (--resync)"
                    );
                    node.resync.store(true, Ordering::Relaxed);
                }
                if is_proxy && reason == RejectReason::Gap {
                    // A proxy can't take a snapshot; drop the cache and restart from the head.
                    tracing::warn!("proxy fell behind upstream log; resetting cache");
                    node.applier.begin_bootstrap().await?;
                }
                return Err(Error::Protocol(format!(
                    "upstream rejected us ({reason:?}): {message}"
                )));
            }
            Down::SnapshotBegin { index, hash } => {
                tracing::info!("receiving snapshot at index {index}");
                node.applier.begin_bootstrap().await?;
                snapshot = Some((index, hash));
            }
            Down::SnapshotChunk { items } => {
                if snapshot.is_none() {
                    return Err(Error::Protocol("snapshot chunk outside snapshot".into()));
                }
                let items = items
                    .into_iter()
                    .map(|(k, v)| (k.into_vec(), v.into_vec()))
                    .collect();
                node.applier.snapshot_chunk(items).await?;
            }
            Down::SnapshotEnd => {
                let (index, hash) = snapshot
                    .take()
                    .ok_or_else(|| Error::Protocol("snapshot end without begin".into()))?;
                let id = db_id.ok_or_else(|| Error::Protocol("snapshot before welcome".into()))?;
                node.applier.finish_bootstrap(id, index, hash).await?;
                node.resync.store(false, Ordering::Relaxed);
                Metrics::inc(&node.metrics.snapshots_installed, 1);
                tracing::info!("snapshot installed at index {index}");
            }
            Down::StartAt { index, hash } => {
                let id = db_id.ok_or_else(|| Error::Protocol("start before welcome".into()))?;
                if resync || node.store.state().db_id.is_none() {
                    node.applier.begin_bootstrap().await?;
                    node.applier.finish_bootstrap(id, index, hash).await?;
                    node.resync.store(false, Ordering::Relaxed);
                }
            }
            Down::Entries { mut entries } => {
                // Coalesce frames that have already arrived into one commit.
                while entries.len() < MAX_APPLY_BATCH {
                    let Some(frame) = conn.next().now_or_never() else {
                        break;
                    };
                    let frame =
                        frame.ok_or_else(|| Error::Protocol("upstream closed".into()))??;
                    match protocol::decode::<Down>(&frame)? {
                        Down::Entries { entries: more } => entries.extend(more),
                        other => {
                            queued = Some(other);
                            break;
                        }
                    }
                }
                let n = entries.len() as u64;
                node.applier.apply(entries).await?;
                Metrics::inc(&node.metrics.entries_applied, n);
            }
            Down::Heartbeat {
                root_index,
                root_time_ms,
            } => node.record_heartbeat(root_index, root_time_ms),
        }
    }
}
