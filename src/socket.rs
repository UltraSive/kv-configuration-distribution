//! Local Unix-socket API for services on the same machine.
//!
//! Each frame is a 4-byte big-endian length followed by a postcard-encoded
//! [`LocalReq`]; every request gets exactly one [`LocalResp`] frame, in
//! order, so clients may pipeline. Keys and values are raw bytes: unlike the
//! HTTP API nothing is re-encoded as JSON or base64.
//!
//! [`LocalReq::Watch`] is the exception: it turns the connection into a
//! one-way stream of changes (see [`watch`]) until either side closes it.
//!
//! New variants are only ever appended, so older clients keep working.

use crate::error::WriteError;
use crate::feed::FeedEvent;
use crate::node::{Node, NodeError, ReadOpts, Status};
use crate::protocol;
use crate::types::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixListener;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalReq {
    /// Values of `keys` at one consistent index, once this node has
    /// reached `min_index`.
    Get {
        keys: Vec<ByteBuf>,
        min_index: Index,
    },
    Scan {
        prefix: ByteBuf,
        after: Option<ByteBuf>,
        limit: u32,
        min_index: Index,
    },
    /// `wait`: return only once the commit is visible on this node.
    Txn {
        txn: Txn,
        wait: bool,
    },
    Status,
    /// Stream every committed change to keys under any of `prefixes` (all
    /// keys if empty), starting at index `from`, or with the next commit if
    /// `from` is 0. The connection then carries only watch frames.
    Watch {
        prefixes: Vec<ByteBuf>,
        from: Index,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub key: ByteBuf,
    /// `None` if the key was deleted.
    pub value: Option<ByteBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub write_index: Index,
    pub value: ByteBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalResp {
    Values {
        index: Index,
        items: Vec<Option<Item>>,
    },
    Scanned {
        index: Index,
        items: Vec<(ByteBuf, Item)>,
    },
    Committed {
        index: Index,
    },
    Status(Box<Status>),
    Error(ClientError),
    /// First frame of a watch: changes after `index` follow.
    Watching {
        index: Index,
    },
    /// The matching changes of one committed transaction.
    Changes {
        index: Index,
        timestamp_ms: u64,
        changes: Vec<Change>,
    },
    /// Sent when idle: nothing matching was committed up to `index`.
    Progress {
        index: Index,
    },
    /// This node's data was replaced wholesale (snapshot install or proxy
    /// reset) at `index`. Re-read what you need at `index`; changes after
    /// it follow.
    Resync {
        index: Index,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ClientError {
    #[error("check failed for key {key:?}: expected index {expected}, found {actual}")]
    Conflict {
        key: String,
        expected: Index,
        actual: Index,
    },
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("timed out waiting for index {wanted} (at {index})")]
    Timeout { index: Index, wanted: Index },
    #[error("not ready: {0}")]
    NotReady(String),
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("log entries before {floor} are no longer retained")]
    Gone { floor: Index },
    /// A forwarded write may or may not have committed.
    #[error("write outcome unknown: {0}")]
    UnknownOutcome(String),
    #[error("internal: {0}")]
    Internal(String),
    /// Client side: the socket failed.
    #[error("io: {0}")]
    Io(String),
    /// Client side: the server sent something unexpected.
    #[error("protocol: {0}")]
    Protocol(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
}

impl From<NodeError> for ClientError {
    fn from(e: NodeError) -> Self {
        match e {
            NodeError::Write(WriteError::Conflict {
                key,
                expected,
                actual,
            }) => ClientError::Conflict {
                key,
                expected,
                actual,
            },
            NodeError::Write(WriteError::Invalid(m)) | NodeError::BadRequest(m) => {
                ClientError::BadRequest(m)
            }
            NodeError::Write(WriteError::Unavailable(m)) | NodeError::Unavailable(m) => {
                ClientError::Unavailable(m)
            }
            NodeError::Write(WriteError::Unknown(m)) => ClientError::UnknownOutcome(m),
            NodeError::Write(WriteError::Storage(m)) | NodeError::Internal(m) => {
                ClientError::Internal(m)
            }
            NodeError::Timeout { index, wanted } => ClientError::Timeout { index, wanted },
            NodeError::NotReady(m) => ClientError::NotReady(m),
            NodeError::Gone { floor } => ClientError::Gone { floor },
            NodeError::Forbidden(m) | NodeError::Write(WriteError::Forbidden(m)) => {
                ClientError::Forbidden(m)
            }
        }
    }
}

fn item(r: Record) -> Item {
    Item {
        write_index: r.index,
        value: ByteBuf::from(r.value.unwrap_or_default()),
    }
}

pub async fn handle(node: &Node, req: LocalReq) -> LocalResp {
    let opts = |min_index| ReadOpts {
        min_index,
        wait: node.default_wait(),
    };
    let res = match req {
        LocalReq::Get { keys, min_index } => {
            let keys = keys.into_iter().map(ByteBuf::into_vec).collect();
            node.get(keys, opts(min_index))
                .await
                .map(|(index, records)| LocalResp::Values {
                    index,
                    items: records.into_iter().map(|r| r.map(item)).collect(),
                })
        }
        LocalReq::Scan {
            prefix,
            after,
            limit,
            min_index,
        } => {
            let limit = (limit as usize).clamp(1, 10_000);
            node.scan(
                prefix.into_vec(),
                after.map(ByteBuf::into_vec),
                limit,
                opts(min_index),
            )
            .await
            .map(|(index, items)| LocalResp::Scanned {
                index,
                items: items
                    .into_iter()
                    .map(|(k, r)| (ByteBuf::from(k), item(r)))
                    .collect(),
            })
        }
        LocalReq::Txn { txn, wait } => node
            .write(txn, wait)
            .await
            .map(|index| LocalResp::Committed { index }),
        LocalReq::Status => Ok(LocalResp::Status(Box::new(node.status()))),
        LocalReq::Watch { .. } => Err(NodeError::BadRequest(
            "watch needs a streaming connection".into(),
        )),
    };
    res.unwrap_or_else(|e| LocalResp::Error(e.into()))
}

/// Binds the socket, replacing a stale one, readable/writable by owner and group.
pub fn bind(path: &Path) -> std::io::Result<UnixListener> {
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;
    Ok(listener)
}

pub async fn serve(
    node: Arc<Node>,
    listener: UnixListener,
    cancel: CancellationToken,
    tracker: TaskTracker,
) {
    loop {
        let (stream, _) = tokio::select! {
            _ = cancel.cancelled() => return,
            r = listener.accept() => match r {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("socket accept failed: {e}");
                    continue;
                }
            },
        };
        let node = node.clone();
        let cancel = cancel.clone();
        tracker.spawn(async move {
            let mut conn = Framed::new(stream, protocol::codec());
            let work = async {
                while let Some(frame) = conn.next().await {
                    let resp = match protocol::decode::<LocalReq>(&frame?) {
                        Ok(LocalReq::Watch { prefixes, from }) => {
                            let prefixes = prefixes.into_iter().map(ByteBuf::into_vec).collect();
                            return watch(&node, &mut conn, prefixes, from).await;
                        }
                        Ok(req) => handle(&node, req).await,
                        // Framing is intact, so the connection can carry on.
                        Err(e) => LocalResp::Error(ClientError::BadRequest(format!(
                            "undecodable request: {e}"
                        ))),
                    };
                    protocol::send(&mut conn, &resp).await?;
                }
                Ok::<_, crate::error::Error>(())
            };
            tokio::select! {
                _ = cancel.cancelled() => {}
                r = work => if let Err(e) = r {
                    tracing::debug!("socket client: {e}");
                },
            }
        });
    }
}

const PROGRESS_EVERY: Duration = Duration::from_secs(5);
const CATCH_UP_BATCH: usize = 1000;

/// Entries from `next` on that were applied before the watcher went live:
/// from the log, or a proxy's in-memory tail. `Err(floor)` if `next` is no
/// longer available.
fn catch_up(node: &Node, next: Index) -> crate::error::Result<Result<Vec<LogEntry>, Index>> {
    let (entries, floor) = match node.feed.recent_from(next, CATCH_UP_BATCH) {
        Some(Err(oldest)) => return Ok(Err(oldest)),
        Some(Ok(entries)) => (entries, node.applied() + 1),
        None => (
            node.store.log_range(next, CATCH_UP_BATCH, 8 << 20)?,
            node.store.state().log_floor,
        ),
    };
    Ok(if entries.is_empty() {
        Err(floor)
    } else {
        Ok(entries)
    })
}

/// Serves a watch until the client hangs up. Every committed index is
/// accounted for in order: changes are sent per transaction, `Progress`
/// covers quiet periods, and if the history needed is gone the client gets
/// `Resync` (data replaced) or `Gone` (log truncated) and must re-read.
pub async fn watch<S>(
    node: &Node,
    conn: &mut Framed<S, LengthDelimitedCodec>,
    prefixes: Vec<Vec<u8>>,
    from: Index,
) -> crate::error::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Subscribe before reading `applied`, so nothing falls in between.
    let mut rx = node.feed.subscribe();
    let mut resets = node.feed.resets();
    let mut next = if from == 0 { node.applied() + 1 } else { from };
    protocol::send(conn, &LocalResp::Watching { index: next - 1 }).await?;

    let matching = |e: &LogEntry| -> Option<LocalResp> {
        let changes: Vec<Change> = e
            .ops
            .iter()
            .filter(|op| prefixes.is_empty() || prefixes.iter().any(|p| op.key().starts_with(p)))
            .map(|op| Change {
                key: ByteBuf::from(op.key()),
                value: op.value().map(ByteBuf::from),
            })
            .collect();
        (!changes.is_empty()).then_some(LocalResp::Changes {
            index: e.index,
            timestamp_ms: e.timestamp_ms,
            changes,
        })
    };

    'catch_up: loop {
        if node.feed.resets() > resets {
            // A reset was missed while lagging.
            resets = node.feed.resets();
            next = node.applied() + 1;
            protocol::send(conn, &LocalResp::Resync { index: next - 1 }).await?;
        }
        while next <= node.applied() {
            let entries = match catch_up(node, next)? {
                Ok(entries) => entries,
                Err(floor) => {
                    let gone = LocalResp::Error(ClientError::Gone { floor });
                    return protocol::send(conn, &gone).await;
                }
            };
            for e in &entries {
                if let Some(resp) = matching(e) {
                    protocol::send(conn, &resp).await?;
                }
                next = e.index + 1;
            }
        }

        let mut progress = tokio::time::interval(PROGRESS_EVERY);
        progress.reset();
        loop {
            tokio::select! {
                ev = rx.recv() => match ev {
                    Ok(FeedEvent::Entries(entries)) => {
                        for e in entries.iter() {
                            if e.index < next {
                                continue;
                            }
                            if e.index != next {
                                continue 'catch_up;
                            }
                            if let Some(resp) = matching(e) {
                                protocol::send(conn, &resp).await?;
                                progress.reset();
                            }
                            next += 1;
                        }
                    }
                    Ok(FeedEvent::Reset { index, seq }) => {
                        if seq > resets {
                            resets = seq;
                            next = index + 1;
                            protocol::send(conn, &LocalResp::Resync { index }).await?;
                            progress.reset();
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue 'catch_up,
                    Err(RecvError::Closed) => return Ok(()),
                },
                _ = progress.tick() => {
                    protocol::send(conn, &LocalResp::Progress { index: next - 1 }).await?;
                }
                // Watch connections take no further requests; this only
                // notices the client going away.
                frame = conn.next() => if !matches!(frame, Some(Ok(_))) {
                    return Ok(());
                },
            }
        }
    }
}
