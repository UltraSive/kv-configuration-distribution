//! Local Unix-socket API for services on the same machine.
//!
//! Each frame is a 4-byte big-endian length followed by a postcard-encoded
//! [`LocalReq`]; every request gets exactly one [`LocalResp`] frame, in
//! order, so clients may pipeline. Keys and values are raw bytes: unlike the
//! HTTP API nothing is re-encoded as JSON or base64.
//!
//! New variants are only ever appended, so older clients keep working.

use crate::error::WriteError;
use crate::node::{Node, NodeError, ReadOpts, Status};
use crate::protocol;
use crate::types::*;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio_util::codec::Framed;
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
