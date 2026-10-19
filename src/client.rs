//! Client for the local Unix socket.
//!
//! ```no_run
//! # async fn demo() -> Result<(), kvdist::socket::ClientError> {
//! let mut kv = kvdist::client::LocalClient::connect("/run/kvdist.sock").await?;
//! let index = kv.set("service/ttl", "300").await?;
//! let (_, item) = kv.get_one("service/ttl", index).await?;
//! assert_eq!(item.unwrap().value.as_slice(), b"300");
//! # Ok(()) }
//! ```

use crate::node::Status;
use crate::protocol;
use crate::socket::{Change, ClientError, Item, LocalReq, LocalResp};
use crate::types::*;
use serde_bytes::ByteBuf;
use std::path::Path;
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct LocalClient {
    conn: Framed<UnixStream, LengthDelimitedCodec>,
}

fn io(e: impl std::fmt::Display) -> ClientError {
    ClientError::Io(e.to_string())
}

fn unexpected(resp: LocalResp) -> ClientError {
    ClientError::Protocol(format!("unexpected response {resp:?}"))
}

impl LocalClient {
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let stream = UnixStream::connect(path).await.map_err(io)?;
        Ok(Self {
            conn: Framed::new(stream, protocol::codec()),
        })
    }

    pub async fn call(&mut self, req: &LocalReq) -> Result<LocalResp, ClientError> {
        protocol::send(&mut self.conn, req).await.map_err(io)?;
        match protocol::recv::<_, LocalResp>(&mut self.conn)
            .await
            .map_err(io)?
        {
            None => Err(ClientError::Io("server closed the connection".into())),
            Some(LocalResp::Error(e)) => Err(e),
            Some(resp) => Ok(resp),
        }
    }

    /// Reads several keys at one consistent index (absent keys are `None`).
    pub async fn get<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        min_index: Index,
    ) -> Result<(Index, Vec<Option<Item>>), ClientError> {
        let keys = keys.iter().map(|k| ByteBuf::from(k.as_ref())).collect();
        match self.call(&LocalReq::Get { keys, min_index }).await? {
            LocalResp::Values { index, items } => Ok((index, items)),
            other => Err(unexpected(other)),
        }
    }

    pub async fn get_one(
        &mut self,
        key: impl AsRef<[u8]>,
        min_index: Index,
    ) -> Result<(Index, Option<Item>), ClientError> {
        let (index, mut items) = self.get(&[key], min_index).await?;
        Ok((index, items.pop().flatten()))
    }

    /// Commits a transaction and waits until it is visible on this node.
    pub async fn txn(&mut self, txn: Txn) -> Result<Index, ClientError> {
        match self.call(&LocalReq::Txn { txn, wait: true }).await? {
            LocalResp::Committed { index } => Ok(index),
            other => Err(unexpected(other)),
        }
    }

    pub async fn set(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Index, ClientError> {
        let op = Op::Set {
            key: key.into(),
            value: value.into(),
        };
        self.txn(Txn {
            checks: vec![],
            ops: vec![op],
        })
        .await
    }

    pub async fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<Index, ClientError> {
        let op = Op::Delete { key: key.into() };
        self.txn(Txn {
            checks: vec![],
            ops: vec![op],
        })
        .await
    }

    /// Live keys under `prefix`, strictly after `after`.
    pub async fn scan(
        &mut self,
        prefix: impl Into<Vec<u8>>,
        after: Option<Vec<u8>>,
        limit: u32,
        min_index: Index,
    ) -> Result<(Index, Vec<(Vec<u8>, Item)>), ClientError> {
        let req = LocalReq::Scan {
            prefix: ByteBuf::from(prefix.into()),
            after: after.map(ByteBuf::from),
            limit,
            min_index,
        };
        match self.call(&req).await? {
            LocalResp::Scanned { index, items } => Ok((
                index,
                items.into_iter().map(|(k, v)| (k.into_vec(), v)).collect(),
            )),
            other => Err(unexpected(other)),
        }
    }

    pub async fn status(&mut self) -> Result<Status, ClientError> {
        match self.call(&LocalReq::Status).await? {
            LocalResp::Status(s) => Ok(*s),
            other => Err(unexpected(other)),
        }
    }

    /// Streams changes under `prefixes` (all keys if empty) from index
    /// `from`, or from the next commit if `from` is 0. The usual pattern is
    /// to `scan` first and then watch from the scan's index + 1, so nothing
    /// is missed in between. Takes over the connection.
    pub async fn watch<P: AsRef<[u8]>>(
        mut self,
        prefixes: &[P],
        from: Index,
    ) -> Result<Watcher, ClientError> {
        let prefixes = prefixes.iter().map(|p| ByteBuf::from(p.as_ref())).collect();
        match self.call(&LocalReq::Watch { prefixes, from }).await? {
            LocalResp::Watching { index } => Ok(Watcher {
                conn: self.conn,
                index,
            }),
            other => Err(unexpected(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    /// The matching changes of one committed transaction.
    Changes {
        index: Index,
        timestamp_ms: u64,
        changes: Vec<Change>,
    },
    /// The node's data was replaced at `index`: re-read the keys you care
    /// about (e.g. `scan` with `min_index: index`); changes after it follow.
    Resync { index: Index },
}

/// A watch stream from [`LocalClient::watch`].
pub struct Watcher {
    conn: Framed<UnixStream, LengthDelimitedCodec>,
    index: Index,
}

impl Watcher {
    /// Everything up to and including this index has been delivered.
    pub fn index(&self) -> Index {
        self.index
    }

    /// Waits for the next change. `Err(ClientError::Gone)` means the
    /// history needed to continue was dropped: re-read and watch again.
    pub async fn next(&mut self) -> Result<WatchEvent, ClientError> {
        loop {
            let resp = protocol::recv::<_, LocalResp>(&mut self.conn)
                .await
                .map_err(io)?
                .ok_or_else(|| ClientError::Io("server closed the connection".into()))?;
            match resp {
                LocalResp::Progress { index } => self.index = index,
                LocalResp::Changes {
                    index,
                    timestamp_ms,
                    changes,
                } => {
                    self.index = index;
                    return Ok(WatchEvent::Changes {
                        index,
                        timestamp_ms,
                        changes,
                    });
                }
                LocalResp::Resync { index } => {
                    self.index = index;
                    return Ok(WatchEvent::Resync { index });
                }
                LocalResp::Error(e) => return Err(e),
                other => return Err(unexpected(other)),
            }
        }
    }
}
