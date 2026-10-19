//! Peer wire protocol: length-delimited postcard frames over TCP.
//!
//! A connection starts with a [`Hello`]. `Replicate` turns it into a
//! one-way stream of [`Down`] messages; `Rpc` turns it into a
//! request/response channel of [`RpcReq`]/[`RpcResp`].

use crate::error::{Error, Result, WriteError};
use crate::types::*;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_bytes::ByteBuf;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME: usize = 64 << 20;

#[derive(Debug, Serialize, Deserialize)]
pub enum Hello {
    Replicate {
        proto: u16,
        node_id: NodeId,
        /// `None` for a node with no data yet.
        db_id: Option<DbId>,
        last_index: Index,
        last_hash: Hash,
        /// May the upstream send a full snapshot if the log can't resume us?
        allow_snapshot: bool,
        /// Skip history: start at the upstream's current head (proxies).
        newest_only: bool,
    },
    Rpc {
        proto: u16,
        node_id: NodeId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    Protocol,
    SelfReplication,
    DbMismatch,
    /// Our log disagrees with the downstream's at its last index.
    Diverged,
    /// The downstream is too far behind and can't take a snapshot.
    Gap,
    /// This node can't serve replication right now.
    NotReady,
    /// The downstream is ahead of us; try another upstream.
    Behind,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Down {
    Welcome {
        db_id: DbId,
        node_id: NodeId,
        head: Index,
    },
    Rejected {
        reason: RejectReason,
        message: String,
    },
    SnapshotBegin {
        index: Index,
        hash: Hash,
    },
    SnapshotChunk {
        items: Vec<(ByteBuf, ByteBuf)>,
    },
    SnapshotEnd,
    /// Stream starts after `index` (newest_only).
    StartAt {
        index: Index,
        hash: Hash,
    },
    Entries {
        entries: Vec<LogEntry>,
    },
    /// Root head position and time, relayed down the tree for lag metrics.
    Heartbeat {
        root_index: Index,
        root_time_ms: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcReq {
    /// Values as of `min(at, upstream applied)`; fails with `Behind` if the
    /// upstream hasn't reached `min`.
    GetAt {
        keys: Vec<ByteBuf>,
        at: Index,
        min: Index,
    },
    Scan {
        prefix: ByteBuf,
        after: Option<ByteBuf>,
        limit: u32,
        min: Index,
    },
    Write {
        txn: Txn,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcResp {
    /// Raw encoded records; `None` means absent at `index`.
    Values {
        index: Index,
        records: Vec<Option<ByteBuf>>,
    },
    Scanned {
        index: Index,
        items: Vec<(ByteBuf, ByteBuf)>,
    },
    Behind {
        index: Index,
    },
    HistoryUnavailable {
        floor: Index,
    },
    Committed {
        index: Index,
    },
    WriteFailed {
        error: WriteError,
    },
    Error {
        message: String,
    },
}

pub type Conn = Framed<TcpStream, LengthDelimitedCodec>;

/// Frames are a 4-byte big-endian length followed by a postcard message.
/// Used for both peer TCP connections and the local Unix socket.
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_codec()
}

pub fn framed(stream: TcpStream) -> Conn {
    let _ = stream.set_nodelay(true);
    Framed::new(stream, codec())
}

pub async fn send<S, T>(conn: &mut Framed<S, LengthDelimitedCodec>, msg: &T) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = postcard::to_allocvec(msg)?;
    conn.send(bytes::Bytes::from(bytes)).await?;
    Ok(())
}

pub async fn recv<S, T>(conn: &mut Framed<S, LengthDelimitedCodec>) -> Result<Option<T>>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: DeserializeOwned,
{
    match conn.next().await {
        None => Ok(None),
        Some(Ok(frame)) => Ok(Some(postcard::from_bytes(&frame)?)),
        Some(Err(e)) => Err(Error::Io(e)),
    }
}

pub fn decode<T: DeserializeOwned>(frame: &[u8]) -> Result<T> {
    Ok(postcard::from_bytes(frame)?)
}
