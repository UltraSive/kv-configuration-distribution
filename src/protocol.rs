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
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::{TlsAcceptor, TlsConnector};
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

/// A peer connection: plain TCP, or mutual TLS when a cluster CA is configured.
pub enum PeerStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::TlsStream<TcpStream>>),
}

impl AsyncRead for PeerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_flush(cx),
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            PeerStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

pub type Conn = Framed<PeerStream, LengthDelimitedCodec>;

/// Frames are a 4-byte big-endian length followed by a postcard message.
/// Used for both peer TCP connections and the local Unix socket.
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME)
        .new_codec()
}

/// Dials a peer, performing the TLS handshake if `tls` is set.
pub async fn connect(addr: &str, tls: Option<&TlsConnector>, limit: Duration) -> Result<Conn> {
    let dial = async {
        let tcp = TcpStream::connect(addr).await?;
        let _ = tcp.set_nodelay(true);
        let stream = match tls {
            None => PeerStream::Plain(tcp),
            Some(connector) => {
                let name =
                    crate::tls::server_name(addr).map_err(|e| Error::Protocol(e.to_string()))?;
                PeerStream::Tls(Box::new(connector.connect(name, tcp).await?.into()))
            }
        };
        Ok(Framed::new(stream, codec()))
    };
    timeout(limit, dial)
        .await
        .map_err(|_| Error::Protocol(format!("connect to {addr} timed out")))?
}

/// Accepts a peer, requiring a client certificate if `tls` is set.
pub async fn accept(tcp: TcpStream, tls: Option<&TlsAcceptor>) -> Result<Conn> {
    let _ = tcp.set_nodelay(true);
    let stream = match tls {
        None => PeerStream::Plain(tcp),
        Some(acceptor) => {
            let tls = timeout(Duration::from_secs(10), acceptor.accept(tcp))
                .await
                .map_err(|_| Error::Protocol("tls handshake timed out".into()))??;
            PeerStream::Tls(Box::new(tls.into()))
        }
    };
    Ok(Framed::new(stream, codec()))
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
