//! Client side of the peer RPC channel, with a small per-address pool.

use crate::error::{Error, Result};
use crate::protocol::{self, Conn, Hello, PROTOCOL_VERSION, RpcReq, RpcResp};
use crate::types::NodeId;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CALL_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_IDLE: usize = 16;

/// Why a call failed, which matters for whether a write may be retried.
#[derive(Debug)]
pub enum CallError {
    /// The request was never sent; safe to retry anywhere.
    NotSent(Error),
    /// The request may have been processed.
    Lost(Error),
}

pub struct RpcPool {
    pub addrs: Vec<String>,
    node_id: NodeId,
    idle: Vec<Mutex<Vec<Conn>>>,
    next: AtomicUsize,
}

impl RpcPool {
    pub fn new(addrs: Vec<String>, node_id: NodeId) -> Self {
        let idle = addrs.iter().map(|_| Mutex::new(Vec::new())).collect();
        Self {
            addrs,
            node_id,
            idle,
            next: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.addrs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.addrs.is_empty()
    }

    /// Round-robin starting point for spreading reads.
    pub fn next_start(&self) -> usize {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    async fn connect(&self, i: usize) -> Result<Conn> {
        let addr = &self.addrs[i];
        let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Protocol(format!("connect to {addr} timed out")))??;
        let mut conn = protocol::framed(stream);
        protocol::send(
            &mut conn,
            &Hello::Rpc {
                proto: PROTOCOL_VERSION,
                node_id: self.node_id,
            },
        )
        .await?;
        Ok(conn)
    }

    /// Calls peer `i`. `pooled` reuses idle connections (only safe for
    /// idempotent requests, since a stale connection fails ambiguously).
    pub async fn call(&self, i: usize, req: &RpcReq, pooled: bool) -> Result<RpcResp, CallError> {
        let i = i % self.addrs.len();
        let reused = if pooled {
            self.idle[i].lock().unwrap().pop()
        } else {
            None
        };
        let mut conn = match reused {
            Some(c) => c,
            None => self.connect(i).await.map_err(CallError::NotSent)?,
        };
        let res = timeout(CALL_TIMEOUT, async {
            protocol::send(&mut conn, req).await?;
            protocol::recv::<RpcResp>(&mut conn)
                .await?
                .ok_or_else(|| Error::Protocol("connection closed".into()))
        })
        .await
        .unwrap_or_else(|_| Err(Error::Protocol("rpc timed out".into())));
        match res {
            Ok(resp) => {
                let mut idle = self.idle[i].lock().unwrap();
                if idle.len() < MAX_IDLE {
                    idle.push(conn);
                }
                Ok(resp)
            }
            Err(e) => Err(CallError::Lost(e)),
        }
    }
}
