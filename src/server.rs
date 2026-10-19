//! Wires a [`Node`] to its listeners and background tasks.

use crate::config::{Config, Role};
use crate::node::Node;
use crate::tls::TlsListener;
use crate::{api, replication, socket};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub struct Running {
    pub node: Arc<Node>,
    pub http_addr: SocketAddr,
    pub peer_addr: Option<SocketAddr>,
    cancel: CancellationToken,
    tracker: TaskTracker,
    applier: JoinHandle<()>,
}

pub async fn start(mut cfg: Config) -> anyhow::Result<Running> {
    cfg.validate().map_err(anyhow::Error::msg)?;
    let (node, applier) = Node::open(cfg.clone())?;
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();

    let peer_addr = if cfg.peer_addr.is_empty() {
        None
    } else {
        let listener = TcpListener::bind(&cfg.peer_addr).await?;
        let addr = listener.local_addr()?;
        tracker.spawn(replication::serve(
            node.clone(),
            listener,
            cancel.clone(),
            tracker.clone(),
        ));
        Some(addr)
    };

    let listener = TcpListener::bind(&cfg.http_addr).await?;
    let http_addr = listener.local_addr()?;
    let router = api::router(node.clone());
    match node.tls.http.clone() {
        Some(acceptor) => {
            let tls = TlsListener::spawn(listener, acceptor, cancel.clone(), tracker.clone())?;
            spawn_http(&tracker, &cancel, tls, router.clone());
        }
        None => spawn_http(&tracker, &cancel, listener, router.clone()),
    }
    if let Some(path) = &cfg.http_socket {
        spawn_http(&tracker, &cancel, socket::bind(path)?, router);
    }

    if let Some(path) = &cfg.socket {
        let listener = socket::bind(path)?;
        tracker.spawn(socket::serve(
            node.clone(),
            listener,
            cancel.clone(),
            tracker.clone(),
        ));
    }

    if cfg.role != Role::Root {
        tracker.spawn(replication::follow(node.clone(), cancel.clone()));
    }

    // Retention pruning (root/replica) or cache eviction (proxy).
    let maint = node.clone();
    let maint_cancel = cancel.clone();
    tracker.spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = maint_cancel.cancelled() => return,
                _ = tick.tick() => maint.applier.maintain(),
            }
        }
    });

    if node.tokens.is_none() {
        tracing::warn!("HTTP API has no authentication; set --auth-tokens");
    }
    if cfg.tls_ca.is_none() && (peer_addr.is_some() || cfg.role != Role::Root) {
        tracing::warn!("peer connections are unencrypted and unauthenticated; set --tls-ca");
    }
    tracing::info!(
        "{:?} node {} serving {} on {http_addr}{}",
        cfg.role,
        node.store.node_id(),
        if node.tls.http.is_some() {
            "https"
        } else {
            "http"
        },
        peer_addr
            .map(|a| format!(", peers on {a}"))
            .unwrap_or_default()
    );
    Ok(Running {
        node,
        http_addr,
        peer_addr,
        cancel,
        tracker,
        applier,
    })
}

impl Running {
    /// Stops all tasks, then the applier, and flushes storage.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        self.tracker.close();
        self.tracker.wait().await;
        for path in [&self.node.cfg.socket, &self.node.cfg.http_socket]
            .into_iter()
            .flatten()
        {
            let _ = std::fs::remove_file(path);
        }
        // Dropping the last node handle closes the applier's queue.
        drop(self.node);
        let _ = tokio::task::spawn_blocking(move || self.applier.join()).await;
    }
}

fn spawn_http<L>(
    tracker: &TaskTracker,
    cancel: &CancellationToken,
    listener: L,
    router: axum::Router,
) where
    L: axum::serve::Listener,
    L::Addr: std::fmt::Debug,
{
    let shutdown = cancel.clone().cancelled_owned();
    tracker.spawn(async move {
        if let Err(e) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::error!("http server failed: {e}");
        }
    });
}
