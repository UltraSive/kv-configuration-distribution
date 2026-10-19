//! TLS setup.
//!
//! - Peer connections (replication + RPC) use mutual TLS: every node
//!   presents a certificate signed by the cluster CA and verifies the other
//!   side against it. Node certificates therefore need both the serverAuth
//!   and clientAuth extended key usages, and a SAN matching the address
//!   other nodes dial (DNS name or IP).
//! - The HTTP API uses ordinary server TLS; clients authenticate with
//!   bearer tokens (see [`crate::auth`]).

use crate::config::Config;
use anyhow::{Context, anyhow, bail};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
pub struct TlsSettings {
    /// HTTPS for the client API.
    pub http: Option<TlsAcceptor>,
    /// mTLS for inbound peer connections.
    pub peer_acceptor: Option<TlsAcceptor>,
    /// mTLS for outbound peer connections.
    pub peer_connector: Option<TlsConnector>,
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn read(path: &Path) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("reading {}", path.display()))
}

fn load_certs(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_slice_iter(&read(path)?)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parsing certificates in {}", path.display()))?;
    if certs.is_empty() {
        bail!("no certificates in {}", path.display());
    }
    Ok(certs)
}

fn load_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_slice(&read(path)?)
        .with_context(|| format!("parsing private key in {}", path.display()))
}

impl TlsSettings {
    pub fn load(cfg: &Config) -> anyhow::Result<Self> {
        let (Some(cert_path), Some(key_path)) = (&cfg.tls_cert, &cfg.tls_key) else {
            return Ok(Self::default());
        };
        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;

        let mut http = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(certs.clone(), key.clone_key())
            .context("invalid TLS certificate or key")?;
        http.alpn_protocols = vec![b"http/1.1".to_vec()];
        let mut settings = Self {
            http: Some(TlsAcceptor::from(Arc::new(http))),
            ..Self::default()
        };

        let Some(ca_path) = &cfg.tls_ca else {
            return Ok(settings);
        };
        let mut roots = RootCertStore::empty();
        for ca in load_certs(ca_path)? {
            roots.add(ca).context("invalid CA certificate")?;
        }
        let roots = Arc::new(roots);

        let verifier =
            WebPkiClientVerifier::builder_with_provider(roots.clone(), provider()).build()?;
        let peer_server = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.clone(), key.clone_key())?;
        let peer_client = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_client_auth_cert(certs, key)?;
        settings.peer_acceptor = Some(TlsAcceptor::from(Arc::new(peer_server)));
        settings.peer_connector = Some(TlsConnector::from(Arc::new(peer_client)));
        Ok(settings)
    }
}

/// The name a peer's certificate must match: the host part of `host:port`.
pub fn server_name(addr: &str) -> anyhow::Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    ServerName::try_from(host.to_string())
        .map_err(|_| anyhow!("invalid host in peer address {addr:?}"))
}

/// HTTPS listener for axum. Handshakes run in their own tasks so a slow or
/// malicious client can't stall `accept`.
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local: SocketAddr,
}

impl TlsListener {
    pub fn spawn(
        listener: TcpListener,
        acceptor: TlsAcceptor,
        cancel: CancellationToken,
        tracker: TaskTracker,
    ) -> std::io::Result<Self> {
        let local = listener.local_addr()?;
        let (tx, rx) = mpsc::channel(128);
        let inner = tracker.clone();
        tracker.spawn(async move {
            loop {
                let (tcp, peer) = tokio::select! {
                    _ = cancel.cancelled() => return,
                    r = listener.accept() => match r {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::warn!("https accept failed: {e}");
                            continue;
                        }
                    },
                };
                let acceptor = acceptor.clone();
                let tx = tx.clone();
                inner.spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                        Ok(Ok(tls)) => {
                            let _ = tx.send((tls, peer)).await;
                        }
                        Ok(Err(e)) => tracing::debug!("https handshake with {peer} failed: {e}"),
                        Err(_) => tracing::debug!("https handshake with {peer} timed out"),
                    }
                });
            }
        });
        Ok(Self { rx, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(conn) => conn,
            // Shutting down: graceful shutdown stops the server.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}
