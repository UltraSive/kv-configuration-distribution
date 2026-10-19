//! mTLS between nodes, HTTPS, bearer tokens and the HTTP Unix socket.

use kvdist::config::{Config, Role};
use kvdist::node::ReadOpts;
use kvdist::server::{self, Running};
use kvdist::types::{Op, Txn};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

struct Ca {
    issuer: CertifiedIssuer<'static, KeyPair>,
    pem: PathBuf,
}

fn make_ca(dir: &Path, name: &str) -> Ca {
    let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    p.distinguished_name.push(DnType::CommonName, name);
    let issuer = CertifiedIssuer::self_signed(p, KeyPair::generate().unwrap()).unwrap();
    let pem = dir.join(format!("{name}.pem"));
    std::fs::write(&pem, issuer.pem()).unwrap();
    Ca { issuer, pem }
}

/// A node certificate valid for localhost/127.0.0.1, usable as server and client.
fn issue(dir: &Path, ca: &Ca, name: &str) -> (PathBuf, PathBuf) {
    let key = KeyPair::generate().unwrap();
    let mut p =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
    p.distinguished_name.push(DnType::CommonName, name);
    p.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let cert = p.signed_by(&key, &ca.issuer).unwrap();
    let (c, k) = (
        dir.join(format!("{name}.crt")),
        dir.join(format!("{name}.key")),
    );
    std::fs::write(&c, cert.pem()).unwrap();
    std::fs::write(&k, key.serialize_pem()).unwrap();
    (c, k)
}

fn cfg(role: Role, dir: &TempDir, upstreams: &[&Running]) -> Config {
    let mut c = Config::new(role, dir.path().join("data"));
    c.http_addr = "127.0.0.1:0".into();
    c.peer_addr = "127.0.0.1:0".into();
    c.upstreams = upstreams
        .iter()
        .map(|r| r.peer_addr.unwrap().to_string())
        .collect();
    c
}

fn with_tls(mut c: Config, cert: (PathBuf, PathBuf), ca: &Path) -> Config {
    c.tls_cert = Some(cert.0);
    c.tls_key = Some(cert.1);
    c.tls_ca = Some(ca.to_path_buf());
    c
}

fn set(key: &str, value: &str) -> Txn {
    Txn {
        checks: vec![],
        ops: vec![Op::Set {
            key: key.into(),
            value: value.into(),
        }],
    }
}

#[tokio::test]
async fn mutual_tls_between_nodes() {
    let pki = TempDir::new().unwrap();
    let ca = make_ca(pki.path(), "cluster-ca");
    let rogue_ca = make_ca(pki.path(), "rogue-ca");
    let dirs: Vec<TempDir> = (0..5).map(|_| TempDir::new().unwrap()).collect();

    let root = server::start(with_tls(
        cfg(Role::Root, &dirs[0], &[]),
        issue(pki.path(), &ca, "root"),
        &ca.pem,
    ))
    .await
    .unwrap();
    let idx = root.node.write(set("secret", "s3"), true).await.unwrap();

    let rep_cfg = with_tls(
        cfg(Role::Replica, &dirs[1], &[&root]),
        issue(pki.path(), &ca, "rep"),
        &ca.pem,
    );
    let rep = server::start(rep_cfg).await.unwrap();
    let proxy_cfg = with_tls(
        cfg(Role::Proxy, &dirs[2], &[&rep]),
        issue(pki.path(), &ca, "proxy"),
        &ca.pem,
    );
    let proxy = server::start(proxy_cfg).await.unwrap();

    // Replication, cache-miss fetches and write forwarding all run over mTLS.
    let opts = || ReadOpts {
        min_index: idx,
        wait: Duration::from_secs(10),
    };
    let (_, recs) = rep
        .node
        .get(vec![b"secret".to_vec()], opts())
        .await
        .unwrap();
    assert_eq!(recs[0].as_ref().unwrap().value.as_deref(), Some(&b"s3"[..]));
    let idx = proxy.node.write(set("via-proxy", "p"), true).await.unwrap();
    let (_, recs) = proxy
        .node
        .get(
            vec![b"secret".to_vec(), b"via-proxy".to_vec()],
            ReadOpts {
                min_index: idx,
                ..opts()
            },
        )
        .await
        .unwrap();
    assert!(recs.iter().all(Option::is_some));

    // A node whose certificate isn't signed by the cluster CA is refused by
    // the root, even though it trusts the root.
    let rogue_cfg = with_tls(
        cfg(Role::Replica, &dirs[3], &[&root]),
        issue(pki.path(), &rogue_ca, "rogue"),
        &ca.pem,
    );
    let rogue = server::start(rogue_cfg).await.unwrap();
    // A node without TLS can't talk to it either.
    let plain = server::start(cfg(Role::Replica, &dirs[4], &[&root]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for n in [&rogue, &plain] {
        assert!(n.node.metrics.replication_errors.load(Ordering::Relaxed) > 0);
        assert_eq!(
            n.node.store.state().db_id,
            None,
            "unauthenticated node received data"
        );
    }

    for n in [plain, rogue, proxy, rep, root] {
        n.shutdown().await;
    }
}

/// Minimal HTTP/1.1 client over any stream; returns (status, body).
async fn http<S: AsyncRead + AsyncWrite + Unpin>(
    mut s: S,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> (u16, String) {
    let auth = token
        .map(|t| format!("authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nhost: localhost\r\n{auth}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out).await;
    let text = String::from_utf8_lossy(&out).into_owned();
    let status = text
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    (status, body)
}

#[tokio::test]
async fn https_with_bearer_tokens() {
    let pki = TempDir::new().unwrap();
    let ca = make_ca(pki.path(), "ca");
    let dir = TempDir::new().unwrap();
    let tokens = dir.path().join("tokens");
    let hashed = blake3::hash(b"reader-token").to_hex();
    std::fs::write(
        &tokens,
        format!("read blake3:{hashed}\nwrite writer-token\n"),
    )
    .unwrap();

    let mut c = cfg(Role::Root, &dir, &[]);
    let (cert, key) = issue(pki.path(), &ca, "root");
    c.tls_cert = Some(cert);
    c.tls_key = Some(key);
    c.auth_tokens = Some(tokens);
    c.http_socket = Some(dir.path().join("http.sock"));
    let root = server::start(c).await.unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from_pem_file(&ca.pem).unwrap())
        .unwrap();
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    let addr = root.http_addr;
    let https = |method: &'static str,
                 path: &'static str,
                 token: Option<&'static str>,
                 body: &'static str| {
        let connector = connector.clone();
        async move {
            let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            let name = ServerName::try_from("localhost").unwrap();
            let tls = connector.connect(name, tcp).await.unwrap();
            http(tls, method, path, token, body).await
        }
    };

    assert_eq!(https("GET", "/v1/status", None, "").await.0, 401);
    assert_eq!(https("GET", "/v1/status", Some("guess"), "").await.0, 401);
    assert_eq!(
        https("GET", "/v1/status", Some("reader-token"), "").await.0,
        200
    );
    assert_eq!(
        https("GET", "/metrics", Some("reader-token"), "").await.0,
        200
    );

    // Read tokens can't write, by any route.
    assert_eq!(
        https("PUT", "/v1/kv/flag", Some("reader-token"), "on")
            .await
            .0,
        403
    );
    assert_eq!(
        https("DELETE", "/v1/kv/flag", Some("reader-token"), "")
            .await
            .0,
        403
    );
    let txn = r#"{"type":"txn","ops":[{"op":"set","key":"flag","value":1}]}"#;
    assert_eq!(
        https("POST", "/v1/query", Some("reader-token"), txn)
            .await
            .0,
        403
    );

    assert_eq!(
        https("PUT", "/v1/kv/flag", Some("writer-token"), "on")
            .await
            .0,
        200
    );
    assert_eq!(
        https("POST", "/v1/query", Some("writer-token"), txn)
            .await
            .0,
        200
    );
    let get = r#"{"type":"get","keys":["flag"]}"#;
    let (status, body) = https("POST", "/v1/query", Some("reader-token"), get).await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""value":1"#), "{body}");

    // Plain HTTP on the HTTPS port gets nowhere.
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        http(tcp, "GET", "/v1/status", Some("reader-token"), "")
            .await
            .0,
        0
    );

    // The HTTP API over a Unix socket, with the same token rules.
    let sock = dir.path().join("http.sock");
    let unix = || async { tokio::net::UnixStream::connect(&sock).await.unwrap() };
    assert_eq!(
        http(unix().await, "GET", "/v1/kv/flag", None, "").await.0,
        401
    );
    let (status, body) = http(unix().await, "GET", "/v1/kv/flag", Some("reader-token"), "").await;
    assert_eq!((status, body.as_str()), (200, "1"));
    assert_eq!(
        http(
            unix().await,
            "PUT",
            "/v1/kv/sock",
            Some("writer-token"),
            "x"
        )
        .await
        .0,
        200
    );

    root.shutdown().await;
    assert!(!sock.exists());
}
