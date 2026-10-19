//! End-to-end tests over real sockets: root -> replica -> proxy.

use kvdist::config::{Config, Role};
use kvdist::error::WriteError;
use kvdist::node::{NodeError, ReadOpts};
use kvdist::server::{self, Running};
use kvdist::store::AtRead;
use kvdist::types::{Check, Index, Op, Txn};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tempfile::TempDir;

fn cfg(role: Role, dir: &TempDir, upstreams: &[&Running]) -> Config {
    let mut c = Config::new(role, dir.path());
    c.http_addr = "127.0.0.1:0".into();
    c.peer_addr = "127.0.0.1:0".into();
    c.upstreams = upstreams
        .iter()
        .map(|r| r.peer_addr.unwrap().to_string())
        .collect();
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

fn opts(min_index: Index) -> ReadOpts {
    ReadOpts {
        min_index,
        wait: Duration::from_secs(10),
    }
}

async fn read(r: &Running, key: &str, min_index: Index) -> (Index, Option<String>) {
    let (index, mut recs) = r.node.get(vec![key.into()], opts(min_index)).await.unwrap();
    let v = recs
        .pop()
        .unwrap()
        .map(|r| String::from_utf8(r.value.unwrap()).unwrap());
    (index, v)
}

#[tokio::test]
async fn root_writes_checks_and_reads() {
    let dir = TempDir::new().unwrap();
    let root = server::start(cfg(Role::Root, &dir, &[])).await.unwrap();

    let i1 = root.node.write(set("a", "1"), true).await.unwrap();
    assert_eq!(i1, 1);
    assert_eq!(read(&root, "a", 0).await, (1, Some("1".into())));

    // CAS on write index.
    let cas = |index| Txn {
        checks: vec![Check {
            key: "a".into(),
            index,
        }],
        ops: vec![Op::Set {
            key: "a".into(),
            value: "2".into(),
        }],
    };
    let err = root.node.write(cas(0), true).await.unwrap_err();
    assert!(
        matches!(
            err,
            NodeError::Write(WriteError::Conflict { actual: 1, .. })
        ),
        "{err:?}"
    );
    assert_eq!(root.node.write(cas(1), true).await.unwrap(), 2);

    // Multi-op transaction is one index.
    let t = Txn {
        checks: vec![Check {
            key: "new".into(),
            index: 0,
        }],
        ops: vec![
            Op::Set {
                key: "b".into(),
                value: "x".into(),
            },
            Op::Delete { key: "a".into() },
            Op::Set {
                key: "new".into(),
                value: "y".into(),
            },
        ],
    };
    assert_eq!(root.node.write(t, true).await.unwrap(), 3);
    assert_eq!(read(&root, "a", 0).await, (3, None));
    assert_eq!(read(&root, "b", 0).await.1, Some("x".into()));

    // MVCC: read the past.
    let keys = vec![b"a".to_vec(), b"b".to_vec()];
    match root.node.store.get_at(&keys, 1).unwrap() {
        AtRead::Ok { index, records } => {
            assert_eq!(index, 1);
            assert_eq!(
                records[0].as_ref().unwrap().value.as_deref(),
                Some(&b"1"[..])
            );
            assert!(records[1].is_none());
        }
        AtRead::HistoryUnavailable { .. } => panic!("history should be available"),
    }
    match root.node.store.get_at(&keys, 2).unwrap() {
        AtRead::Ok { records, .. } => assert_eq!(
            records[0].as_ref().unwrap().value.as_deref(),
            Some(&b"2"[..])
        ),
        _ => panic!(),
    }

    // Invalid transactions are rejected without consuming an index.
    let dup = Txn {
        checks: vec![],
        ops: vec![
            Op::Delete { key: "z".into() },
            Op::Delete { key: "z".into() },
        ],
    };
    assert!(matches!(
        root.node.write(dup, true).await,
        Err(NodeError::Write(WriteError::Invalid(_)))
    ));
    assert_eq!(root.node.write(set("c", "1"), true).await.unwrap(), 4);
    root.shutdown().await;
}

#[tokio::test]
async fn replica_follows_and_forwards_writes() {
    let (d1, d2, d3) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let root = server::start(cfg(Role::Root, &d1, &[])).await.unwrap();
    for i in 0..50 {
        root.node
            .write(set(&format!("k{i}"), &i.to_string()), true)
            .await
            .unwrap();
    }
    let mid = server::start(cfg(Role::Replica, &d2, &[&root]))
        .await
        .unwrap();
    // A second tier: replica of a replica.
    let edge = server::start(cfg(Role::Replica, &d3, &[&mid]))
        .await
        .unwrap();

    assert_eq!(read(&edge, "k49", 50).await.1, Some("49".into()));

    // Writes on the edge are forwarded to the root and become visible locally.
    let idx = edge.node.write(set("from-edge", "hi"), true).await.unwrap();
    assert_eq!(idx, 51);
    assert_eq!(read(&edge, "from-edge", 0).await, (51, Some("hi".into())));
    assert_eq!(read(&root, "from-edge", 0).await.1, Some("hi".into()));

    // Same lineage, same hash chain everywhere.
    assert_eq!(root.node.store.state().hash, edge.node.store.state().hash);
    assert_eq!(edge.node.store.state().db_id, root.node.store.state().db_id);

    // Heartbeats reach the second tier.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let st = edge.node.status();
    assert_eq!(st.root_index, Some(51));
    assert_eq!(st.lag_entries, Some(0));

    edge.shutdown().await;
    mid.shutdown().await;
    root.shutdown().await;
}

#[tokio::test]
async fn replica_resumes_from_last_index_after_restart() {
    let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let root = server::start(cfg(Role::Root, &d1, &[])).await.unwrap();
    root.node.write(set("a", "1"), true).await.unwrap();

    let rep = server::start(cfg(Role::Replica, &d2, &[&root]))
        .await
        .unwrap();
    assert_eq!(read(&rep, "a", 1).await.1, Some("1".into()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        rep.node.metrics.snapshots_installed.load(Ordering::Relaxed),
        1
    );
    let node_id = rep.node.store.node_id();
    rep.shutdown().await;

    for i in 0..10 {
        root.node
            .write(set("a", &format!("v{i}")), true)
            .await
            .unwrap();
    }
    let rep = server::start(cfg(Role::Replica, &d2, &[&root]))
        .await
        .unwrap();
    assert_eq!(rep.node.store.node_id(), node_id);
    assert_eq!(read(&rep, "a", 11).await, (11, Some("v9".into())));
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Resumed from the log; no second snapshot.
    assert_eq!(
        rep.node.metrics.snapshots_installed.load(Ordering::Relaxed),
        0
    );
    assert_eq!(rep.node.metrics.entries_applied.load(Ordering::Relaxed), 10);
    rep.shutdown().await;
    root.shutdown().await;
}

#[tokio::test]
async fn far_behind_replica_bootstraps_from_snapshot() {
    let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let mut c = cfg(Role::Root, &d1, &[]);
    c.log_retention = 5;
    c.history_window = 5;
    let root = server::start(c).await.unwrap();
    for i in 0..100 {
        root.node
            .write(set(&format!("k{}", i % 10), &i.to_string()), true)
            .await
            .unwrap();
    }
    root.node
        .write(
            Txn {
                checks: vec![],
                ops: vec![Op::Delete { key: "k0".into() }],
            },
            true,
        )
        .await
        .unwrap();
    // Let the pruner run.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let st = root.node.store.state();
    assert!(st.log_floor > 90, "log was not pruned: {st:?}");
    assert!(st.history_floor > 90);

    let rep = server::start(cfg(Role::Replica, &d2, &[&root]))
        .await
        .unwrap();
    assert_eq!(read(&rep, "k9", 101).await.1, Some("99".into()));
    assert_eq!(read(&rep, "k0", 101).await.1, None);
    assert_eq!(rep.node.store.state().origin, 101);

    // Streaming continues after the snapshot.
    root.node.write(set("after", "snap"), true).await.unwrap();
    assert_eq!(read(&rep, "after", 102).await.1, Some("snap".into()));
    assert_eq!(rep.node.store.state().hash, root.node.store.state().hash);

    // History older than the window is refused rather than answered wrongly.
    assert!(matches!(
        root.node.store.get_at(&[b"k1".to_vec()], 10).unwrap(),
        AtRead::HistoryUnavailable { .. }
    ));
    rep.shutdown().await;
    root.shutdown().await;
}

#[tokio::test]
async fn proxy_caches_consistently() {
    let (d1, d2, d3) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let root = server::start(cfg(Role::Root, &d1, &[])).await.unwrap();
    for i in 0..20 {
        root.node
            .write(set(&format!("old{i}"), "v"), true)
            .await
            .unwrap();
    }
    let rep = server::start(cfg(Role::Replica, &d2, &[&root]))
        .await
        .unwrap();
    assert!(rep.node.wait_for(20, Duration::from_secs(10)).await);

    let mut pc = cfg(Role::Proxy, &d3, &[&rep]);
    pc.proxy_window = 5;
    // ~19 bytes per cached key: room for the protected window plus a few more.
    pc.proxy_soft_limit = 150;
    let proxy = server::start(pc).await.unwrap();
    // Proxies start at the head without a snapshot.
    for _ in 0..100 {
        if proxy.node.store.state().db_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(proxy.node.store.state().origin, 20);

    // Miss -> fetched from the replica, then a hit.
    assert_eq!(read(&proxy, "old3", 0).await.1, Some("v".into()));
    // The cache fill is asynchronous.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(read(&proxy, "old3", 0).await.1, Some("v".into()));
    assert_eq!(proxy.node.metrics.cache_misses.load(Ordering::Relaxed), 1);
    assert_eq!(proxy.node.metrics.cache_hits.load(Ordering::Relaxed), 1);
    // Negative lookups are cached too.
    assert_eq!(read(&proxy, "nope", 0).await.1, None);

    // Writes through the proxy, with read-your-writes.
    let idx = proxy.node.write(set("old3", "new"), true).await.unwrap();
    assert_eq!(read(&proxy, "old3", idx).await, (idx, Some("new".into())));
    let idx = proxy.node.write(set("nope", "now"), true).await.unwrap();
    assert_eq!(read(&proxy, "nope", idx).await.1, Some("now".into()));

    // Overflow the cache; evicted keys are re-fetched with the right value.
    for i in 0..20 {
        read(&proxy, &format!("old{i}"), 0).await;
    }
    for i in 0..10 {
        root.node
            .write(set(&format!("hot{i}"), "h"), true)
            .await
            .unwrap();
    }
    assert!(
        proxy
            .node
            .wait_for(root.node.applied(), Duration::from_secs(10))
            .await
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let st = proxy.node.status();
    assert!(
        st.cache_bytes.unwrap() <= 150,
        "eviction did not run: {st:?}"
    );
    assert!(st.cached_keys.unwrap() < 20);
    assert!(st.cache_evictions.unwrap() > 0);
    assert_eq!(read(&proxy, "old3", 0).await.1, Some("new".into()));
    assert_eq!(read(&proxy, "old7", 0).await.1, Some("v".into()));

    // Scans are served by a replica at least as new as the proxy.
    let (idx, items) = proxy
        .node
        .scan(b"hot".to_vec(), None, 100, opts(0))
        .await
        .unwrap();
    assert!(idx >= proxy.node.applied());
    assert_eq!(items.len(), 10);

    proxy.shutdown().await;
    rep.shutdown().await;
    root.shutdown().await;
}

#[tokio::test]
async fn rejects_foreign_database_and_self_replication() {
    let (d1, d2, d3) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let root_a = server::start(cfg(Role::Root, &d1, &[])).await.unwrap();
    let root_b = server::start(cfg(Role::Root, &d2, &[])).await.unwrap();
    root_a.node.write(set("a", "A"), true).await.unwrap();
    root_b.node.write(set("a", "B"), true).await.unwrap();

    let rep = server::start(cfg(Role::Replica, &d3, &[&root_a]))
        .await
        .unwrap();
    assert_eq!(read(&rep, "a", 1).await.1, Some("A".into()));
    rep.shutdown().await;

    // Same data dir pointed at a different lineage: refused, data untouched.
    let rep = server::start(cfg(Role::Replica, &d3, &[&root_b]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(rep.node.metrics.replication_errors.load(Ordering::Relaxed) > 0);
    assert_eq!(read(&rep, "a", 0).await.1, Some("A".into()));
    assert_eq!(
        rep.node.store.state().db_id,
        root_a.node.store.state().db_id
    );
    rep.shutdown().await;

    root_a.shutdown().await;
    root_b.shutdown().await;
}

#[tokio::test]
async fn resync_follows_a_rebuilt_root() {
    let dirs: Vec<TempDir> = (0..4).map(|_| TempDir::new().unwrap()).collect();
    let old_root = server::start(cfg(Role::Root, &dirs[0], &[])).await.unwrap();
    let new_root = server::start(cfg(Role::Root, &dirs[1], &[])).await.unwrap();
    let i = old_root.node.write(set("a", "old"), true).await.unwrap();
    old_root.node.write(set("gone", "x"), true).await.unwrap();
    new_root.node.write(set("a", "new"), true).await.unwrap();

    // root -> replica -> leaf, both allowed to resync; the new root is the
    // replica's second upstream.
    let mut c = cfg(Role::Replica, &dirs[2], &[&old_root, &new_root]);
    c.resync = true;
    let rep = server::start(c).await.unwrap();
    let mut c = cfg(Role::Replica, &dirs[3], &[&rep]);
    c.resync = true;
    let leaf = server::start(c).await.unwrap();
    assert_eq!(read(&leaf, "a", i + 1).await.1, Some("old".into()));

    // The old root goes away: the replica fails over to the new one, finds
    // another database and takes its snapshot; the leaf follows.
    old_root.shutdown().await;
    let new_db = new_root.node.store.state().db_id;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while leaf.node.store.state().db_id != new_db {
        assert!(std::time::Instant::now() < deadline, "leaf never resynced");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(rep.node.store.state().db_id, new_db);
    assert_eq!(read(&leaf, "a", 0).await.1, Some("new".into()));
    assert_eq!(read(&leaf, "gone", 0).await.1, None);

    // And keeps following it.
    let i = new_root.node.write(set("a", "newer"), true).await.unwrap();
    assert_eq!(read(&leaf, "a", i).await.1, Some("newer".into()));

    leaf.shutdown().await;
    rep.shutdown().await;
    new_root.shutdown().await;
}

#[tokio::test]
async fn http_api() {
    let dir = TempDir::new().unwrap();
    let root = server::start(cfg(Role::Root, &dir, &[])).await.unwrap();
    let base = format!("http://{}", root.http_addr);
    let http = reqwest::Client::new();

    let r = http
        .put(format!("{base}/v1/kv/app/flag"))
        .body("on")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-kv-index"], "1");

    let r = http
        .get(format!("{base}/v1/kv/app/flag"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["x-kv-write-index"], "1");
    assert_eq!(r.text().await.unwrap(), "on");

    let r = http
        .put(format!("{base}/v1/kv/app/flag?if_index=0"))
        .body("x")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);

    let body = serde_json::json!({"type": "txn", "ops": [
        {"op": "set", "key": "app/config", "value": {"limit": 5}},
        {"op": "delete", "key": "app/flag"}
    ]});
    let r: serde_json::Value = http
        .post(format!("{base}/v1/query"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["index"], 2);

    let body = serde_json::json!({"type": "get", "keys": ["app/config", "app/flag"]});
    let r: serde_json::Value = http
        .post(format!("{base}/v1/query"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["items"]["app/config"]["value"]["limit"], 5);
    assert!(r["items"]["app/flag"].is_null());
    let r: serde_json::Value = http
        .get(format!("{base}/v1/log?from=1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r["entries"].as_array().unwrap().len(), 2);
    assert_eq!(r["entries"][1]["ops"][0]["key"], "app/config");

    let r = http
        .get(format!("{base}/v1/kv/app/flag?min_index=99&wait_ms=100"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 504);

    let m = http
        .get(format!("{base}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(m.contains("kvdist_applied_index 2"));
    root.shutdown().await;
}

#[tokio::test]
async fn proxy_hard_limit_stops_caching_but_still_serves() {
    let (d1, d2, d3) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    let root = server::start(cfg(Role::Root, &d1, &[])).await.unwrap();
    root.node.write(set("cold", "c"), true).await.unwrap();
    let rep = server::start(cfg(Role::Replica, &d2, &[&root]))
        .await
        .unwrap();
    assert!(rep.node.wait_for(1, Duration::from_secs(10)).await);

    let mut pc = cfg(Role::Proxy, &d3, &[&rep]);
    pc.proxy_soft_limit = 0;
    pc.proxy_hard_limit = 0;
    let proxy = server::start(pc).await.unwrap();
    for _ in 0..100 {
        if proxy.node.store.state().db_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Over the hard limit: misses are served but never cached.
    for _ in 0..3 {
        assert_eq!(read(&proxy, "cold", 0).await.1, Some("c".into()));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let st = proxy.node.status();
    assert_eq!(proxy.node.metrics.cache_misses.load(Ordering::Relaxed), 3);
    assert_eq!(st.cache_rejected_fills, Some(3));

    // Logged writes are still applied (the recent-writes window needs them).
    let idx = root.node.write(set("fresh", "f"), true).await.unwrap();
    assert_eq!(read(&proxy, "fresh", idx).await.1, Some("f".into()));
    assert_eq!(proxy.node.metrics.cache_misses.load(Ordering::Relaxed), 3);

    proxy.shutdown().await;
    rep.shutdown().await;
    root.shutdown().await;
}

#[tokio::test]
async fn unix_socket_postcard_api() {
    use futures::{SinkExt, StreamExt};
    use kvdist::client::LocalClient;
    use kvdist::socket::{ClientError, LocalReq, LocalResp};
    use tokio_util::codec::Framed;

    let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let root = server::start(cfg(Role::Root, &d1, &[])).await.unwrap();
    let mut rc = cfg(Role::Replica, &d2, &[&root]);
    let sock = d2.path().join("kv.sock");
    rc.socket = Some(sock.clone());
    rc.read_wait_ms = 200;
    let rep = server::start(rc).await.unwrap();
    let mut kv = LocalClient::connect(&sock).await.unwrap();

    // Writes through the replica's socket are forwarded to the root, and
    // raw binary values round-trip untouched.
    let blob = vec![0u8, 159, 146, 150, 255, b'\n'];
    let idx = kv.set("bin", blob.clone()).await.unwrap();
    let (index, item) = kv.get_one("bin", idx).await.unwrap();
    assert_eq!(index, idx);
    let item = item.unwrap();
    assert_eq!(item.value.as_slice(), &blob[..]);
    assert_eq!(item.write_index, idx);
    assert_eq!(root.node.store.version_of(b"bin").unwrap(), idx);

    // Typed errors.
    let stale = Txn {
        checks: vec![Check {
            key: "bin".into(),
            index: 0,
        }],
        ops: vec![Op::Delete { key: "bin".into() }],
    };
    assert!(
        matches!(kv.txn(stale).await, Err(ClientError::Conflict { actual, .. }) if actual == idx)
    );
    assert!(matches!(
        kv.get_one("bin", 999).await,
        Err(ClientError::Timeout { wanted: 999, .. })
    ));

    kv.set("cfg/a", "1").await.unwrap();
    let last = kv.set("cfg/b", "2").await.unwrap();
    let (_, items) = kv.scan("cfg/", None, 10, last).await.unwrap();
    let keys: Vec<_> = items.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, [&b"cfg/a"[..], b"cfg/b"]);
    let del = kv.delete("cfg/a").await.unwrap();
    assert_eq!(kv.get_one("cfg/a", del).await.unwrap().1, None);
    assert_eq!(kv.status().await.unwrap().applied_index, del);

    // Garbage gets an error frame without killing the connection, and
    // requests can be pipelined.
    let stream = tokio::net::UnixStream::connect(&sock).await.unwrap();
    let mut raw = Framed::new(stream, kvdist::protocol::codec());
    SinkExt::<bytes::Bytes>::send(&mut raw, bytes::Bytes::from_static(&[0xff, 0xff, 0xff]))
        .await
        .unwrap();
    let get = LocalReq::Get {
        keys: vec![serde_bytes::ByteBuf::from(&b"cfg/b"[..])],
        min_index: 0,
    };
    for _ in 0..2 {
        SinkExt::<bytes::Bytes>::send(&mut raw, postcard::to_allocvec(&get).unwrap().into())
            .await
            .unwrap();
    }
    let mut replies = Vec::new();
    for _ in 0..3 {
        let frame = raw.next().await.unwrap().unwrap();
        replies.push(postcard::from_bytes::<LocalResp>(&frame).unwrap());
    }
    assert!(matches!(
        replies[0],
        LocalResp::Error(ClientError::BadRequest(_))
    ));
    for r in &replies[1..] {
        match r {
            LocalResp::Values { items, .. } => {
                assert_eq!(items[0].as_ref().unwrap().value.as_slice(), b"2")
            }
            other => panic!("{other:?}"),
        }
    }

    rep.shutdown().await;
    assert!(!sock.exists());
    root.shutdown().await;
}
