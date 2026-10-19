//! Watching key prefixes over the Unix socket.

use kvdist::client::{LocalClient, WatchEvent, Watcher};
use kvdist::config::{Config, Role};
use kvdist::server::{self, Running};
use kvdist::socket::{Change, ClientError};
use kvdist::types::{Index, Op, Txn};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

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

fn with_socket(mut c: Config, dir: &TempDir) -> (Config, PathBuf) {
    let sock = dir.path().join("kv.sock");
    c.socket = Some(sock.clone());
    (c, sock)
}

fn set(key: &str, value: &str) -> Op {
    Op::Set {
        key: key.into(),
        value: value.into(),
    }
}

fn txn(ops: Vec<Op>) -> Txn {
    Txn {
        checks: vec![],
        ops,
    }
}

fn change(key: &str, value: Option<&str>) -> Change {
    Change {
        key: key.as_bytes().to_vec().into(),
        value: value.map(|v| v.as_bytes().to_vec().into()),
    }
}

async fn next(w: &mut Watcher) -> Result<WatchEvent, ClientError> {
    tokio::time::timeout(Duration::from_secs(15), w.next())
        .await
        .expect("no watch event")
}

async fn changes(w: &mut Watcher) -> (Index, Vec<Change>) {
    match next(w).await.unwrap() {
        WatchEvent::Changes { index, changes, .. } => (index, changes),
        other => panic!("expected changes, got {other:?}"),
    }
}

#[tokio::test]
async fn watch_streams_changes_on_replica_and_proxy() {
    let dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().unwrap()).collect();
    let root = server::start(cfg(Role::Root, &dirs[0], &[])).await.unwrap();
    let i1 = root
        .node
        .write(txn(vec![set("app/a", "0")]), true)
        .await
        .unwrap();
    let (rc, rep_sock) = with_socket(cfg(Role::Replica, &dirs[1], &[&root]), &dirs[1]);
    let rep = server::start(rc).await.unwrap();
    let (pc, proxy_sock) = with_socket(cfg(Role::Proxy, &dirs[2], &[&rep]), &dirs[2]);
    let proxy = server::start(pc).await.unwrap();

    // Scan, then watch from just after the scan: nothing is missed.
    let mut kv = LocalClient::connect(&rep_sock).await.unwrap();
    let (at, items) = kv.scan("app/", None, 100, i1).await.unwrap();
    assert_eq!(items.len(), 1);
    let mut rep_w = kv.watch(&["app/"], at + 1).await.unwrap();
    assert_eq!(rep_w.index(), at);

    // A proxy watch from the next commit, once the proxy has caught up.
    let mut pkv = LocalClient::connect(&proxy_sock).await.unwrap();
    pkv.get_one("app/a", i1).await.unwrap();
    let mut proxy_w = pkv.watch(&["app/", "flags/"], 0).await.unwrap();

    let i2 = root
        .node
        .write(txn(vec![set("other/x", "1")]), true)
        .await
        .unwrap();
    let i3 = root
        .node
        .write(
            txn(vec![
                set("app/b", "2"),
                Op::Delete {
                    key: "app/a".into(),
                },
                set("other/y", "3"),
            ]),
            true,
        )
        .await
        .unwrap();
    let i4 = root
        .node
        .write(txn(vec![set("flags/on", "yes")]), true)
        .await
        .unwrap();

    // Only matching ops, one event per transaction, skipping unrelated ones.
    let expected3 = vec![change("app/b", Some("2")), change("app/a", None)];
    assert_eq!(changes(&mut rep_w).await, (i3, expected3.clone()));
    assert_eq!(changes(&mut proxy_w).await, (i3, expected3));
    assert_eq!(
        changes(&mut proxy_w).await,
        (i4, vec![change("flags/on", Some("yes"))])
    );
    assert!(i2 < i3);

    // Replicas replay from their log; an empty prefix list means all keys.
    // This one joined at index 1, so its log starts after that.
    let rep_sock = &rep_sock;
    let watch_all = |from| async move {
        LocalClient::connect(rep_sock)
            .await
            .unwrap()
            .watch::<&str>(&[], from)
            .await
            .unwrap()
    };
    assert!(matches!(
        next(&mut watch_all(i1).await).await,
        Err(ClientError::Gone { floor }) if floor == i2
    ));
    let mut all = watch_all(i2).await;
    for (want, key) in [(i2, "other/x"), (i3, "app/b"), (i4, "flags/on")] {
        let (index, ch) = changes(&mut all).await;
        assert_eq!((index, ch[0].key.as_slice()), (want, key.as_bytes()));
    }

    // A proxy started at the head has no older history to replay.
    let err = LocalClient::connect(&proxy_sock)
        .await
        .unwrap()
        .watch(&["app/"], 1)
        .await;
    let err = match err {
        Ok(mut w) => next(&mut w).await.unwrap_err(),
        Err(e) => e,
    };
    assert!(matches!(err, ClientError::Gone { .. }), "{err:?}");

    // Idle watchers get progress, so they know they're current.
    tokio::time::sleep(Duration::from_millis(5500)).await;
    let i5 = root
        .node
        .write(txn(vec![set("app/c", "4")]), true)
        .await
        .unwrap();
    assert_eq!(changes(&mut rep_w).await.0, i5);
    assert_eq!(rep_w.index(), i5);

    for n in [proxy, rep, root] {
        n.shutdown().await;
    }
}

#[tokio::test]
async fn watch_reports_truncated_log_and_snapshot_resync() {
    let (d1, d2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let root_cfg = || {
        let mut c = cfg(Role::Root, &d1, &[]);
        c.log_retention = 5;
        c.history_window = 5;
        c.socket = Some(d1.path().join("root.sock"));
        c
    };
    let root = server::start(root_cfg()).await.unwrap();
    let mut last = 0;
    for i in 0..50 {
        last = root
            .node
            .write(txn(vec![set(&format!("k{}", i % 5), "v")]), true)
            .await
            .unwrap();
    }
    // Let the pruner run.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // History older than the log's retention is gone.
    let mut w = LocalClient::connect(d1.path().join("root.sock"))
        .await
        .unwrap()
        .watch(&["k"], 1)
        .await
        .unwrap();
    match next(&mut w).await {
        Err(ClientError::Gone { floor }) => assert!(floor > 40, "{floor}"),
        other => panic!("expected gone, got {other:?}"),
    }

    // Stop the root, start a watched replica, then bring the root back:
    // the replica has to install a snapshot, and its watcher is told to
    // re-read.
    let root_peer = root.peer_addr.unwrap().to_string();
    root.shutdown().await;
    let mut rc = cfg(Role::Replica, &d2, &[]);
    rc.upstreams = vec![root_peer.clone()];
    let (rc, sock) = with_socket(rc, &d2);
    let rep = server::start(rc).await.unwrap();
    let mut w = LocalClient::connect(&sock)
        .await
        .unwrap()
        .watch(&["k"], 0)
        .await
        .unwrap();
    let mut c = root_cfg();
    c.peer_addr = root_peer;
    let root = server::start(c).await.unwrap();

    assert_eq!(
        next(&mut w).await.unwrap(),
        WatchEvent::Resync { index: last }
    );
    let after = root
        .node
        .write(txn(vec![set("k-after", "1")]), true)
        .await
        .unwrap();
    assert_eq!(
        changes(&mut w).await,
        (after, vec![change("k-after", Some("1"))])
    );

    rep.shutdown().await;
    root.shutdown().await;
}
