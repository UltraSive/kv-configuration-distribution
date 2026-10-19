# kvdist — replicated configuration distribution

A key-value store for distributing configuration from one writer to many
readers, modelled on Cloudflare's [Quicksilver]. It's written in Rust on top of
the [fjall] LSM storage engine.

Writes go to a single **root**, which assigns every transaction a
monotonically increasing **replication index**. **Replicas** pull the log in a
tree (root → intermediate → edge) and serve reads locally from disk.
**Proxies** follow the same log but only keep hot values on disk. They fetch
misses from replicas *at a consistent index*, so they never return data older
or newer than their own position.

[Quicksilver]: https://blog.cloudflare.com/introducing-quicksilver-configuration-distribution-at-internet-scale/
[fjall]: https://github.com/fjall-rs/fjall

## Concepts borrowed from Quicksilver

| Quicksilver | kvdist |
|---|---|
| Single write aggregator, monotonically increasing index per change | `root` role; each transaction is one log entry with one index |
| Log entry and data change committed in one storage transaction | One atomic fjall batch writes the data, MVCC history, log entry and replication position |
| Gap detection (`index == last + 1`) | Every applied entry is checked for contiguity |
| Incremental hash over the log | blake3 hash chain; entry N's hash covers N‑1 |
| Per-KV CRC, verified on read | CRC32 over key + record on every read, snapshot export and ingest |
| Handshake with `DBID`, `LastIndex`; process IDs to prevent self-replication | `Hello{node_id, db_id, last_index, last_hash}`. Rejects foreign databases, self-replication and diverged logs |
| Resume at `LastIndex + 1`; secondaries with longer history | Resume from the log while it's retained, otherwise automatic snapshot bootstrap; ordered upstream failover |
| Consistent snapshots to bootstrap new machines | fjall cross-keyspace snapshot streamed in chunks; crash-safe install |
| Batched commits | Group commit on the root (one fsync per batch); followers coalesce frames into one apply |
| Heartbeat from the top of the tree for lag | Root head index and clock relayed down the tree; `lag_entries`, `heartbeat_age_ms` |
| v1.5 replicas with MVCC (read at index) | `history` keyspace, `(key, superseded_at)` → previous version, pruned by retention |
| v1.5 proxies with a recent-writes window | Proxy applies the whole log, never evicts keys written in the last `proxy_window` indexes, and fetches misses with `GetAt{at, min}` |
| Persistent cache; LRU eviction at a disk soft limit, no new keys past a hard limit | Proxy cache lives in fjall (survives restarts) with byte-based soft/hard limits plus free-disk soft/hard limits (see below) |
| Key/value separation for large values | fjall blob separation on the data keyspace avoids the LMDB fragmentation problem |

Beyond Quicksilver's published design:

- **Multi-key transactions with compare-and-set.** Each check is `key must be at
  write index N` (0 = absent). All checks and ops commit atomically or not at all.
- **Read-your-writes / monotonic reads anywhere.** Every response carries the
  index it was served at. Pass it back as `min_index` on any node and that node
  waits until it has caught up.
- **Writes from any node.** Replicas and proxies forward writes up the tree.
  A forwarded write is retried on another upstream only if it provably never
  left the node, so a write is never applied twice.
- **Change feed** (`/v1/log`), long-polling by index.
- **Followers don't fsync.** fjall recovers to a consistent prefix and a
  follower re-fetches any lost tail. Only the root fsyncs before acknowledging.

## How proxy reads stay consistent

A proxy at index `P` applies every log entry, and never evicts a key written
after `P - proxy_window`. So if a key is missing locally, its last write was at
or before `L = max(origin, P - proxy_window)`. The proxy sends
`GetAt{at: P, min: L}` to a replica:

- a replica behind `L` answers `Behind`, and the proxy tries another;
- a replica between `L` and `P` has the same value the proxy would see at `P`;
- a replica ahead of `P` answers from MVCC history *as of `P`*.

Either way the proxy returns exactly the state at index `P`.

## Proxy cache limits

Each cached key counts its key plus record size. The limits work like
Quicksilver's soft and hard disk limits:

| Flag | Default | Effect |
|---|---|---|
| `--proxy-soft-limit` | `1GiB` | Over this, least-recently-used values are evicted back down to it. |
| `--proxy-hard-limit` | `2GiB` | Once the cache reaches this, values fetched on a miss are served but not cached. |
| `--proxy-disk-soft-free` | `0` (off) | Free space on the data disk below this also triggers eviction, 5% per pass. |
| `--proxy-disk-hard-free` | `0` (off) | Free space below this also stops caching fetched values. |

Sizes accept `4096`, `512MiB`, `2GB`, `1.5G` and so on.

Two kinds of data ignore the limits, because consistency depends on them:

- Every write in the log is still applied.
- Keys written within the last `--proxy-window` indexes are never evicted.

So size the soft limit well above what one window of writes takes up.
Evicted data is deleted from the LSM straight away, but the disk space only
comes back after compaction.

`/v1/status` and `/metrics` report `cache_bytes`, `cached_keys`,
`cache_evictions_total`, `cache_rejected_fills_total` and `disk_free_bytes`.

## Running

```bash
make build       # cargo build --release
make test        # unit + multi-node integration tests
make cluster     # docker compose: root :8080, replica :8081, proxy :8082
```

Manually:

```bash
kvdist --role root    --data-dir data/root --http-addr 127.0.0.1:8080 --peer-addr 127.0.0.1:7070
kvdist --role replica --data-dir data/rep  --http-addr 127.0.0.1:8081 --peer-addr 127.0.0.1:7071 --upstream 127.0.0.1:7070
kvdist --role proxy   --data-dir data/px   --http-addr 127.0.0.1:8082 --peer-addr ""    --upstream 127.0.0.1:7071 --socket /tmp/kv.sock
```

Every flag has a `KV_*` environment variable (see `kvdist --help` and
`.env.example`).

## HTTP API

Raw values:

```bash
curl -X PUT  localhost:8080/v1/kv/service/ttl -d 300            # {"index":1}
curl -X PUT 'localhost:8080/v1/kv/service/ttl?if_index=1' -d 600 # CAS; 409 on conflict
curl -i      localhost:8082/v1/kv/service/ttl?min_index=2        # x-kv-index, x-kv-write-index
curl -X DELETE localhost:8080/v1/kv/service/ttl
```

JSON queries (`POST /v1/query`):

```bash
# Atomic multi-key transaction with checks
curl localhost:8080/v1/query -H 'content-type: application/json' -d '{
  "type": "txn",
  "checks": [{"key": "service/ttl", "index": 2}],
  "ops": [
    {"op": "set", "key": "service/ttl", "value": 900},
    {"op": "set", "key": "service/waf", "value": {"enabled": true}},
    {"op": "delete", "key": "service/legacy"}
  ]}'
# {"index":3}

# Consistent multi-get at one index
curl localhost:8082/v1/query -d '{"type":"get","keys":["service/ttl","nope"],"min_index":3}' -H 'content-type: application/json'
# {"index":3,"items":{"nope":null,"service/ttl":{"value":900,"write_index":3}}}

# Prefix scan with pagination (pass "next" back as "after")
curl localhost:8080/v1/query -d '{"type":"scan","prefix":"service/","limit":100}' -H 'content-type: application/json'
```

Values that are valid JSON come back inline as `value`; anything else comes back as
`value_b64`. Sets accept either `value` (any JSON) or `value_b64`.

Other endpoints:

- `GET /v1/log?from=N&limit=100&wait_ms=30000`: change feed (root/replicas). Returns `410` once `N` has been pruned.
- `GET /v1/status`: role, index, hash, floors, upstream, lag.
- `GET /metrics`: Prometheus text format.

Errors are JSON with a `kind`: `conflict` (409), `bad_request` (400),
`timeout` (504, `min_index` not reached), `unavailable` (503), `gone` (410),
`unknown_outcome` (502: a forwarded write may or may not have committed).

## Unix socket

The fast path for services on the same machine. Start a node with
`--socket /run/kvdist.sock`. The socket is created with mode `0660`, so
access goes by Unix user and group.

**Wire format.** Each frame is a 4-byte big-endian length followed by a
[postcard]-encoded `LocalReq`. Every request gets exactly one `LocalResp`
frame, in order, so a client can pipeline requests on one connection. Keys and
values are raw bytes, with no JSON or base64 re-encoding. The message types
are in `src/socket.rs`.

```rust
enum LocalReq  { Get { keys, min_index }, Scan { prefix, after, limit, min_index },
                 Txn { txn, wait }, Status }
enum LocalResp { Values { index, items: Vec<Option<Item>> }, Scanned { index, items },
                 Committed { index }, Status(Status), Error(ClientError) }
```

Errors are typed (`Conflict`, `Timeout`, `Unavailable`, `UnknownOutcome`, ...).
A frame that can't be decoded gets an `Error(BadRequest)` reply and the
connection stays open. New message variants are only ever appended, so older
clients keep working.

**Rust client** (`kvdist::client::LocalClient`):

```rust
let mut kv = LocalClient::connect("/run/kvdist.sock").await?;
let index = kv.set("service/ttl", "300").await?;
let (_, item) = kv.get_one("service/ttl", index).await?; // read-your-writes
```

**CLI** (`kvctl`, uses `KV_SOCKET`):

```bash
kvctl set service/ttl 300            # prints the commit index
printf '\x00\xff' | kvctl set blob -  # value from stdin
kvctl get service/ttl                # raw value; exit 1 if absent
kvctl --min-index 42 get service/ttl # wait until this node has index 42
kvctl scan service/                  # key<TAB>value lines
kvctl status
```

Clients in other languages need a postcard implementation; the format is
[specified](https://postcard.jamesmunns.com/wire-format) and easy to write.
The HTTP JSON API is still there for anything else.

[postcard]: https://postcard.jamesmunns.com/

## Security

There are three layers, each enabled independently. With none of them set, the
node logs a warning at startup.

| Surface | Protection | Flags |
|---|---|---|
| Peer port (replication, RPC, forwarded writes) | Mutual TLS: both sides present a certificate signed by the cluster CA | `--tls-cert`, `--tls-key`, `--tls-ca` |
| HTTP API (TCP) | HTTPS, plus bearer tokens with `read`/`write` roles | `--tls-cert`, `--tls-key`, `--auth-tokens` |
| Unix sockets (`--socket`, `--http-socket`) | File permissions: created `0660` | `--http-socket` also enforces tokens |

**Peer mTLS.** With `--tls-ca`, a node only accepts peers whose certificate
chains to that CA, and only dials peers that present one. A node without
the right certificate gets nothing: no snapshot, no log, no reads. Node
certificates need:

- a SAN matching the address other nodes dial, as a DNS name or an IP;
- both the `serverAuth` and `clientAuth` extended key usages.

```bash
# Cluster CA
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
  -subj "/CN=kvdist-ca" -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" -keyout ca.key -out ca.crt

# One per node; put the names/IPs peers use to reach it in subjectAltName
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "/CN=root-1" \
  -keyout root-1.key -out root-1.csr
openssl x509 -req -in root-1.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 \
  -out root-1.crt -extfile <(printf '%s\n' \
    "subjectAltName=DNS:root-1.internal,IP:10.0.0.5" \
    "extendedKeyUsage=serverAuth,clientAuth" "basicConstraints=CA:FALSE" \
    "keyUsage=critical,digitalSignature")

kvdist --role root --tls-cert root-1.crt --tls-key root-1.key --tls-ca ca.crt ...
```

The same certificate serves HTTPS. Without `--tls-ca`, the HTTP API still uses
TLS, but peer connections stay plaintext.

**Tokens.** `--auth-tokens` points to a file of `<role> <token>` lines.
`write` implies `read`. Every HTTP route needs a valid
`Authorization: Bearer <token>`, `/metrics` included. `PUT`, `DELETE` and
`txn` queries need `write`. Without a valid token the reply is `401`; with a
read token on a write route it's `403`. Store tokens hashed so the file holds
no secrets:

```bash
kvctl hash-token read  "$READ_TOKEN"  >> tokens   # read blake3:6f2c...
kvctl hash-token write "$DEPLOY_TOKEN" >> tokens
curl --cacert ca.crt -H "authorization: Bearer $READ_TOKEN" https://node:8080/v1/kv/service/ttl
```

A write sent to a replica or proxy is authorized by that node's token file,
then forwarded over mTLS. Keep token files consistent across the cluster.

**HTTP over a Unix socket.** `--http-socket /run/kvdist-http.sock` serves the
same HTTP API locally, with no TCP and no TLS. This suits non-Rust local
clients that would rather not speak postcard:

```bash
curl --unix-socket /run/kvdist-http.sock -H "authorization: Bearer $READ_TOKEN" http://x/v1/kv/service/ttl
```

## Layout

```
src/types.rs        indexes, ids, ops, hash-chained LogEntry, CRC'd Record
src/store.rs        fjall keyspaces: data, history (MVCC), log, meta
src/applier.rs      single writer thread: group commit, CAS, pruning, eviction
src/protocol.rs     peer wire protocol (postcard frames)
src/replication.rs  upstream server (handshake, snapshot, stream) + follower
src/rpc.rs          peer RPC pool (proxy reads, write forwarding)
src/tls.rs          mTLS for peers, HTTPS listener
src/auth.rs         HTTP bearer tokens
src/node.rs         per-role read/write paths
src/api.rs          HTTP API
src/socket.rs       Unix socket API (postcard)
src/client.rs       Unix socket client library
src/bin/kvctl.rs    socket CLI
tests/cluster.rs    multi-node tests over real sockets
tests/security.rs   mTLS, HTTPS, tokens, HTTP socket
```

## Not (yet) implemented

- Any node with a cluster-CA certificate is fully trusted: it can replicate
  everything and submit writes. There are no per-node permissions.
- No certificate revocation (CRLs/OCSP); rotate the CA to cut a node off.
- Single root: there's no leader election or root failover.
- A diverged log (for example a root restored from an old backup) is refused,
  not repaired automatically. Wipe the replica's data dir to re-bootstrap it.
- No Quicksilver v2 data-center tier (sharded L2 cache, relays, reactive prefetch).
