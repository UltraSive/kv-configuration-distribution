//! Client-facing HTTP API: REST plus a JSON query endpoint. The local
//! Unix socket speaks postcard instead; see [`crate::socket`].

use crate::auth::Access;
use crate::error::WriteError;
use crate::node::{Node, NodeError, ReadOpts};
use crate::types::*;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

pub const INDEX_HEADER: &str = "x-kv-index";
pub const WRITE_INDEX_HEADER: &str = "x-kv-write-index";

// ---- JSON model ----------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum OpJson {
    Set {
        key: String,
        value: Option<Value>,
        value_b64: Option<String>,
    },
    Delete {
        key: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct CheckJson {
    pub key: String,
    /// Expected write index; 0 means the key must not exist.
    pub index: Index,
}

#[derive(Debug, Deserialize)]
pub struct TxnJson {
    #[serde(default)]
    pub checks: Vec<CheckJson>,
    pub ops: Vec<OpJson>,
    /// Wait until the commit is visible on this node (default true).
    #[serde(default = "yes")]
    pub wait: bool,
}

fn yes() -> bool {
    true
}

impl TxnJson {
    fn into_txn(self) -> Result<(Txn, bool), NodeError> {
        let mut ops = Vec::with_capacity(self.ops.len());
        for op in self.ops {
            ops.push(match op {
                OpJson::Delete { key } => Op::Delete {
                    key: key.into_bytes(),
                },
                OpJson::Set {
                    key,
                    value,
                    value_b64,
                } => {
                    let value = match (value, value_b64) {
                        (Some(v), None) => serde_json::to_vec(&v)
                            .map_err(|e| NodeError::BadRequest(e.to_string()))?,
                        (None, Some(b)) => B64
                            .decode(b)
                            .map_err(|e| NodeError::BadRequest(format!("value_b64: {e}")))?,
                        _ => {
                            return Err(NodeError::BadRequest(format!(
                                "set {key:?} needs exactly one of value or value_b64"
                            )));
                        }
                    };
                    Op::Set {
                        key: key.into_bytes(),
                        value,
                    }
                }
            });
        }
        let checks = self
            .checks
            .into_iter()
            .map(|c| Check {
                key: c.key.into_bytes(),
                index: c.index,
            })
            .collect();
        Ok((Txn { checks, ops }, self.wait))
    }
}

#[derive(Debug, Serialize)]
pub struct ItemJson {
    pub write_index: Index,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_b64: Option<String>,
}

/// Values that are valid JSON are returned inline; anything else as base64.
fn item_json(r: &Record) -> ItemJson {
    let bytes = r.value.as_deref().unwrap_or_default();
    match serde_json::from_slice::<Value>(bytes) {
        Ok(v) => ItemJson {
            write_index: r.index,
            value: Some(v),
            value_b64: None,
        },
        Err(_) => ItemJson {
            write_index: r.index,
            value: None,
            value_b64: Some(B64.encode(bytes)),
        },
    }
}

fn ops_json(ops: &[Op]) -> Vec<Value> {
    ops.iter()
        .map(|op| match op {
            Op::Set { key, value } => {
                let item = item_json(&Record {
                    index: 0,
                    value: Some(value.clone()),
                });
                let mut v = serde_json::to_value(item).unwrap_or_default();
                v["op"] = json!("set");
                v["key"] = json!(String::from_utf8_lossy(key));
                if let Some(o) = v.as_object_mut() {
                    o.remove("write_index");
                }
                v
            }
            Op::Delete { key } => json!({"op": "delete", "key": String::from_utf8_lossy(key)}),
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ClientQuery {
    Get {
        keys: Vec<String>,
        #[serde(default)]
        min_index: Index,
    },
    Scan {
        #[serde(default)]
        prefix: String,
        after: Option<String>,
        limit: Option<usize>,
        #[serde(default)]
        min_index: Index,
    },
    Txn(TxnJson),
    Status,
}

pub async fn run_query(node: &Node, q: ClientQuery) -> Result<Value, NodeError> {
    let opts = |min_index| ReadOpts {
        min_index,
        wait: node.default_wait(),
    };
    match q {
        ClientQuery::Get { keys, min_index } => {
            let raw: Vec<Vec<u8>> = keys.iter().map(|k| k.clone().into_bytes()).collect();
            let (index, records) = node.get(raw, opts(min_index)).await?;
            let items: BTreeMap<String, Option<ItemJson>> = keys
                .into_iter()
                .zip(records)
                .map(|(k, r)| (k, r.as_ref().map(item_json)))
                .collect();
            Ok(json!({"index": index, "items": items}))
        }
        ClientQuery::Scan {
            prefix,
            after,
            limit,
            min_index,
        } => {
            let limit = limit.unwrap_or(1000).clamp(1, 10_000);
            let (index, items) = node
                .scan(
                    prefix.into_bytes(),
                    after.map(String::into_bytes),
                    limit,
                    opts(min_index),
                )
                .await?;
            let next = (items.len() == limit).then(|| {
                items
                    .last()
                    .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            });
            let items: Vec<Value> = items
                .iter()
                .map(|(k, r)| {
                    let mut v = serde_json::to_value(item_json(r)).unwrap_or_default();
                    v["key"] = json!(String::from_utf8_lossy(k));
                    v
                })
                .collect();
            Ok(json!({"index": index, "items": items, "next": next.flatten()}))
        }
        ClientQuery::Txn(t) => {
            let (txn, wait) = t.into_txn()?;
            let index = node.write(txn, wait).await?;
            Ok(json!({"index": index}))
        }
        ClientQuery::Status => Ok(serde_json::to_value(node.status()).unwrap_or_default()),
    }
}

// ---- errors ------------------------------------------------------------------

pub struct ApiError(pub NodeError);

impl From<NodeError> for ApiError {
    fn from(e: NodeError) -> Self {
        ApiError(e)
    }
}

pub fn error_json(e: &NodeError) -> (StatusCode, Value) {
    let msg = e.to_string();
    match e {
        NodeError::Write(WriteError::Conflict {
            key,
            expected,
            actual,
        }) => (
            StatusCode::CONFLICT,
            json!({"error": msg, "kind": "conflict", "key": key, "expected": expected, "actual": actual}),
        ),
        NodeError::Write(WriteError::Invalid(_)) | NodeError::BadRequest(_) => (
            StatusCode::BAD_REQUEST,
            json!({"error": msg, "kind": "bad_request"}),
        ),
        NodeError::Write(WriteError::Unknown(_)) => (
            StatusCode::BAD_GATEWAY,
            json!({"error": msg, "kind": "unknown_outcome"}),
        ),
        NodeError::Timeout { index, wanted } => (
            StatusCode::GATEWAY_TIMEOUT,
            json!({"error": msg, "kind": "timeout", "index": index, "wanted": wanted}),
        ),
        NodeError::Forbidden(_) => (
            StatusCode::FORBIDDEN,
            json!({"error": msg, "kind": "forbidden"}),
        ),
        NodeError::Gone { floor } => (
            StatusCode::GONE,
            json!({"error": msg, "kind": "gone", "log_floor": floor}),
        ),
        NodeError::NotReady(_)
        | NodeError::Unavailable(_)
        | NodeError::Write(WriteError::Unavailable(_)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": msg, "kind": "unavailable"}),
        ),
        NodeError::Internal(_) | NodeError::Write(WriteError::Storage(_)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": msg, "kind": "internal"}),
        ),
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, body) = error_json(&self.0);
        (code, Json(body)).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

// ---- HTTP --------------------------------------------------------------------

pub fn router(node: Arc<Node>) -> Router {
    let limit = node.cfg.max_value_bytes + (1 << 20);
    Router::new()
        .route(
            "/v1/kv/{*key}",
            get(get_raw).put(put_raw).delete(delete_raw),
        )
        .route("/v1/query", post(query))
        .route("/v1/log", get(log_feed))
        .route("/v1/status", get(status))
        .route("/metrics", get(metrics))
        .route_layer(middleware::from_fn_with_state(node.clone(), authorize))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(node)
}

/// Bearer-token check. Every route needs at least read access; PUT and
/// DELETE (and `txn` queries, checked in [`query`]) need write access.
async fn authorize(State(node): State<Arc<Node>>, mut req: Request, next: Next) -> Response {
    let access = match &node.tokens {
        None => Access::Write,
        Some(tokens) => {
            let presented = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            match presented.and_then(|t| tokens.check(t.trim())) {
                Some(a) => a,
                None => {
                    let body =
                        json!({"error": "missing or invalid bearer token", "kind": "unauthorized"});
                    return (
                        StatusCode::UNAUTHORIZED,
                        [(header::WWW_AUTHENTICATE, "Bearer")],
                        Json(body),
                    )
                        .into_response();
                }
            }
        }
    };
    if matches!(*req.method(), Method::PUT | Method::DELETE) && access < Access::Write {
        return ApiError(NodeError::Forbidden("this token is read-only".into())).into_response();
    }
    req.extensions_mut().insert(access);
    next.run(req).await
}

#[derive(Deserialize)]
struct ReadParams {
    #[serde(default)]
    min_index: Index,
    wait_ms: Option<u64>,
}

#[derive(Deserialize)]
struct WriteParams {
    if_index: Option<Index>,
    #[serde(default = "yes")]
    wait: bool,
}

fn index_headers(index: Index, write_index: Option<Index>) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(INDEX_HEADER, HeaderValue::from(index));
    if let Some(w) = write_index {
        h.insert(WRITE_INDEX_HEADER, HeaderValue::from(w));
    }
    h
}

async fn get_raw(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    Query(p): Query<ReadParams>,
) -> ApiResult<Response> {
    let wait = p.wait_ms.map_or(node.default_wait(), Duration::from_millis);
    let opts = ReadOpts {
        min_index: p.min_index,
        wait,
    };
    let (index, mut records) = node.get(vec![key.into_bytes()], opts).await?;
    Ok(match records.pop().flatten() {
        Some(r) => {
            let mut h = index_headers(index, Some(r.index));
            h.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            (StatusCode::OK, h, r.value.unwrap_or_default()).into_response()
        }
        None => (StatusCode::NOT_FOUND, index_headers(index, None)).into_response(),
    })
}

async fn write_one(
    node: &Node,
    key: String,
    value: Option<Vec<u8>>,
    p: WriteParams,
) -> ApiResult<Response> {
    let key = key.into_bytes();
    let checks = p
        .if_index
        .map(|index| Check {
            key: key.clone(),
            index,
        })
        .into_iter()
        .collect();
    let op = match value {
        Some(value) => Op::Set { key, value },
        None => Op::Delete { key },
    };
    let index = node
        .write(
            Txn {
                checks,
                ops: vec![op],
            },
            p.wait,
        )
        .await?;
    Ok((
        index_headers(index, Some(index)),
        Json(json!({"index": index})),
    )
        .into_response())
}

async fn put_raw(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    Query(p): Query<WriteParams>,
    body: Bytes,
) -> ApiResult<Response> {
    write_one(&node, key, Some(body.to_vec()), p).await
}

async fn delete_raw(
    State(node): State<Arc<Node>>,
    Path(key): Path<String>,
    Query(p): Query<WriteParams>,
) -> ApiResult<Response> {
    write_one(&node, key, None, p).await
}

async fn query(
    State(node): State<Arc<Node>>,
    Extension(access): Extension<Access>,
    Json(q): Json<ClientQuery>,
) -> ApiResult<Json<Value>> {
    if matches!(q, ClientQuery::Txn(_)) && access < Access::Write {
        return Err(NodeError::Forbidden("this token is read-only".into()).into());
    }
    Ok(Json(run_query(&node, q).await?))
}

#[derive(Deserialize)]
struct LogParams {
    from: Index,
    limit: Option<usize>,
    wait_ms: Option<u64>,
}

/// Change feed: committed transactions starting at `from`. With `wait_ms`
/// it long-polls until at least one entry exists.
async fn log_feed(
    State(node): State<Arc<Node>>,
    Query(p): Query<LogParams>,
) -> ApiResult<Json<Value>> {
    if node.proxy.is_some() {
        return Err(NodeError::BadRequest("proxies do not keep the log".into()).into());
    }
    let from = p.from.max(1);
    if let Some(ms) = p.wait_ms {
        node.wait_for(from, Duration::from_millis(ms.min(60_000)))
            .await;
    }
    let st = node.store.state();
    if from < st.log_floor {
        return Err(NodeError::Gone {
            floor: st.log_floor,
        }
        .into());
    }
    let limit = p.limit.unwrap_or(100).clamp(1, 1000);
    let entries = node
        .store
        .log_range(from, limit, 8 << 20)
        .map_err(NodeError::from)?;
    let entries: Vec<Value> = entries
        .iter()
        .map(|e| json!({"index": e.index, "timestamp_ms": e.timestamp_ms, "hash": hash_hex(&e.hash), "ops": ops_json(&e.ops)}))
        .collect();
    Ok(Json(json!({"applied": st.applied, "entries": entries})))
}

async fn status(State(node): State<Arc<Node>>) -> Json<Value> {
    Json(serde_json::to_value(node.status()).unwrap_or_default())
}

async fn metrics(State(node): State<Arc<Node>>) -> impl IntoResponse {
    let s = node.status();
    let m = &node.metrics;
    let mut out = String::new();
    let mut line = |name: &str, kind: &str, v: u64| {
        out.push_str(&format!("# TYPE kvdist_{name} {kind}\nkvdist_{name} {v}\n"));
    };
    line("applied_index", "gauge", s.applied_index);
    line("log_floor", "gauge", s.log_floor);
    line("history_floor", "gauge", s.history_floor);
    if let Some(r) = s.root_index {
        line("root_index", "gauge", r);
    }
    if let Some(l) = s.lag_entries {
        line("lag_entries", "gauge", l);
    }
    if let Some(a) = s.heartbeat_age_ms {
        line("heartbeat_age_ms", "gauge", a);
    }
    for (name, kind, v) in [
        ("cached_keys", "gauge", s.cached_keys.map(|c| c as u64)),
        ("cache_bytes", "gauge", s.cache_bytes),
        ("cache_soft_limit_bytes", "gauge", s.cache_soft_limit),
        ("cache_hard_limit_bytes", "gauge", s.cache_hard_limit),
        ("cache_evictions_total", "counter", s.cache_evictions),
        (
            "cache_rejected_fills_total",
            "counter",
            s.cache_rejected_fills,
        ),
        ("disk_free_bytes", "gauge", s.disk_free_bytes),
    ] {
        if let Some(v) = v {
            line(name, kind, v);
        }
    }
    for (name, c) in [
        ("reads_total", &m.reads),
        ("writes_total", &m.writes),
        ("write_conflicts_total", &m.write_conflicts),
        ("cache_hits_total", &m.cache_hits),
        ("cache_misses_total", &m.cache_misses),
        ("upstream_fetch_errors_total", &m.upstream_fetch_errors),
        ("entries_applied_total", &m.entries_applied),
        ("replication_errors_total", &m.replication_errors),
        ("snapshots_installed_total", &m.snapshots_installed),
        ("snapshots_served_total", &m.snapshots_served),
    ] {
        line(name, "counter", c.load(Ordering::Relaxed));
    }
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out)
}
