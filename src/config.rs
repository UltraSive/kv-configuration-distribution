use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// The single write aggregator. Assigns indexes and owns the DbId.
    Root,
    /// Full copy of the data. Follows an upstream, serves reads, downstream
    /// replicas and proxies.
    Replica,
    /// Follows the log but only keeps recently written and recently read
    /// values; misses are fetched from replicas at a consistent index.
    Proxy,
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "kvdist",
    version,
    about = "Replicated configuration KV store inspired by Cloudflare Quicksilver"
)]
pub struct Config {
    #[arg(long, env = "KV_ROLE", value_enum, default_value = "root")]
    pub role: Role,

    #[arg(long, env = "KV_DATA_DIR", default_value = "./data")]
    pub data_dir: PathBuf,

    /// Client HTTP API listen address.
    #[arg(long, env = "KV_HTTP_ADDR", default_value = "0.0.0.0:8080")]
    pub http_addr: String,

    /// Peer (replication + RPC) listen address. Empty to disable.
    #[arg(long, env = "KV_PEER_ADDR", default_value = "0.0.0.0:7070")]
    pub peer_addr: String,

    /// Optional Unix socket for local clients (length-prefixed JSON).
    #[arg(long, env = "KV_SOCKET")]
    pub socket: Option<PathBuf>,

    /// Upstream peers to replicate from, in preference order.
    #[arg(long = "upstream", env = "KV_UPSTREAMS", value_delimiter = ',')]
    pub upstreams: Vec<String>,

    /// Proxy only: replicas to fetch cache misses from (defaults to the upstreams).
    #[arg(long = "replica", env = "KV_REPLICAS", value_delimiter = ',')]
    pub replicas: Vec<String>,

    /// Log entries kept for resuming downstream nodes; older ones require a snapshot.
    #[arg(long, env = "KV_LOG_RETENTION", default_value_t = 1_000_000)]
    pub log_retention: u64,

    /// Indexes of MVCC history kept for read-at-index requests from proxies.
    #[arg(long, env = "KV_HISTORY_WINDOW", default_value_t = 100_000)]
    pub history_window: u64,

    /// Proxy only: values written within this many indexes are never evicted.
    #[arg(long, env = "KV_PROXY_WINDOW", default_value_t = 10_000)]
    pub proxy_window: u64,

    /// Proxy only: target number of cached keys.
    #[arg(long, env = "KV_PROXY_CAPACITY", default_value_t = 1_000_000)]
    pub proxy_capacity: usize,

    /// Max transactions group-committed in one batch on the root.
    #[arg(long, env = "KV_MAX_BATCH", default_value_t = 1024)]
    pub max_batch: usize,

    /// How long a read with `min_index` waits for this node to catch up.
    #[arg(long, env = "KV_READ_WAIT_MS", default_value_t = 5000)]
    pub read_wait_ms: u64,

    #[arg(long, env = "KV_MAX_VALUE_BYTES", default_value_t = 16 * 1024 * 1024)]
    pub max_value_bytes: usize,

    #[arg(long, env = "KV_MAX_TXN_OPS", default_value_t = 10_000)]
    pub max_txn_ops: usize,
}

impl Config {
    pub fn validate(&mut self) -> Result<(), String> {
        if self.role != Role::Root && self.upstreams.is_empty() {
            return Err("replica and proxy nodes need at least one --upstream".into());
        }
        if self.history_window > self.log_retention {
            return Err("--history-window must not exceed --log-retention".into());
        }
        if self.role == Role::Proxy && self.replicas.is_empty() {
            self.replicas = self.upstreams.clone();
        }
        Ok(())
    }

    /// A minimal config for tests and embedding.
    pub fn new(role: Role, data_dir: impl Into<PathBuf>) -> Self {
        let mut c = Config::parse_from(["kvdist"]);
        c.role = role;
        c.data_dir = data_dir.into();
        c
    }
}
