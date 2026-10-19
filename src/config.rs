use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, serde::Serialize, serde::Deserialize)]
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

    /// Optional Unix socket for local clients (postcard frames, see `kvctl`).
    #[arg(long, env = "KV_SOCKET")]
    pub socket: Option<PathBuf>,

    /// Optional Unix socket serving the HTTP API (e.g. for `curl --unix-socket`).
    #[arg(long, env = "KV_HTTP_SOCKET")]
    pub http_socket: Option<PathBuf>,

    /// PEM certificate chain for this node. Enables HTTPS on the HTTP API.
    #[arg(long, env = "KV_TLS_CERT", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// PEM private key for --tls-cert.
    #[arg(long, env = "KV_TLS_KEY", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

    /// PEM cluster CA. Enables mutual TLS on peer connections: every node's
    /// certificate must be signed by it (needs --tls-cert/--tls-key).
    #[arg(long, env = "KV_TLS_CA", requires = "tls_cert")]
    pub tls_ca: Option<PathBuf>,

    /// File of `<read|write> <token>` lines. Enables bearer-token auth on
    /// the HTTP API.
    #[arg(long, env = "KV_AUTH_TOKENS")]
    pub auth_tokens: Option<PathBuf>,

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

    /// Proxy only: evict least-recently-used values while the cache is
    /// larger than this (e.g. 512MiB, 2GB).
    #[arg(long, env = "KV_PROXY_SOFT_LIMIT", default_value = "1GiB", value_parser = parse_bytes)]
    pub proxy_soft_limit: u64,

    /// Proxy only: stop caching values fetched on a miss once the cache
    /// reaches this size. Reads are still served.
    #[arg(long, env = "KV_PROXY_HARD_LIMIT", default_value = "2GiB", value_parser = parse_bytes)]
    pub proxy_hard_limit: u64,

    /// Proxy only: also evict while free space on the data disk is below
    /// this (0 = off).
    #[arg(long, env = "KV_PROXY_DISK_SOFT_FREE", default_value = "0", value_parser = parse_bytes)]
    pub proxy_disk_soft_free: u64,

    /// Proxy only: also stop caching fetched values while free space on the
    /// data disk is below this (0 = off).
    #[arg(long, env = "KV_PROXY_DISK_HARD_FREE", default_value = "0", value_parser = parse_bytes)]
    pub proxy_disk_hard_free: u64,

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
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err("--tls-cert and --tls-key must be given together".into());
        }
        if self.tls_ca.is_some() && self.tls_cert.is_none() {
            return Err("--tls-ca needs --tls-cert and --tls-key".into());
        }
        if self.role != Role::Root && self.upstreams.is_empty() {
            return Err("replica and proxy nodes need at least one --upstream".into());
        }
        if self.history_window > self.log_retention {
            return Err("--history-window must not exceed --log-retention".into());
        }
        if self.proxy_soft_limit > self.proxy_hard_limit {
            return Err("--proxy-soft-limit must not exceed --proxy-hard-limit".into());
        }
        if self.proxy_disk_hard_free > self.proxy_disk_soft_free {
            return Err("--proxy-disk-hard-free must not exceed --proxy-disk-soft-free".into());
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

/// Parses sizes like `4096`, `512MiB`, `2GB` or `1.5G` (K/M/G/T are binary).
pub fn parse_bytes(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let num: f64 = num.parse().map_err(|_| format!("invalid size {s:?}"))?;
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        "t" | "tb" | "tib" => 1 << 40,
        u => return Err(format!("unknown size unit {u:?}")),
    };
    Ok((num * mult as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::parse_bytes;

    #[test]
    fn sizes() {
        assert_eq!(parse_bytes("4096"), Ok(4096));
        assert_eq!(parse_bytes("512MiB"), Ok(512 << 20));
        assert_eq!(parse_bytes("2GB"), Ok(2 << 30));
        assert_eq!(parse_bytes("1.5g"), Ok(3 << 29));
        assert!(parse_bytes("lots").is_err());
    }
}
