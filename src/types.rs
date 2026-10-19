//! Core data model: replication indexes, identities, operations, log entries
//! and on-disk records.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Position in the replicated log. Index `N` is the state after applying
/// entries `1..=N`; index 0 is the empty database.
pub type Index = u64;

/// Incremental hash over the whole log (blake3). Entry `N`'s hash covers
/// entry `N-1`'s hash, so any lost, reordered or altered entry is detected.
pub type Hash = [u8; 32];

pub const GENESIS_HASH: Hash = [0u8; 32];

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn hash_hex(h: &Hash) -> String {
    to_hex(h)
}

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub [u8; 16]);

        impl $name {
            pub fn random() -> Self {
                Self(rand::random())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&to_hex(&self.0))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({self})", stringify!($name))
            }
        }
    };
}

id_type!(
    /// Identity of a database lineage, created once by the root. Replicas
    /// refuse to follow an upstream with a different DbId.
    DbId
);
id_type!(
    /// Identity of a single node's data directory. Used to stop a node from
    /// replicating from itself.
    NodeId
);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    Set {
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
    Delete {
        #[serde(with = "serde_bytes")]
        key: Vec<u8>,
    },
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Set { key, .. } | Op::Delete { key } => key,
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Op::Set { value, .. } => Some(value),
            Op::Delete { .. } => None,
        }
    }
}

/// Optimistic-concurrency precondition: the key's current write index must
/// equal `index`. `index == 0` means "the key must not exist".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    pub index: Index,
}

/// A client transaction. If every check passes, all ops are committed
/// atomically as a single log entry with a single index.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Txn {
    pub checks: Vec<Check>,
    pub ops: Vec<Op>,
}

/// One committed transaction in the replication log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: Index,
    /// Root wall-clock time at commit, used for replication lag metrics.
    pub timestamp_ms: u64,
    pub ops: Vec<Op>,
    pub hash: Hash,
}

impl LogEntry {
    pub fn new(prev: &Hash, index: Index, timestamp_ms: u64, ops: Vec<Op>) -> Self {
        let hash = chain_hash(prev, index, timestamp_ms, &ops);
        Self {
            index,
            timestamp_ms,
            ops,
            hash,
        }
    }

    pub fn verify(&self, prev: &Hash) -> bool {
        chain_hash(prev, self.index, self.timestamp_ms, &self.ops) == self.hash
    }
}

/// Hash with an explicit, serializer-independent encoding so the chain stays
/// stable even if the wire format changes.
fn chain_hash(prev: &Hash, index: Index, timestamp_ms: u64, ops: &[Op]) -> Hash {
    let mut h = blake3::Hasher::new();
    h.update(prev);
    h.update(&index.to_be_bytes());
    h.update(&timestamp_ms.to_be_bytes());
    for op in ops {
        match op {
            Op::Set { key, value } => {
                h.update(&[1]);
                h.update(&(key.len() as u64).to_be_bytes());
                h.update(key);
                h.update(&(value.len() as u64).to_be_bytes());
                h.update(value);
            }
            Op::Delete { key } => {
                h.update(&[2]);
                h.update(&(key.len() as u64).to_be_bytes());
                h.update(key);
            }
        }
    }
    *h.finalize().as_bytes()
}

/// A stored version of a key: the index that wrote it, and its value
/// (`None` is a tombstone).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub index: Index,
    pub value: Option<Vec<u8>>,
}

const REC_HEADER: usize = 4 + 8 + 1;

impl Record {
    pub const HEADER_LEN: usize = REC_HEADER;

    /// Layout: `crc32 (4) | index (8) | tag (1) | value`. The CRC covers the
    /// key as well, so a record served under the wrong key is detected.
    pub fn encode(&self, key: &[u8]) -> Vec<u8> {
        let value = self.value.as_deref().unwrap_or_default();
        let mut out = Vec::with_capacity(REC_HEADER + value.len());
        out.extend_from_slice(&[0; 4]);
        out.extend_from_slice(&self.index.to_be_bytes());
        out.push(u8::from(self.value.is_some()));
        out.extend_from_slice(value);
        let crc = record_crc(key, &out[4..]);
        out[..4].copy_from_slice(&crc.to_be_bytes());
        out
    }

    pub fn decode(key: &[u8], raw: &[u8]) -> Result<Self, &'static str> {
        if raw.len() < REC_HEADER {
            return Err("record too short");
        }
        let crc = u32::from_be_bytes(raw[..4].try_into().unwrap());
        if crc != record_crc(key, &raw[4..]) {
            return Err("checksum mismatch");
        }
        let index = u64::from_be_bytes(raw[4..12].try_into().unwrap());
        let value = match raw[12] {
            0 if raw.len() == REC_HEADER => None,
            1 => Some(raw[REC_HEADER..].to_vec()),
            _ => return Err("bad record tag"),
        };
        Ok(Self { index, value })
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}

fn record_crc(key: &[u8], body: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(key);
    h.update(body);
    h.finalize()
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip_and_crc() {
        let r = Record {
            index: 42,
            value: Some(b"hello".to_vec()),
        };
        let raw = r.encode(b"k");
        assert_eq!(Record::decode(b"k", &raw).unwrap(), r);
        assert!(Record::decode(b"other", &raw).is_err());
        let mut bad = raw.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(Record::decode(b"k", &bad).is_err());

        let t = Record {
            index: 7,
            value: None,
        };
        assert_eq!(Record::decode(b"k", &t.encode(b"k")).unwrap(), t);
    }

    #[test]
    fn hash_chain_detects_tampering() {
        let a = LogEntry::new(
            &GENESIS_HASH,
            1,
            10,
            vec![Op::Delete { key: b"x".to_vec() }],
        );
        let b = LogEntry::new(&a.hash, 2, 11, vec![]);
        assert!(a.verify(&GENESIS_HASH));
        assert!(b.verify(&a.hash));
        assert!(!b.verify(&GENESIS_HASH));
        let mut c = b.clone();
        c.index = 3;
        assert!(!c.verify(&a.hash));
    }
}
