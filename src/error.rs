use crate::types::Index;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] fjall::Error),
    #[error("encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt record for key {key:?}: {reason}")]
    Corrupt { key: String, reason: &'static str },
    #[error("replication gap: expected index {expected}, got {got}")]
    Gap { expected: Index, got: Index },
    #[error("hash chain mismatch at index {0}")]
    HashMismatch(Index),
    #[error("applier stopped")]
    ApplierStopped,
    #[error("protocol: {0}")]
    Protocol(String),
}

impl Error {
    pub fn corrupt(key: &[u8], reason: &'static str) -> Self {
        Error::Corrupt {
            key: String::from_utf8_lossy(key).into_owned(),
            reason,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write failures. Serializable because writes are forwarded up the tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum WriteError {
    #[error("check failed for key {key:?}: expected index {expected}, found {actual}")]
    Conflict {
        key: String,
        expected: Index,
        actual: Index,
    },
    #[error("invalid transaction: {0}")]
    Invalid(String),
    #[error("write unavailable: {0}")]
    Unavailable(String),
    /// The request reached the root but the reply was lost; it may or may not
    /// have committed.
    #[error("write outcome unknown: {0}")]
    Unknown(String),
    #[error("storage failure: {0}")]
    Storage(String),
    /// The sending peer isn't allowed to write (--peer-writer).
    #[error("write forbidden: {0}")]
    Forbidden(String),
}
