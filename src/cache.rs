//! Proxy cache accounting: LRU order plus the logical bytes each cached key
//! occupies, and the disk-space limits that drive eviction and admission.

use crate::types::Record;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub type SharedCache = Arc<Mutex<CacheIndex>>;

/// Bytes a cached key accounts for: key plus encoded record.
pub fn entry_size(key: &[u8], record: &Record) -> u64 {
    (key.len() + Record::HEADER_LEN + record.value.as_ref().map_or(0, Vec::len)) as u64
}

pub fn free_disk_bytes(path: &Path) -> Option<u64> {
    let st = rustix::fs::statvfs(path).ok()?;
    Some(st.f_bavail.saturating_mul(st.f_frsize))
}

#[derive(Clone, Debug)]
pub struct CacheLimits {
    pub data_dir: PathBuf,
    /// Evict least-recently-used keys while cached bytes exceed this.
    pub soft_bytes: u64,
    /// Only cache fetched values while cached bytes are below this.
    pub hard_bytes: u64,
    /// Evict while free disk is below this (0 = off).
    pub disk_soft_free: u64,
    /// Stop caching fetched values while free disk is below this (0 = off).
    pub disk_hard_free: u64,
}

impl CacheLimits {
    fn free_disk(&self) -> Option<u64> {
        if self.disk_soft_free == 0 && self.disk_hard_free == 0 {
            return None;
        }
        free_disk_bytes(&self.data_dir)
    }

    /// Byte target eviction should bring the cache down to, if it is over one.
    pub fn eviction_target(&self, cached: u64) -> Option<u64> {
        let disk_low = self.free_disk().is_some_and(|f| f < self.disk_soft_free);
        // Freed space only shows up on disk after compaction, so under disk
        // pressure shrink gradually (5% per pass) instead of all at once.
        let target = if disk_low {
            self.soft_bytes.min(cached - cached / 20)
        } else {
            self.soft_bytes
        };
        (cached > target).then_some(target)
    }

    /// Whether newly fetched values may be added to the cache.
    pub fn admits(&self, cached: u64) -> bool {
        cached < self.hard_bytes && self.free_disk().is_none_or(|f| f >= self.disk_hard_free)
    }
}

pub struct CacheIndex {
    lru: lru::LruCache<Vec<u8>, u64>,
    bytes: u64,
    pub evictions: u64,
    pub rejected_fills: u64,
}

impl Default for CacheIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheIndex {
    pub fn new() -> Self {
        Self {
            lru: lru::LruCache::unbounded(),
            bytes: 0,
            evictions: 0,
            rejected_fills: 0,
        }
    }

    /// Inserts or resizes a key and marks it most recently used.
    pub fn upsert(&mut self, key: Vec<u8>, size: u64) {
        if let Some(old) = self.lru.put(key, size) {
            self.bytes -= old;
        }
        self.bytes += size;
    }

    pub fn touch(&mut self, key: &[u8]) {
        self.lru.promote(key);
    }

    pub fn pop_lru(&mut self) -> Option<(Vec<u8>, u64)> {
        let (k, size) = self.lru.pop_lru()?;
        self.bytes -= size;
        Some((k, size))
    }

    pub fn clear(&mut self) {
        self.lru.clear();
        self.bytes = 0;
    }

    pub fn len(&self) -> usize {
        self.lru.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lru.is_empty()
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_bytes_through_updates() {
        let mut c = CacheIndex::new();
        c.upsert(b"a".to_vec(), 10);
        c.upsert(b"b".to_vec(), 5);
        c.upsert(b"a".to_vec(), 3);
        assert_eq!(c.bytes(), 8);
        c.touch(b"b");
        assert_eq!(c.pop_lru(), Some((b"a".to_vec(), 3)));
        assert_eq!(c.bytes(), 5);
    }

    #[test]
    fn limits() {
        let l = CacheLimits {
            data_dir: ".".into(),
            soft_bytes: 100,
            hard_bytes: 200,
            disk_soft_free: 0,
            disk_hard_free: 0,
        };
        assert_eq!(l.eviction_target(50), None);
        assert_eq!(l.eviction_target(150), Some(100));
        assert!(l.admits(199));
        assert!(!l.admits(200));

        let starved = CacheLimits {
            disk_soft_free: u64::MAX,
            disk_hard_free: u64::MAX,
            ..l
        };
        assert_eq!(starved.eviction_target(50), Some(48));
        assert!(!starved.admits(0));
    }
}
