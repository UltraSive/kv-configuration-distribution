//! fjall-backed storage.
//!
//! Keyspaces:
//! - `data`:    key -> latest [`Record`] (tombstones kept until they leave the history window)
//! - `history`: (key, superseded_at) -> the [`Record`] that was current before `superseded_at`
//! - `log`:     index -> [`LogEntry`]
//! - `meta`:    replication state ([`State`]) and identities
//!
//! Every log entry is applied in the same atomic batch as its data, history
//! and meta updates, so the store is always at exactly one log position.

use crate::error::{Error, Result};
use crate::types::*;
use fjall::{
    Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, OwnedWriteBatch, PersistMode,
    Readable,
};
use std::collections::HashMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::RwLock;

const DB_ID: &[u8] = b"db_id";
const NODE_ID: &[u8] = b"node_id";
const APPLIED: &[u8] = b"applied";
const HASH: &[u8] = b"hash";
const LOG_FLOOR: &[u8] = b"log_floor";
const LOG_FLOOR_HASH: &[u8] = b"log_floor_hash";
const HISTORY_FLOOR: &[u8] = b"history_floor";
const ORIGIN: &[u8] = b"origin";
const BOOTSTRAPPING: &[u8] = b"bootstrapping";

/// Replication position and retention bounds of this store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    pub db_id: Option<DbId>,
    /// Last applied log index.
    pub applied: Index,
    /// Chain hash of entry `applied`.
    pub hash: Hash,
    /// Lowest index still present in the log.
    pub log_floor: Index,
    /// Chain hash of entry `log_floor - 1`, so resumption at the floor can be verified.
    pub log_floor_hash: Hash,
    /// Lowest index that historical (read-at-index) lookups can serve.
    pub history_floor: Index,
    /// Index this store's knowledge starts at (0, or the snapshot it was bootstrapped from).
    pub origin: Index,
}

impl State {
    fn empty() -> Self {
        Self {
            db_id: None,
            applied: 0,
            hash: GENESIS_HASH,
            log_floor: 1,
            log_floor_hash: GENESIS_HASH,
            history_floor: 0,
            origin: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ApplyMode {
    /// Keep MVCC history (replicas, so they can serve proxies at an index).
    pub history: bool,
    /// Keep the log (nodes that can serve downstream replicas).
    pub log: bool,
}

pub type Scanned = (Index, Vec<(Vec<u8>, Record)>);

pub enum AtRead {
    Ok {
        index: Index,
        records: Vec<Option<Record>>,
    },
    HistoryUnavailable {
        floor: Index,
    },
}

pub struct Store {
    db: Database,
    data: Keyspace,
    history: Keyspace,
    log: Keyspace,
    meta: Keyspace,
    node_id: NodeId,
    state: RwLock<State>,
}

pub fn history_key(key: &[u8], superseded_at: Index) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len() + 8);
    k.extend_from_slice(&(key.len() as u32).to_be_bytes());
    k.extend_from_slice(key);
    k.extend_from_slice(&superseded_at.to_be_bytes());
    k
}

fn index_key(i: Index) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn as_u64(v: Option<fjall::Slice>) -> Option<u64> {
    v.and_then(|v| <[u8; 8]>::try_from(&v[..]).ok())
        .map(u64::from_be_bytes)
}

fn as_hash(v: Option<fjall::Slice>) -> Option<Hash> {
    v.and_then(|v| <[u8; 32]>::try_from(&v[..]).ok())
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let db = Database::builder(path).open()?;
        let data = db.keyspace("data", || {
            // Large values go to blob files instead of being rewritten by
            // every compaction, which avoids the fragmentation problems
            // Quicksilver hit with large values in LMDB.
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;
        let history = db.keyspace("history", KeyspaceCreateOptions::default)?;
        let log = db.keyspace("log", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;

        let node_id = match meta.get(NODE_ID)? {
            Some(v) if v.len() == 16 => NodeId(v[..].try_into().unwrap()),
            _ => {
                let id = NodeId::random();
                meta.insert(NODE_ID, id.0.to_vec())?;
                db.persist(PersistMode::SyncAll)?;
                id
            }
        };

        let store = Self {
            db,
            data,
            history,
            log,
            meta,
            node_id,
            state: RwLock::new(State::empty()),
        };
        if store.meta.get(BOOTSTRAPPING)?.is_some() {
            tracing::warn!("found interrupted bootstrap; wiping local data");
            store.begin_bootstrap()?;
        }
        let st = store.load_state()?;
        *store.state.write().unwrap() = st;
        Ok(store)
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn state(&self) -> State {
        self.state.read().unwrap().clone()
    }

    pub fn persist(&self) -> Result<()> {
        Ok(self.db.persist(PersistMode::SyncAll)?)
    }

    fn load_state(&self) -> Result<State> {
        let e = State::empty();
        Ok(State {
            db_id: self
                .meta
                .get(DB_ID)?
                .and_then(|v| <[u8; 16]>::try_from(&v[..]).ok())
                .map(DbId),
            applied: as_u64(self.meta.get(APPLIED)?).unwrap_or(e.applied),
            hash: as_hash(self.meta.get(HASH)?).unwrap_or(e.hash),
            log_floor: as_u64(self.meta.get(LOG_FLOOR)?).unwrap_or(e.log_floor),
            log_floor_hash: as_hash(self.meta.get(LOG_FLOOR_HASH)?).unwrap_or(e.log_floor_hash),
            history_floor: as_u64(self.meta.get(HISTORY_FLOOR)?).unwrap_or(e.history_floor),
            origin: as_u64(self.meta.get(ORIGIN)?).unwrap_or(e.origin),
        })
    }

    fn put_state(&self, b: &mut OwnedWriteBatch, s: &State) {
        if let Some(id) = s.db_id {
            b.insert(&self.meta, DB_ID, id.0.to_vec());
        }
        b.insert(&self.meta, APPLIED, s.applied.to_be_bytes().to_vec());
        b.insert(&self.meta, HASH, s.hash.to_vec());
        b.insert(&self.meta, LOG_FLOOR, s.log_floor.to_be_bytes().to_vec());
        b.insert(&self.meta, LOG_FLOOR_HASH, s.log_floor_hash.to_vec());
        b.insert(
            &self.meta,
            HISTORY_FLOOR,
            s.history_floor.to_be_bytes().to_vec(),
        );
        b.insert(&self.meta, ORIGIN, s.origin.to_be_bytes().to_vec());
    }

    fn batch(&self, mode: PersistMode) -> OwnedWriteBatch {
        self.db.batch().durability(Some(mode))
    }

    /// Creates the database identity on a fresh root.
    pub fn init_root(&self) -> Result<DbId> {
        let mut st = self.state();
        if let Some(id) = st.db_id {
            return Ok(id);
        }
        let id = DbId::random();
        st.db_id = Some(id);
        let mut b = self.batch(PersistMode::SyncAll);
        self.put_state(&mut b, &st);
        b.commit()?;
        *self.state.write().unwrap() = st;
        Ok(id)
    }

    // ---- reads -------------------------------------------------------------

    fn decode(key: &[u8], raw: &[u8]) -> Result<Record> {
        Record::decode(key, raw).map_err(|r| Error::corrupt(key, r))
    }

    pub fn get_record(&self, key: &[u8]) -> Result<Option<Record>> {
        self.data
            .get(key)?
            .map(|raw| Self::decode(key, &raw))
            .transpose()
    }

    /// Write index of the key's live value, or 0 if absent.
    pub fn version_of(&self, key: &[u8]) -> Result<Index> {
        Ok(match self.get_record(key)? {
            Some(r) if !r.is_tombstone() => r.index,
            _ => 0,
        })
    }

    /// Reads the latest value of several keys from one consistent snapshot.
    pub fn read_latest(&self, keys: &[Vec<u8>]) -> Result<(Index, Vec<Option<Record>>)> {
        let snap = self.db.snapshot();
        let index = as_u64(snap.get(&self.meta, APPLIED)?).unwrap_or(0);
        let records = keys
            .iter()
            .map(|k| {
                snap.get(&self.data, k)?
                    .map(|raw| Self::decode(k, &raw))
                    .transpose()
            })
            .collect::<Result<_>>()?;
        Ok((index, records))
    }

    /// Reads keys as of index `min(at, applied)` using MVCC history. A `None`
    /// record means the key did not exist at that index.
    pub fn get_at(&self, keys: &[Vec<u8>], at: Index) -> Result<AtRead> {
        let snap = self.db.snapshot();
        let applied = as_u64(snap.get(&self.meta, APPLIED)?).unwrap_or(0);
        let floor = as_u64(snap.get(&self.meta, HISTORY_FLOOR)?).unwrap_or(0);
        let served = at.min(applied);
        if served < floor {
            return Ok(AtRead::HistoryUnavailable { floor });
        }
        let mut records = Vec::with_capacity(keys.len());
        for key in keys {
            let current = snap
                .get(&self.data, key)?
                .map(|raw| Self::decode(key, &raw))
                .transpose()?;
            let rec = match current {
                Some(r) if r.index <= served => Some(r),
                _ => {
                    // The version valid at `served` is the one with the
                    // smallest superseded_at > served.
                    let lo = history_key(key, served.saturating_add(1));
                    let hi = history_key(key, Index::MAX);
                    match snap.range(&self.history, lo..=hi).next() {
                        Some(g) => {
                            let (_, raw) = g.into_inner()?;
                            let r = Self::decode(key, &raw)?;
                            (r.index <= served).then_some(r)
                        }
                        None => None,
                    }
                }
            };
            records.push(rec);
        }
        Ok(AtRead::Ok {
            index: served,
            records,
        })
    }

    /// Live keys with `prefix`, strictly after `after`, from one snapshot.
    pub fn scan(&self, prefix: &[u8], after: Option<&[u8]>, limit: usize) -> Result<Scanned> {
        let snap = self.db.snapshot();
        let index = as_u64(snap.get(&self.meta, APPLIED)?).unwrap_or(0);
        let lo = match after.filter(|a| *a >= prefix) {
            Some(a) => Bound::Excluded(a.to_vec()),
            None => Bound::Included(prefix.to_vec()),
        };
        let mut out = Vec::new();
        for g in snap.range(&self.data, (lo, Bound::<Vec<u8>>::Unbounded)) {
            if out.len() >= limit {
                break;
            }
            let (k, raw) = g.into_inner()?;
            if !k.starts_with(prefix) {
                break;
            }
            let r = Self::decode(&k, &raw)?;
            if !r.is_tombstone() {
                out.push((k.to_vec(), r));
            }
        }
        Ok((index, out))
    }

    pub fn data_keys(&self) -> impl Iterator<Item = Result<Vec<u8>>> + '_ {
        self.data
            .iter()
            .map(|g| g.key().map(|k| k.to_vec()).map_err(Error::from))
    }

    pub fn cached_len(&self) -> usize {
        self.data.approximate_len()
    }

    fn log_entry(&self, i: Index) -> Result<Option<LogEntry>> {
        self.log
            .get(index_key(i))?
            .map(|raw| postcard::from_bytes(&raw).map_err(Error::from))
            .transpose()
    }

    /// Contiguous log entries starting at `from`, bounded by count and bytes.
    pub fn log_range(
        &self,
        from: Index,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>> {
        let mut out = Vec::new();
        let mut bytes = 0;
        for g in self.log.range(index_key(from)..) {
            if out.len() >= max_entries || bytes >= max_bytes {
                break;
            }
            let (_, raw) = g.into_inner()?;
            let e: LogEntry = postcard::from_bytes(&raw)?;
            if e.index != from + out.len() as u64 {
                break;
            }
            bytes += raw.len();
            out.push(e);
        }
        Ok(out)
    }

    /// Chain hash at index `i`, if this store can still prove it.
    pub fn hash_at(&self, i: Index) -> Result<Option<Hash>> {
        let st = self.state();
        if i > st.applied || i + 1 < st.log_floor {
            return Ok(None);
        }
        if i + 1 == st.log_floor {
            return Ok(Some(st.log_floor_hash));
        }
        Ok(self.log_entry(i)?.map(|e| e.hash))
    }

    // ---- writes (single writer: only the applier thread calls these) -------

    /// Applies verified, contiguous log entries in one atomic batch.
    pub fn apply_entries(
        &self,
        entries: &[LogEntry],
        mode: ApplyMode,
        durability: PersistMode,
    ) -> Result<()> {
        let mut st = self.state();
        let mut overlay: HashMap<&[u8], Record> = HashMap::new();
        let mut b = self.batch(durability);
        for e in entries {
            if e.index != st.applied + 1 {
                return Err(Error::Gap {
                    expected: st.applied + 1,
                    got: e.index,
                });
            }
            if !e.verify(&st.hash) {
                return Err(Error::HashMismatch(e.index));
            }
            for op in &e.ops {
                let key = op.key();
                if mode.history {
                    let old = match overlay.get(key) {
                        Some(r) => Some(r.clone()),
                        None => self.get_record(key)?,
                    };
                    if let Some(old) = old {
                        b.insert(&self.history, history_key(key, e.index), old.encode(key));
                    }
                }
                let rec = Record {
                    index: e.index,
                    value: op.value().map(<[u8]>::to_vec),
                };
                b.insert(&self.data, key.to_vec(), rec.encode(key));
                if mode.history {
                    overlay.insert(key, rec);
                }
            }
            if mode.log {
                b.insert(&self.log, index_key(e.index), postcard::to_allocvec(e)?);
            }
            st.applied = e.index;
            st.hash = e.hash;
        }
        self.put_state(&mut b, &st);
        b.commit()?;
        *self.state.write().unwrap() = st;
        Ok(())
    }

    /// Marks a bootstrap in progress and clears all replicated data. If the
    /// process dies before [`Store::finish_bootstrap`], the next open wipes again.
    pub fn begin_bootstrap(&self) -> Result<()> {
        self.meta.insert(BOOTSTRAPPING, vec![1])?;
        self.db.persist(PersistMode::SyncAll)?;
        self.data.clear()?;
        self.history.clear()?;
        self.log.clear()?;
        for k in [
            DB_ID,
            APPLIED,
            HASH,
            LOG_FLOOR,
            LOG_FLOOR_HASH,
            HISTORY_FLOOR,
            ORIGIN,
        ] {
            self.meta.remove(k)?;
        }
        *self.state.write().unwrap() = State::empty();
        Ok(())
    }

    /// Ingests raw snapshot records, verifying each checksum.
    pub fn ingest_snapshot(&self, items: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let mut b = self.batch(PersistMode::Buffer);
        for (k, raw) in items {
            let r = Self::decode(k, raw)?;
            if !r.is_tombstone() {
                b.insert(&self.data, k.clone(), raw.clone());
            }
        }
        b.commit()?;
        Ok(())
    }

    pub fn finish_bootstrap(&self, db_id: DbId, index: Index, hash: Hash) -> Result<()> {
        let st = State {
            db_id: Some(db_id),
            applied: index,
            hash,
            log_floor: index + 1,
            log_floor_hash: hash,
            history_floor: index,
            origin: index,
        };
        let mut b = self.batch(PersistMode::SyncAll);
        self.put_state(&mut b, &st);
        b.remove(&self.meta, BOOTSTRAPPING);
        b.commit()?;
        *self.state.write().unwrap() = st;
        Ok(())
    }

    /// Consistent snapshot of live data for bootstrapping a downstream node.
    /// Calls `sink` with chunks of `(key, raw record)`; stops if it returns false.
    pub fn export_snapshot(
        &self,
        chunk_bytes: usize,
        mut start: impl FnMut(Index, Hash) -> bool,
        mut sink: impl FnMut(Vec<(Vec<u8>, Vec<u8>)>) -> bool,
    ) -> Result<()> {
        let snap = self.db.snapshot();
        let index = as_u64(snap.get(&self.meta, APPLIED)?).unwrap_or(0);
        let hash = as_hash(snap.get(&self.meta, HASH)?).unwrap_or(GENESIS_HASH);
        if !start(index, hash) {
            return Ok(());
        }
        let mut chunk = Vec::new();
        let mut size = 0;
        for g in snap.iter(&self.data) {
            let (k, raw) = g.into_inner()?;
            // Verify before forwarding so corruption never spreads downstream.
            if Self::decode(&k, &raw)?.is_tombstone() {
                continue;
            }
            size += k.len() + raw.len();
            chunk.push((k.to_vec(), raw.to_vec()));
            if size >= chunk_bytes {
                if !sink(std::mem::take(&mut chunk)) {
                    return Ok(());
                }
                size = 0;
            }
        }
        if !chunk.is_empty() {
            sink(chunk);
        }
        Ok(())
    }

    /// Proxy cache fill: inserts records for keys that are still absent.
    pub fn fill_absent(&self, items: &[(Vec<u8>, Record)]) -> Result<Vec<Vec<u8>>> {
        let mut b = self.batch(PersistMode::Buffer);
        let mut inserted = Vec::new();
        for (k, r) in items {
            if self.data.get(k)?.is_none() {
                b.insert(&self.data, k.clone(), r.encode(k));
                inserted.push(k.clone());
            }
        }
        b.commit()?;
        Ok(inserted)
    }

    pub fn remove_data(&self, keys: &[Vec<u8>]) -> Result<()> {
        let mut b = self.batch(PersistMode::Buffer);
        for k in keys {
            b.remove(&self.data, k.clone());
        }
        b.commit()?;
        Ok(())
    }

    /// Advances the history and log floors toward the retention targets,
    /// doing at most `max_steps` entries of work. Returns true if more remains.
    pub fn prune(&self, log_keep: u64, history_keep: u64, max_steps: usize) -> Result<bool> {
        let mut st = self.state();
        let target_hist = st
            .applied
            .saturating_sub(history_keep)
            .max(st.history_floor);
        let mut b = self.batch(PersistMode::Buffer);
        let mut steps = 0;

        // History versions superseded at i <= floor can never be read again.
        while st.history_floor < target_hist && steps < max_steps {
            let i = st.history_floor + 1;
            if let Some(e) = self.log_entry(i)? {
                for op in &e.ops {
                    b.remove_weak(&self.history, history_key(op.key(), i));
                    if let Op::Delete { key } = op
                        && let Some(r) = self.get_record(key)?
                        && r.index == i
                        && r.is_tombstone()
                    {
                        b.remove(&self.data, key.clone());
                    }
                }
            }
            st.history_floor = i;
            steps += 1;
        }

        // The log must keep every entry history pruning still needs.
        let target_log = (st.applied.saturating_sub(log_keep) + 1).min(st.history_floor + 1);
        while st.log_floor < target_log && steps < max_steps {
            let i = st.log_floor;
            let Some(e) = self.log_entry(i)? else { break };
            b.remove_weak(&self.log, index_key(i));
            st.log_floor = i + 1;
            st.log_floor_hash = e.hash;
            steps += 1;
        }

        if steps == 0 {
            return Ok(false);
        }
        self.put_state(&mut b, &st);
        b.commit()?;
        *self.state.write().unwrap() = st.clone();
        Ok(st.history_floor < target_hist || st.log_floor < target_log)
    }
}
