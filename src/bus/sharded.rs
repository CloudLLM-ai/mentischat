//! The generic concurrent-map layer: cache-line padding, a cheap hash, and a
//! map split into independently locked shards. Knows nothing of mail.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};

use parking_lot::RwLock;

/// Shards per map. Sixteen is enough that a handful of busy conversations
/// rarely share one, and few enough that walking every shard stays cheap.
const SHARDS: usize = 16;

/// A value alone on its cache lines, so a lock or counter written by one
/// thread does not slow reads of its neighbors.
#[derive(Debug, Default)]
#[repr(align(128))]
pub(super) struct Padded<T>(pub T);

/// FNV-1a: a hash that costs a few cycles on the short names that key every
/// map here. The names are roster names a local process chose, capped at 256,
/// so there is nothing for a collision attack to gain and no reason to pay for
/// SipHash on every lookup.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Fnv;

/// The state of one [`Fnv`] hash.
#[derive(Debug)]
pub(super) struct FnvHasher(u64);

impl BuildHasher for Fnv {
    type Hasher = FnvHasher;

    fn build_hasher(&self) -> FnvHasher {
        FnvHasher(0xcbf2_9ce4_8422_2325)
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
        }
    }
}

type Shard<V> = RwLock<HashMap<String, V, Fnv>>;

/// A map split by key hash into independently locked shards.
pub(super) struct Sharded<V> {
    shards: Box<[Padded<Shard<V>>]>,
}

impl<V: Clone> Sharded<V> {
    pub(super) fn new() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Padded(RwLock::default())).collect(),
        }
    }

    fn shard(&self, key: &str) -> &Shard<V> {
        // The map inside hashes the same way, so mix the high bits in: the
        // low ones pick a slot there.
        let h = Fnv.hash_one(key);
        &self.shards[(h >> 32) as usize % SHARDS].0
    }

    /// The value under `key`.
    pub(super) fn get(&self, key: &str) -> Option<V> {
        self.shard(key).read().get(key).cloned()
    }

    /// The value under `key`, made by `make` and inserted if there is none.
    pub(super) fn get_or_insert_with(&self, key: &str, make: impl FnOnce() -> V) -> V {
        if let Some(found) = self.get(key) {
            return found;
        }
        self.shard(key)
            .write()
            .entry(key.to_string())
            .or_insert_with(make)
            .clone()
    }

    /// Insert `value` unless `key` is taken; hands the value back if so.
    pub(super) fn try_insert(&self, key: &str, value: V) -> Result<(), V> {
        use std::collections::hash_map::Entry;
        match self.shard(key).write().entry(key.to_string()) {
            Entry::Occupied(_) => Err(value),
            Entry::Vacant(slot) => {
                slot.insert(value);
                Ok(())
            }
        }
    }

    pub(super) fn insert(&self, key: String, value: V) {
        self.shard(&key).write().insert(key, value);
    }

    pub(super) fn remove(&self, key: &str) -> Option<V> {
        self.shard(key).write().remove(key)
    }

    /// Every entry, one shard at a time (so not a consistent cut).
    pub(super) fn entries(&self) -> Vec<(String, V)> {
        self.shards
            .iter()
            .flat_map(|s| {
                let map = s.0.read();
                map.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Remove the entry under `key` if `is_it` accepts its value.
    pub(super) fn remove_if(&self, key: &str, is_it: impl Fn(&V) -> bool) {
        let mut map = self.shard(key).write();
        if map.get(key).is_some_and(is_it) {
            map.remove(key);
        }
    }

    /// Every value whose key `keep` accepts.
    pub(super) fn values_where(&self, keep: impl Fn(&str) -> bool) -> Vec<V> {
        self.shards
            .iter()
            .flat_map(|s| {
                let map = s.0.read();
                map.iter()
                    .filter(|(k, _)| keep(k))
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// The value with the smallest `rank`, ignoring those that rank `None`.
    /// Walks the shards without copying anything but the winner.
    pub(super) fn min_by_rank<R: Ord>(&self, rank: impl Fn(&V) -> Option<R>) -> Option<V> {
        let mut best: Option<(R, V)> = None;
        for shard in self.shards.iter() {
            for value in shard.0.read().values() {
                let Some(r) = rank(value) else { continue };
                if best.as_ref().is_none_or(|(b, _)| r < *b) {
                    best = Some((r, value.clone()));
                }
            }
        }
        best.map(|(_, v)| v)
    }

    /// Call `visit` on every value, one shard at a time, holding that
    /// shard's read lock (so `visit` may lock an agent, but not change the
    /// map).
    pub(super) fn for_each(&self, mut visit: impl FnMut(&V)) {
        for shard in self.shards.iter() {
            shard.0.read().values().for_each(&mut visit);
        }
    }

    pub(super) fn len(&self) -> usize {
        self.shards.iter().map(|s| s.0.read().len()).sum()
    }

    /// Remove every entry `keep` rejects, shard by shard; returns the keys
    /// removed.
    pub(super) fn retain_keyed(&self, mut keep: impl FnMut(&V) -> bool) -> Vec<String> {
        let mut removed = Vec::new();
        for shard in self.shards.iter() {
            shard.0.write().retain(|k, v| {
                let kept = keep(v);
                if !kept {
                    removed.push(k.clone());
                }
                kept
            });
        }
        removed
    }

    /// Remove and return every value.
    pub(super) fn drain(&self) -> Vec<V> {
        self.shards
            .iter()
            .flat_map(|s| s.0.write().drain().map(|(_, v)| v).collect::<Vec<_>>())
            .collect()
    }
}
