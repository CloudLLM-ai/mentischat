//! The conversation map with a per-agent index: which conversations a name
//! takes part in, so that `history` and a deregistration cost the agent's
//! own conversations, not every conversation on the bus.
//!
//! The index is a hint that is never allowed to miss: a conversation is
//! indexed (under both names) right after it is created and before anyone can
//! file a message in it, and a key leaves the index only under the set's lock
//! while the conversation is absent from the map. A stale key (a conversation
//! gone since) is skipped when read.
//!
//! Lock order, below the roster and agents: a map shard, then (never while
//! holding one) an index set; the pruning path takes a set, then reads one
//! map shard, and nothing holds a map shard while taking a set.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use super::log::Convo;
use super::sharded::Sharded;

/// The keys of the conversations one name is in.
#[derive(Default)]
struct Keys {
    set: HashSet<String>,
    /// Set once the entry has left the index map: an adder that finds it
    /// looks the name up again instead of filing into an orphan.
    dead: bool,
}

type KeySet = Arc<Mutex<Keys>>;

/// Conversations by [`super::state::convo_key`], plus the index by agent.
pub(super) struct ConvoMap {
    map: Sharded<Arc<Convo>>,
    by_agent: Sharded<KeySet>,
}

/// The two folded names a conversation key joins.
fn sides(key: &str) -> impl Iterator<Item = &str> {
    key.split('\n')
}

impl ConvoMap {
    pub(super) fn new() -> Self {
        Self {
            map: Sharded::new(),
            by_agent: Sharded::new(),
        }
    }

    /// The conversation under `key`, if any.
    pub(super) fn get(&self, key: &str) -> Option<Arc<Convo>> {
        self.map.get(key)
    }

    /// The conversation under `key`, made by `make` (and indexed under both
    /// names) if there is none.
    pub(super) fn get_or_insert_with(
        &self,
        key: &str,
        make: impl FnOnce() -> Arc<Convo>,
    ) -> Arc<Convo> {
        if let Some(found) = self.map.get(key) {
            return found;
        }
        let mut created = false;
        let convo = self.map.get_or_insert_with(key, || {
            created = true;
            make()
        });
        if created {
            sides(key).for_each(|side| self.index(side, key));
        }
        convo
    }

    fn index(&self, side: &str, key: &str) {
        loop {
            let keys = self.by_agent.get_or_insert_with(side, KeySet::default);
            let mut k = keys.lock();
            if !k.dead {
                k.set.insert(key.to_string());
                return;
            }
        }
    }

    /// Take `key` out of both names' sets if its conversation is gone, and
    /// drop a set left empty.
    fn prune(&self, key: &str) {
        for side in sides(key) {
            let Some(keys) = self.by_agent.get(side) else {
                continue;
            };
            let emptied = {
                let mut k = keys.lock();
                if self.map.get(key).is_none() {
                    k.set.remove(key);
                }
                k.set.is_empty()
            };
            if emptied {
                self.by_agent.remove_if(side, |s| {
                    let mut k = s.lock();
                    let empty = k.set.is_empty();
                    k.dead |= empty;
                    empty
                });
            }
        }
    }

    /// Remove the conversation under `key` if `is_it` accepts it.
    pub(super) fn remove_if(&self, key: &str, is_it: impl Fn(&Arc<Convo>) -> bool) {
        self.map.remove_if(key, is_it);
        self.prune(key);
    }

    /// Every conversation `folded` takes part in, with its key.
    pub(super) fn of_agent(&self, folded: &str) -> Vec<(String, Arc<Convo>)> {
        let Some(keys) = self.by_agent.get(folded) else {
            return Vec::new();
        };
        let keys: Vec<String> = keys.lock().set.iter().cloned().collect();
        keys.into_iter()
            .filter_map(|k| self.map.get(&k).map(|c| (k, c)))
            .collect()
    }

    /// Every conversation whose key `keep` accepts.
    pub(super) fn values_where(&self, keep: impl Fn(&str) -> bool) -> Vec<Arc<Convo>> {
        self.map.values_where(keep)
    }

    /// Remove every conversation `keep` rejects.
    pub(super) fn retain(&self, keep: impl FnMut(&Arc<Convo>) -> bool) {
        let removed = self.map.retain_keyed(keep);
        removed.iter().for_each(|key| self.prune(key));
    }

    /// Remove and return every conversation.
    pub(super) fn drain(&self) -> Vec<Arc<Convo>> {
        self.by_agent.drain();
        self.map.drain()
    }
}
