//! The key registry: which public key owns which name, kept on disk.
//!
//! Mail lives only in memory, but identity does not: an agent's name, once
//! bound to an Ed25519 public key (`PUT /agents/{agent}/key`), stays reserved
//! for that key across idle expiry, crashes and restarts. Only a deregister
//! signed by the key releases it. A name in the registry that is not on the
//! roster comes back only through a `reclaim` proven by its key, which binds
//! the key to the new registration at once.
//!
//! The registry is opt-in for the library ([`Bus::with_key_registry`]) and
//! always on in the daemon, at `{MENTISCHAT_DIR}/keys.jsonl`. It holds public
//! data only: names, public keys and the proofs of possession that bound them.
//!
//! # File format
//!
//! One JSON object per line, appended and `fsync`ed before the change it
//! records takes effect, in a `0600` file inside a `0700` directory:
//!
//! ```text
//! {"bind":{"agent":"Vega","key":"<64 hex>","proof":"<128 hex>"}}
//! {"counter":{"agent":"vega","counter":1731300000000000}}
//! {"release":{"agent":"vega"}}
//! {"rotate":{"agent":"vega","old_key":"<64 hex>","new_key":"<64 hex>","counter":1731300000000001,"signature":"<128 hex>","proof":"<128 hex>"}}
//! ```
//!
//! - `bind` records the name as registered and the signature over
//!   [`register_key_payload`] that bound the key. On load the proof is checked
//!   again, so an edited file cannot bind a key nobody holds; a record that
//!   fails is dropped and counted.
//! - `counter` raises the highest counter the key has signed for its name
//!   (reclaims, deregisters, KeyPackages), so a captured proof cannot be
//!   replayed after a restart.
//! - `release` deletes the binding (a signed deregister).
//! - `rotate` replaces the key: `signature` is the old key's, over
//!   [`rotate_key_payload`], and `proof` the new key's, over
//!   [`register_key_payload`]. On load both are checked again and the old key
//!   must be the one bound; compaction writes the new binding as a `bind`.
//!
//! On open the log is compacted: rewritten, atomically, with one `bind` and
//! one `counter` line per live binding. Binding is rare (once per identity),
//! so every write is an append plus `fsync` and never touches the message
//! path. What a writer of the file can still do is delete or roll back
//! records, the same power as editing the snapshot; it cannot forge a binding.
//!
//! [`Bus::with_key_registry`]: super::Bus::with_key_registry

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::signed::{register_key_payload, rotate_key_payload, PublicKey, Signature};

/// The registry's file name under the daemon's data directory.
pub const KEY_REGISTRY_FILE: &str = "keys.jsonl";

/// One binding: the name as registered, its key, the proof that bound it, and
/// the highest counter the key has signed for the name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Bound {
    pub key: PublicKey,
    pub proof: Signature,
    pub counter: u64,
}

/// What [`Bus::with_key_registry`](super::Bus::with_key_registry) found in an
/// existing registry file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KeyRegistryLoad {
    /// Bindings restored.
    pub bound: usize,
    /// Lines dropped: unparsable, or a `bind` whose proof does not verify.
    pub dropped: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Record {
    Bind {
        agent: String,
        key: String,
        proof: String,
    },
    Counter {
        agent: String,
        counter: u64,
    },
    Release {
        agent: String,
    },
    Rotate {
        agent: String,
        old_key: String,
        new_key: String,
        counter: u64,
        signature: String,
        proof: String,
    },
}

/// A key rotation as the registry records it.
#[derive(Debug, Clone, Copy)]
pub(super) struct Rotation {
    /// The key being replaced.
    pub old: PublicKey,
    /// The key replacing it.
    pub new: PublicKey,
    /// The signer's counter.
    pub counter: u64,
    /// The old key's signature over [`rotate_key_payload`].
    pub signature: Signature,
    /// The new key's signature over [`register_key_payload`].
    pub proof: Signature,
}

impl Rotation {
    /// Whether both signatures hold for `name`.
    pub(super) fn verifies(&self, name: &str) -> bool {
        self.old
            .verify(
                &rotate_key_payload(name, self.counter, &self.new),
                &self.signature,
            )
            .is_ok()
            && self
                .new
                .verify(&register_key_payload(name, &self.new), &self.proof)
                .is_ok()
    }
}

/// The on-disk key registry, with its bindings in memory for lookups.
pub(super) struct KeyRing {
    path: PathBuf,
    /// Folded name -> (name as registered, binding).
    entries: Mutex<HashMap<String, (String, Bound)>>,
    file: Mutex<fs::File>,
}

impl std::fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KeyRing({}, {} bound)",
            self.path.display(),
            self.entries.lock().len()
        )
    }
}

fn fold(name: &str) -> String {
    name.to_ascii_lowercase()
}

impl KeyRing {
    /// Open the registry at `path`, creating its directory (`0700`) and file
    /// (`0600`) if missing, replaying and verifying what is there, and
    /// compacting it.
    ///
    /// # Errors
    ///
    /// Any filesystem error creating, reading or rewriting the file.
    pub fn open(path: &Path) -> io::Result<(Self, KeyRegistryLoad)> {
        if let Some(dir) = path.parent() {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir)?;
        }
        let mut entries: HashMap<String, (String, Bound)> = HashMap::new();
        let mut load = KeyRegistryLoad::default();
        match fs::File::open(path) {
            Ok(file) => {
                for line in io::BufReader::new(file).lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    if !apply(&mut entries, &line) {
                        load.dropped += 1;
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        load.bound = entries.len();
        compact(path, &entries)?;
        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Ok((
            Self {
                path: path.to_path_buf(),
                entries: Mutex::new(entries),
                file: Mutex::new(file),
            },
            load,
        ))
    }

    /// The binding for the folded name `key`, if any.
    pub fn get(&self, key: &str) -> Option<Bound> {
        self.entries.lock().get(key).map(|(_, b)| *b)
    }

    /// Whether the folded name `key` is reserved by a binding.
    pub fn contains(&self, key: &str) -> bool {
        self.entries.lock().contains_key(key)
    }

    /// Record `key` as bound to `name` by `proof`, durably, then in memory,
    /// as one step under the registry's lock, so two binds of one name
    /// cannot both win. `Ok(false)` when the name is already bound to a
    /// different key (nothing changes); a name already bound to `key` is left
    /// as it is.
    ///
    /// # Errors
    ///
    /// The append or `fsync` failed; the binding then applies in memory only.
    pub fn bind(&self, name: &str, key: PublicKey, proof: Signature) -> io::Result<bool> {
        let folded = fold(name);
        let mut entries = self.entries.lock();
        match entries.get(&folded) {
            Some((_, b)) if b.key == key => return Ok(true),
            Some(_) => return Ok(false),
            None => {}
        }
        entries.insert(
            folded,
            (
                name.to_string(),
                Bound {
                    key,
                    proof,
                    counter: 0,
                },
            ),
        );
        self.append(&Record::Bind {
            agent: name.to_string(),
            key: key.to_hex(),
            proof: proof.to_hex(),
        })
        .map(|()| true)
    }

    /// Raise the counter of the folded name `key` to `counter`, durably, if
    /// it is bound and the counter is higher.
    pub fn raise_counter(&self, key: &str, counter: u64) -> io::Result<()> {
        if !self.get(key).is_some_and(|b| counter > b.counter) {
            return Ok(());
        }
        self.append(&Record::Counter {
            agent: key.to_string(),
            counter,
        })?;
        if let Some((_, b)) = self.entries.lock().get_mut(key) {
            b.counter = b.counter.max(counter);
        }
        Ok(())
    }

    /// Replace the key bound to `name` with `rotation.new`, durably, if
    /// `rotation.old` is the one bound now. `Ok(false)` when it is not, or the
    /// counter is not above the binding's.
    ///
    /// # Errors
    ///
    /// The append or `fsync` failed; nothing changes in memory.
    pub fn rotate(&self, name: &str, rotation: Rotation) -> io::Result<bool> {
        let folded = fold(name);
        let mut entries = self.entries.lock();
        match entries.get(&folded) {
            Some((_, b)) if b.key == rotation.old && rotation.counter > b.counter => {}
            _ => return Ok(false),
        }
        self.append(&Record::Rotate {
            agent: name.to_string(),
            old_key: rotation.old.to_hex(),
            new_key: rotation.new.to_hex(),
            counter: rotation.counter,
            signature: rotation.signature.to_hex(),
            proof: rotation.proof.to_hex(),
        })?;
        entries.insert(
            folded,
            (
                name.to_string(),
                Bound {
                    key: rotation.new,
                    proof: rotation.proof,
                    counter: rotation.counter,
                },
            ),
        );
        Ok(true)
    }

    /// Delete the binding of the folded name `key`, durably.
    pub fn release(&self, key: &str) -> io::Result<()> {
        if !self.contains(key) {
            return Ok(());
        }
        self.append(&Record::Release {
            agent: key.to_string(),
        })?;
        self.entries.lock().remove(key);
        Ok(())
    }

    fn append(&self, record: &Record) -> io::Result<()> {
        let mut line = serde_json::to_vec(record).map_err(io::Error::other)?;
        line.push(b'\n');
        let mut file = self.file.lock();
        file.write_all(&line)?;
        file.sync_data()
    }
}

/// Apply one log line to `entries`; `false` when it is dropped.
fn apply(entries: &mut HashMap<String, (String, Bound)>, line: &str) -> bool {
    let Ok(record) = serde_json::from_str::<Record>(line) else {
        return false;
    };
    match record {
        Record::Bind { agent, key, proof } => {
            let (Ok(key), Ok(proof)) = (key.parse::<PublicKey>(), proof.parse::<Signature>())
            else {
                return false;
            };
            if crate::names::AgentName::parse(&agent).is_err()
                || key
                    .verify(&register_key_payload(&agent, &key), &proof)
                    .is_err()
            {
                return false;
            }
            let counter = entries
                .get(&fold(&agent))
                .filter(|(_, b)| b.key == key)
                .map_or(0, |(_, b)| b.counter);
            entries.insert(
                fold(&agent),
                (
                    agent,
                    Bound {
                        key,
                        proof,
                        counter,
                    },
                ),
            );
            true
        }
        Record::Counter { agent, counter } => match entries.get_mut(&fold(&agent)) {
            Some((_, b)) => {
                b.counter = b.counter.max(counter);
                true
            }
            None => false,
        },
        Record::Release { agent } => entries.remove(&fold(&agent)).is_some(),
        Record::Rotate {
            agent,
            old_key,
            new_key,
            counter,
            signature,
            proof,
        } => {
            let parsed = (
                old_key.parse::<PublicKey>(),
                new_key.parse::<PublicKey>(),
                signature.parse::<Signature>(),
                proof.parse::<Signature>(),
            );
            let (Ok(old), Ok(new), Ok(signature), Ok(proof)) = parsed else {
                return false;
            };
            let rotation = Rotation {
                old,
                new,
                counter,
                signature,
                proof,
            };
            match entries.get_mut(&fold(&agent)) {
                Some((_, b))
                    if b.key == old && counter > b.counter && rotation.verifies(&agent) =>
                {
                    *b = Bound {
                        key: new,
                        proof,
                        counter,
                    };
                    true
                }
                _ => false,
            }
        }
    }
}

/// Rewrite `path` with one `bind` and one `counter` line per binding, via a
/// temporary file renamed over it, so a crash leaves the old or the new log.
fn compact(path: &Path, entries: &HashMap<String, (String, Bound)>) -> io::Result<()> {
    let tmp = path.with_extension("jsonl.tmp");
    let mut out = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    let mut names: Vec<&String> = entries.keys().collect();
    names.sort();
    for folded in names {
        let (agent, b) = &entries[folded];
        let mut lines = serde_json::to_vec(&Record::Bind {
            agent: agent.clone(),
            key: b.key.to_hex(),
            proof: b.proof.to_hex(),
        })
        .map_err(io::Error::other)?;
        lines.push(b'\n');
        if b.counter > 0 {
            lines.extend(
                serde_json::to_vec(&Record::Counter {
                    agent: folded.clone(),
                    counter: b.counter,
                })
                .map_err(io::Error::other)?,
            );
            lines.push(b'\n');
        }
        out.write_all(&lines)?;
    }
    out.sync_all()?;
    fs::rename(&tmp, path)
}
