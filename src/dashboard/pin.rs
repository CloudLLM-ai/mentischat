//! The dashboard PIN: where it comes from and how it is kept.
//!
//! `MENTISCHAT_DASHBOARD_PIN` wins and cannot be changed from the page.
//! Otherwise the PIN is the one set on the dashboard itself, the first time
//! it is opened, kept in `<data dir>/dashboard.pin` as a salted, iterated
//! SHA-256 (`0600`), never in the clear. Until one is set the dashboard shows
//! only the form that sets it.
//!
//! Anyone who can reach the loopback listener before the operator does could
//! set the PIN first; that is the same reach the daemon already trusts with
//! every unauthenticated read. Set `MENTISCHAT_DASHBOARD_PIN` to close it.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::signed::{hex_decode, hex_encode};

/// The file that keeps a PIN set on the dashboard.
pub const PIN_FILE: &str = "dashboard.pin";
/// Shortest PIN accepted.
pub const MIN_PIN_CHARS: usize = 6;
/// Longest PIN accepted.
pub const MAX_PIN_CHARS: usize = 128;
/// SHA-256 rounds over the salted PIN.
const ROUNDS: u32 = 100_000;

/// Where the PIN comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinSource {
    /// `MENTISCHAT_DASHBOARD_PIN`; fixed for the run.
    Env,
    /// Set on the dashboard; can be changed there.
    Dashboard,
    /// None yet: the dashboard asks for one.
    Unset,
}

#[derive(Clone)]
enum Secret {
    Plain(String),
    Hashed { salt: [u8; 16], hash: [u8; 32] },
}

/// The PIN and its file.
pub(super) struct Pin {
    env: bool,
    secret: Option<Secret>,
    file: Option<PathBuf>,
}

impl std::fmt::Debug for Pin {
    /// Never prints the PIN or its hash.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pin({:?})", self.source())
    }
}

fn hash(salt: &[u8; 16], pin: &str) -> [u8; 32] {
    let mut out: [u8; 32] = Sha256::new()
        .chain_update(salt)
        .chain_update(pin.as_bytes())
        .finalize()
        .into();
    for _ in 1..ROUNDS {
        out = Sha256::new()
            .chain_update(out)
            .chain_update(salt)
            .finalize()
            .into();
    }
    out
}

/// Equal in time independent of where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

/// Why a new PIN was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinRefused {
    /// Shorter than [`MIN_PIN_CHARS`] or longer than [`MAX_PIN_CHARS`].
    Length,
    /// Fixed by `MENTISCHAT_DASHBOARD_PIN`.
    FromEnv,
    /// It could not be written.
    Storage,
}

impl Pin {
    /// The PIN from `env`, else from `dir/dashboard.pin`, else none.
    pub(super) fn load(env: Option<String>, dir: Option<&Path>) -> Self {
        let file = dir.map(|d| d.join(PIN_FILE));
        if let Some(pin) = env {
            return Self {
                env: true,
                secret: Some(Secret::Plain(pin)),
                file,
            };
        }
        let secret = file.as_deref().and_then(|path| {
            let text = fs::read_to_string(path).ok()?;
            let mut parts = text.split_whitespace();
            match (parts.next(), parts.next(), parts.next(), parts.next()) {
                (Some("v1"), Some(salt), Some(h), None) => Some(Secret::Hashed {
                    salt: hex_decode::<16>(salt).ok()?,
                    hash: hex_decode::<32>(h).ok()?,
                }),
                _ => {
                    tracing::warn!(file = %path.display(), "ignoring a dashboard PIN file this build cannot read");
                    None
                }
            }
        });
        Self {
            env: false,
            secret,
            file,
        }
    }

    pub(super) fn source(&self) -> PinSource {
        match (&self.secret, self.env) {
            (None, _) => PinSource::Unset,
            (Some(_), true) => PinSource::Env,
            (Some(_), false) => PinSource::Dashboard,
        }
    }

    /// Whether `given` is the PIN. Always false while none is set.
    pub(super) fn matches(&self, given: &str) -> bool {
        match &self.secret {
            None => false,
            Some(Secret::Plain(pin)) => same(pin.as_bytes(), given.as_bytes()),
            Some(Secret::Hashed { salt, hash: h }) => same(h, &hash(salt, given)),
        }
    }

    /// Replace the PIN with `new` (the caller checked the old one, or there
    /// was none), and write it to the file when there is one.
    pub(super) fn set(&mut self, new: &str) -> Result<(), PinRefused> {
        if self.env {
            return Err(PinRefused::FromEnv);
        }
        let chars = new.chars().count();
        if !(MIN_PIN_CHARS..=MAX_PIN_CHARS).contains(&chars) {
            return Err(PinRefused::Length);
        }
        let mut salt = [0u8; 16];
        salt.copy_from_slice(&uuid::Uuid::new_v4().into_bytes());
        let h = hash(&salt, new);
        if let Some(path) = &self.file {
            write_private(path, &format!("v1 {} {}\n", hex_encode(&salt), hex_encode(&h)))
                .map_err(|e| {
                    tracing::error!(error = %e, file = %path.display(), "could not save the dashboard PIN");
                    PinRefused::Storage
                })?;
        }
        self.secret = Some(Secret::Hashed { salt, hash: h });
        Ok(())
    }
}

/// Write `text` to `path` through a `0600` temporary file and a rename.
fn write_private(path: &Path, text: &str) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("pin.tmp");
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)?;
    f.write_all(text.as_bytes())?;
    f.sync_all()?;
    fs::rename(&tmp, path)
}
