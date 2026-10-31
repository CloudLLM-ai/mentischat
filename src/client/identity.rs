//! The agent's Ed25519 identity and the client's on-disk state.
//!
//! Layout under the client directory (default `~/.cloudllm/mentischat/client`,
//! `MENTISCHAT_CLIENT_DIR` overrides), kept apart from the daemon's data
//! directory so a daemon wipe never deletes an identity:
//!
//! ```text
//! <dir>/<agent>/identity.key   64 hex digits: the 32-byte Ed25519 seed, 0600
//! <dir>/<agent>/state.json     MLS storage and the room plaintext log, 0600
//! ```
//!
//! The directory is `0700`. Files are written to a temporary name with
//! `0600` and renamed over the old one, so a crash never leaves a half-written
//! key or state file, and no moment exists at which another user could read
//! one.

use std::fs;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use openmls_traits::random::OpenMlsRand;

use super::ClientError;
use crate::signed::{hex_decode, hex_encode, Keypair, PublicKey};

/// The client directory: `MENTISCHAT_CLIENT_DIR`, else
/// `$HOME/.cloudllm/mentischat/client`.
///
/// # Errors
///
/// [`ClientError::Storage`] when neither the variable nor `HOME` is set.
pub fn default_client_dir() -> Result<PathBuf, ClientError> {
    match std::env::var_os("MENTISCHAT_CLIENT_DIR").filter(|v| !v.is_empty()) {
        Some(dir) => Ok(PathBuf::from(dir)),
        None => std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".cloudllm/mentischat/client"))
            .ok_or_else(|| ClientError::Storage("HOME is not set".into())),
    }
}

/// One agent's signing identity, loaded from or created in its key file.
pub struct Identity {
    agent: String,
    seed: [u8; 32],
    keys: Keypair,
    dir: PathBuf,
}

impl std::fmt::Debug for Identity {
    /// Never prints the seed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Identity({} {})", self.agent, self.keys.public())
    }
}

impl Identity {
    /// Load `agent`'s identity under `client_dir`, creating a fresh random one
    /// (and the directory) if none exists.
    ///
    /// # Errors
    ///
    /// [`ClientError::Storage`] when a file cannot be read, written, or holds
    /// something other than 64 hex digits, or its permissions admit others.
    pub fn open(client_dir: &Path, agent: &str) -> Result<Self, ClientError> {
        let dir = client_dir.join(agent.to_ascii_lowercase());
        make_private_dir(&dir)?;
        let path = dir.join("identity.key");
        let seed = match fs::read_to_string(&path) {
            Ok(text) => {
                refuse_loose_permissions(&path)?;
                hex_decode::<32>(text.trim())
                    .map_err(|e| ClientError::Storage(format!("{}: {e}", path.display())))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let seed = fresh_seed()?;
                write_private(&path, format!("{}\n", hex_encode(&seed)).as_bytes())?;
                seed
            }
            Err(e) => return Err(ClientError::Storage(format!("{}: {e}", path.display()))),
        };
        Ok(Self {
            agent: agent.to_string(),
            keys: Keypair::from_seed(seed),
            seed,
            dir,
        })
    }

    /// The agent name this identity acts as.
    pub fn agent(&self) -> &str {
        &self.agent
    }

    /// The public key, 32 raw bytes as MentisDB's `public_key_bytes`.
    pub fn public(&self) -> PublicKey {
        self.keys.public()
    }

    /// The signing half, for the client's own use.
    pub fn keypair(&self) -> &Keypair {
        &self.keys
    }

    /// The raw 32-byte seed, for importing the same key into the MLS
    /// credential. Never send it anywhere.
    pub(super) fn seed(&self) -> &[u8; 32] {
        &self.seed
    }

    /// The directory holding this agent's files.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// A random 32-byte seed from the OS, through the same provider OpenMLS uses.
fn fresh_seed() -> Result<[u8; 32], ClientError> {
    openmls_rust_crypto::RustCrypto::default()
        .random_array::<32>()
        .map_err(|e| ClientError::Storage(format!("no randomness: {e:?}")))
}

fn make_private_dir(dir: &Path) -> Result<(), ClientError> {
    let io = |e: std::io::Error| ClientError::Storage(format!("{}: {e}", dir.display()));
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(io)?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(io)
}

/// A key file readable by group or others is a leaked key: refuse it.
fn refuse_loose_permissions(path: &Path) -> Result<(), ClientError> {
    let mode = fs::metadata(path)
        .map_err(|e| ClientError::Storage(format!("{}: {e}", path.display())))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        return Err(ClientError::Storage(format!(
            "{} is accessible to other users (mode {:o}); chmod 600 it",
            path.display(),
            mode & 0o777
        )));
    }
    Ok(())
}

/// Write `bytes` to `path` atomically with mode `0600`.
pub(super) fn write_private(path: &Path, bytes: &[u8]) -> Result<(), ClientError> {
    let io = |e: std::io::Error| ClientError::Storage(format!("{}: {e}", path.display()));
    let tmp = path.with_extension("tmp");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(io)?;
    file.write_all(bytes).map_err(io)?;
    file.sync_all().map_err(io)?;
    fs::rename(&tmp, path).map_err(io)
}
