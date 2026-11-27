//! The dashboard's own agent: the identity every room action taken from the
//! dashboard is signed with.
//!
//! The daemon still holds no agent's key. This is a key of its own, for an
//! agent of its own (`operator` unless `MENTISCHAT_OPERATOR` names another),
//! and it obeys the room rules like any agent: it is admin of the rooms it
//! creates, and in a room an agent created it can manage only after an admin
//! promotes it. Every operation it signs is journaled and verified exactly as
//! a client's would be.
//!
//! The 32-byte seed lives in `<data dir>/operator.key` (64 hex digits, `0600`)
//! and survives restarts, so the operator keeps its name binding in the key
//! registry. Without a data directory (a library bus) it is random per
//! process. The operator registers lazily, on the first action, and is a
//! [resident](crate::bus::Bus::keep_resident): the idle sweep never drops it.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::bus::{Bus, KeyProof, Registration, SignedOutcome, SignedRequest};
use crate::errors::MailboxError;
use crate::names::{AgentName, Provider, RoomName};
use crate::signed::{
    direct_payload, hex_decode, hex_encode, reclaim_payload, register_key_payload, Keypair,
    PublicKey, SignedOp,
};

/// The operator's name when `MENTISCHAT_OPERATOR` is unset.
pub const DEFAULT_OPERATOR: &str = "operator";
/// The seed file under the data directory.
pub const OPERATOR_KEY_FILE: &str = "operator.key";

/// The dashboard's signing identity.
pub struct Operator {
    name: AgentName,
    keys: Keypair,
    /// The last counter signed; strictly increasing, and serializes actions.
    last_counter: tokio::sync::Mutex<u64>,
}

impl std::fmt::Debug for Operator {
    /// Never prints the seed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Operator({} {})", self.name, self.keys.public())
    }
}

/// 32 bytes from the operating system's generator.
fn random_seed() -> std::io::Result<[u8; 32]> {
    let mut seed = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut seed)?;
    Ok(seed)
}

/// Microseconds since the Unix epoch.
fn micros_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

impl Operator {
    /// The operator `name` with the seed in `dir/operator.key`, created
    /// (`0600`) when missing; with no `dir`, a random key for this process.
    ///
    /// # Errors
    ///
    /// An I/O error when the file cannot be read or written, or
    /// `InvalidData` when it does not hold 64 hex digits.
    pub fn open(name: AgentName, dir: Option<&Path>) -> std::io::Result<Self> {
        let seed = match dir {
            None => random_seed()?,
            Some(dir) => {
                let path = dir.join(OPERATOR_KEY_FILE);
                match fs::read_to_string(&path) {
                    Ok(text) => hex_decode::<32>(text.trim()).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("{}: {e}", path.display()),
                        )
                    })?,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        let seed = random_seed()?;
                        fs::create_dir_all(dir)?;
                        let tmp = dir.join(format!("{OPERATOR_KEY_FILE}.tmp"));
                        let mut f = fs::OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .mode(0o600)
                            .open(&tmp)?;
                        f.write_all(format!("{}\n", hex_encode(&seed)).as_bytes())?;
                        f.sync_all()?;
                        fs::rename(&tmp, &path)?;
                        seed
                    }
                    Err(e) => return Err(e),
                }
            }
        };
        Ok(Self {
            name,
            keys: Keypair::from_seed(seed),
            last_counter: tokio::sync::Mutex::new(micros_now()),
        })
    }

    /// The operator's name.
    pub fn name(&self) -> &AgentName {
        &self.name
    }

    /// The operator's public key.
    pub fn public_key(&self) -> PublicKey {
        self.keys.public()
    }

    /// Put the operator on the roster with its key bound, if it is not
    /// already: reclaim the name with a proof when the key registry holds it,
    /// register it otherwise, then bind the key. Marks it resident.
    async fn ensure(&self, bus: &Bus, counter: &mut u64) -> Result<(), MailboxError> {
        bus.keep_resident(&self.name);
        let mine = self.keys.public();
        match bus.agent_key(&self.name).await {
            Ok(Some(bound)) if bound.public_key == mine.to_hex() => return Ok(()),
            Ok(Some(_)) => {
                return Err(MailboxError::KeyConflict {
                    agent: self.name.to_string(),
                })
            }
            Ok(None) => {}
            Err(MailboxError::Unregistered { .. }) => {
                *counter = micros_now().max(*counter + 1);
                let opts = Registration {
                    description: Some("the operator, acting from the dashboard".into()),
                    provider: Provider::parse("mentischat-dashboard").ok(),
                    reclaim: true,
                    proof: Some(KeyProof {
                        counter: *counter,
                        signature: self
                            .keys
                            .sign(&reclaim_payload(self.name.as_str(), *counter)),
                    }),
                };
                bus.register_with(&self.name, opts).await?;
                if bus.agent_key(&self.name).await?.is_some() {
                    return Ok(());
                }
            }
            Err(e) => return Err(e),
        }
        let proof = self
            .keys
            .sign(&register_key_payload(self.name.as_str(), &mine));
        bus.register_key(&self.name, mine, &proof).await?;
        Ok(())
    }

    /// Sign and apply one room operation as the operator, retrying once with
    /// a higher counter if the room has seen a higher one (after a restart
    /// with a clock that went back).
    ///
    /// # Errors
    ///
    /// Whatever [`Bus::signed_op`] fails with; `key_conflict` when another
    /// key holds the operator's name.
    pub async fn act(
        &self,
        bus: &Bus,
        room: &RoomName,
        op: SignedOp,
    ) -> Result<SignedOutcome, MailboxError> {
        let mut counter = self.last_counter.lock().await;
        self.ensure(bus, &mut counter).await?;
        for attempt in 0..2 {
            *counter = micros_now().max(*counter + 1);
            let request = SignedRequest {
                agent: self.name.clone(),
                room: room.clone(),
                counter: *counter,
                signature: self
                    .keys
                    .sign(&op.payload(room.as_str(), self.name.as_str(), *counter)),
                op: op.clone(),
            };
            match bus.signed_op(request).await {
                Err(MailboxError::StaleCounter { last, .. }) if attempt == 0 => {
                    *counter = (*counter).max(last);
                }
                other => return other,
            }
        }
        unreachable!("the second attempt returns")
    }

    /// Send `body` to `to` as a signed direct message.
    ///
    /// # Errors
    ///
    /// Whatever [`Bus::send_signed`] fails with.
    pub async fn direct(
        &self,
        bus: &Bus,
        to: &AgentName,
        body: &str,
    ) -> Result<crate::bus::SendReceipt, MailboxError> {
        let mut counter = self.last_counter.lock().await;
        self.ensure(bus, &mut counter).await?;
        for attempt in 0..2 {
            *counter = micros_now().max(*counter + 1);
            let signature = self.keys.sign(&direct_payload(
                self.name.as_str(),
                to.as_str(),
                *counter,
                body,
            ));
            match bus
                .send_signed(&self.name, to, body.to_string(), *counter, &signature)
                .await
            {
                Err(MailboxError::StaleCounter { last, .. }) if attempt == 0 => {
                    *counter = (*counter).max(last);
                }
                other => return other,
            }
        }
        unreachable!("the second attempt returns")
    }

    /// Make sure the operator is registered (for reads that need a roster
    /// name, such as its inbox).
    ///
    /// # Errors
    ///
    /// As [`act`](Self::act).
    pub async fn present(&self, bus: &Bus) -> Result<(), MailboxError> {
        let mut counter = self.last_counter.lock().await;
        self.ensure(bus, &mut counter).await
    }
}
