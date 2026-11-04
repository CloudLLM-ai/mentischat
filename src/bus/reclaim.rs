//! Who may `reclaim` a name: the proof check behind
//! [`Bus::register_with`](super::Bus::register_with).
//!
//! A name that is registered, or held after it left the roster, is taken back
//! only by the holder of the key its registration bound, shown by a signature
//! over [`reclaim_payload`] at a counter above every one that key has used. A
//! registration that bound no key cannot be proven: a registered keyless name
//! is never taken back, and a held keyless one is taken back without proof.

use super::model::ReclaimProof;
use super::MailErr;
use crate::names::AgentName;
use crate::signed::{reclaim_payload, PublicKey};

/// What a reclaim's proof shows, given the key the name's registration has
/// (or had) bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Ownership {
    /// The registration bound no key, so nothing can be proven.
    Keyless,
    /// The registration bound a key and the caller sent no proof.
    Unproven,
    /// The proof verifies under the key at this counter.
    Proven(u64),
}

/// Check `proof` for a reclaim of `name` against `bound`: the key the
/// registration bound and the highest counter that key has signed.
///
/// # Errors
///
/// `invalid_signature` when the proof does not verify under the key;
/// `stale_counter` (with the key's highest counter) when its counter is not
/// above it.
pub(super) fn ownership(
    name: &AgentName,
    bound: Option<(PublicKey, u64)>,
    proof: Option<ReclaimProof>,
) -> Result<Ownership, MailErr> {
    let Some((key, last)) = bound else {
        return Ok(Ownership::Keyless);
    };
    let Some(proof) = proof else {
        return Ok(Ownership::Unproven);
    };
    key.verify(
        &reclaim_payload(name.as_str(), proof.counter),
        &proof.signature,
    )
    .map_err(|_| MailErr::InvalidSignature)?;
    if proof.counter <= last {
        return Err(MailErr::StaleCounter {
            room: String::new(),
            last,
        });
    }
    Ok(Ownership::Proven(proof.counter))
}
