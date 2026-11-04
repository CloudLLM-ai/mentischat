//! Who owns a name: the proof check behind a `reclaim`
//! ([`Bus::register_with`](super::Bus::register_with)).
//!
//! A registration that bound a key is owned by the holder of that key, shown
//! by a signature over the operation's payload ([`reclaim_payload`]) at a
//! counter above every one the key has used. A registration that bound no key
//! cannot be proven; each operation decides what that means.
//!
//! [`reclaim_payload`]: crate::signed::reclaim_payload

use super::model::KeyProof;
use super::MailErr;
use crate::names::AgentName;
use crate::signed::PublicKey;

/// What a proof shows, given the key the name's registration has (or had)
/// bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Ownership {
    /// The registration bound no key, so nothing can be proven.
    Keyless,
    /// The registration bound a key and the caller sent no proof.
    Unproven,
    /// The proof verifies under the key at this counter.
    Proven(u64),
}

/// Check `proof` for an operation on `name` against `bound`: the key the
/// registration bound and the highest counter that key has signed.
/// `payload` builds the bytes the operation signs from the name and counter.
///
/// # Errors
///
/// `invalid_signature` when the proof does not verify under the key;
/// `stale_counter` (with the key's highest counter) when its counter is not
/// above it.
pub(super) fn ownership(
    name: &AgentName,
    bound: Option<(PublicKey, u64)>,
    proof: Option<KeyProof>,
    payload: fn(&str, u64) -> Vec<u8>,
) -> Result<Ownership, MailErr> {
    let Some((key, last)) = bound else {
        return Ok(Ownership::Keyless);
    };
    let Some(proof) = proof else {
        return Ok(Ownership::Unproven);
    };
    key.verify(&payload(name.as_str(), proof.counter), &proof.signature)
        .map_err(|_| MailErr::InvalidSignature)?;
    if proof.counter <= last {
        return Err(MailErr::StaleCounter {
            room: String::new(),
            last,
        });
    }
    Ok(Ownership::Proven(proof.counter))
}
