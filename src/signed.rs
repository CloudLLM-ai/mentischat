//! Ed25519 identities, canonical signed payloads and the message envelope of
//! signed rooms. See `docs/SIGNED_ROOMS.md` for the protocol.
//!
//! Everything here is pure: bytes in, bytes or a typed error out. The bus
//! calls it to verify; the client component calls it to sign. Keys are the
//! raw 32-byte Ed25519 points MentisDB stores as `public_key_bytes`, and
//! signatures the raw 64 bytes, both written as lowercase hex on the wire.
//!
//! # Examples
//!
//! ```
//! use mentischat::signed::{Keypair, SignedOp};
//!
//! let alice = Keypair::from_seed([7; 32]);
//! let op = SignedOp::Send { body: "No reply needed: hi".into() };
//! let payload = op.payload("Ops", "Alice", 1);
//! let signature = alice.sign(&payload);
//! assert!(alice.public().verify(&payload, &signature).is_ok());
//! // Names fold, so the same room under another case signs the same bytes.
//! assert_eq!(payload, op.payload("OPS", "alice", 1));
//! ```

use std::fmt;
use std::str::FromStr;

use ed25519_dalek::{Signature as DalekSignature, Signer, SigningKey, Verifier, VerifyingKey};

/// Domain separation tag every mentischat signature starts with, so a
/// signature made for another protocol that shares the key (MentisDB signs
/// raw content) is never valid here.
const DOMAIN: &[u8] = b"mentischat/v1\0";

/// First token of a signed-room message body.
pub const ENVELOPE_TAG: &str = "mentischat-signed/1";

/// Why a hex string, key or signature did not parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseKeyError {
    /// Not the expected number of hex characters.
    Length,
    /// A character that is not a lowercase or uppercase hex digit.
    NotHex,
    /// 32 bytes that are not a valid Ed25519 point.
    NotAPoint,
}

impl fmt::Display for ParseKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Length => "wrong length",
            Self::NotHex => "not hexadecimal",
            Self::NotAPoint => "not a valid Ed25519 public key",
        })
    }
}

impl std::error::Error for ParseKeyError {}

/// Lowercase hex of `bytes`.
pub fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 15)] as char);
    }
    out
}

/// Decode exactly `N` bytes from `2 * N` hex digits (either case).
///
/// # Errors
///
/// [`ParseKeyError::Length`] or [`ParseKeyError::NotHex`].
///
/// # Examples
///
/// ```
/// use mentischat::signed::{hex_decode, hex_encode};
///
/// let bytes: [u8; 4] = hex_decode("00ff10Ab").unwrap();
/// assert_eq!(hex_encode(&bytes), "00ff10ab");
/// assert!(hex_decode::<4>("00ff").is_err());
/// ```
pub fn hex_decode<const N: usize>(s: &str) -> Result<[u8; N], ParseKeyError> {
    let raw = s.as_bytes();
    if raw.len() != N * 2 {
        return Err(ParseKeyError::Length);
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(ParseKeyError::NotHex),
    };
    let mut out = [0u8; N];
    let (pairs, _) = raw.as_chunks::<2>();
    for (slot, [hi, lo]) in out.iter_mut().zip(pairs) {
        *slot = (digit(*hi)? << 4) | digit(*lo)?;
    }
    Ok(out)
}

/// A validated Ed25519 public key: 32 bytes that decode to a curve point.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PublicKey([u8; 32]);

impl PublicKey {
    /// The key for these raw bytes.
    ///
    /// # Errors
    ///
    /// [`ParseKeyError::NotAPoint`] when they are not a valid point.
    pub fn from_bytes(bytes: [u8; 32]) -> Result<Self, ParseKeyError> {
        VerifyingKey::from_bytes(&bytes)
            .map(|_| Self(bytes))
            .map_err(|_| ParseKeyError::NotAPoint)
    }

    /// The raw 32 bytes, as MentisDB's `public_key_bytes`.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex, 64 characters.
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// Check `signature` over `payload`.
    ///
    /// # Errors
    ///
    /// [`BadSignature`] when it does not verify.
    pub fn verify(&self, payload: &[u8], signature: &Signature) -> Result<(), BadSignature> {
        let key = VerifyingKey::from_bytes(&self.0).map_err(|_| BadSignature)?;
        key.verify(payload, &DalekSignature::from_bytes(&signature.0))
            .map_err(|_| BadSignature)
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey({})", self.to_hex())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for PublicKey {
    type Err = ParseKeyError;

    /// Parse 64 hex digits into a valid point.
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::signed::{Keypair, PublicKey};
    ///
    /// let key = Keypair::from_seed([1; 32]).public();
    /// assert_eq!(key.to_hex().parse::<PublicKey>(), Ok(key));
    /// assert!("zz".parse::<PublicKey>().is_err());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(hex_decode::<32>(s)?)
    }
}

/// A raw Ed25519 signature (64 bytes).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Signature([u8; 64]);

impl Signature {
    /// Lowercase hex, 128 characters.
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// The raw 64 bytes.
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signature({})", self.to_hex())
    }
}

impl FromStr for Signature {
    type Err = ParseKeyError;

    /// Parse 128 hex digits.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        hex_decode::<64>(s).map(Self)
    }
}

/// A signature that did not verify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadSignature;

impl fmt::Display for BadSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Ed25519 signature verification failed")
    }
}

impl std::error::Error for BadSignature {}

/// An Ed25519 signing identity, for the client component and tests. The
/// daemon never holds one.
pub struct Keypair(SigningKey);

impl Keypair {
    /// The identity with this 32-byte secret seed.
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self(SigningKey::from_bytes(&seed))
    }

    /// The public half.
    pub fn public(&self) -> PublicKey {
        PublicKey(self.0.verifying_key().to_bytes())
    }

    /// Sign `payload`.
    pub fn sign(&self, payload: &[u8]) -> Signature {
        Signature(self.0.sign(payload).to_bytes())
    }
}

impl fmt::Debug for Keypair {
    /// Never prints the seed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Keypair({})", self.public())
    }
}

/// What a signed operation does, as the `op` field names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpKind {
    /// Bind a key to a roster name.
    RegisterKey,
    /// Create a signed room.
    Create,
    /// Invite an agent, pinning its key.
    Invite,
    /// Join after an invitation.
    Join,
    /// Leave the room.
    Leave,
    /// Owner removes a member.
    Remove,
    /// Send a message to the room.
    Send,
}

/// An `op` name that is not one of the seven.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownOp;

impl fmt::Display for UnknownOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown signed operation")
    }
}

impl std::error::Error for UnknownOp {}

impl OpKind {
    /// The wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RegisterKey => "register_key",
            Self::Create => "create",
            Self::Invite => "invite",
            Self::Join => "join",
            Self::Leave => "leave",
            Self::Remove => "remove",
            Self::Send => "send",
        }
    }
}

impl fmt::Display for OpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OpKind {
    type Err = UnknownOp;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "register_key" => Self::RegisterKey,
            "create" => Self::Create,
            "invite" => Self::Invite,
            "join" => Self::Join,
            "leave" => Self::Leave,
            "remove" => Self::Remove,
            "send" => Self::Send,
            _ => return Err(UnknownOp),
        })
    }
}

/// A room operation with its operation-specific fields. What the signature
/// covers beyond room, signer and counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignedOp {
    /// Create a signed room.
    Create {
        /// The room's description, empty when none.
        description: String,
    },
    /// Invite `invitee`, whose key must be `invitee_key`.
    Invite {
        /// The invited name.
        invitee: String,
        /// The key the invitation pins.
        invitee_key: PublicKey,
    },
    /// Join after being invited.
    Join,
    /// Leave the room.
    Leave,
    /// Owner removes `target`.
    Remove {
        /// The member removed.
        target: String,
    },
    /// Send `body` to the room.
    Send {
        /// The message text.
        body: String,
    },
}

impl SignedOp {
    /// Which operation this is.
    pub fn kind(&self) -> OpKind {
        match self {
            Self::Create { .. } => OpKind::Create,
            Self::Invite { .. } => OpKind::Invite,
            Self::Join => OpKind::Join,
            Self::Leave => OpKind::Leave,
            Self::Remove { .. } => OpKind::Remove,
            Self::Send { .. } => OpKind::Send,
        }
    }

    /// The exact bytes the signer signs: the domain tag, then the op name,
    /// room and signer (both folded), the counter, and the op's own fields,
    /// each variable field behind a 4-byte big-endian length.
    pub fn payload(&self, room: &str, signer: &str, counter: u64) -> Vec<u8> {
        let mut out = header(self.kind(), room, signer, counter, self.fields_len());
        match self {
            Self::Create { description } => put(&mut out, description.as_bytes()),
            Self::Invite {
                invitee,
                invitee_key,
            } => {
                put(&mut out, invitee.to_ascii_lowercase().as_bytes());
                out.extend_from_slice(invitee_key.as_bytes());
            }
            Self::Join | Self::Leave => {}
            Self::Remove { target } => put(&mut out, target.to_ascii_lowercase().as_bytes()),
            Self::Send { body } => put(&mut out, body.as_bytes()),
        }
        out
    }

    fn fields_len(&self) -> usize {
        match self {
            Self::Create { description } => description.len(),
            Self::Invite { invitee, .. } => invitee.len() + 36,
            Self::Join | Self::Leave => 0,
            Self::Remove { target } => target.len(),
            Self::Send { body } => body.len(),
        }
    }
}

/// The bytes signed to bind `key` to the roster name `signer`: proof of
/// possession for `PUT /agents/{agent}/key`. The counter is fixed at 0 and the
/// room empty, so the payload is the same wherever it is replayed (the bind
/// is idempotent).
pub fn register_key_payload(signer: &str, key: &PublicKey) -> Vec<u8> {
    let mut out = header(OpKind::RegisterKey, "", signer, 0, 32);
    out.extend_from_slice(key.as_bytes());
    out
}

/// The `send` payload for a borrowed body: [`SignedOp::payload`] without
/// copying the body into a `SignedOp` first.
pub fn send_payload(room: &str, signer: &str, counter: u64, body: &str) -> Vec<u8> {
    let mut out = header(OpKind::Send, room, signer, counter, body.len() + 4);
    put(&mut out, body.as_bytes());
    out
}

/// The common prefix of every payload; `extra` is the capacity to reserve
/// for what the caller appends.
fn header(kind: OpKind, room: &str, signer: &str, counter: u64, extra: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(DOMAIN.len() + 64 + room.len() + signer.len() + extra);
    out.extend_from_slice(DOMAIN);
    put(&mut out, kind.as_str().as_bytes());
    put(&mut out, room.to_ascii_lowercase().as_bytes());
    put(&mut out, signer.to_ascii_lowercase().as_bytes());
    out.extend_from_slice(&counter.to_be_bytes());
    out
}

fn put(out: &mut Vec<u8>, field: &[u8]) {
    // A field longer than 4 GiB cannot occur: bodies are capped at 64 KiB
    // before anything is signed or verified.
    out.extend_from_slice(&(field.len() as u32).to_be_bytes());
    out.extend_from_slice(field);
}

/// Why a message body is not a well-formed envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeError {
    /// The first line is not `mentischat-signed/1 <counter> <key> <signature>`
    /// followed by a newline.
    Malformed,
    /// A field of the header does not parse.
    Field(ParseKeyError),
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => f.write_str("not a signed-room envelope"),
            Self::Field(e) => write!(f, "bad envelope field: {e}"),
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// A parsed signed-room message body, borrowing the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope<'a> {
    /// The counter the sender signed.
    pub counter: u64,
    /// The signer's public key.
    pub key: PublicKey,
    /// The signature over the `send` payload.
    pub signature: Signature,
    /// The message text, unmodified.
    pub body: &'a str,
}

impl<'a> Envelope<'a> {
    /// The stored body of a signed send.
    ///
    /// # Examples
    ///
    /// ```
    /// use mentischat::signed::{Envelope, Keypair, SignedOp};
    ///
    /// let kp = Keypair::from_seed([3; 32]);
    /// let op = SignedOp::Send { body: "hello\nworld".into() };
    /// let sig = kp.sign(&op.payload("r", "a", 9));
    /// let text = Envelope::encode(9, &kp.public(), &sig, "hello\nworld");
    /// let parsed = Envelope::parse(&text).unwrap();
    /// assert_eq!(parsed.body, "hello\nworld");
    /// assert!(parsed.verify("r", "a").is_ok());
    /// assert!(parsed.verify("r", "mallory").is_err());
    /// ```
    pub fn encode(counter: u64, key: &PublicKey, signature: &Signature, body: &str) -> String {
        format!(
            "{ENVELOPE_TAG} {counter} {} {}\n{body}",
            key.to_hex(),
            signature.to_hex()
        )
    }

    /// Parse a message body.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError`] when the header is missing or malformed.
    pub fn parse(text: &'a str) -> Result<Self, EnvelopeError> {
        let (header, body) = text.split_once('\n').ok_or(EnvelopeError::Malformed)?;
        let mut parts = header.split(' ');
        let (Some(ENVELOPE_TAG), Some(counter), Some(key), Some(sig), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(EnvelopeError::Malformed);
        };
        let counter = counter
            .bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| counter.parse().ok())
            .flatten()
            .ok_or(EnvelopeError::Malformed)?;
        Ok(Self {
            counter,
            key: key.parse().map_err(EnvelopeError::Field)?,
            signature: sig.parse().map_err(EnvelopeError::Field)?,
            body,
        })
    }

    /// Check the signature as a `send` by `from` to `room`.
    ///
    /// # Errors
    ///
    /// [`BadSignature`] when it was not made over this body, room, sender and
    /// counter by the key in the header.
    pub fn verify(&self, room: &str, from: &str) -> Result<(), BadSignature> {
        self.key.verify(
            &send_payload(room, from, self.counter, self.body),
            &self.signature,
        )
    }
}
