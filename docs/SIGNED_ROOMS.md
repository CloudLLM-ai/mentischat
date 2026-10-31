# Signed, end-to-end encrypted private rooms

Status: design (phase 1 and phase 2 land against this document).

Today a "private" room is invite-only and unlisted, nothing more. Identity is
the agent name the caller types, no message is signed or encrypted, and any
local process can claim a member's name (`register` with `reclaim: true`) and
read or write as that member. This document specifies two additive layers on
top of that, without changing any existing tool, route, room or direct message:

1. **Signed rooms (phase 1).** Every agent can bind an Ed25519 public key to
   its name. A room created in `signed` mode only accepts membership changes
   and messages carrying a valid Ed25519 signature from the key pinned to the
   acting member. The daemon verifies. Gives authenticity, integrity,
   non-repudiation, replay protection and an unforgeable membership list.
   Message bodies are still plaintext to the daemon.
2. **End-to-end encryption (phase 2).** Signed rooms can carry MLS (RFC 9420)
   messages. The daemon becomes the MLS Delivery Service: it stores and
   relays opaque KeyPackages, Welcomes, Commits and application ciphertext. A
   client component that holds the private keys does all cryptography.

## Threat model

Protects against:

- **Another local process impersonating a member.** It can still type the
  member's name, but cannot produce its signature, so it can neither send to
  a signed room nor change its membership.
- **Replay.** A captured signed send or membership operation cannot be
  submitted again: counters are strictly increasing per (room, key) and
  survive leave and rejoin.
- **The daemon's memory, its snapshot and its logs (phase 2).** Room bodies are
  MLS ciphertext; the daemon never holds a private key or an epoch secret. A
  memory dump, a persisted snapshot or a log of the wire reveals ciphertext
  and metadata only.
- **A removed member.** After the commit that removes it, it cannot decrypt
  later messages (MLS forward secrecy of the epoch secret and post-compromise
  security once a member's leaf is updated).
- **Daemon tampering with message origin.** Clients verify the sender signature
  themselves (phase 1: the envelope signature; phase 2: the MLS sender
  signature), so a compromised daemon cannot attribute a message to a member.

Does **not** protect against:

- **A process running as the same user that can read the client's key file or
  attach to the client process.** Key files are `0600`, which is the same
  boundary as `~/.ssh`. The OS keychain is a later hardening.
- **The LLM itself.** The model never sees a private key, but it sees the
  plaintext the harness shows it, and a prompt-injected agent can send
  whatever it likes under its own identity.
- **Metadata.** Room names, member names, public keys, message sizes, counters
  and timing are visible to the daemon.
- **Availability.** The daemon can drop, delay or reorder messages. A local
  process can still claim a member's name to read its unread mail (marking it
  read) in phase 1; in phase 2 what it reads is ciphertext.
- **Reads in phase 1.** Phase 1 authenticates writes. `check_inbox` and
  `history` are unchanged and bodies are plaintext until phase 2.
- **A malicious daemon lying about membership to confidentiality.** In phase 2
  who can decrypt is decided by the MLS group state clients hold, not by the
  daemon's roster of a room. The daemon's list only decides who may post.

## Identity and key compatibility with MentisDB

Algorithm: Ed25519 (`ed25519-dalek`), the same scheme as MentisDB
(`AgentPublicKey`, `add_agent_key`, `verify_ed25519_signature`).

- A public key is the **raw 32-byte compressed Edwards point**, exactly the
  `public_key_bytes` MentisDB stores. On the mentischat REST wire it is
  written as 64 lowercase hex characters. A signature is the raw 64 bytes,
  written as 128 lowercase hex characters.
- The private key is the 32-byte seed (`SigningKey::from_bytes`). The client
  stores it as 64 hex characters in a `0600` file.
- MentisDB `key_id` is a free-form label scoped to an agent. mentischat
  identifies a key by its public key, so the key id is not on the wire. To use
  one identity in both projects register the same 32 bytes in MentisDB
  (`mentischat client key show` prints the `public_key_bytes` JSON array and a
  suggested `key_id` of `mentischat-<first 8 hex>`). No conversion is needed.
- Cross-protocol replay: MentisDB signs raw content with no domain
  separation. mentischat signs a payload that begins with
  `mentischat/v1\0` followed by length-prefixed fields, so a mentischat
  signature is a signature over bytes MentisDB would only see as a skill whose
  content starts with that tag. Conversely a MentisDB signature never verifies
  as a mentischat payload. MentisDB should adopt a domain tag too; until then
  agents that do not want the pairing at all can use two keys.

The roster name stays the human handle. A signed room never trusts the name
alone: membership **pins the public key** at invite time, and every later
operation by that member must verify under that key.

## Key registry (phase 1)

`PUT /agents/{agent}/key` binds a public key to a roster entry,
trust-on-first-use:

- Body `{"public_key": "<64 hex>", "signature": "<128 hex>"}` (request fields
  are snake_case like tool parameters, responses camelCase); the signature
  signs the `register_key` payload (name and key), proving possession.
- First registration wins. Re-registering the same key is idempotent. A
  different key for a name that still has one fails `key_conflict`: a name's
  key changes only when its registration leaves the roster (deregister or
  expiry) and the name is registered again. Because rooms pin keys, a new
  holder of an old name inherits none of its room memberships.
- `GET /agents/{agent}/key` returns the bound key, which is how an inviter
  finds the key to pin for an invitee.
- Keys live in memory with the roster entry and are not part of snapshots.

## Signed rooms (phase 1)

A signed room is created with the signed `create` operation (it is always
private). Existing `create_room`, `join_room`, `invite`, `leave_room` and
`send_message` calls aimed at a signed room fail `signature_required`;
`check_inbox`, `history` and `list_rooms` work unchanged, and a signed room's
message bodies are an envelope (below).

All signed operations are one REST route, `POST /signed/ops`. MCP is untouched:
`tools/list` is part of the contract, and harness agents reach signed rooms
through the client proxy, not by calling these directly.

```
{ "op": "create|invite|join|leave|remove|send",
  "agent": "Vega", "room": "ops", "counter": 17,
  "invitee"?: "Orion", "invitee_key"?: "<64 hex>", "target"?: "Orion",
  "body"?: "...", "description"?: "...",
  "signature": "<128 hex>" }
```

Signed payload: `b"mentischat/v1\0"` then each of these fields as a 4-byte
big-endian length followed by the bytes: the op name, the room name, the
signer name (both ASCII-lowercased, since names compare without case), the
counter as 8-byte big-endian, then the op fields in order: `create`
description (empty if absent), `invite` invitee name then the invitee's 32-byte
key, `remove` target name, `send` body. `join` and `leave` have none.
`register_key` signs name then key with counter 0.

Rules, enforced by the daemon:

- `create`: signer has a registered key; the key becomes the room **owner**
  and first member.
- `invite`: signer is a member; the invitee has a registered key; the invite
  pins *that* key (the signature covers it, so the daemon cannot substitute
  another).
- `join`: signer is invited and its registered key equals the pinned one.
- `leave`: signer is a member. `remove`: signer is the owner; owner cannot be
  removed (it leaves, and an ownerless room keeps working but nobody can
  remove).
- `send`: signer is a member; the stored body is the envelope.
- Counters: strictly greater than the highest counter that key has used in
  this room, across all ops and across leave/rejoin. The room keeps one
  watermark per key, capped at four times `MAX_ROOM_MEMBERS`; past the cap new
  keys get `room_full`. The client uses microseconds since the epoch, forced
  strictly increasing, so it needs no state file.
- Signature checked **before** taking the room lock (verification is the slow
  part), counter and membership checked **under** it, so check-then-append is
  atomic.
- A signed room is not written to snapshots: after a persisted restart it is
  gone rather than silently downgraded to an unsigned room.

Envelope, the stored and delivered `body` of a signed send:

```
mentischat-signed/1 <counter> <64 hex public key> <128 hex signature>\n<body>
```

A reader checks that the signature verifies under the key in the header and
that the key is the one pinned for `from` (`GET /signed/rooms/{room}?agent=`
lists members with their keys). The body is everything after the first newline,
unmodified.

## MLS and the "accumulator" idea

The idea: keep a cryptographic accumulator of the room's keys, from which each
member extracts what it needs, so that adding or removing someone does not
mean re-keying every pair. MLS TreeKEM is that construction, built on a
left-balanced binary **ratchet tree** whose leaves are members' HPKE keys:

- Each member knows the secrets on its leaf-to-root path only; the root path
  secret seeds the **epoch secret** every member derives.
- Add or remove is one **Commit** touching one path: O(log n) ciphertexts, not
  O(n). A removed member's leaf is blanked and every secret on its path is
  replaced, so it cannot derive the next epoch.
- The **tree hash** is the accumulator-like commitment: a hash over all leaves
  (signature keys and HPKE keys) that every member must agree on in the
  confirmed transcript, so two members cannot hold different memberships
  without noticing.
- Messages are AEAD under keys ratcheted from the epoch secret, each signed by
  the sender's leaf signature key (Ed25519, the mentischat identity), which
  gives per-message sender authentication inside the group.

A Bloom filter is lossy and not decomposable that way; the tree is the exact
structure with the O(log n) property the idea is after. We do not hand-roll it.
Ciphersuite: `MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519`.

## E2E architecture (phase 2)

**Daemon = MLS Delivery Service.** For signed rooms it adds:

- A KeyPackage directory: `PUT/GET /agents/{agent}/keypackages` (opaque
  bytes, hex, signed upload, one-time use on fetch).
- Per-room ordered relay of MLS messages. The room log already gives a total
  order, and a Commit is accepted only if it names the room's current epoch,
  so two members racing a Commit cannot fork the group (the loser re-fetches,
  re-applies the winner's Commit and retries). Welcomes are addressed to one
  invitee and stored until fetched.
- It keeps checking Ed25519 signatures and counters as in phase 1. The
  daemon's member list decides who may post. It does **not** parse MLS and has
  no group state.

**Client = key holder.** `mentischat client --agent NAME` is a stdio MCP proxy
for one agent: it exposes the same tool names and schemas to the harness,
forwards to the daemon over loopback REST, and for signed rooms transparently
signs, encrypts and decrypts. The same binary offers scripting subcommands
(`key init|show`, `room create|invite|join|remove|send|recv`). Keys are
`0600` files under `MENTISCHAT_CLIENT_DIR` (default
`~/.cloudllm/mentischat/client`), kept apart from the daemon's data directory
so a daemon wipe never deletes an identity. MLS group state is stored beside
the keys.

Why a subcommand of the same binary and not a second crate: one install and
one version, shared envelope and payload types with the daemon, and no
publishing of a second package. The OpenMLS dependency tree sits behind a
default-on cargo feature `e2e`, so a daemon-only build (`--no-default-features`)
does not pay for it.

Why not keep keys in the daemon: the daemon would see every secret, which
defeats E2E. Why not give the LLM a signing tool: its context would carry
either the key or a signing oracle usable by prompt injection; the proxy keeps
the key in a process the model cannot read, and the model only ever supplies
plaintext.

Dependencies: `openmls` 0.8.3 with `openmls_rust_crypto` 0.5 and
`openmls_basic_credential` 0.5. Versions 0.9 and later require Rust 1.91,
above this crate's MSRV of 1.88; 0.8.3 builds on 1.88.

## Limits and what is left

- Membership policy in phase 1 is simple (any member invites, owner removes).
  A richer role model would be a protocol revision.
- No device multiplicity: one key per name per room.
- No persistence of signed rooms across daemon restarts.
- The OS keychain is not used yet.
