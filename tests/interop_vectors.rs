//! Interop vectors shared with MentisDB: one fixed seed, its public key, the
//! suggested key id, and one signature per mentischat domain tag over a fixed
//! payload. Ed25519 is deterministic, so both projects can pin the same bytes.
//! The conventions: raw 32-byte public key and 64-byte signature as lowercase
//! hex, pure Ed25519 (RFC 8032, no prehash, no context), seed file 64 hex plus a
//! newline. Domain tags are mentischat's own (`mentischat/<op>/v1\0`), never
//! MentisDB's `mentisdb/...`.
//!
//! The public key and all nine signatures were also reproduced independently
//! with OpenSSL 3.3 (`openssl pkeyutl -sign -rawin` on the DER-wrapped seed), so
//! they pin Ed25519 itself and not this crate's use of it.

use mentischat::client::cli::suggested_key_id;
use mentischat::signed::{
    hex_decode, hex_decode_vec, hex_encode, key_packages_payload, register_key_payload, Keypair,
    PublicKey, Signature, SignedOp,
};

/// The seed `0x01..0x20`.
fn seed() -> [u8; 32] {
    std::array::from_fn(|i| i as u8 + 1)
}

const PUBLIC_KEY: &str = "79b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664";

/// `(op, payload hex, signature hex)` as the other side pins them.
const PINNED: [(&str, &str, &str); 9] = [
    (
        "register_key",
        "6d656e746973636861742f72656769737465722d6b65792f7631000000000000000005616c696365000000000000000079b5562e8fe654f94078b112e8a98ba7901f853ae695bed7e0e3910bad049664",
        "cc5a84e97bf94a431d2e50d10050e669805e46667321f2f716b3f2399af6ddbee8d51bac756f222ee13dc3e887793c9734513c7bd1832fa47587f297d720b009",
    ),
    (
        "create",
        "6d656e746973636861742f6372656174652f7631000000000364656e00000005616c6963650000000000000007000000077468652064656e",
        "b321fdf1d6fe8bdd24d5d2c50179a07e3a94b0633d9225315d0cf3ab229e032c33fc2f72fc9a8568dd919069b9853272cc666a49e504be4bf009c5d2411c6a00",
    ),
    (
        "invite",
        "6d656e746973636861742f696e766974652f7631000000000364656e00000005616c696365000000000000000700000003626f628139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394",
        "eb0bf1d4cb4a6fc45573a035ac580fec975fbc431d9cc0cb9a0ae134ef054860dd138cab6925d06169ab50cff994578f47a01678bb0da78c327d52f804755006",
    ),
    (
        "join",
        "6d656e746973636861742f6a6f696e2f7631000000000364656e00000005616c6963650000000000000007",
        "3a22cd642bef0d80bf4cff234675df8383361e99af0d91da2af370bc42214e195158f79f17d457e24f5012a6c9764b7ec3e667ba87d6babf0b446176ed86f206",
    ),
    (
        "leave",
        "6d656e746973636861742f6c656176652f7631000000000364656e00000005616c6963650000000000000007",
        "d5533b9d2a715dbc327021070654280d2f1a375b5fedb2c92b56e1bae5e0c43084c72666b77fd9bbd844a3cf3648dee39cb97f2bdacee181a1ad03fe1d3c590d",
    ),
    (
        "remove",
        "6d656e746973636861742f72656d6f76652f7631000000000364656e00000005616c696365000000000000000700000003626f62",
        "f78f80d00a951517f2b3a9df3b73d1f9792c742813ea9d5b5f6731b9bf71f365133d3753aa9dcc5002442081588af5ddc82ac98d97d42693286cef32c615d500",
    ),
    (
        "send",
        "6d656e746973636861742f73656e642f7631000000000364656e00000005616c6963650000000000000007000000164e6f207265706c79206e65656465643a2068656c6c6f",
        "23f31b594bbd815b2341d989a865b11117fc205888d33379a0caeea7cf9f2c3e94d7731ddab81dac36f33de4b06662d1e372ebe864b9023aef20ea58ef009802",
    ),
    (
        "welcome",
        "6d656e746973636861742f77656c636f6d652f7631000000000364656e00000005616c696365000000000000000700000003626f6200000003010203",
        "07d5f256b2919994621e85da9a7ee7f9f7b34565ac350c3147912ccd49ab07e725877a292780a3f3d86a38e0d1d62af43094db0a611fc0b75d9fc773ec1c8f02",
    ),
    (
        "key_packages",
        "6d656e746973636861742f6b65792d7061636b616765732f7631000000000000000005616c6963650000000000000007000000020000000201020000000103010000000109",
        "255d0823aa9fe5fa1c06fef6b666c6e7bc0f9c24570833376929e34f4c8939a5608d89a5526d9b4914abbd533d2a523ca00e1c74559283802e4a329b33153d08",
    ),
];

/// `(name, payload, signature)` for every mentischat operation over fixed fields.
fn vectors() -> Vec<(&'static str, Vec<u8>, String)> {
    let keys = Keypair::from_seed(seed());
    let bob: PublicKey = Keypair::from_seed([2; 32]).public();
    let (room, agent, counter) = ("den", "alice", 7u64);
    let ops: Vec<(&'static str, Vec<u8>)> = vec![
        ("register_key", register_key_payload(agent, &keys.public())),
        (
            "create",
            SignedOp::Create {
                description: "the den".into(),
            }
            .payload(room, agent, counter),
        ),
        (
            "invite",
            SignedOp::Invite {
                invitee: "bob".into(),
                invitee_key: bob,
            }
            .payload(room, agent, counter),
        ),
        ("join", SignedOp::Join.payload(room, agent, counter)),
        ("leave", SignedOp::Leave.payload(room, agent, counter)),
        (
            "remove",
            SignedOp::Remove {
                target: "bob".into(),
            }
            .payload(room, agent, counter),
        ),
        (
            "send",
            SignedOp::Send {
                body: "No reply needed: hello".into(),
            }
            .payload(room, agent, counter),
        ),
        (
            "welcome",
            SignedOp::Welcome {
                target: "bob".into(),
                welcome: vec![1, 2, 3],
            }
            .payload(room, agent, counter),
        ),
        (
            "key_packages",
            key_packages_payload(agent, counter, &[vec![1, 2], vec![3]], Some(&[9])),
        ),
    ];
    ops.into_iter()
        .map(|(name, payload)| {
            let sig = keys.sign(&payload).to_hex();
            (name, payload, sig)
        })
        .collect()
}

#[test]
fn the_seed_gives_the_pinned_public_key_and_key_id() {
    let keys = Keypair::from_seed(seed());
    assert_eq!(keys.public().to_hex(), PUBLIC_KEY);
    assert_eq!(
        keys.public().to_hex().parse::<PublicKey>(),
        Ok(keys.public())
    );
    assert_eq!(suggested_key_id("alice", "202610"), "alice-202610");
}

/// Every payload is byte-for-byte what is pinned, and so is its signature.
#[test]
fn payloads_and_signatures_match_the_pinned_vectors() {
    let computed = vectors();
    assert_eq!(computed.len(), PINNED.len());
    for ((name, payload, sig), (pinned_name, pinned_payload, pinned_sig)) in
        computed.iter().zip(PINNED.iter())
    {
        assert_eq!(name, pinned_name);
        assert_eq!(&hex_encode(payload), pinned_payload, "payload of {name}");
        assert_eq!(sig, pinned_sig, "signature of {name}");
    }
}

/// The pinned signatures verify with a plain RFC 8032 verifier over the raw
/// key and the raw payload (no prehash, no context), the semantics MentisDB
/// uses, and fail for a changed payload or a tag from another operation.
#[test]
fn pinned_signatures_verify_as_plain_ed25519() {
    use ed25519_dalek::{Signature as Sig, VerifyingKey};
    let key_bytes: [u8; 32] = hex_decode(PUBLIC_KEY).unwrap();
    let key = VerifyingKey::from_bytes(&key_bytes).unwrap();
    for (name, payload_hex, sig_hex) in PINNED {
        let payload = hex_decode_vec(payload_hex).unwrap();
        let sig = Sig::from_bytes(&hex_decode::<64>(sig_hex).unwrap());
        assert!(key.verify_strict(&payload, &sig).is_ok(), "{name}");
        let mut changed = payload.clone();
        *changed.last_mut().unwrap() ^= 1;
        assert!(key.verify_strict(&changed, &sig).is_err(), "{name} changed");
        assert!(payload.starts_with(b"mentischat/"), "{name}");
        assert!(!payload.starts_with(b"mentisdb/"), "{name}");
        // The daemon's own check agrees.
        let ours: PublicKey = PUBLIC_KEY.parse().unwrap();
        assert!(ours
            .verify(&payload, &sig_hex.parse::<Signature>().unwrap())
            .is_ok());
    }
    // A signature for one operation does not verify as another.
    let (_, send_payload_hex, _) = PINNED[6];
    let (_, _, join_sig) = PINNED[3];
    let payload = hex_decode_vec(send_payload_hex).unwrap();
    assert!(PublicKey::from_bytes(key_bytes)
        .unwrap()
        .verify(&payload, &join_sig.parse().unwrap())
        .is_err());
}

/// The seed file format both projects use: 64 lowercase hex digits and a newline.
#[test]
fn the_seed_file_format_is_hex_plus_newline() {
    let text = format!("{}\n", hex_encode(&seed()));
    assert_eq!(text.len(), 65);
    assert!(
        text.ends_with('\n')
            && text[..64]
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    );
    let parsed: [u8; 32] = hex_decode(text.trim()).unwrap();
    assert_eq!(parsed, seed());
}
