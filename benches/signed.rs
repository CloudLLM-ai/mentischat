//! What signed and encrypted rooms cost per message.
//!
//! Run: `cargo bench --bench signed`
//!
//! - `crypto`: one Ed25519 sign, one verify and one envelope parse-and-verify
//!   of a 256-byte send, the raw cost the daemon pays per signed message.
//! - `send`: a signed send through the [`Bus`] (verification, pinned-key and
//!   counter checks, append) next to a plain `send_room` of the same body, so
//!   the difference is the price of signing. Requests are signed before the
//!   clock starts; one iteration is one message.
//! - `mls`: MLS application-message encrypt and decrypt with OpenMLS
//!   (X25519 / AES-128-GCM / Ed25519), in groups of 2 and 64 members and for
//!   256-byte and 4 KiB messages. Group setup is outside the timed region.
//!   This is the client's cost; the daemon never does it.

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mentischat::bus::{Bus, SignedRequest};
use mentischat::names::{AgentName, RoomName};
use mentischat::signed::{register_key_payload, Envelope, Keypair, SignedOp};
use openmls::prelude::tls_codec::Deserialize as _;
use openmls::prelude::*;
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;
use openmls_traits::types::SignatureScheme;

const SUITE: Ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;

fn body(len: usize) -> String {
    "x".repeat(len)
}

fn name(s: &str) -> AgentName {
    AgentName::parse(s).expect("valid bench name")
}

fn crypto(c: &mut Criterion) {
    let keys = Keypair::from_seed([7; 32]);
    let payload = SignedOp::Send { body: body(256) }.payload("room", "alice", 1);
    let sig = keys.sign(&payload);
    let text = Envelope::encode(1, &keys.public(), &sig, &body(256));
    let mut group = c.benchmark_group("signed/crypto");
    group.bench_function("sign_256B", |b| b.iter(|| keys.sign(black_box(&payload))));
    group.bench_function("verify_256B", |b| {
        b.iter(|| keys.public().verify(black_box(&payload), &sig).unwrap())
    });
    group.bench_function("envelope_parse_and_verify_256B", |b| {
        b.iter(|| {
            let env = Envelope::parse(black_box(&text)).unwrap();
            env.verify("room", "alice").unwrap();
        })
    });
    group.finish();
}

/// A bus with `alice` (a bound key) and `bob` in a signed room `r`, plus a
/// plain room `plain` with the same two.
fn room_pair(rt: &tokio::runtime::Runtime, dir: &std::path::Path) -> (Bus, Keypair) {
    rt.block_on(async {
        let bus = Bus::new(dir).expect("bus");
        for who in ["alice", "bob"] {
            bus.register(&name(who)).await.expect("register");
        }
        let keys = Keypair::from_seed([1; 32]);
        let bob_keys = Keypair::from_seed([2; 32]);
        for (who, k) in [("alice", &keys), ("bob", &bob_keys)] {
            let sig = k.sign(&register_key_payload(who, &k.public()));
            bus.register_key(&name(who), k.public(), &sig)
                .await
                .expect("bind");
        }
        let op = |who: &str, k: &Keypair, counter, op: SignedOp| {
            let signature = k.sign(&op.payload("r", who, counter));
            SignedRequest {
                agent: name(who),
                room: RoomName::parse("r").unwrap(),
                counter,
                op,
                signature,
            }
        };
        let create = SignedOp::Create {
            description: String::new(),
        };
        bus.signed_op(op("alice", &keys, 1, create))
            .await
            .expect("create");
        let invite = SignedOp::Invite {
            invitee: "bob".into(),
            invitee_key: bob_keys.public(),
        };
        bus.signed_op(op("alice", &keys, 2, invite))
            .await
            .expect("invite");
        bus.signed_op(op("bob", &bob_keys, 1, SignedOp::Join))
            .await
            .expect("join");
        let plain = RoomName::parse("plain").unwrap();
        bus.create_room(&name("alice"), &plain, false, None)
            .await
            .expect("room");
        bus.join_room(&name("bob"), &plain).await.expect("join");
        (bus, keys)
    })
}

fn send(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (bus, keys) = room_pair(&rt, dir.path());
    let text = body(256);
    let mut group = c.benchmark_group("signed/send");
    group.throughput(Throughput::Elements(1));
    let mut counter = 100u64;
    group.bench_function("signed_256B", |b| {
        b.iter_custom(|iters| {
            let requests: Vec<SignedRequest> = (0..iters)
                .map(|_| {
                    counter += 1;
                    let op = SignedOp::Send { body: text.clone() };
                    let signature = keys.sign(&op.payload("r", "alice", counter));
                    SignedRequest {
                        agent: name("alice"),
                        room: RoomName::parse("r").unwrap(),
                        counter,
                        op,
                        signature,
                    }
                })
                .collect();
            let start = Instant::now();
            rt.block_on(async {
                for req in requests {
                    black_box(bus.signed_op(req).await.unwrap());
                }
            });
            start.elapsed()
        })
    });
    let (alice, plain) = (name("alice"), RoomName::parse("plain").unwrap());
    group.bench_function("plain_256B", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt.block_on(async {
                for _ in 0..iters {
                    black_box(bus.send_room(&alice, &plain, text.clone()).await.unwrap());
                }
            });
            start.elapsed()
        })
    });
    group.finish();
}

/// One MLS participant with an in-memory provider.
struct Peer {
    provider: OpenMlsRustCrypto,
    signer: SignatureKeyPair,
    credential: CredentialWithKey,
}

impl Peer {
    fn new(identity: &str) -> Self {
        let signer = SignatureKeyPair::new(SignatureScheme::ED25519).expect("keys");
        let credential = CredentialWithKey {
            credential: BasicCredential::new(identity.as_bytes().to_vec()).into(),
            signature_key: signer.to_public_vec().into(),
        };
        Self {
            provider: OpenMlsRustCrypto::default(),
            signer,
            credential,
        }
    }

    fn key_package(&self) -> KeyPackage {
        KeyPackage::builder()
            .build(SUITE, &self.provider, &self.signer, self.credential.clone())
            .expect("key package")
            .key_package()
            .clone()
    }
}

/// Alice's and Bob's groups, in sync, inside a group of `size` members.
fn mls_pair(size: usize) -> (Peer, MlsGroup, Peer, MlsGroup) {
    let (alice, bob) = (Peer::new("alice"), Peer::new("bob"));
    let config = MlsGroupCreateConfig::builder()
        .ciphersuite(SUITE)
        .use_ratchet_tree_extension(true)
        .max_past_epochs(3)
        .build();
    let mut a = MlsGroup::new(
        &alice.provider,
        &alice.signer,
        &config,
        alice.credential.clone(),
    )
    .expect("group");
    let (_, welcome, _) = a
        .add_members(&alice.provider, &alice.signer, &[bob.key_package()])
        .expect("add bob");
    a.merge_pending_commit(&alice.provider).expect("merge");
    let welcome = match MlsMessageIn::tls_deserialize_exact(welcome.to_bytes().unwrap())
        .unwrap()
        .extract()
    {
        MlsMessageBodyIn::Welcome(w) => w,
        _ => unreachable!("a welcome"),
    };
    let join = MlsGroupJoinConfig::builder()
        .use_ratchet_tree_extension(true)
        .build();
    let mut b = StagedWelcome::new_from_welcome(&bob.provider, &join, welcome, None)
        .expect("stage")
        .into_group(&bob.provider)
        .expect("join");
    if size > 2 {
        let others: Vec<KeyPackage> = (2..size)
            .map(|i| Peer::new(&format!("m{i}")).key_package())
            .collect();
        let (commit, _, _) = a
            .add_members(&alice.provider, &alice.signer, &others)
            .expect("add rest");
        a.merge_pending_commit(&alice.provider).expect("merge");
        let message = MlsMessageIn::tls_deserialize_exact(commit.to_bytes().unwrap()).unwrap();
        let processed = b
            .process_message(&bob.provider, message.try_into_protocol_message().unwrap())
            .expect("bob processes");
        if let ProcessedMessageContent::StagedCommitMessage(staged) = processed.into_content() {
            b.merge_staged_commit(&bob.provider, *staged)
                .expect("merge");
        }
    }
    (alice, a, bob, b)
}

fn mls(c: &mut Criterion) {
    let mut group = c.benchmark_group("mls");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    for size in [2usize, 64] {
        for len in [256usize, 4096] {
            let text = body(len);
            let id = format!("{size}members_{len}B");
            group.throughput(Throughput::Elements(1));
            let (alice, mut a, _bob, _b) = mls_pair(size);
            group.bench_with_input(BenchmarkId::new("encrypt", &id), &text, |bch, text| {
                bch.iter(|| {
                    black_box(
                        a.create_message(&alice.provider, &alice.signer, text.as_bytes())
                            .unwrap(),
                    )
                })
            });
            let (alice, mut a, bob, mut b) = mls_pair(size);
            group.bench_with_input(BenchmarkId::new("decrypt", &id), &text, |bch, text| {
                bch.iter_custom(|iters| {
                    let wire: Vec<Vec<u8>> = (0..iters)
                        .map(|_| {
                            a.create_message(&alice.provider, &alice.signer, text.as_bytes())
                                .unwrap()
                                .to_bytes()
                                .unwrap()
                        })
                        .collect();
                    let start = Instant::now();
                    for bytes in wire {
                        let message = MlsMessageIn::tls_deserialize_exact(bytes).unwrap();
                        let protocol = message.try_into_protocol_message().unwrap();
                        black_box(b.process_message(&bob.provider, protocol).unwrap());
                    }
                    start.elapsed()
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, crypto, send, mls);
criterion_main!(benches);
