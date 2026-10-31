//! Signed-room wire parsing and verification. Three properties on arbitrary
//! text: an envelope that parses re-encodes to one that parses to the same
//! thing; a key or signature that parses prints back as the same hex; and a
//! correctly signed send with an arbitrary body and counter either lands as an
//! envelope that verifies for its sender or is rejected with a documented
//! code. JSON that is not a valid signed request is a typed error, never a
//! panic.
#![no_main]

use std::sync::{Arc, OnceLock};

use libfuzzer_sys::fuzz_target;
use mentischat::bus::{Bus, SignedRequest};
use mentischat::names::{AgentName, RoomName};
use mentischat::signed::{register_key_payload, Envelope, Keypair, PublicKey, Signature, SignedOp};
use serde_json::Value;

/// Codes a signed operation may fail with.
const CODES: [&str; 14] = [
    "invalid_body",
    "invalid_name",
    "invalid_room",
    "invalid_description",
    "invalid_signature",
    "invalid_key",
    "key_required",
    "key_conflict",
    "stale_counter",
    "room_not_found",
    "not_a_member",
    "room_full",
    "storage_full",
    "unregistered",
];

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .expect("runtime")
    })
}

fn check_parsers(s: &str) {
    if let Ok(env) = Envelope::parse(s) {
        let again = Envelope::encode(env.counter, &env.key, &env.signature, env.body);
        assert_eq!(Envelope::parse(&again), Ok(env.clone()));
        let _ = env.verify("r", "a");
    }
    if let Ok(key) = s.parse::<PublicKey>() {
        assert!(key.to_hex().eq_ignore_ascii_case(s));
    }
    if let Ok(sig) = s.parse::<Signature>() {
        assert!(sig.to_hex().eq_ignore_ascii_case(s));
    }
}

async fn primed(keys: &Keypair) -> Arc<Bus> {
    let bus = Arc::new(Bus::new(std::path::Path::new("/nonexistent/mentischat-fuzz")).unwrap());
    let (a, b) = (AgentName::parse("a").unwrap(), AgentName::parse("b").unwrap());
    bus.register(&a).await.unwrap();
    bus.register(&b).await.unwrap();
    let key = keys.public();
    let sig = keys.sign(&register_key_payload("a", &key));
    bus.register_key(&a, key, &sig).await.unwrap();
    let create = SignedOp::Create {
        description: String::new(),
    };
    let payload = create.payload("r", "a", 1);
    bus.signed_op(SignedRequest {
        agent: a,
        room: RoomName::parse("r").unwrap(),
        counter: 1,
        op: create,
        signature: keys.sign(&payload),
    })
    .await
    .unwrap();
    bus
}

fuzz_target!(|data: &[u8]| {
    let Ok(s) = std::str::from_utf8(data) else {
        return;
    };
    check_parsers(s);
    let keys = Keypair::from_seed([1; 32]);
    runtime().block_on(async {
        let bus = primed(&keys).await;
        if let Ok(v) = serde_json::from_str::<Value>(s) {
            if let Err(e) = mentischat::signed_api::signed_op(&bus, &v).await {
                assert!(CODES.contains(&e.code()), "unknown code {}", e.code());
            }
        }
        // A correctly signed send: counter from the first byte, body the rest.
        let Some((&first, body)) = s.as_bytes().split_first() else {
            return;
        };
        let Ok(body) = std::str::from_utf8(body) else {
            return;
        };
        let counter = u64::from(first);
        let op = SignedOp::Send { body: body.into() };
        let signature = keys.sign(&op.payload("r", "a", counter));
        let req = SignedRequest {
            agent: AgentName::parse("a").unwrap(),
            room: RoomName::parse("r").unwrap(),
            counter,
            op,
            signature,
        };
        match bus.signed_op(req).await {
            Ok(_) => {
                // Alice needs a partner to read it: read it back as a member.
                let b = AgentName::parse("b").unwrap();
                let a = AgentName::parse("a").unwrap();
                let r = RoomName::parse("r").unwrap();
                let page = bus.room_history(&a, &r, None).await.unwrap();
                let last = page.messages.last().expect("the send is in the room");
                let env = Envelope::parse(&last.body).expect("stored body is an envelope");
                assert_eq!(env.body, body);
                assert!(env.verify("r", "a").is_ok());
                let _ = b;
            }
            Err(e) => assert!(CODES.contains(&e.code()), "unknown code {}", e.code()),
        }
    });
});
