//! MCP tool arguments: arbitrary JSON into `MailboxProtocol::execute` on a
//! primed bus. Only an unknown tool name is a Rust `Err`; every domain
//! failure is a failed result whose body is a JSON object whose `error` is a
//! documented code.
//!
//! Input: first byte picks the tool, the rest is a stream of JSON values;
//! each value is one call, the tool advancing by one per call, so sequences
//! such as register, send, check_inbox are reachable.
#![no_main]

use std::sync::{Arc, OnceLock};

use libfuzzer_sys::fuzz_target;
use mcp::protocol::ToolProtocol;
use mentischat::bus::Bus;
use mentischat::tools::MailboxProtocol;
use serde_json::Value;

const TOOLS: [&str; 12] = [
    "register",
    "deregister",
    "send_message",
    "check_inbox",
    "history",
    "list_agents",
    "create_room",
    "join_room",
    "invite",
    "leave_room",
    "list_rooms",
    // Not a tool: the only name that may be a Rust error.
    "bogus",
];

/// Every `error` code in the wire contract.
const CODES: [&str; 24] = [
    "invalid_name",
    "invalid_body",
    "invalid_wait",
    "invalid_description",
    "invalid_cursor",
    "invalid_limit",
    "name_taken",
    "unregistered",
    "queue_full",
    "storage_full",
    "storage_error",
    "roster_full",
    "too_many_waiters",
    "invalid_room",
    "room_not_found",
    "room_exists",
    "not_a_member",
    "private_room",
    "rooms_full",
    "too_many_rooms",
    "room_full",
    "not_found",
    "method_not_allowed",
    "shutting_down",
];

/// One current-thread runtime for the whole process. The clock is paused, so
/// a `wait_ms` of 30000 elapses instantly and deterministically.
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

fuzz_target!(|data: &[u8]| {
    let Some((first, rest)) = data.split_first() else {
        return;
    };
    // The data directory does not exist, so nothing is restored or written.
    let bus = Arc::new(Bus::new(std::path::Path::new("/nonexistent/mentischat-fuzz")).unwrap());
    let proto = MailboxProtocol::new(bus);
    let calls = serde_json::Deserializer::from_slice(rest)
        .into_iter::<Value>()
        .map_while(Result::ok)
        .take(6);
    runtime().block_on(async {
        // A primed bus: two agents and a room, so calls get past validation.
        for (tool, args) in [
            ("register", serde_json::json!({"agent": "a"})),
            ("register", serde_json::json!({"agent": "b"})),
            (
                "create_room",
                serde_json::json!({"agent": "a", "room": "r"}),
            ),
        ] {
            let r = proto.execute(tool, args).await.expect("priming call");
            assert!(r.success, "priming {tool}: {:?}", r.error);
        }
        for (i, args) in calls.enumerate() {
            let tool = TOOLS[(usize::from(*first) + i) % TOOLS.len()];
            match proto.execute(tool, args).await {
                Err(_) => assert_eq!(tool, "bogus", "only an unknown tool is an Err"),
                Ok(result) => {
                    assert_ne!(tool, "bogus");
                    if !result.success {
                        let body = result.error.expect("a failure carries its body");
                        let v: Value = serde_json::from_str(&body).expect("error body is JSON");
                        let code = v["error"].as_str().expect("error is a string");
                        assert!(CODES.contains(&code), "unknown code {code:?} for {tool}");
                    }
                }
            }
        }
    });
});
