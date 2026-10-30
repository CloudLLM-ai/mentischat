//! WebSocket client frames: `ws::parse_frame` against a reference reading of
//! the documented grammar. Only `invalid_body` and `invalid_cursor` come
//! back, and the frame's `ref` is echoed on every error that had an object
//! to read it from.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mentischat::errors::MailboxError;
use mentischat::ws::{parse_frame, ClientFrame};
use serde_json::Value;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let parsed: Option<Value> = serde_json::from_str(text).ok();
    let object = parsed.as_ref().and_then(Value::as_object);
    let want_ref = object.and_then(|o| o.get("ref")).cloned();
    let kind = object.and_then(|o| o.get("type")).and_then(Value::as_str);

    match parse_frame(text) {
        Ok(frame) => {
            let o = object.expect("only a JSON object is a frame");
            match frame {
                ClientFrame::Ack { up_to, reference } => {
                    assert_eq!(kind, Some("ack"));
                    assert_eq!(reference, want_ref);
                    let expect = match &o["upTo"] {
                        Value::Number(n) => n.as_u64(),
                        Value::String(s) => s.parse::<u64>().ok(),
                        _ => None,
                    };
                    assert_eq!(Some(up_to), expect, "upTo read as an id");
                }
                ClientFrame::Send { reference, item } => {
                    assert_eq!(kind, Some("send"));
                    assert_eq!(reference, want_ref);
                    assert_eq!(&item, parsed.as_ref().unwrap());
                }
                ClientFrame::Batch { reference, items } => {
                    assert_eq!(kind, Some("batch"));
                    assert_eq!(reference, want_ref);
                    assert_eq!(Some(&Value::Array(items)), o.get("items"));
                }
            }
        }
        Err(e) => {
            assert_eq!(e.reference, want_ref, "ref echoed on {text:?}");
            match e.error {
                MailboxError::InvalidBody => {}
                MailboxError::InvalidCursor => assert_eq!(kind, Some("ack")),
                other => panic!("undocumented error {other:?}"),
            }
            if kind == Some("batch") {
                let items = object.unwrap().get("items");
                assert!(!matches!(items, Some(Value::Array(_))));
            }
        }
    }
});
