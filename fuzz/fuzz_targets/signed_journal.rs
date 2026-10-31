//! A signed room's saved records and envelopes: the journal replay that
//! restore runs, driven with arbitrary bytes through `verify_signed_record`.
//!
//! Input: JSON `{"journal": [...], "watermarks": [...], "retained": [...]}` for
//! the room `r`; seeds are real journals and the tampered variants of the
//! tests, so the fuzzer mutates text that gets deep into the replay.
//!
//! Invariants on any input: no panic; the answer is deterministic. On an input
//! that verifies: the room has members and an epoch no larger than its record
//! count; changing any signature, repeating any record, or editing any signed
//! field of any record is rejected; and every prefix of the journal (without
//! retained messages) verifies with an epoch that never exceeds the whole's and
//! never goes down as the prefix grows (truncation can roll a room back but
//! never forge a state).
#![no_main]

use libfuzzer_sys::fuzz_target;
use mentischat::bus::verify_signed_record;
use serde_json::Value;

const ROOM: &str = "r";

fn verify(value: &Value) -> Result<mentischat::bus::RecordSummary, &'static str> {
    verify_signed_record(ROOM, value.to_string().as_bytes())
}

/// Flip the first hex digit of a string field.
fn flip(text: &str) -> String {
    let first = if text.starts_with('0') { '1' } else { '0' };
    format!("{first}{}", text.get(1..).unwrap_or(""))
}

fuzz_target!(|data: &[u8]| {
    let first = verify_signed_record(ROOM, data);
    assert_eq!(first, verify_signed_record(ROOM, data), "not deterministic");
    let Ok(summary) = first else { return };
    // The typed reader ignores unknown fields, so it can accept text the
    // generic JSON reader refuses (an out-of-range number in a field nobody
    // reads); the mutation checks below need the generic form.
    let Ok(input) = serde_json::from_slice::<Value>(data) else {
        return;
    };
    assert!(!summary.members.is_empty());
    assert!(summary.epoch <= summary.records as u64);
    let Some(journal) = input["journal"].as_array().cloned() else {
        panic!("a record verified without a journal");
    };

    for i in 0..journal.len() {
        let mut forged = input.clone();
        let sig = journal[i]["signature"].as_str().unwrap_or("").to_string();
        forged["journal"][i]["signature"] = Value::String(flip(&sig));
        assert!(verify(&forged).is_err(), "signature of record {i} not checked");

        let mut repeated = input.clone();
        repeated["journal"]
            .as_array_mut()
            .expect("journal is an array")
            .insert(i, journal[i].clone());
        assert!(verify(&repeated).is_err(), "record {i} repeated and accepted");

        for field in ["agent", "invitee", "target", "body", "welcome", "description"] {
            let Some(old) = journal[i][field].as_str() else { continue };
            let mut edited = input.clone();
            edited["journal"][i][field] = Value::String(format!("{old}x"));
            assert!(verify(&edited).is_err(), "field {field} of record {i} not signed");
        }
    }

    let mut last_epoch = 0;
    for keep in 1..=journal.len() {
        let mut prefix = input.clone();
        prefix["journal"] = Value::Array(journal[..keep].to_vec());
        prefix["retained"] = Value::Array(Vec::new());
        let Ok(p) = verify(&prefix) else {
            panic!("a prefix of {keep} records of a valid journal was rejected");
        };
        assert!(p.epoch >= last_epoch && p.epoch <= summary.epoch, "epoch not monotone");
        last_epoch = p.epoch;
    }
});
