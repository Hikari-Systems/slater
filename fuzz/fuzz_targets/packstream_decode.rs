#![no_main]
//! Fuzz the PackStream value decoder. Property: `packstream::from_slice` must
//! never panic on arbitrary bytes — a truncated, over-nested, or malformed value
//! must come back as `Err` (SECURITY_WORKLIST item 5). This is the decoder that
//! runs on every message body a peer sends, including pre-`LOGON`.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = slater::bolt::packstream::from_slice(data);
});
