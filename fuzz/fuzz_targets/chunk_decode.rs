#![no_main]
//! Fuzz the Bolt chunk-framing decoder — the outermost untrusted entry point,
//! reached before `LOGON`. Property: `chunk::decode_message` must never panic on
//! arbitrary bytes; it must either return a framed message, signal "need more
//! bytes" (`Ok(None)`), or reject with `Err` (the framing-flood caps live here).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = slater::bolt::chunk::decode_message(data);
});
