#![no_main]
//! Fuzz the shared property-value wire codec (`graph_format::wire::read_value`).
//! Property: decoding arbitrary bytes must never panic and must never drive a giant
//! pre-allocation — a truncated value, a bad utf-8 string, a forged list/vector
//! length, or a deeply nested list must come back as `Err`, never an
//! `unwrap()`/index panic or an out-of-memory abort.
//!
//! `read_value` is the recursive, length-prefixed codec that sits underneath *both*
//! the delta WAL decoder (`slater_delta::wal`) and the L0 segment decoders, as well
//! as the core-segment record decoders — so one target guards the value codec that
//! every delta/segment byte stream funnels through. The `n.min(remaining)`
//! pre-allocation bound (mirroring `packstream::read_list`) is what this exercises.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Decode values back-to-back until the input is exhausted or a decode errors, so
    // a single seed can drive many independent value shapes (lists, vectors, strings).
    let mut r = data;
    while !r.is_empty() {
        if graph_format::wire::read_value(&mut r).is_err() {
            break;
        }
    }
});
