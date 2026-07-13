#![no_main]
//! Fuzz the delta WAL segment decoder (`slater_delta::wal::replay_bytes_for_fuzz`).
//! Property: replaying an arbitrary in-memory segment image must never panic and
//! must never drive a giant pre-allocation — a bad/missing magic, a torn frame, a
//! crc mismatch, an unknown op tag, a truncated record body, or a forged
//! patch/prop/label count must come back as `Err` or a cleanly-truncated replay,
//! never an `unwrap()`/index panic or an out-of-memory abort.
//!
//! This is the write path's on-disk-format decoder: `replay_bytes` frames
//! (`len ‖ crc ‖ payload`) then `decode_record_body` parses each `WalRecord`,
//! pre-allocating `patch`/`prop`/`label` vectors from untrusted uvarint counts
//! (bounded by remaining bytes, à la `packstream::read_list`). Though WAL segments
//! are crc-framed at rest, the decoder is held to the same never-panic contract as
//! the core-segment and wire decoders.
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = slater_delta::wal::replay_bytes_for_fuzz(data);
});
