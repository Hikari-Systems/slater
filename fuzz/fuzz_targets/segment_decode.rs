#![no_main]
//! Fuzz the core-segment record + meta decoders (the segmented-core track). Property:
//! every byte-level segment decoder must never panic on arbitrary bytes — a truncated
//! record, a bad utf-8 label/reltype, or a bogus count must come back as `Err` (and, in
//! particular, must not abort on a giant pre-allocation). These parse block-file payloads
//! and `meta.bin`; though those bytes are MAC/crc-authenticated at rest, the decoders are
//! held to the same never-panic contract as the wire decoders.
use libfuzzer_sys::fuzz_target;

use graph_format::segment::{decode_adj_fragment, decode_segment_meta, EdgeRow, NodeRow};

fuzz_target!(|data: &[u8]| {
    let _ = NodeRow::decode(data);
    let _ = EdgeRow::decode(data);
    let _ = decode_adj_fragment(data);
    let _ = decode_segment_meta(data);
});
