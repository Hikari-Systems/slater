// SPDX-License-Identifier: Apache-2.0
//! HIK-80: on-disk decoders must not pre-allocate from an untrusted length prefix.
//!
//! Every decoder here reads a count — a uvarint, or an on-disk `u32`/`u64` — and then sizes a
//! `Vec` from it. In a plaintext generation (a supported mode: no key, or `verifyIntegrity =
//! false`) an attacker with data-dir or bucket write access forges those bytes, and every one
//! of these paths is reached during an ordinary scan or at generation open.
//!
//! The forged records below are all **tiny** — a handful of bytes declaring billions of
//! elements — and each test asserts only that the decode returns `Err`. That is a far stronger
//! assertion than it looks, because *reaching* the assertion is the result:
//!
//! * A decoder that still pre-allocates does not *fail* these tests, it **dies** in them —
//!   `Vec::with_capacity` on a forged count either panics with `capacity overflow` (the
//!   requested bytes exceed `isize::MAX`) or asks the allocator for the memory and aborts the
//!   process when it cannot get it. Neither is a catchable error at the call site; the second
//!   is what an OOM-kill looks like on a node under a 100–200 MB limit. Verified against the
//!   unfixed decoders: `topology::decode_adj` on the 10-byte record below panics
//!   `capacity overflow` at `raw_vec`, having been asked to reserve 2^64 `Adj`s.
//! * The Elias–Fano case is the same shape with a different ending: pre-fix, `select1` walks
//!   off the end of the high bitmap — verified, `index out of bounds: the len is 0 but the
//!   index is 0` — on a plane whose byte-length check passes.
//!
//! So: `Err` means the decoder looked at how many bytes it actually had, and refused the claim
//! before acting on it. Nothing else can produce that outcome from these inputs.

use graph_format::wire::DecodeRejected;
use graph_format::{
    columns, degree_ef, histogram, hubdegree, nodelabels, plane, topology, vectors,
};

/// LEB128, so a forged count is written exactly the way the format writes an honest one.
fn uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// The `Err` of a decode whose `Ok` type is not `Debug` (so `unwrap_err` is unavailable).
fn err_of<T>(r: anyhow::Result<T>, what: &str) -> anyhow::Error {
    match r {
        Ok(_) => panic!("{what}: decoder accepted a forged length prefix"),
        Err(e) => e,
    }
}

/// A record whose leading count is absurd and which then simply stops.
fn forged_count(count: u64) -> Vec<u8> {
    let mut rec = Vec::new();
    uvarint(&mut rec, count);
    rec
}

#[test]
fn topology_adjacency_count_is_bounded_by_the_record() {
    // `decode_adj` peeks the leading edge count to size the `Vec` exactly — a hub node's
    // 10M-edge record is the reason it does, and that reservation is worth keeping. But the
    // count is a *claim*: bound it by what the record's bytes could possibly hold.
    assert!(topology::decode_adj(&forged_count(u64::MAX), true).is_err());
    assert!(topology::decode_adj(&forged_count(u64::MAX), false).is_err());
    // The honest path — including a hub record, whose exact reservation the clamp must not
    // cost anything (`count` ≤ bytes remaining, so `capacity_for` returns `count` unchanged) —
    // is covered by the CSR round-trip tests in `topology` itself.
}

#[test]
fn node_label_count_is_bounded_by_the_record() {
    assert!(nodelabels::decode_labels(&forged_count(u64::MAX), false).is_err());
}

#[test]
fn property_record_count_is_bounded_by_the_record() {
    assert!(columns::decode_props(&forged_count(u64::MAX)).is_err());
}

#[test]
fn hub_and_histogram_counts_are_bounded_by_the_record() {
    assert!(hubdegree::decode_hub_list(&forged_count(u64::MAX)).is_err());
    assert!(histogram::decode_histogram(&forged_count(u64::MAX)).is_err());
}

#[test]
fn vector_dim_product_cannot_wrap_past_the_length_check() {
    // `vectors::decode` gates on `r.len() != dim * 4` — a length check that looks sufficient.
    // But that product is computed from on-disk data in `usize`, so it *wraps*: dim = 2^62
    // makes `dim * 4` exactly 0 (mod 2^64), which equals the zero bytes remaining. The check
    // passes, and `Vec::with_capacity(2^62)` runs. A forged length that satisfies a length
    // check *by overflowing it* is the same defect HIK-75 fixed in `plane`'s `n * 8`, and it
    // is why the guard has to be a checked multiply rather than a comparison.
    let mut rec = Vec::new();
    uvarint(&mut rec, 7); // node_id
    uvarint(&mut rec, 1u64 << 62); // dim, chosen so dim * 4 ≡ 0 (mod 2^64)
    assert_eq!(
        rec.len(),
        10,
        "1-byte node_id + 9-byte dim varint, and nothing after it"
    );

    let err = vectors::decode_vector(&rec).unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<DecodeRejected>(),
            Some(DecodeRejected::LengthOverflow { .. })
        ),
        "expected a typed LengthOverflow, got: {err}"
    );

    // An honest record — same shape, a dim the bytes actually honour — still round-trips.
    let mut ok = Vec::new();
    uvarint(&mut ok, 7); // node_id
    uvarint(&mut ok, 3); // dim
    for v in [1.0f32, 2.0, 3.0] {
        ok.extend_from_slice(&v.to_le_bytes());
    }
    let got = vectors::decode_vector(&ok).unwrap();
    assert_eq!(got.node_id, 7);
    assert_eq!(got.vector, vec![1.0, 2.0, 3.0]);
}

#[test]
fn vamana_dim_and_degree_are_bounded_by_the_record() {
    // Neither the vector dim (4 bytes each) nor the neighbour count (≥1 byte each) was
    // length-checked before its reservation. The v8 record dropped its leading `node_id`
    // (it is now `dim ‖ vec ‖ degree ‖ adj`), so both forged lengths sit one field earlier
    // — the guards had to move with them, and this is what proves they did.
    let mut rec = Vec::new();
    uvarint(&mut rec, u64::MAX); // dim
    assert!(graph_format::vamana::decode_node(&rec).is_err());

    let mut rec = Vec::new();
    uvarint(&mut rec, 0); // dim: no vector
    uvarint(&mut rec, u64::MAX); // degree
    assert!(graph_format::vamana::decode_node(&rec).is_err());

    // A dim the record *nearly* honours is rejected too: 2 elements declared, only one
    // element's bytes present. `capacity_for` clamps the reservation to what the remaining
    // bytes could hold, and the short read then errors — it must not quietly truncate.
    let mut rec = Vec::new();
    uvarint(&mut rec, 2);
    rec.extend_from_slice(&1.0f32.to_le_bytes());
    assert!(graph_format::vamana::decode_node(&rec).is_err());

    // An honest record in the new layout still round-trips.
    let mut ok = Vec::new();
    uvarint(&mut ok, 2); // dim
    for v in [1.0f32, 2.0] {
        ok.extend_from_slice(&v.to_le_bytes());
    }
    uvarint(&mut ok, 3); // degree
    for nb in [7u64, 8, 9] {
        uvarint(&mut ok, nb);
    }
    let got = graph_format::vamana::decode_node(&ok).unwrap();
    assert_eq!(got.vector, vec![1.0, 2.0]);
    assert_eq!(got.neighbours, vec![7, 8, 9]);
}

#[test]
fn elias_fano_high_bitmap_must_hold_the_one_bits_it_declares() {
    // The EF body's *byte* length is validated; its content is not. `select1` finds the i-th
    // one-bit by walking `highs` counting ones — with fewer ones than the header's `m`, it
    // walks off the end of the slice. A plain `value_at` on a forged plane is then an
    // out-of-bounds panic rather than a decode error, and it is reached from an ordinary query.
    let ef = plane::EfMono::encode(&[1, 5, 9, 40, 41, 900]);
    let mut body = ef.serialize();
    assert!(plane::EfMono::deserialize(&body).is_ok());

    // Zero the high bitmap, leaving the header — and therefore every length — exactly as it
    // was, so the existing byte-length check still passes. Only the one-bit count is wrong.
    let m = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let l = body[4] as usize;
    let low_bytes = (m * l).div_ceil(8);
    for b in body[9 + low_bytes..].iter_mut() {
        *b = 0;
    }

    let err = err_of(plane::EfMono::deserialize(&body), "ef-mono bit count");
    match err.downcast_ref::<DecodeRejected>() {
        Some(DecodeRejected::EfBitCount {
            declared, found, ..
        }) => {
            assert_eq!(*declared, m);
            assert_eq!(*found, 0);
        }
        _ => panic!("expected a typed EfBitCount rejection, got: {err}"),
    }
}

#[test]
fn rle_plane_element_count_cannot_declare_a_34gb_materialisation() {
    // An RLE plane's `n` is the one plane count *not* bounded by the record's byte length: run
    // lengths are uvarints, so these few bytes are a well-formed single run declaring 2^32
    // values — and `to_values` would then materialise a 34 GB `Vec`.
    let mut rec = vec![4u8]; // PlaneKind::Rle
    uvarint(&mut rec, u32::MAX as u64); // n
    uvarint(&mut rec, 1); // run_count
    uvarint(&mut rec, 7); // value
    uvarint(&mut rec, u32::MAX as u64); // run length
    assert!(rec.len() < 16, "the whole bomb is {} bytes", rec.len());

    let err = err_of(plane::decode_plane(&rec), "rle plane bomb");
    assert!(
        matches!(
            err.downcast_ref::<DecodeRejected>(),
            Some(DecodeRejected::TooManyElements { .. })
        ),
        "expected a typed TooManyElements, got: {err}"
    );

    // A forged *run count* is the ordinary case: reservation clamped, then the short read.
    let mut rec = vec![4u8];
    uvarint(&mut rec, 3); // n
    uvarint(&mut rec, u64::MAX); // run_count
    assert!(plane::decode_plane(&rec).is_err());

    // Honest planes still round-trip through every codec the encoder can pick.
    for values in [
        vec![1u64, 2, 3, 900],           // sparse / monotone → EF
        vec![5u64; 300],                 // one run → RLE
        (0..500u64).collect::<Vec<_>>(), // dense
    ] {
        let enc = plane::encode_plane(&values, &plane::PlaneCodecOpts::default()).unwrap();
        assert_eq!(plane::decode_plane(&enc).unwrap().to_values(), values);
    }
}

#[test]
fn degree_chunk_counts_are_bounded() {
    // The same two defects in the degree column's own EF/RLE codecs, which the degree-sum
    // count fast path faults on every k-hop count.
    let mut rec = vec![4u8]; // ChunkKind::Rle
    uvarint(&mut rec, u32::MAX as u64); // n
    uvarint(&mut rec, 1); // run_count
    uvarint(&mut rec, 3); // value
    uvarint(&mut rec, u32::MAX as u64); // length
    let err = err_of(degree_ef::decode_chunk(&rec), "rle degree-chunk bomb");
    assert!(
        matches!(
            err.downcast_ref::<DecodeRejected>(),
            Some(DecodeRejected::TooManyElements { .. })
        ),
        "expected a typed TooManyElements, got: {err}"
    );

    let mut rec = vec![4u8];
    uvarint(&mut rec, 4); // n
    uvarint(&mut rec, u64::MAX); // run_count
    assert!(degree_ef::decode_chunk(&rec).is_err());

    // An EF chunk's high bitmap, likewise, must hold the ones it declares.
    let degrees: Vec<u32> = (0..64u32).map(|i| i % 5).collect();
    let enc = degree_ef::encode_chunk(&degrees, &degree_ef::DegreeCodecOpts::default()).unwrap();
    assert_eq!(degree_ef::decode_chunk(&enc).unwrap().to_degrees(), degrees);
    assert_eq!(enc[0], 1, "this chunk should encode as EF");

    let mut body = enc.clone();
    let n = u32::from_le_bytes(body[1..5].try_into().unwrap()) as usize;
    let l = body[5] as usize;
    let low_bytes = ((n + 1) * l).div_ceil(8);
    for b in body[1 + 9 + low_bytes..].iter_mut() {
        *b = 0;
    }
    let err = err_of(degree_ef::decode_chunk(&body), "ef degree-chunk bit count");
    assert!(
        matches!(
            err.downcast_ref::<DecodeRejected>(),
            Some(DecodeRejected::EfBitCount { .. })
        ),
        "expected a typed EfBitCount, got: {err}"
    );
}
