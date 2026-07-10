// SPDX-License-Identifier: Apache-2.0
//! Per-(label, property) value→count histograms — the precomputed answer to a
//! whole-label group-by / `count(DISTINCT)` on a low-cardinality indexed property.
//!
//! One block file, `prop_hist.blk`, sits beside `topology.csr.blk`. It holds one
//! record per stored histogram; the record index equals the position of the
//! matching [`crate::manifest::PropertyHistogramDesc`] in
//! `Manifest::property_histograms` (the same dense-index-is-record trick the CSR,
//! label store, and endpoint postings use). A record is
//! `uvarint(count) ‖ (value ‖ uvarint(run))…`, the value encoded with
//! [`crate::wire::write_value`] and the pairs in ascending key order — i.e. exactly
//! the `Vec<(Value, u64)>` that [`crate::isam::IsamReader::distinct_key_counts`]
//! returns for the index this histogram derives from.
//!
//! That equality is the whole point: the executor's grouped-index fast path already
//! computes `distinct_key_counts()` by walking the *entire* ISAM (O(index entries)
//! of zstd decode); the histogram hands back the same small vector in O(distinct).
//! Built offline so a cold query never pays the walk.
//!
//! Only **node** range indexes get a histogram, and only when their distinct-key
//! count is `<= max_distinct` (see [`derive_histogram_from_isam`]) — a unique
//! property (e.g. `name`) would store a histogram as large as the index for no
//! benefit, so it is skipped (the query path then falls back to the walk: slower,
//! never incorrect).

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::blockfile::BlockFileWriter;
use crate::crypto::BlockCipher;
use crate::ids::Value;
use crate::isam::IsamReader;
use crate::wire::{read_uvarint, read_value, write_uvarint, write_value};

/// Default cap on a histogram's distinct-key count. A `(label, property)` whose
/// index has more than this many distinct values is not given a histogram (the
/// build can override via `--histogram-max-distinct`; `0` disables them entirely).
pub const DEFAULT_HISTOGRAM_MAX_DISTINCT: u64 = 4096;

/// Encode one histogram: a count-prefixed list of `(value, run_count)` pairs in
/// ascending key order (as produced by `distinct_key_counts`).
pub fn encode_histogram(pairs: &[(Value, u64)]) -> Vec<u8> {
    let mut rec = Vec::new();
    write_uvarint(&mut rec, pairs.len() as u64);
    for (v, n) in pairs {
        write_value(&mut rec, v);
        write_uvarint(&mut rec, *n);
    }
    rec
}

/// Decode one histogram back into ascending `(value, run_count)` pairs.
pub fn decode_histogram(rec: &[u8]) -> Result<Vec<(Value, u64)>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let v = read_value(&mut r)?;
        let n = read_uvarint(&mut r)?;
        out.push((v, n));
    }
    Ok(out)
}

/// Derive a `(value, count)` histogram from a finished node range-index ISAM by
/// run-length-counting its keys — the *same* computation the query path performs,
/// so the stored result is identical to `distinct_key_counts()` by construction
/// (parity holds across both builders, which both read the finished `.isam`).
///
/// Returns `Ok(None)` — meaning "do not store a histogram" — when `max_distinct`
/// is `0` (the feature is disabled for this build) or the index has more than
/// `max_distinct` distinct keys. The caller logs the skip.
pub fn derive_histogram_from_isam(
    isam_path: impl AsRef<Path>,
    cipher: Option<Arc<BlockCipher>>,
    max_distinct: u64,
) -> Result<Option<Vec<(Value, u64)>>> {
    if max_distinct == 0 {
        return Ok(None);
    }
    let reader = IsamReader::open_with_cipher(isam_path, cipher)?;
    // Abandon *during* the scan, not after it. Counting every distinct key and then
    // checking the length costs O(distinct) resident memory for an index we are about
    // to decline: on 91.6M Wikidata nodes, `node_Entity_wikidata_id` is near-unique, and
    // the discarded `Vec` was the peak RSS of the whole build.
    reader.distinct_key_counts_bounded(max_distinct)
}

/// Write `prop_hist.blk`: one record per encoded histogram, in `records` order
/// (which must match `Manifest::property_histograms`). Always called — a build
/// with no stored histograms writes an empty (zero-record) file so the inventory
/// and content hash stay stable.
pub fn write_property_histograms(
    path: impl AsRef<Path>,
    records: &[Vec<u8>],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<()> {
    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    for rec in records {
        w.append_record(rec)?;
    }
    w.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockfile::BlockFileReader;
    use crate::isam::write_isam;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_hist_{}_{}", std::process::id(), name))
    }

    #[test]
    fn histogram_roundtrips_across_value_kinds() {
        for pairs in [
            vec![],
            vec![(Value::Null, 1u64)],
            vec![
                (Value::Int(1), 3),
                (Value::Int(7), 1),
                (Value::Str("drug".into()), 200),
                (Value::Str("organism".into()), 12345),
            ],
        ] {
            let rec = encode_histogram(&pairs);
            assert_eq!(decode_histogram(&rec).unwrap(), pairs);
        }
    }

    #[test]
    fn derive_matches_distinct_key_counts_and_obeys_cap() {
        // type-like low-cardinality column: 4 distinct values over 8 rows.
        let entries = vec![
            (Value::Str("a".into()), 0u64),
            (Value::Str("a".into()), 1),
            (Value::Str("a".into()), 2),
            (Value::Str("b".into()), 3),
            (Value::Str("c".into()), 4),
            (Value::Str("c".into()), 5),
            (Value::Str("d".into()), 6),
            (Value::Str("d".into()), 7),
        ];
        let path = tmp("derive.isam");
        write_isam(&path, entries, 64, 3).unwrap();

        let reader = IsamReader::open(&path).unwrap();
        let want = reader.distinct_key_counts().unwrap();
        assert_eq!(want.len(), 4);

        // Under the cap → stored, identical to the walk.
        let got = derive_histogram_from_isam(&path, None, 4096).unwrap();
        assert_eq!(got.as_deref(), Some(want.as_slice()));
        // At the cap (distinct == 4) → still stored.
        assert!(derive_histogram_from_isam(&path, None, 4)
            .unwrap()
            .is_some());
        // Below distinct count → skipped.
        assert!(derive_histogram_from_isam(&path, None, 3)
            .unwrap()
            .is_none());
        // Disabled → skipped regardless.
        assert!(derive_histogram_from_isam(&path, None, 0)
            .unwrap()
            .is_none());

        // The bounded scan abandons rather than counting everything and checking the
        // length afterwards: `None` the moment a (max_distinct + 1)-th key appears, so
        // a near-unique index never materialises the `Vec` it is about to discard.
        assert_eq!(
            reader.distinct_key_counts_bounded(4).unwrap(),
            Some(want.clone())
        );
        assert_eq!(reader.distinct_key_counts_bounded(3).unwrap(), None);
        assert_eq!(reader.distinct_key_counts_bounded(1).unwrap(), None);
        assert_eq!(reader.distinct_key_counts_bounded(0).unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_then_read_records_in_order() {
        let h0 = vec![(Value::Int(1), 2u64), (Value::Int(2), 5)];
        let h1 = vec![(Value::Str("x".into()), 9u64)];
        let recs = vec![encode_histogram(&h0), encode_histogram(&h1)];
        let path = tmp("store.blk");
        write_property_histograms(&path, &recs, 4096, 3, None).unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.total_records(), 2);
        assert_eq!(
            decode_histogram(&r.read_record_global(0).unwrap()).unwrap(),
            h0
        );
        assert_eq!(
            decode_histogram(&r.read_record_global(1).unwrap()).unwrap(),
            h1
        );
        let _ = std::fs::remove_file(&path);
    }
}
