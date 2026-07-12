// SPDX-License-Identifier: Apache-2.0
//! Hub-degree sidecar — per-node out/in degree for high-degree ("hub") nodes, so a
//! reader can decide a node is a hub with O(1) memory and **zero adjacency I/O**.
//!
//! One block file, `hub_degrees.blk`, sits beside `topology.csr.blk`. It holds exactly
//! two records: record 0 is the **out**-hub list, record 1 is the **in**-hub list. Each
//! is `uvarint(count) ‖ (uvarint(id_delta) ‖ uvarint(degree))…`, node ids ascending and
//! delta-encoded, listing every node whose degree in that direction is `>= floor` (the
//! build-time [`crate::manifest::HubDegreeDesc::floor`]). A node below the floor in a
//! direction is simply absent from that list.
//!
//! Why two per-direction lists rather than one `(id, out, in)` list: the reader only
//! needs an exact degree for a node it might *stream*, which happens at/above a query
//! threshold that is always `>= floor`. A node absent from the out-list therefore has
//! out-degree `< floor <= threshold` and can never be an out-hub — so its exact (sub-
//! floor) out-degree is irrelevant, and collecting the two lists independently avoids
//! pairing the forward and reverse degree passes (no dense per-node table at build time).
//!
//! The reader loads both lists resident (a few MB even on a 91.6M-node graph — hubs are
//! rare) and binary-searches by id. Absent file / older generation ⇒ the reader declines
//! and the query falls back to reading the record's leading edge count: slower, correct.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::blockfile::BlockFileWriter;
use crate::crypto::BlockCipher;
use crate::wire::{read_uvarint, write_uvarint};

/// Default degree floor at/above which a node is recorded in the sidecar. Chosen well
/// below any sane query-side stream threshold so the sidecar holds an exact degree for
/// every node a query might stream; the resulting list is a few MB on Wikidata.
pub const DEFAULT_HUB_DEGREE_FLOOR: u32 = 1024;

/// Encode one hub list — a count-prefixed run of `(id_delta, degree)` uvarints. Entries
/// **must** be ascending by id (the delta encoding and the reader's binary search both
/// rely on it).
pub fn encode_hub_list(entries: &[(u64, u32)]) -> Vec<u8> {
    let mut rec = Vec::new();
    write_uvarint(&mut rec, entries.len() as u64);
    let mut prev = 0u64;
    for &(id, deg) in entries {
        debug_assert!(id >= prev, "hub list must be ascending by id");
        write_uvarint(&mut rec, id - prev);
        write_uvarint(&mut rec, deg as u64);
        prev = id;
    }
    rec
}

/// Decode one hub list back into ascending `(id, degree)` pairs.
pub fn decode_hub_list(rec: &[u8]) -> Result<Vec<(u64, u32)>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count);
    let mut prev = 0u64;
    for _ in 0..count {
        let id = prev + read_uvarint(&mut r)?;
        let deg = read_uvarint(&mut r)? as u32;
        out.push((id, deg));
        prev = id;
    }
    Ok(out)
}

/// Write `hub_degrees.blk`: record 0 = out-hubs, record 1 = in-hubs (each ascending by
/// id). Always called by a build so the file — and thus the inventory and content hash —
/// stays stable; an empty build writes two empty (count-0) records.
pub fn write_hub_degrees(
    path: impl AsRef<Path>,
    out_hubs: &[(u64, u32)],
    in_hubs: &[(u64, u32)],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<()> {
    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    w.append_record(&encode_hub_list(out_hubs))?;
    w.append_record(&encode_hub_list(in_hubs))?;
    w.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockfile::BlockFileReader;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_hubdeg_{}_{}", std::process::id(), name))
    }

    #[test]
    fn hub_list_roundtrips() {
        for entries in [
            vec![],
            vec![(0u64, 1024u32)],
            vec![
                (3, 5000),
                (7, 1024),
                (1_000_000, 3_269_338),
                (u32::MAX as u64 + 10, 2048),
            ],
        ] {
            let rec = encode_hub_list(&entries);
            assert_eq!(decode_hub_list(&rec).unwrap(), entries);
        }
    }

    #[test]
    fn write_then_read_two_records() {
        let out = vec![(2u64, 4096u32), (9, 1024), (50, 100_000)];
        let inn = vec![(1u64, 2048u32), (9, 8192)];
        let path = tmp("hd.blk");
        write_hub_degrees(&path, &out, &inn, 4096, 3, None).unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.total_records(), 2);
        assert_eq!(
            decode_hub_list(&r.read_record_global(0).unwrap()).unwrap(),
            out
        );
        assert_eq!(
            decode_hub_list(&r.read_record_global(1).unwrap()).unwrap(),
            inn
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_lists_write_two_empty_records() {
        let path = tmp("empty.blk");
        write_hub_degrees(&path, &[], &[], 4096, 3, None).unwrap();
        let r = BlockFileReader::open(&path).unwrap();
        assert_eq!(r.total_records(), 2);
        assert!(decode_hub_list(&r.read_record_global(0).unwrap())
            .unwrap()
            .is_empty());
        assert!(decode_hub_list(&r.read_record_global(1).unwrap())
            .unwrap()
            .is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
