// SPDX-License-Identifier: Apache-2.0
//! Per-reltype endpoint postings — the precomputed driving sets for a
//! relationship-type scan.
//!
//! Two block files sit beside `topology.csr.blk`:
//!   * `reltype_src.post` — record `t` holds the ascending distinct **source**
//!     node ids that have an *outgoing* edge of reltype id `t`.
//!   * `reltype_tgt.post` — record `t` holds the ascending distinct **target**
//!     node ids that have an *incoming* edge of reltype id `t`.
//!
//! The record index equals the reltype id (the same dense-id-is-record-index
//! trick the CSR and label store use); a reltype with no edges gets an empty
//! (count 0) record to keep the alignment. A record is
//! `uvarint(count) ‖ uvarint(first) ‖ uvarint(delta)…` — ascending ids ⇒ small
//! deltas ⇒ the per-block zstd packs them tightly.
//!
//! These let an unanchored typed traversal `(a)-[:T]->(b)` drive from the ~8% of
//! nodes that actually have a `T` edge instead of label-scanning every node and
//! probing adjacency. They are built offline so a cold query never pays to
//! enumerate them.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::blockfile::BlockFileWriter;
use crate::crypto::BlockCipher;
use crate::topology::Edge;
use crate::wire::{read_uvarint, write_uvarint};

/// Encode one reltype's endpoint posting: a delta-varint list of ascending,
/// already-distinct node ids. Callers must pass ids sorted ascending with no
/// duplicates (see [`write_reltype_endpoint_postings`]).
pub fn encode_endpoint_posting(ids: &[u64]) -> Vec<u8> {
    let mut rec = Vec::new();
    write_uvarint(&mut rec, ids.len() as u64);
    let mut prev = 0u64;
    for &id in ids {
        write_uvarint(&mut rec, id - prev);
        prev = id;
    }
    rec
}

/// Decode one reltype's endpoint posting back into ascending node ids.
pub fn decode_endpoint_posting(rec: &[u8]) -> Result<Vec<u64>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count);
    let mut prev = 0u64;
    for _ in 0..count {
        prev += read_uvarint(&mut r)?;
        out.push(prev);
    }
    Ok(out)
}

/// Build `reltype_src.post` and `reltype_tgt.post` from an edge list, returning
/// the per-reltype distinct source/target counts (index = reltype id) for the
/// manifest. Offline, in-memory path — mirrors [`crate::topology::write_csr`].
/// The external (bounded-memory) builder writes the same files via its own
/// external sort, reusing [`encode_endpoint_posting`].
pub fn write_reltype_endpoint_postings(
    src_path: impl AsRef<Path>,
    tgt_path: impl AsRef<Path>,
    reltype_count: u32,
    edges: &[Edge],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<(Vec<u64>, Vec<u64>)> {
    let rt = reltype_count as usize;
    let mut src_buckets: Vec<Vec<u64>> = vec![Vec::new(); rt];
    let mut tgt_buckets: Vec<Vec<u64>> = vec![Vec::new(); rt];
    for e in edges {
        let t = e.reltype as usize;
        src_buckets[t].push(e.src.0);
        tgt_buckets[t].push(e.dst.0);
    }

    let src_counts = write_buckets(
        src_path,
        &mut src_buckets,
        target_block_bytes,
        zstd_level,
        cipher.clone(),
    )?;
    let tgt_counts = write_buckets(
        tgt_path,
        &mut tgt_buckets,
        target_block_bytes,
        zstd_level,
        cipher,
    )?;
    Ok((src_counts, tgt_counts))
}

/// Write the endpoint postings from a stream already sorted ascending by
/// `(reltype, node)` — the bounded-memory path used by the external builder,
/// which feeds an [`crate::extsort::ExtSorter`] and drains it here. Emits exactly
/// one record per reltype id `0..reltype_count` (empty for reltypes with no
/// endpoints), deduping adjacent equal nodes. Returns the per-reltype distinct
/// counts. The stream must be sorted by reltype then node, or this errors.
pub fn write_endpoint_postings_from_sorted(
    path: impl AsRef<Path>,
    reltype_count: u32,
    sorted: impl Iterator<Item = Result<(u32, u64)>>,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<Vec<u64>> {
    use anyhow::bail;
    let mut counts = Vec::with_capacity(reltype_count as usize);
    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    let mut cur_rt: u32 = 0;
    let mut bucket: Vec<u64> = Vec::new();
    let mut last: Option<u64> = None;
    let flush =
        |w: &mut BlockFileWriter, counts: &mut Vec<u64>, bucket: &mut Vec<u64>| -> Result<()> {
            counts.push(bucket.len() as u64);
            w.append_record(&encode_endpoint_posting(bucket))?;
            bucket.clear();
            Ok(())
        };
    for item in sorted {
        let (rt, node) = item?;
        if rt >= reltype_count {
            bail!("endpoint posting reltype {rt} >= reltype_count {reltype_count}");
        }
        if rt < cur_rt {
            bail!("endpoint posting stream not sorted by reltype ({rt} after {cur_rt})");
        }
        while cur_rt < rt {
            flush(&mut w, &mut counts, &mut bucket)?;
            cur_rt += 1;
            last = None;
        }
        if last != Some(node) {
            bucket.push(node);
            last = Some(node);
        }
    }
    // Flush the final reltype, then pad empties out to reltype_count.
    while (cur_rt as usize) < reltype_count as usize {
        flush(&mut w, &mut counts, &mut bucket)?;
        cur_rt += 1;
    }
    w.finish()?;
    Ok(counts)
}

/// Sort+dedup each per-reltype bucket in place, write one record per reltype, and
/// return the per-reltype distinct counts.
fn write_buckets(
    path: impl AsRef<Path>,
    buckets: &mut [Vec<u64>],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<Vec<u64>> {
    let mut counts = Vec::with_capacity(buckets.len());
    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    for bucket in buckets.iter_mut() {
        bucket.sort_unstable();
        bucket.dedup();
        counts.push(bucket.len() as u64);
        w.append_record(&encode_endpoint_posting(bucket))?;
    }
    w.finish()?;
    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockfile::BlockFileReader;
    use crate::crypto::BlockCipher;
    use crate::ids::{EdgeId, NodeId};
    use std::collections::BTreeSet;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_post_{}_{}", std::process::id(), name))
    }

    fn edge(src: u64, dst: u64, reltype: u32, edge: u64) -> Edge {
        Edge {
            src: NodeId(src),
            dst: NodeId(dst),
            reltype,
            edge: EdgeId(edge),
        }
    }

    #[test]
    fn posting_roundtrips_including_empty() {
        for ids in [vec![], vec![0u64], vec![0, 1, 5, 5000, 5001]] {
            let rec = encode_endpoint_posting(&ids);
            assert_eq!(decode_endpoint_posting(&rec).unwrap(), ids);
        }
    }

    #[test]
    fn postings_match_independently_derived_endpoints() {
        // reltype 0: edges from sources {1,1,3} (dup src 1) → distinct {1,3};
        //            targets {2,4,4} → distinct {2,4}.
        // reltype 1: a self-loop 5->5 → src {5}, tgt {5}.
        // node 9 is isolated → in no posting.
        let edges = vec![
            edge(1, 2, 0, 0),
            edge(1, 4, 0, 1), // same src 1, reltype 0 → deduped
            edge(3, 4, 0, 2), // same dst 4 → deduped in target posting
            edge(5, 5, 1, 3), // self-loop
        ];
        let sp = tmp("derived_src");
        let tp = tmp("derived_tgt");
        let (sc, tc) = write_reltype_endpoint_postings(&sp, &tp, 2, &edges, 4096, 3, None).unwrap();
        assert_eq!(sc, vec![2, 1]); // reltype0 srcs {1,3}; reltype1 src {5}
        assert_eq!(tc, vec![2, 1]); // reltype0 tgts {2,4}; reltype1 tgt {5}

        let sr = BlockFileReader::open(&sp).unwrap();
        let tr = BlockFileReader::open(&tp).unwrap();
        // Independently derive expected sets per reltype.
        for t in 0u32..2 {
            let want_src: Vec<u64> = edges
                .iter()
                .filter(|e| e.reltype == t)
                .map(|e| e.src.0)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let want_tgt: Vec<u64> = edges
                .iter()
                .filter(|e| e.reltype == t)
                .map(|e| e.dst.0)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let got_src =
                decode_endpoint_posting(&sr.read_record_global(t as u64).unwrap()).unwrap();
            let got_tgt =
                decode_endpoint_posting(&tr.read_record_global(t as u64).unwrap()).unwrap();
            assert_eq!(got_src, want_src, "src posting reltype {t}");
            assert_eq!(got_tgt, want_tgt, "tgt posting reltype {t}");
        }
        // node 9 appears nowhere.
        assert!(!decode_endpoint_posting(&sr.read_record_global(0).unwrap())
            .unwrap()
            .contains(&9));
    }

    #[test]
    fn encrypted_postings_roundtrip_and_hide_plaintext() {
        let cipher = Some(Arc::new(BlockCipher::from_key(&[7u8; 32])));
        // Source id 0x4242 should not appear verbatim in the ciphertext.
        let edges = vec![edge(0x4242, 1, 0, 0)];
        let sp = tmp("enc_src");
        let tp = tmp("enc_tgt");
        write_reltype_endpoint_postings(&sp, &tp, 1, &edges, 4096, 3, cipher.clone()).unwrap();

        let raw = std::fs::read(&sp).unwrap();
        assert!(!raw.windows(2).any(|w| w == [0x42, 0x42]));

        let sr = BlockFileReader::open_with_cipher(&sp, cipher).unwrap();
        let got = decode_endpoint_posting(&sr.read_record_global(0).unwrap()).unwrap();
        assert_eq!(got, vec![0x4242]);
    }
}
