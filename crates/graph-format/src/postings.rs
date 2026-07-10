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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;

use crate::blockfile::BlockFileWriter;
use crate::crypto::BlockCipher;
use crate::topology::Edge;
use crate::wire::{read_uvarint, uvarint_len, write_uvarint};

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

/// One dense bit plane per reltype over the node id space: bit `n` of plane `t`
/// is set iff node `n` is an endpoint of a reltype-`t` edge on this side.
///
/// This is the whole answer a posting file needs — a per-reltype *set* of node
/// ids — computed with no sort at all. The external builder used to reach the
/// same answer by pushing one `(reltype, node)` record per edge into an
/// `ExtSorter` (2.98 B of them at Wikidata scale), sorting by `(reltype, node)`
/// and run-length-collapsing the drained stream. Setting a bit is idempotent, so
/// the dedup is free and the result is independent of the order edges arrive in.
///
/// Cost is `reltype_count × ceil(node_count / 8)` bytes per side, which is why
/// the caller must check it against the memory budget before allocating: the
/// product is tiny for every shape we build (one reltype over 91.6M nodes, or 63
/// reltypes over 1.5M, both ≈ 11.5 MB) but a graph that is *both* large and
/// richly typed would not fit. See `write_endpoint_postings_from_sorted` for the
/// bounded-memory fallback.
///
/// `set` is `&self` and lock-free: bands write disjoint slices of the source
/// plane, but a band's *targets* scatter across every other band's range, so the
/// planes are `AtomicU64` and the workers `fetch_or` into them. Union is
/// commutative and monotone — no ordering beyond `Relaxed` is needed, and the
/// rayon join that ends the band phase supplies the happens-before edge.
pub struct EndpointPlanes {
    reltype_count: u32,
    node_count: u64,
    words_per_plane: usize,
    words: Vec<AtomicU64>,
}

impl EndpointPlanes {
    /// Resident bytes one side's planes would occupy. Reserve this against the
    /// `MemoryBudget` before calling [`EndpointPlanes::new`].
    pub fn bytes_for(reltype_count: u32, node_count: u64) -> u64 {
        (reltype_count as u64) * node_count.div_ceil(64) * 8
    }

    pub fn new(reltype_count: u32, node_count: u64) -> Self {
        let words_per_plane = node_count.div_ceil(64) as usize;
        let total = words_per_plane * reltype_count as usize;
        Self {
            reltype_count,
            node_count,
            words_per_plane,
            words: std::iter::repeat_with(|| AtomicU64::new(0))
                .take(total)
                .collect(),
        }
    }

    pub fn reltype_count(&self) -> u32 {
        self.reltype_count
    }

    /// Record that `node` is an endpoint of a `reltype` edge.
    ///
    /// The plain `load` first is not just an optimisation of the atomic: the
    /// forward pass walks edges grouped by source node, so a node's ~16 outgoing
    /// Wikidata edges set the same bit 16 times in a row. Skipping the locked
    /// read-modify-write when the bit is already set elides the overwhelming
    /// majority of them. Racing setters cannot lose an update — bits only ever
    /// go 0→1, so a `fetch_or` that another thread beat us to is a no-op.
    pub fn set(&self, reltype: u32, node: u64) {
        debug_assert!(reltype < self.reltype_count && node < self.node_count);
        let idx = reltype as usize * self.words_per_plane + (node >> 6) as usize;
        let bit = 1u64 << (node & 63);
        let word = &self.words[idx];
        if word.load(Ordering::Relaxed) & bit == 0 {
            word.fetch_or(bit, Ordering::Relaxed);
        }
    }

    fn plane(&self, reltype: u32) -> &[AtomicU64] {
        let lo = reltype as usize * self.words_per_plane;
        &self.words[lo..lo + self.words_per_plane]
    }

    /// Visit plane `reltype`'s set node ids in ascending order.
    fn for_each_node(&self, reltype: u32, mut f: impl FnMut(u64)) {
        for (w, word) in self.plane(reltype).iter().enumerate() {
            let mut bits = word.load(Ordering::Relaxed);
            while bits != 0 {
                f((w as u64) * 64 + bits.trailing_zeros() as u64);
                bits &= bits - 1;
            }
        }
    }

    /// `(distinct node count, exact encoded record length)` for plane `reltype`.
    fn plane_stat(&self, reltype: u32) -> (u64, usize) {
        let count: u64 = self
            .plane(reltype)
            .iter()
            .map(|w| w.load(Ordering::Relaxed).count_ones() as u64)
            .sum();
        let mut len = uvarint_len(count);
        let mut prev = 0u64;
        self.for_each_node(reltype, |id| {
            len += uvarint_len(id - prev);
            prev = id;
        });
        (count, len)
    }

    /// Largest record [`write_endpoint_postings_from_planes`] will build. The
    /// writer reuses one buffer, so this is its peak — reserve it (and the two
    /// copies `BlockFileWriter` makes of an over-target record) before writing.
    pub fn max_record_bytes(&self) -> usize {
        (0..self.reltype_count)
            .map(|t| self.plane_stat(t).1)
            .max()
            .unwrap_or(0)
    }
}

/// Write one endpoint posting file from a side's [`EndpointPlanes`], returning
/// the per-reltype distinct counts for the manifest.
///
/// Emits byte-for-byte what [`write_endpoint_postings_from_sorted`] emits for
/// the same edge set: one record per reltype id `0..reltype_count` (empty for
/// reltypes with no endpoints), each `uvarint(count) ‖ delta-varints`.
pub fn write_endpoint_postings_from_planes(
    path: impl AsRef<Path>,
    planes: &EndpointPlanes,
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<Vec<u64>> {
    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    let mut counts = Vec::with_capacity(planes.reltype_count as usize);
    let mut rec: Vec<u8> = Vec::new();
    for t in 0..planes.reltype_count {
        let (count, len) = planes.plane_stat(t);
        rec.clear();
        rec.reserve(len);
        write_uvarint(&mut rec, count);
        let mut prev = 0u64;
        planes.for_each_node(t, |id| {
            write_uvarint(&mut rec, id - prev);
            prev = id;
        });
        debug_assert_eq!(rec.len(), len, "plane_stat disagreed with the encoder");
        counts.push(count);
        w.append_record(&rec)?;
    }
    w.finish()?;
    Ok(counts)
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

    /// Truth is the `BTreeSet` of endpoints, derived from the edge list without
    /// reference to either writer.
    #[test]
    fn bitmap_postings_match_independently_derived_endpoints() {
        // reltype 0: srcs {1,1,3} → {1,3}; tgts {2,4,4} → {2,4}.
        // reltype 1: self-loop 5->5.
        // reltype 2: no edges at all → must still get an empty record.
        // node 0 is a source, to catch an off-by-one at the bottom of plane 0.
        let edges = vec![
            edge(1, 2, 0, 0),
            edge(1, 4, 0, 1),
            edge(3, 4, 0, 2),
            edge(0, 7, 0, 3),
            edge(5, 5, 1, 4),
        ];
        let node_count = 9; // node 8 is isolated
        let src = EndpointPlanes::new(3, node_count);
        let tgt = EndpointPlanes::new(3, node_count);
        for e in &edges {
            src.set(e.reltype, e.src.0);
            tgt.set(e.reltype, e.dst.0);
        }

        let sp = tmp("bitmap_src");
        let tp = tmp("bitmap_tgt");
        let sc = write_endpoint_postings_from_planes(&sp, &src, 4096, 3, None).unwrap();
        let tc = write_endpoint_postings_from_planes(&tp, &tgt, 4096, 3, None).unwrap();
        assert_eq!(sc, vec![3, 1, 0]);
        assert_eq!(tc, vec![3, 1, 0]);

        let sr = BlockFileReader::open(&sp).unwrap();
        let tr = BlockFileReader::open(&tp).unwrap();
        for t in 0u32..3 {
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
    }

    /// The two writers must agree *byte for byte*, not merely set for set: the
    /// generation's content hash folds these files in, and the builder picks
    /// between the two paths on a memory-budget check. A build that spills to the
    /// sorter must publish the same generation as one that fits in bitmaps.
    #[test]
    fn bitmap_and_sorted_paths_write_identical_bytes() {
        // Spread ids across a word boundary and leave gaps, so deltas are a mix
        // of 1s and multi-byte varints.
        let mut edges = Vec::new();
        for i in 0..400u64 {
            edges.push(edge(i * 7 % 300, i * 13 % 300, (i % 5) as u32, i));
        }
        let node_count = 300;
        let planes = EndpointPlanes::new(5, node_count);
        for e in &edges {
            planes.set(e.reltype, e.src.0);
        }
        let a = tmp("ident_bitmap");
        let counts_bitmap = write_endpoint_postings_from_planes(&a, &planes, 256, 3, None).unwrap();

        // Same edge set through the sorted drain: (reltype, src) sorted + deduped.
        let mut pairs: Vec<(u32, u64)> = edges.iter().map(|e| (e.reltype, e.src.0)).collect();
        pairs.sort_unstable();
        pairs.dedup();
        let b = tmp("ident_sorted");
        let counts_sorted =
            write_endpoint_postings_from_sorted(&b, 5, pairs.into_iter().map(Ok), 256, 3, None)
                .unwrap();

        assert_eq!(counts_bitmap, counts_sorted);
        assert_eq!(std::fs::read(&a).unwrap(), std::fs::read(&b).unwrap());
    }

    #[test]
    fn set_is_idempotent_and_order_independent() {
        let a = EndpointPlanes::new(2, 200);
        let b = EndpointPlanes::new(2, 200);
        for (t, n) in [(0u32, 7u64), (1, 199), (0, 7), (0, 0), (1, 64)] {
            a.set(t, n);
        }
        for (t, n) in [(1u32, 64u64), (0, 0), (0, 7), (1, 199), (1, 199)] {
            b.set(t, n);
        }
        let pa = tmp("idem_a");
        let pb = tmp("idem_b");
        write_endpoint_postings_from_planes(&pa, &a, 256, 3, None).unwrap();
        write_endpoint_postings_from_planes(&pb, &b, 256, 3, None).unwrap();
        assert_eq!(std::fs::read(&pa).unwrap(), std::fs::read(&pb).unwrap());
    }

    #[test]
    fn bytes_for_matches_allocation_and_handles_degenerate_shapes() {
        // The two real shapes, per side: Wikidata (1 reltype × 91.6M nodes) and
        // Monarch-KG (63 × 1.46M) both land at ~11.5 MB. Planes are word-padded.
        assert_eq!(EndpointPlanes::bytes_for(1, 91_600_504), 11_450_064);
        assert_eq!(EndpointPlanes::bytes_for(63, 1_462_594), 11_518_416);
        assert_eq!(EndpointPlanes::bytes_for(0, 1_000), 0);
        assert_eq!(EndpointPlanes::bytes_for(5, 0), 0);

        // No reltypes → no records. No nodes → one empty record per reltype.
        let p = tmp("degenerate");
        assert!(
            write_endpoint_postings_from_planes(&p, &EndpointPlanes::new(0, 10), 256, 3, None)
                .unwrap()
                .is_empty()
        );
        let counts =
            write_endpoint_postings_from_planes(&p, &EndpointPlanes::new(3, 0), 256, 3, None)
                .unwrap();
        assert_eq!(counts, vec![0, 0, 0]);
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
