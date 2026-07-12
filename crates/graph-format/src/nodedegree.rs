// SPDX-License-Identifier: Apache-2.0
//! Dense per-node degree column (`node_degrees.blk`) — every node's exact out- and
//! in-degree, indexed by dense node id, so a degree lookup is an O(1) resident array
//! access with **no adjacency read**.
//!
//! This is the "store the count one hop away" artifact: it turns a k-hop `count(endpoint)`
//! into a (k-1)-hop walk plus a degree sum over the penultimate frontier, where each of
//! the (potentially millions of) penultimate degrees is a 4-byte lookup instead of a CSR
//! block read. Where the sparse [`crate::hubdegree`] sidecar records only high-degree
//! ("hub") nodes for the streaming probe, this covers **every** node exactly, for the
//! degree-sum count fast path.
//!
//! Layout: the out-degrees as a run of [`DEGREES_PER_RECORD`]-wide records, then the
//! in-degrees the same way — so record `< records_per_half` is out, the rest is in. Each
//! record is a **per-chunk Elias–Fano** encoding (see [`crate::degree_ef`]), stored in a
//! **raw** (uncompressed) block container: EF is already the compact, queryable form, so a
//! chunk fault is a bare `pread` + parse with no zstd decompress. The reader gates on the
//! **file's existence**, so a generation retrofitted with the column (or built with it) uses
//! it, and one without falls back to reading the CSR record's leading count — slower,
//! identical answer.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::blockfile::{BlockCodec, BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::degree_ef::{decode_chunk, encode_chunk, DegreeChunk, DegreeCodecOpts};

/// Degrees per stored record (u32 each ⇒ 1 MiB records at 2^18). Bounds a record's size
/// while keeping the record count small (a few hundred on a 91.6M-node graph).
pub const DEGREES_PER_RECORD: usize = 1 << 18;

/// Records the out (or in) half occupies for `node_count` nodes.
pub fn records_per_half(node_count: usize) -> usize {
    node_count.div_ceil(DEGREES_PER_RECORD)
}

/// Write `node_degrees.blk`: the out-degrees, then the in-degrees, each a run of
/// [`DEGREES_PER_RECORD`]-wide **per-chunk Elias–Fano** records in a raw block container.
/// `out_degs` and `in_degs` must both have length `node_count` (degree of dense id `i` at
/// index `i`). `codec` tunes only the per-chunk codec selection (the block container itself is
/// uncompressed); see [`DegreeCodecOpts`] for the fs-vs-object-store trade-off.
pub fn write_node_degrees(
    path: impl AsRef<Path>,
    out_degs: &[u32],
    in_degs: &[u32],
    target_block_bytes: usize,
    codec: DegreeCodecOpts,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<()> {
    if out_degs.len() != in_degs.len() {
        bail!(
            "node-degree columns differ in length: out {} != in {}",
            out_degs.len(),
            in_degs.len()
        );
    }
    let mut w = BlockFileWriter::create_with_codec(
        path,
        target_block_bytes,
        BlockCodec::Raw,
        codec.zstd_level,
        cipher,
    )?;
    for degs in [out_degs, in_degs] {
        for chunk in degs.chunks(DEGREES_PER_RECORD) {
            let rec = encode_chunk(chunk, &codec)?;
            w.append_record(&rec)?;
        }
    }
    w.finish()?;
    Ok(())
}

/// Fault one [`DEGREES_PER_RECORD`]-wide degree **chunk** (record) into its compact resident
/// [`DegreeChunk`] form, for chunk-lazy residency — a single fault is one raw `pread` + EF
/// parse that serves the next [`DEGREES_PER_RECORD`] ids, so a query touching only part of the
/// id space never materialises the whole column. `per_half` is [`records_per_half`] for the
/// generation's node count; `outgoing` picks the half (out records are `0..per_half`, in
/// records follow); `chunk` is the record index *within* that half (`0..per_half`). The chunk
/// covers [`DEGREES_PER_RECORD`] ids except the last of a half, which is
/// `node_count % DEGREES_PER_RECORD` (or full when an exact multiple), and yields the same
/// degrees [`read_node_degrees`] would place at that range.
pub fn read_degree_chunk(
    reader: &BlockFileReader,
    per_half: usize,
    outgoing: bool,
    chunk: usize,
) -> Result<DegreeChunk> {
    if chunk >= per_half {
        bail!("degree chunk {chunk} out of range (half has {per_half} records)");
    }
    let global = if outgoing { chunk } else { per_half + chunk };
    let bytes = reader.read_record_global(global as u64)?;
    decode_chunk(&bytes)
}

/// Read the dense out/in degree columns resident. `node_count` is the generation's node
/// count (the manifest's), used to size the halves. Returns `(out_degrees, in_degrees)`,
/// each `node_count` long.
pub fn read_node_degrees(
    reader: &BlockFileReader,
    node_count: usize,
) -> Result<(Vec<u32>, Vec<u32>)> {
    let per_half = records_per_half(node_count);
    let total = reader.total_records() as usize;
    if total != per_half * 2 {
        bail!(
            "node_degrees.blk has {total} records, expected {} for {node_count} nodes",
            per_half * 2
        );
    }
    let mut out = Vec::with_capacity(node_count);
    let mut inn = Vec::with_capacity(node_count);
    for r in 0..total {
        let bytes = reader.read_record_global(r as u64)?;
        let dst = if r < per_half { &mut out } else { &mut inn };
        dst.extend_from_slice(&decode_chunk(&bytes)?.to_degrees());
    }
    if out.len() != node_count || inn.len() != node_count {
        bail!(
            "node_degrees.blk decoded {} out / {} in degrees, expected {node_count}",
            out.len(),
            inn.len()
        );
    }
    Ok((out, inn))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_nodedeg_{}_{}", std::process::id(), name))
    }

    #[test]
    fn dense_degrees_roundtrip() {
        // Span more than one record to exercise the chunking.
        let n = DEGREES_PER_RECORD + 5;
        let out: Vec<u32> = (0..n as u32).map(|i| i % 7).collect();
        let inn: Vec<u32> = (0..n as u32).map(|i| (i % 3) + 1).collect();
        let path = tmp("nd.blk");
        write_node_degrees(&path, &out, &inn, 1 << 16, DegreeCodecOpts::default(), None).unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        let (got_out, got_in) = read_node_degrees(&r, n).unwrap();
        assert_eq!(got_out, out);
        assert_eq!(got_in, inn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn chunk_read_matches_eager_slice() {
        // Two-plus records per half so we exercise a full chunk, a partial last chunk, and
        // the out/in half split.
        let n = 2 * DEGREES_PER_RECORD + 37;
        let out: Vec<u32> = (0..n as u32).map(|i| i.wrapping_mul(2654435761)).collect();
        let inn: Vec<u32> = (0..n as u32).map(|i| i % 11).collect();
        let path = tmp("chunk.blk");
        write_node_degrees(&path, &out, &inn, 1 << 16, DegreeCodecOpts::default(), None).unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        let per_half = records_per_half(n);
        assert_eq!(per_half, 3);
        for chunk in 0..per_half {
            let lo = chunk * DEGREES_PER_RECORD;
            let hi = (lo + DEGREES_PER_RECORD).min(n);
            assert_eq!(
                read_degree_chunk(&r, per_half, true, chunk)
                    .unwrap()
                    .to_degrees(),
                out[lo..hi],
                "out chunk {chunk}"
            );
            assert_eq!(
                read_degree_chunk(&r, per_half, false, chunk)
                    .unwrap()
                    .to_degrees(),
                inn[lo..hi],
                "in chunk {chunk}"
            );
        }
        assert!(read_degree_chunk(&r, per_half, true, per_half).is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_graph_roundtrip() {
        let path = tmp("empty.blk");
        write_node_degrees(&path, &[], &[], 1 << 16, DegreeCodecOpts::default(), None).unwrap();
        let r = BlockFileReader::open(&path).unwrap();
        let (o, i) = read_node_degrees(&r, 0).unwrap();
        assert!(o.is_empty() && i.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
