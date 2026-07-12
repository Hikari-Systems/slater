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
//! Layout: the out-degrees as a run of fixed [`DEGREES_PER_RECORD`]-wide records (u32 LE),
//! then the in-degrees the same way — so record `< records_per_half` is out, the rest is
//! in. The reader loads both halves resident (`node_count × 4` bytes per direction). The
//! reader gates on the **file's existence**, so a generation retrofitted with the column
//! (or built with it) uses it, and one without falls back to reading the CSR record's
//! leading count — slower, identical answer.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;

/// Degrees per stored record (u32 each ⇒ 1 MiB records at 2^18). Bounds a record's size
/// while keeping the record count small (a few hundred on a 91.6M-node graph).
pub const DEGREES_PER_RECORD: usize = 1 << 18;

/// Records the out (or in) half occupies for `node_count` nodes.
pub fn records_per_half(node_count: usize) -> usize {
    node_count.div_ceil(DEGREES_PER_RECORD)
}

/// Write `node_degrees.blk`: the out-degrees, then the in-degrees, each a run of
/// [`DEGREES_PER_RECORD`]-wide u32-LE records. `out_degs` and `in_degs` must both have
/// length `node_count` (degree of dense id `i` at index `i`).
pub fn write_node_degrees(
    path: impl AsRef<Path>,
    out_degs: &[u32],
    in_degs: &[u32],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<()> {
    if out_degs.len() != in_degs.len() {
        bail!(
            "node-degree columns differ in length: out {} != in {}",
            out_degs.len(),
            in_degs.len()
        );
    }
    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    for degs in [out_degs, in_degs] {
        for chunk in degs.chunks(DEGREES_PER_RECORD) {
            let mut rec = Vec::with_capacity(chunk.len() * 4);
            for &d in chunk {
                rec.extend_from_slice(&d.to_le_bytes());
            }
            w.append_record(&rec)?;
        }
    }
    w.finish()?;
    Ok(())
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
        for c in bytes.chunks_exact(4) {
            dst.push(u32::from_le_bytes([c[0], c[1], c[2], c[3]]));
        }
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
        write_node_degrees(&path, &out, &inn, 1 << 16, 3, None).unwrap();

        let r = BlockFileReader::open(&path).unwrap();
        let (got_out, got_in) = read_node_degrees(&r, n).unwrap();
        assert_eq!(got_out, out);
        assert_eq!(got_in, inn);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_graph_roundtrip() {
        let path = tmp("empty.blk");
        write_node_degrees(&path, &[], &[], 1 << 16, 3, None).unwrap();
        let r = BlockFileReader::open(&path).unwrap();
        let (o, i) = read_node_degrees(&r, 0).unwrap();
        assert!(o.is_empty() && i.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
