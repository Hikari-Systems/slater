// SPDX-License-Identifier: Apache-2.0
//! One-off retrofit: compute a generation's dense per-node degree column
//! (`node_degrees.blk`) from its already-written `topology.csr.blk`, so a generation
//! built before the column existed can serve the degree-sum count fast path without a
//! full rebuild. Plaintext generations only (no at-rest key wiring here).
//!
//! Usage: `retrofit_degrees <generation_dir>` (the dir holding `MANIFEST.json` +
//! `topology.csr.blk`). Writes `node_degrees.blk` into that dir; the server loads it on
//! existence (it is not added to the manifest inventory, so the content hash is unchanged).

use anyhow::{Context, Result};
use graph_format::manifest::Manifest;
use graph_format::nodedegree::write_node_degrees;
use graph_format::topology::{adj_count, TopologyReader};

fn main() -> Result<()> {
    let dir = std::env::args()
        .nth(1)
        .context("usage: retrofit_degrees <generation_dir>")?;
    let dir = std::path::PathBuf::from(dir);

    let manifest = Manifest::read_from_dir(&dir).context("read MANIFEST.json")?;
    let n = manifest.node_count as usize;
    let topo =
        TopologyReader::open(dir.join("topology.csr.blk")).context("open topology.csr.blk")?;
    assert_eq!(
        topo.node_count() as usize,
        n,
        "manifest node_count != CSR node_count"
    );

    // The CSR holds 2N records: 0..N are outgoing adjacencies (leading count = out-degree),
    // N..2N are incoming (leading count = in-degree). One streamed pass, each block decoded
    // once; only the leading uvarint of each record is read, not the whole neighbour list.
    let mut out = vec![0u32; n];
    let mut inn = vec![0u32; n];
    topo.inner().for_each_record(|g, rec| {
        let deg = adj_count(rec)? as u32;
        let g = g as usize;
        if g < n {
            out[g] = deg;
        } else {
            inn[g - n] = deg;
        }
        Ok(())
    })?;

    let block = *manifest
        .block_sizes
        .get("topology.csr.blk")
        .unwrap_or(&(256 * 1024)) as usize;
    let path = dir.join("node_degrees.blk");
    write_node_degrees(&path, &out, &inn, block, manifest.zstd_level, None)
        .context("write node_degrees.blk")?;
    eprintln!(
        "retrofit_degrees: wrote {} ({} nodes) — out-degree Σ={}, in-degree Σ={}",
        path.display(),
        n,
        out.iter().map(|&d| d as u64).sum::<u64>(),
        inn.iter().map(|&d| d as u64).sum::<u64>(),
    );
    Ok(())
}
