//! CSR adjacency — forward and reverse — so the reader can traverse either
//! direction without a scan.
//!
//! Both directions live in one `topology.csr.blk`: records `0..N` are each node's
//! **outgoing** adjacency in node-id order, records `N..2N` are each node's
//! **incoming** adjacency. A node's adjacency record is
//! `uvarint(count) ‖ count × ( uvarint(reltype_id) ‖ uvarint(neighbour) ‖ uvarint(edge_id) )`.
//! Isolated nodes get an empty (count 0) record so the global index stays aligned
//! with the dense node id.

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};

use crate::blockfile::{BlockFileReader, BlockFileWriter};
use crate::crypto::BlockCipher;
use crate::ids::{EdgeId, NodeId};
use crate::wire::{read_uvarint, write_uvarint};

/// One adjacency entry: a typed edge to a neighbouring node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Adj {
    pub reltype: u32,
    pub neighbour: NodeId,
    pub edge: EdgeId,
}

/// An edge in builder input form.
#[derive(Debug, Clone, Copy)]
pub struct Edge {
    pub src: NodeId,
    pub dst: NodeId,
    pub reltype: u32,
    pub edge: EdgeId,
}

fn encode_adj(list: &[Adj]) -> Vec<u8> {
    let mut rec = Vec::new();
    write_uvarint(&mut rec, list.len() as u64);
    for a in list {
        write_uvarint(&mut rec, a.reltype as u64);
        write_uvarint(&mut rec, a.neighbour.0);
        write_uvarint(&mut rec, a.edge.0);
    }
    rec
}

/// Decode one node's adjacency record
/// (`uvarint(count) ‖ count × (uvarint(reltype) ‖ uvarint(neighbour) ‖ uvarint(edge))`).
/// Public so a cached-block reader can decode a record it already holds.
pub fn decode_adj(rec: &[u8]) -> Result<Vec<Adj>> {
    let mut r = rec;
    let count = read_uvarint(&mut r)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let reltype = read_uvarint(&mut r)? as u32;
        let neighbour = NodeId(read_uvarint(&mut r)?);
        let edge = EdgeId(read_uvarint(&mut r)?);
        out.push(Adj {
            reltype,
            neighbour,
            edge,
        });
    }
    Ok(out)
}

/// Build `topology.csr.blk` from an edge list. Offline only — adjacency is
/// materialised in memory (fine for the builder).
pub fn write_csr(
    path: impl AsRef<Path>,
    node_count: u64,
    edges: &[Edge],
    target_block_bytes: usize,
    zstd_level: i32,
) -> Result<u64> {
    write_csr_with_cipher(
        path,
        node_count,
        edges,
        target_block_bytes,
        zstd_level,
        None,
    )
}

/// Write the CSR adjacency, optionally AEAD-encrypted (`cipher = None` ⇒
/// plaintext, identical to [`write_csr`]).
pub fn write_csr_with_cipher(
    path: impl AsRef<Path>,
    node_count: u64,
    edges: &[Edge],
    target_block_bytes: usize,
    zstd_level: i32,
    cipher: Option<Arc<BlockCipher>>,
) -> Result<u64> {
    let n = node_count as usize;
    let mut fwd: Vec<Vec<Adj>> = vec![Vec::new(); n];
    let mut rev: Vec<Vec<Adj>> = vec![Vec::new(); n];
    for e in edges {
        if e.src.index() >= n || e.dst.index() >= n {
            bail!("edge endpoint out of range (node_count {node_count})");
        }
        fwd[e.src.index()].push(Adj {
            reltype: e.reltype,
            neighbour: e.dst,
            edge: e.edge,
        });
        rev[e.dst.index()].push(Adj {
            reltype: e.reltype,
            neighbour: e.src,
            edge: e.edge,
        });
    }

    let mut w = BlockFileWriter::create_with_cipher(path, target_block_bytes, zstd_level, cipher)?;
    for list in &fwd {
        w.append_record(&encode_adj(list))?;
    }
    for list in &rev {
        w.append_record(&encode_adj(list))?;
    }
    w.finish()
}

/// Reader over the CSR adjacency.
pub struct TopologyReader {
    inner: BlockFileReader,
    node_count: u64,
}

impl TopologyReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open the CSR, supplying the per-generation cipher for an encrypted file.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Arc<BlockCipher>>,
    ) -> Result<Self> {
        let inner = BlockFileReader::open_with_cipher(path, cipher)?;
        let total = inner.total_records();
        if total % 2 != 0 {
            bail!("topology record count {total} is not even (forward+reverse)");
        }
        Ok(Self {
            inner,
            node_count: total / 2,
        })
    }

    pub fn node_count(&self) -> u64 {
        self.node_count
    }

    /// Outgoing adjacency of `node`.
    pub fn outgoing(&self, node: NodeId) -> Result<Vec<Adj>> {
        self.adj(node.0)
    }

    /// Incoming adjacency of `node`.
    pub fn incoming(&self, node: NodeId) -> Result<Vec<Adj>> {
        self.adj(self.node_count + node.0)
    }

    fn adj(&self, global: u64) -> Result<Vec<Adj>> {
        let rec = self.inner.read_record_global(global)?;
        decode_adj(&rec)
    }

    /// Global record index of a node's outgoing adjacency (= the node id).
    pub fn outgoing_global(&self, node: NodeId) -> u64 {
        node.0
    }

    /// Global record index of a node's incoming adjacency (`node_count + id`).
    pub fn incoming_global(&self, node: NodeId) -> u64 {
        self.node_count + node.0
    }

    /// The underlying block file, so a caller holding a block cache can read this
    /// store's adjacency records through it and decode them with [`decode_adj`].
    pub fn inner(&self) -> &BlockFileReader {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_topo_{}_{}", std::process::id(), name))
    }

    #[test]
    fn forward_and_reverse_are_consistent() {
        let path = tmp("csr");
        // 4 nodes; a small directed graph with a couple of relationship types.
        let edges = vec![
            Edge {
                src: NodeId(0),
                dst: NodeId(1),
                reltype: 0,
                edge: EdgeId(0),
            },
            Edge {
                src: NodeId(0),
                dst: NodeId(2),
                reltype: 1,
                edge: EdgeId(1),
            },
            Edge {
                src: NodeId(1),
                dst: NodeId(2),
                reltype: 0,
                edge: EdgeId(2),
            },
            Edge {
                src: NodeId(3),
                dst: NodeId(0),
                reltype: 1,
                edge: EdgeId(3),
            },
        ];
        write_csr(&path, 4, &edges, 256, 3).unwrap();

        let r = TopologyReader::open(&path).unwrap();
        assert_eq!(r.node_count(), 4);

        // Outgoing of 0 → {1 via rt0/e0, 2 via rt1/e1}
        let out0 = r.outgoing(NodeId(0)).unwrap();
        assert_eq!(out0.len(), 2);
        assert!(out0.contains(&Adj {
            reltype: 0,
            neighbour: NodeId(1),
            edge: EdgeId(0)
        }));
        assert!(out0.contains(&Adj {
            reltype: 1,
            neighbour: NodeId(2),
            edge: EdgeId(1)
        }));

        // Node 2 has no outgoing, two incoming (from 0 and 1).
        assert!(r.outgoing(NodeId(2)).unwrap().is_empty());
        let in2 = r.incoming(NodeId(2)).unwrap();
        assert_eq!(in2.len(), 2);
        assert!(in2.contains(&Adj {
            reltype: 1,
            neighbour: NodeId(0),
            edge: EdgeId(1)
        }));
        assert!(in2.contains(&Adj {
            reltype: 0,
            neighbour: NodeId(1),
            edge: EdgeId(2)
        }));

        // Reverse/forward equivalence: every forward edge appears in some reverse list.
        for src in 0..4u64 {
            for a in r.outgoing(NodeId(src)).unwrap() {
                let back = r.incoming(a.neighbour).unwrap();
                assert!(
                    back.iter()
                        .any(|b| b.neighbour == NodeId(src) && b.edge == a.edge),
                    "forward edge {src}->{} (e{}) missing from reverse",
                    a.neighbour.0,
                    a.edge.0
                );
            }
        }
        let _ = std::fs::remove_file(&path);
    }
}
