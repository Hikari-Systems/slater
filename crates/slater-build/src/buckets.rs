// SPDX-License-Identifier: Apache-2.0
//! On-disk spill buckets for the external build.
//!
//! Pass 1 streams the dump once and writes every node into a **node bucket** and
//! every edge into an **edge bucket**, both purely on disk — nothing graph-scale
//! stays resident. Each record carries the *already-encoded* final store bytes
//! (the property / label record blob), which are permutation-invariant, so the
//! emit phase byte-copies them straight into `node_props.blk` / `node_labels.blk`
//! / `edge_props.blk` with no value re-encoding (only `topology.csr.blk`, whose
//! neighbour/edge ids are permuted, is rebuilt at emit).
//!
//! Buckets are transient scratch: a plaintext, zstd-compressed [`graph_format::blockfile`]
//! container (reused so we get streaming + compression for free), deleted once the
//! generation is published.

use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

use graph_format::blockfile::{BlockFileReader, BlockFileWriter};
use graph_format::wire::{read_uvarint, write_uvarint};

#[inline]
fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

pub(crate) fn write_blob(buf: &mut Vec<u8>, b: &[u8]) {
    write_uvarint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

pub(crate) fn read_blob<'a>(r: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = read_uvarint(r)? as usize;
    if r.len() < len {
        bail!("bucket blob truncated (want {len}, have {})", r.len());
    }
    let (b, rest) = r.split_at(len);
    *r = rest;
    Ok(b)
}

/// One node as spilled in pass 1. Holds the pre-encoded label/property record
/// bytes plus any routed vector properties (kept for the vector store).
pub struct NodeRec {
    /// The node's `__dump_id__`, used to resolve edge endpoints. `None` if the
    /// node carried none (then no edge can reference it).
    pub dump_id: Option<i64>,
    /// Pre-encoded `node_labels.blk` record (see [`graph_format::nodelabels::encode_labels_record`]).
    pub labels_blob: Vec<u8>,
    /// Pre-encoded `node_props.blk` record (see [`graph_format::columns::encode_props_record`]).
    pub props_blob: Vec<u8>,
    /// Routed vector properties `(key, vector)` for the vector store (usually empty).
    pub vec_props: Vec<(String, Vec<f32>)>,
}

impl NodeRec {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self.dump_id {
            Some(d) => {
                buf.push(1);
                write_uvarint(buf, zigzag(d));
            }
            None => buf.push(0),
        }
        write_blob(buf, &self.labels_blob);
        write_blob(buf, &self.props_blob);
        write_uvarint(buf, self.vec_props.len() as u64);
        for (k, xs) in &self.vec_props {
            write_blob(buf, k.as_bytes());
            write_uvarint(buf, xs.len() as u64);
            for x in xs {
                buf.extend_from_slice(&x.to_le_bytes());
            }
        }
    }

    pub fn decode(mut r: &[u8]) -> Result<Self> {
        let view = NodeRecView::parse(r)?;
        let dump_id = view.dump_id;
        let labels_blob = view.labels_blob.to_vec();
        let props_blob = view.props_blob.to_vec();
        // Re-parse the vector tail into owned form.
        r = view.vec_tail;
        let n = read_uvarint(&mut r)? as usize;
        let mut vec_props = Vec::with_capacity(n);
        for _ in 0..n {
            let k = std::str::from_utf8(read_blob(&mut r)?)?.to_string();
            let dim = read_uvarint(&mut r)? as usize;
            let mut xs = Vec::with_capacity(dim);
            for _ in 0..dim {
                if r.len() < 4 {
                    bail!("vector f32 truncated");
                }
                xs.push(f32::from_le_bytes([r[0], r[1], r[2], r[3]]));
                r = &r[4..];
            }
            vec_props.push((k, xs));
        }
        Ok(NodeRec {
            dump_id,
            labels_blob,
            props_blob,
            vec_props,
        })
    }
}

/// Zero-copy view over an encoded [`NodeRec`]: slices into the source buffer so the
/// emit fast-path (`--cluster=none`, streaming in prov order) can byte-copy the
/// label/property blobs straight into the stores without an owning decode.
pub struct NodeRecView<'a> {
    pub dump_id: Option<i64>,
    pub labels_blob: &'a [u8],
    pub props_blob: &'a [u8],
    /// The remaining bytes (the encoded vector-properties tail).
    pub vec_tail: &'a [u8],
}

impl<'a> NodeRecView<'a> {
    pub fn parse(rec: &'a [u8]) -> Result<Self> {
        let mut r = rec;
        let dump_id = match r.split_first() {
            Some((1, rest)) => {
                r = rest;
                Some(unzigzag(read_uvarint(&mut r)?))
            }
            Some((0, rest)) => {
                r = rest;
                None
            }
            _ => bail!("node bucket record truncated (no dump flag)"),
        };
        let labels_blob = read_blob(&mut r)?;
        let props_blob = read_blob(&mut r)?;
        Ok(NodeRecView {
            dump_id,
            labels_blob,
            props_blob,
            vec_tail: r,
        })
    }

    /// Cheap dump-id probe for the resolver scan: reads only the flag + id.
    pub fn peek_dump_id(rec: &[u8]) -> Result<Option<i64>> {
        let mut r = rec;
        match r.split_first() {
            Some((1, rest)) => {
                r = rest;
                Ok(Some(unzigzag(read_uvarint(&mut r)?)))
            }
            Some((0, _)) => Ok(None),
            _ => bail!("node bucket record truncated (no dump flag)"),
        }
    }
}

/// One edge as spilled in pass 1, in provisional-id form (endpoints are provisional
/// node ids; the props blob is the pre-encoded `edge_props.blk` record).
pub struct EdgeRec {
    pub prov_edge_id: u64,
    pub src_prov: u64,
    pub dst_prov: u64,
    pub reltype: u32,
    pub props_blob: Vec<u8>,
}

impl EdgeRec {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, self.prov_edge_id);
        write_uvarint(buf, self.src_prov);
        write_uvarint(buf, self.dst_prov);
        write_uvarint(buf, self.reltype as u64);
        write_blob(buf, &self.props_blob);
    }

    pub fn decode(mut r: &[u8]) -> Result<Self> {
        let prov_edge_id = read_uvarint(&mut r)?;
        let src_prov = read_uvarint(&mut r)?;
        let dst_prov = read_uvarint(&mut r)?;
        let reltype = read_uvarint(&mut r)? as u32;
        let props_blob = read_blob(&mut r)?.to_vec();
        Ok(EdgeRec {
            prov_edge_id,
            src_prov,
            dst_prov,
            reltype,
            props_blob,
        })
    }
}

/// An edge as first spilled in pass 1, before the dump→provisional resolver
/// exists: endpoints are still raw `__dump_id__`s. A second pass resolves these
/// into [`EdgeRec`]s. Spilling unresolved lifts the "all nodes before any edge"
/// ordering requirement — endpoints are resolved once every node has been seen.
pub struct UnresolvedEdge {
    pub src_dump: i64,
    pub dst_dump: i64,
    pub reltype: u32,
    pub props_blob: Vec<u8>,
}

impl UnresolvedEdge {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        write_uvarint(buf, zigzag(self.src_dump));
        write_uvarint(buf, zigzag(self.dst_dump));
        write_uvarint(buf, self.reltype as u64);
        write_blob(buf, &self.props_blob);
    }

    pub fn decode(mut r: &[u8]) -> Result<Self> {
        let src_dump = unzigzag(read_uvarint(&mut r)?);
        let dst_dump = unzigzag(read_uvarint(&mut r)?);
        let reltype = read_uvarint(&mut r)? as u32;
        let props_blob = read_blob(&mut r)?.to_vec();
        Ok(UnresolvedEdge {
            src_dump,
            dst_dump,
            reltype,
            props_blob,
        })
    }
}

/// Append-only writer over a transient bucket file.
pub struct BucketWriter {
    inner: BlockFileWriter,
    scratch: Vec<u8>,
}

impl BucketWriter {
    pub fn create(
        path: impl AsRef<Path>,
        target_block_bytes: usize,
        zstd_level: i32,
    ) -> Result<Self> {
        Ok(Self {
            inner: BlockFileWriter::create(path, target_block_bytes, zstd_level)?,
            scratch: Vec::new(),
        })
    }

    pub fn append_node(&mut self, rec: &NodeRec) -> Result<()> {
        self.scratch.clear();
        rec.encode(&mut self.scratch);
        self.inner.append_record(&self.scratch)?;
        Ok(())
    }

    pub fn append_edge(&mut self, rec: &EdgeRec) -> Result<()> {
        self.scratch.clear();
        rec.encode(&mut self.scratch);
        self.inner.append_record(&self.scratch)?;
        Ok(())
    }

    pub fn append_unresolved_edge(&mut self, rec: &UnresolvedEdge) -> Result<()> {
        self.scratch.clear();
        rec.encode(&mut self.scratch);
        self.inner.append_record(&self.scratch)?;
        Ok(())
    }

    pub fn finish(self) -> Result<u64> {
        self.inner.finish()
    }
}

/// The segment file for segment `n` of a bucket (`<base>.<n>`).
pub fn seg_path(base: &Path, n: u64) -> PathBuf {
    let mut s = base.as_os_str().to_os_string();
    s.push(format!(".{n}"));
    PathBuf::from(s)
}

/// All segments of a bucket, in segment order. A bucket is either segmented
/// (`<base>.0`, `<base>.1`, … — the pass-1 resumable buckets) or a single file at
/// `base` (e.g. the resolved edge bucket); this returns whichever exists.
pub fn segments(base: &Path) -> Vec<PathBuf> {
    let parent = base.parent().unwrap_or_else(|| Path::new("."));
    let prefix = match base.file_name().and_then(|n| n.to_str()) {
        Some(n) => format!("{n}."),
        None => return Vec::new(),
    };
    let mut segs: Vec<(u64, PathBuf)> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(parent) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(suf) = name.strip_prefix(&prefix) {
                if let Ok(n) = suf.parse::<u64>() {
                    segs.push((n, e.path()));
                }
            }
        }
    }
    if segs.is_empty() {
        return if base.exists() {
            vec![base.to_path_buf()]
        } else {
            Vec::new()
        };
    }
    segs.sort_by_key(|(n, _)| *n);
    segs.into_iter().map(|(_, p)| p).collect()
}

/// Scan a node bucket (all segments) in append (provisional-id) order.
pub fn for_each_node(
    base: impl AsRef<Path>,
    mut f: impl FnMut(u64, NodeRec) -> Result<()>,
) -> Result<()> {
    let mut prov = 0u64;
    for seg in segments(base.as_ref()) {
        let r = BlockFileReader::open(&seg)?;
        r.for_each_record(|_, rec| {
            let n = NodeRec::decode(rec)?;
            f(prov, n)?;
            prov += 1;
            Ok(())
        })?;
    }
    Ok(())
}

/// Scan only the `__dump_id__` of each node, in provisional-id order — the cheap
/// pass that builds the dump→provisional resolver without touching the blobs.
pub fn for_each_node_dump_id(
    base: impl AsRef<Path>,
    mut f: impl FnMut(u64, Option<i64>) -> Result<()>,
) -> Result<()> {
    let mut prov = 0u64;
    for seg in segments(base.as_ref()) {
        let r = BlockFileReader::open(&seg)?;
        r.for_each_record(|_, rec| {
            let d = NodeRecView::peek_dump_id(rec)?;
            f(prov, d)?;
            prov += 1;
            Ok(())
        })?;
    }
    Ok(())
}

/// Scan an edge bucket (all segments) in append order, decoding each record.
pub fn for_each_edge(
    base: impl AsRef<Path>,
    mut f: impl FnMut(EdgeRec) -> Result<()>,
) -> Result<()> {
    for seg in segments(base.as_ref()) {
        let r = BlockFileReader::open(&seg)?;
        r.for_each_record(|_, rec| {
            let e = EdgeRec::decode(rec)?;
            f(e)
        })?;
    }
    Ok(())
}

/// Scan an unresolved-edge bucket (all segments) in provisional-edge-id order.
pub fn for_each_unresolved_edge(
    base: impl AsRef<Path>,
    mut f: impl FnMut(u64, UnresolvedEdge) -> Result<()>,
) -> Result<()> {
    let mut prov_edge = 0u64;
    for seg in segments(base.as_ref()) {
        let r = BlockFileReader::open(&seg)?;
        r.for_each_record(|_, rec| {
            let e = UnresolvedEdge::decode(rec)?;
            f(prov_edge, e)?;
            prov_edge += 1;
            Ok(())
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_format::columns::encode_props_record;
    use graph_format::ids::Value;
    use graph_format::nodelabels::encode_labels_record;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_buckets_{}_{}", std::process::id(), name))
    }

    #[test]
    fn node_bucket_roundtrips_blobs_and_dump_ids() {
        let path = tmp("nodes");
        let mut w = BucketWriter::create(&path, 4096, 3).unwrap();
        let mut expected = Vec::new();
        for i in 0..1000u64 {
            let labels = encode_labels_record(&[(i % 3) as u32, 7]);
            let props = encode_props_record(&[
                (0, Value::Int(i as i64)),
                (1, Value::Str(format!("name-{i}"))),
            ]);
            let dump = if i % 5 == 0 {
                None
            } else {
                Some((i as i64) * 7 - 3)
            };
            let vecs = if i % 100 == 0 {
                vec![("emb".to_string(), vec![0.5f32, -0.25, (i as f32) * 0.1])]
            } else {
                vec![]
            };
            w.append_node(&NodeRec {
                dump_id: dump,
                labels_blob: labels.clone(),
                props_blob: props.clone(),
                vec_props: vecs.clone(),
            })
            .unwrap();
            expected.push((dump, labels, props, vecs));
        }
        w.finish().unwrap();

        // Full decode round-trip.
        let mut got = Vec::new();
        for_each_node(&path, |prov, n| {
            assert_eq!(prov as usize, got.len());
            got.push((n.dump_id, n.labels_blob, n.props_blob, n.vec_props));
            Ok(())
        })
        .unwrap();
        assert_eq!(got.len(), expected.len());
        for (g, e) in got.iter().zip(&expected) {
            assert_eq!(g.0, e.0);
            assert_eq!(&g.1, &e.1);
            assert_eq!(&g.2, &e.2);
            assert_eq!(g.3.len(), e.3.len());
        }

        // Cheap dump-id-only scan agrees with the full decode.
        let mut dumps = Vec::new();
        for_each_node_dump_id(&path, |_, d| {
            dumps.push(d);
            Ok(())
        })
        .unwrap();
        assert_eq!(dumps, expected.iter().map(|e| e.0).collect::<Vec<_>>());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn edge_bucket_roundtrips() {
        let path = tmp("edges");
        let mut w = BucketWriter::create(&path, 4096, 3).unwrap();
        let mut expected = Vec::new();
        for i in 0..2000u64 {
            let props = if i % 4 == 0 {
                encode_props_record(&[(2, Value::Float(i as f64 / 3.0))])
            } else {
                encode_props_record(&[])
            };
            let e = EdgeRec {
                prov_edge_id: i,
                src_prov: i % 50,
                dst_prov: (i * 3) % 50,
                reltype: (i % 4) as u32,
                props_blob: props,
            };
            w.append_edge(&e).unwrap();
            expected.push((
                e.prov_edge_id,
                e.src_prov,
                e.dst_prov,
                e.reltype,
                e.props_blob,
            ));
        }
        w.finish().unwrap();

        let mut got = Vec::new();
        for_each_edge(&path, |e| {
            got.push((
                e.prov_edge_id,
                e.src_prov,
                e.dst_prov,
                e.reltype,
                e.props_blob,
            ));
            Ok(())
        })
        .unwrap();
        assert_eq!(got, expected);
        let _ = std::fs::remove_file(&path);
    }
}
