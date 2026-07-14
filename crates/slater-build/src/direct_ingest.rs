// SPDX-License-Identifier: Apache-2.0
//! Direct ingestion of a binary consolidation dump
//! ([`graph_format::consolidate_dump`]) — the `--input-format=slater-dump` fast path.
//!
//! A consolidation dump already carries dense ids and global symbol ids, so this
//! bypasses the whole front of the normal build: no parse (pass 1), no node dedup,
//! no endpoint resolution. It writes the two transient buckets the *post-resolve*
//! pipeline consumes — `node_bkt` in [`NodeRec`] form (dump_id `None`, blobs copied
//! verbatim) and `edge_bkt` in [`EdgeRec`] form (endpoints already provisional =
//! compacted dump ids) — plus the interners and range-index DDL, then the build
//! continues at clustering exactly as if resolve had just finished.
//!
//! The provisional node id is the dump's node-record position (its compacted dense
//! id); the provisional edge id is the dump's edge-record position. Both preserve
//! the emit/cluster invariant that a bucket's append order is its provisional-id
//! order.

use std::path::Path;

use anyhow::{bail, Context, Result};

use graph_format::consolidate_dump::DumpReader;
use graph_format::manifest::{EntityKind, Metric};

use crate::buckets::{Blob, BucketWriter, EdgeRec, NodeRec};
use crate::diag::BuildDiag;
use crate::model::{Entity, RangeIndexStmt, VectorIndexStmt};

/// A dump edge names something the dump itself does not contain.
///
/// The ingest is a **trust boundary**: a consolidation dump is an opaque binary file named
/// on the command line, and `--input-format=slater-dump` hands its bytes straight to the
/// post-resolve pipeline — no parse, no dedup, no endpoint resolution, so none of the
/// front-half's checks are in the way. An endpoint or reltype id is therefore a *claim*
/// made by that file, to be checked against what the file declares it holds, not a value to
/// index with.
///
/// Typed (and `pub`) so a caller classifies it with `err.downcast_ref::<DumpEdgeInvalid>()`
/// rather than matching message text: this is a corrupt-or-hostile input, not an I/O fault,
/// and the two want different operator responses.
#[derive(Debug, thiserror::Error)]
pub enum DumpEdgeInvalid {
    /// `src` or `dst` is not a node the dump contains.
    #[error(
        "dump edge {edge_id} has {end} endpoint {node}, but the dump declares only \
         {node_count} node(s) (valid ids are 0..{node_count})"
    )]
    Endpoint {
        edge_id: u64,
        /// `"src"` or `"dst"` — which end is out of range.
        end: &'static str,
        node: u64,
        node_count: u64,
    },
    /// `reltype` is not a relationship type the dump names.
    #[error(
        "dump edge {edge_id} has reltype id {reltype}, but the dump declares only \
         {reltype_count} reltype(s) (valid ids are 0..{reltype_count})"
    )]
    Reltype {
        edge_id: u64,
        reltype: u32,
        reltype_count: usize,
    },
}

/// The post-resolve inputs a dump ingest produces — the same values the Cypher
/// front-half hands the clustering phase.
pub struct IngestResult {
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub reltypes: Vec<String>,
    pub keys: Vec<String>,
    pub range_stmts: Vec<RangeIndexStmt>,
    pub vector_stmts: Vec<VectorIndexStmt>,
}

/// The canonical token for a metric, as `shared::parse_metric` reads it back. Kept
/// explicit (rather than a `Debug`/`Display` derive) so a rename of the enum cannot
/// silently produce a token the builder no longer parses.
fn metric_token(m: Metric) -> &'static str {
    match m {
        Metric::Cosine => "cosine",
        Metric::L2 => "l2",
        Metric::Dot => "dot",
    }
}

/// Ingest the dump at `dump_dir`, writing `node_bkt` / `edge_bkt` as single-segment
/// buckets in the shape clustering + emit expect. `block_bytes` / `zstd_level` match
/// the build's other transient buckets.
pub fn ingest_dump(
    dump_dir: &Path,
    node_bkt: &Path,
    edge_bkt: &Path,
    block_bytes: usize,
    zstd_level: i32,
    diag: &BuildDiag,
) -> Result<IngestResult> {
    let r = DumpReader::open(dump_dir)
        .with_context(|| format!("open consolidation dump {}", dump_dir.display()))?;
    let (node_count, edge_count) = (r.meta().node_count, r.meta().edge_count);

    // Embeddings, ascending by `(node_id, key_id)`. An *indexed* embedding is routed
    // out of the column store (D12), so it is absent from the props blob below and has
    // to be re-attached from its own stream — without this the rebuild silently drops
    // every vector and every vector index.
    //
    // Held resident, which bounds a consolidation by the size of its vector set. That is
    // the same ceiling the rest of the vector path already has (the dump side gathers
    // them to sort, and `build_vamana_index` materialises a whole index group to build
    // its graph), so it adds no new limit — but it is a real one.
    let keys = r.meta().property_keys.clone();
    let mut vectors: Vec<(u64, u32, Vec<f32>)> = Vec::new();
    r.for_each_vector(|node_id, key_id, vector| {
        vectors.push((node_id, key_id, vector));
        Ok(())
    })?;

    // Nodes → node bucket. The dump's `(labels, props)` blobs are already the
    // canonical `encode_labels_record` / `encode_props_record` bytes in global symbol
    // ids, so each is byte-copied into a `NodeRec` with no dump id; its embeddings are
    // merge-joined off `vectors` (both streams ascend by node id, so one cursor does it).
    diag.set_op("ingest dump nodes", "nodes", node_count);
    let mut nw = BucketWriter::create(node_bkt, block_bytes, zstd_level)
        .with_context(|| format!("create node bucket {}", node_bkt.display()))?;
    let mut vcur = 0usize;
    r.for_each_node(|id, labels_blob, props_blob| {
        let mut vec_props: Vec<(String, Vec<f32>)> = Vec::new();
        while let Some((nid, key_id, _)) = vectors.get(vcur) {
            if *nid != id {
                break;
            }
            let name = keys.get(*key_id as usize).cloned().with_context(|| {
                format!("dump vector for node {id} names property key {key_id}, out of range")
            })?;
            let (_, _, v) = std::mem::take(&mut vectors[vcur]);
            vec_props.push((name, v));
            vcur += 1;
        }
        nw.append_node(&NodeRec {
            dump_id: None,
            labels_blob: Blob::from_slice(labels_blob),
            props_blob: Blob::from_slice(props_blob),
            vec_props,
        })?;
        diag.progress_add(1);
        Ok(())
    })?;
    nw.finish().context("finish node bucket")?;
    // Every embedding must have found its node. A leftover means the dump's vector
    // stream names a node the node stream does not contain (or is misordered) — a
    // corrupt dump, and exactly the kind of thing that would otherwise show up much
    // later as an index that is quietly missing rows.
    if vcur != vectors.len() {
        let (orphan, _, _) = vectors[vcur];
        bail!(
            "dump vectors.blk has {} embedding(s) that no node claimed (first names node {orphan}); \
             the stream must be ascending by node id and every node id must exist",
            vectors.len() - vcur
        );
    }

    // Edges → edge bucket. Endpoints are already compacted node ids (= provisional
    // ids); the provisional edge id is the dump record position.
    //
    // They are also the *only* untrusted ids in the build that nothing else resolves: the
    // Cypher front-half earns its endpoints by looking a business key up in a node table,
    // so an edge to a node that does not exist cannot survive resolve. A dump's endpoints
    // skip all of that and are indexed with directly — by the LDG partition table, by the
    // emit band router, by `EndpointPlanes`'s plane arithmetic. Check them here, at the
    // boundary where the dump's bytes become build records, so every downstream consumer
    // (cluster, emit, and a `--resume` that picks the buckets back up) gets ids that are
    // in range by construction. Downstream is *not* a safe place to catch this: an
    // out-of-range endpoint reaches `Permutation::Table::final_of` as a raw `Vec` index
    // (index-out-of-bounds panic, in a scoped worker thread, naming no edge), and
    // `EndpointPlanes::set`'s `reltype * words_per_plane + (node >> 6)` can land
    // *in bounds inside a neighbouring reltype's plane* — a silently wrong posting bit,
    // caught today only because an unrelated emit invariant aborts the build first.
    //
    // Cost: three compares against loop-invariant locals, per edge, always-not-taken on a
    // well-formed dump. Nothing is allocated and nothing is touched that the append below
    // does not already touch — next to the varint decode, the blob copy and the zstd
    // re-compress each edge pays in this same loop, it does not show up.
    let reltype_count = r.meta().reltypes.len();
    diag.set_op("ingest dump edges", "edges", edge_count);
    let mut ew = BucketWriter::create(edge_bkt, block_bytes, zstd_level)
        .with_context(|| format!("create edge bucket {}", edge_bkt.display()))?;
    r.for_each_edge(|id, src, dst, reltype, props_blob| {
        for (end, node) in [("src", src), ("dst", dst)] {
            if node >= node_count {
                return Err(DumpEdgeInvalid::Endpoint {
                    edge_id: id,
                    end,
                    node,
                    node_count,
                }
                .into());
            }
        }
        if reltype as usize >= reltype_count {
            return Err(DumpEdgeInvalid::Reltype {
                edge_id: id,
                reltype,
                reltype_count,
            }
            .into());
        }
        ew.append_edge(&EdgeRec {
            prov_edge_id: id,
            src_prov: src,
            dst_prov: dst,
            reltype,
            props_blob: Blob::from_slice(props_blob),
        })?;
        diag.progress_add(1);
        Ok(())
    })
    .with_context(|| format!("ingest consolidation dump {}", dump_dir.display()))?;
    ew.finish().context("finish edge bucket")?;

    let range_stmts = r
        .meta()
        .range_indexes
        .iter()
        .map(|ri| RangeIndexStmt {
            entity: match ri.entity {
                EntityKind::Node => Entity::Node,
                EntityKind::Edge => Entity::Edge,
            },
            label_or_type: ri.label_or_type.clone(),
            property: ri.property.clone(),
        })
        .collect();

    // The vector-index declarations. The builder re-derives the ANN parameters from its
    // own `BuildOptions` (they are build options, not per-index state), so it re-routes
    // by cardinality exactly as a fresh build would: an index that has shrunk below
    // `ann_threshold` since the last build comes back as brute-force, and one that has
    // grown past it comes back as Vamana.
    let vector_stmts = r
        .meta()
        .vector_indexes
        .iter()
        .map(|vi| VectorIndexStmt {
            label: vi.label.clone(),
            property: vi.property.clone(),
            dim: vi.dim,
            metric: metric_token(vi.metric).to_string(),
        })
        .collect();

    Ok(IngestResult {
        node_count,
        edge_count,
        labels: r.meta().labels.clone(),
        reltypes: r.meta().reltypes.clone(),
        keys: r.meta().property_keys.clone(),
        range_stmts,
        vector_stmts,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use graph_format::consolidate_dump::DumpWriter;
    use graph_format::ids::Value;

    use super::*;

    /// A dump with `NODES` nodes and `RELTYPES` reltypes, plus a well-formed edge and one
    /// caller-supplied edge — which the tests below make out of range in each of the three
    /// ways a hostile dump can. `DumpWriter` validates neither, so it can write them.
    const NODES: u64 = 3;
    const RELTYPES: usize = 2;

    fn scratch(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let d = std::env::temp_dir().join(format!(
            "slater_ingest_{}_{tag}_{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Write a dump whose last edge is `(src, dst, reltype)`, then ingest it.
    fn ingest_with_edge(tag: &str, src: u64, dst: u64, reltype: u32) -> Result<IngestResult> {
        let work = scratch(tag);
        let dump = work.join("dump");
        let mut w = DumpWriter::create(&dump).unwrap();
        for i in 0..NODES {
            w.append_node(&[0], &[(0, Value::Int(i as i64))]).unwrap();
        }
        w.append_edge(0, 1, 0, &[]).unwrap(); // well-formed
        w.append_edge(src, dst, reltype, &[]).unwrap(); // under test
        w.finish(
            vec!["N".into()],
            (0..RELTYPES).map(|i| format!("R{i}")).collect(),
            vec!["k".into()],
            vec![],
            vec![],
        )
        .unwrap();

        ingest_dump(
            &dump,
            &work.join("node_bkt"),
            &work.join("edge_bkt"),
            64 * 1024,
            1,
            &BuildDiag::disabled(),
        )
    }

    /// Ingest a dump carrying this edge and require that it is *refused*. The panic
    /// message here is the pre-fix failure: without the boundary check `ingest_dump`
    /// returns `Ok` and writes the out-of-range id into the edge bucket.
    fn expect_rejected(tag: &str, src: u64, dst: u64, reltype: u32) -> anyhow::Error {
        match ingest_with_edge(tag, src, dst, reltype) {
            Err(e) => e,
            Ok(_) => panic!(
                "hostile dump (src={src}, dst={dst}, reltype={reltype}; {NODES} nodes, \
                 {RELTYPES} reltypes) was ingested instead of rejected"
            ),
        }
    }

    fn endpoint_err(e: &anyhow::Error) -> (&'static str, u64) {
        match e.downcast_ref::<DumpEdgeInvalid>() {
            Some(DumpEdgeInvalid::Endpoint { end, node, .. }) => (end, *node),
            other => panic!("expected DumpEdgeInvalid::Endpoint, got {other:?} ({e:#})"),
        }
    }

    /// The control: a dump whose ids are all in range still ingests. Guards against a
    /// bounds check that is simply wrong (e.g. an off-by-one rejecting the last node).
    #[test]
    fn well_formed_dump_ingests() {
        // Both endpoints at the extremes of the valid range, and the last valid reltype.
        let ing = ingest_with_edge("ok", NODES - 1, 0, RELTYPES as u32 - 1).unwrap();
        assert_eq!(ing.node_count, NODES);
        assert_eq!(ing.edge_count, 2);
        assert_eq!(ing.reltypes.len(), RELTYPES);
    }

    /// A dump edge whose `src` names a node the dump does not contain is refused at the
    /// ingest boundary. Before this check, `ingest_dump` returned `Ok` and wrote the
    /// out-of-range id into the edge bucket, where the *default* `--cluster=ldg` build
    /// then indexed the LDG partition table with it and panicked (`index out of bounds`)
    /// in a scoped worker thread.
    #[test]
    fn out_of_range_src_is_rejected() {
        let err = expect_rejected("src", 999, 1, 0);
        assert_eq!(endpoint_err(&err), ("src", 999));
    }

    /// As above for `dst` — which reached `EndpointPlanes::set`'s plane arithmetic, where
    /// it either panicked on the raw `Vec` index or, worse, landed in bounds inside a
    /// neighbouring reltype's plane and set a posting bit for the wrong reltype.
    #[test]
    fn out_of_range_dst_is_rejected() {
        let err = expect_rejected("dst", 0, 999, 0);
        assert_eq!(endpoint_err(&err), ("dst", 999));
    }

    /// `node_count` itself is out of range — the off-by-one a `>` instead of `>=` lets in.
    #[test]
    fn endpoint_equal_to_node_count_is_rejected() {
        let err = expect_rejected("eq", 0, NODES, 0);
        assert_eq!(endpoint_err(&err), ("dst", NODES));
    }

    /// A reltype id the dump's own reltype table does not name. Endpoints are valid here,
    /// so nothing downstream of the ingest is looking: on the plane path this panicked,
    /// and it is the id that would be written into the CSR and the postings.
    #[test]
    fn out_of_range_reltype_is_rejected() {
        let err = expect_rejected("rt", 0, 1, 7);
        match err.downcast_ref::<DumpEdgeInvalid>() {
            Some(DumpEdgeInvalid::Reltype {
                reltype,
                reltype_count,
                ..
            }) => {
                assert_eq!((*reltype, *reltype_count), (7, RELTYPES));
            }
            other => panic!("expected DumpEdgeInvalid::Reltype, got {other:?} ({err:#})"),
        }
    }

    /// The error survives the `with_context` wrap on the ingest call — i.e. a caller can
    /// still classify it by *type*, which is the whole point of it being typed.
    #[test]
    fn rejection_is_typed_not_a_bare_message() {
        let err = expect_rejected("typed", 999, 1, 0);
        assert!(err.downcast_ref::<DumpEdgeInvalid>().is_some());
        // and the context that names the input is still there for the operator
        assert!(format!("{err:#}").contains("ingest consolidation dump"));
    }
}
