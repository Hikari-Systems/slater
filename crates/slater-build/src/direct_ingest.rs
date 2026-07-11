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

use anyhow::{Context, Result};

use graph_format::consolidate_dump::DumpReader;
use graph_format::manifest::EntityKind;

use crate::buckets::{Blob, BucketWriter, EdgeRec, NodeRec};
use crate::diag::BuildDiag;
use crate::model::{Entity, RangeIndexStmt};

/// The post-resolve inputs a dump ingest produces — the same values the Cypher
/// front-half hands the clustering phase.
pub struct IngestResult {
    pub node_count: u64,
    pub edge_count: u64,
    pub labels: Vec<String>,
    pub reltypes: Vec<String>,
    pub keys: Vec<String>,
    pub range_stmts: Vec<RangeIndexStmt>,
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

    // Nodes → node bucket. The dump's `(labels, props)` blobs are already the
    // canonical `encode_labels_record` / `encode_props_record` bytes in global symbol
    // ids, so each is byte-copied into a `NodeRec` with no dump id and no vectors.
    diag.set_op("ingest dump nodes", "nodes", node_count);
    let mut nw = BucketWriter::create(node_bkt, block_bytes, zstd_level)
        .with_context(|| format!("create node bucket {}", node_bkt.display()))?;
    r.for_each_node(|_, labels_blob, props_blob| {
        nw.append_node(&NodeRec {
            dump_id: None,
            labels_blob: Blob::from_slice(labels_blob),
            props_blob: Blob::from_slice(props_blob),
            vec_props: Vec::new(),
        })?;
        diag.progress_add(1);
        Ok(())
    })?;
    nw.finish().context("finish node bucket")?;

    // Edges → edge bucket. Endpoints are already compacted node ids (= provisional
    // ids); the provisional edge id is the dump record position.
    diag.set_op("ingest dump edges", "edges", edge_count);
    let mut ew = BucketWriter::create(edge_bkt, block_bytes, zstd_level)
        .with_context(|| format!("create edge bucket {}", edge_bkt.display()))?;
    r.for_each_edge(|id, src, dst, reltype, props_blob| {
        ew.append_edge(&EdgeRec {
            prov_edge_id: id,
            src_prov: src,
            dst_prov: dst,
            reltype,
            props_blob: Blob::from_slice(props_blob),
        })?;
        diag.progress_add(1);
        Ok(())
    })?;
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

    Ok(IngestResult {
        node_count,
        edge_count,
        labels: r.meta().labels.clone(),
        reltypes: r.meta().reltypes.clone(),
        keys: r.meta().property_keys.clone(),
        range_stmts,
    })
}
