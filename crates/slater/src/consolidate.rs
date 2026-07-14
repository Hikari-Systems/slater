// SPDX-License-Identifier: Apache-2.0
//! Serialise a read view to a business-key `MERGE` dump — the input a
//! consolidation rebuild feeds back to `slater-build`.
//!
//! Phase 1d consolidation is *dump-and-rebuild* (major compaction, fan-in two): the
//! current core **overlaid with the frozen delta** is written out as
//! `slater-build`-dialect Cypher and rebuilt into a fresh generation. Because the
//! serialiser reads through a [`ReadView`], pointing it at a
//! [`MergedView`](crate::read_view::MergedView) folds the delta in for free — the
//! dump already represents the consolidated state, so the builder needs no
//! delta-awareness and runs unchanged.
//!
//! # Emitted dialect (matches `slater-build`'s default business-key `MERGE` import)
//! - `CREATE INDEX FOR (n:Label) ON (n.prop);` / `CREATE INDEX FOR ()-[r:T]->() ON
//!   (r.prop);` — first, so the rebuild recreates every index (and so future writes
//!   can still resolve their business keys).
//! - `MERGE (n:Label {key: <lit>}) SET n.p = <lit>, …;` — one per node. Delta-born
//!   nodes (Phase 2c) ride the same loop: `node_count()` spans the synthetic id
//!   range and `node_record` reads a born node's label + props from the delta, so a
//!   created node is emitted (and thus survives the rebuild) exactly like a core one.
//! - `MERGE (a:LA {ka: <lit>})-[r:T]->(b:LB {kb: <lit>}) SET r.p = <lit>, …;` — one
//!   per edge, emitted from its source so each edge appears exactly once.
//!
//! # Identity: recover a business key, or refuse (never corrupt)
//! A generation does not record which property is a node's identity. We infer it
//! from the range indexes (`plan::index_for`) — the same signal the write path uses
//! — and **refuse** (a clear error, no silent data loss) when a node has no
//! range-indexed property. A multi-label node round-trips as
//! `MERGE (n:Ident:Other {key: v})`: the range-indexed label is the identity (it
//! leads the list) and every label is written back, so a `SET n:Label` survives a
//! consolidation. Multi-key aliasing of one node is still out of scope.
//!
//! # Determinism
//! Nodes and edges iterate in ascending dense-id order; a node's `SET` assignments
//! and an edge's properties are sorted by property name; the identity property is
//! the first matching range index in manifest order. So a fixed `(core, delta)`
//! serialises byte-identically — the property the consolidation golden gate rests
//! on.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};
use graph_format::consolidate_dump::{DumpRangeIndex, DumpVectorIndex, DumpWriter};
use graph_format::ids::Value;
use graph_format::manifest::{AnnMode, EntityKind, VectorIndexDesc};
use graph_format::vectors::VectorEntry;

use crate::exec::{val_to_value, Engine, NamedProps};
use crate::read_view::ReadView;

/// Serialise `engine`'s view to a business-key `MERGE` dump on `out`. The engine
/// must wrap the [`ReadView`] being dumped (a `MergedView` to capture the delta, or
/// a bare `Generation` for a plain export).
///
/// **Refuses a graph that carries vector indexes.** The `MERGE` dialect has no
/// spelling for an embedding — `merge_build` rejects a vector literal outright — so a
/// text dump of a vector-carrying graph is lossy *by construction*: every embedding
/// would render as the literal `null` and the rebuilt graph would have none. That used
/// to happen silently. It is a hard error now; consolidation itself takes the binary
/// path ([`serialise_binary_dump`]), which does carry vectors.
pub fn serialise_merge_dump<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    out: &mut impl Write,
) -> Result<()> {
    let vidx = &view.manifest().vector_indexes;
    if !vidx.is_empty() {
        let names: Vec<String> = vidx
            .iter()
            .map(|d| format!("(:{} {{{}}})", d.label, d.property))
            .collect();
        bail!(
            "cannot write a MERGE dump for a graph with vector indexes ({}): the MERGE \
             dialect cannot carry embeddings, so the dump would silently drop them. Use the \
             binary consolidation dump (`serialise_binary_dump`), which carries vectors.",
            names.join(", ")
        );
    }
    emit_index_ddl(view, out)?;
    let n = view.node_count();
    for id in 0..n {
        emit_node(engine, view, id, out)?;
    }
    for src in 0..n {
        emit_edges_from(engine, view, src, out)?;
    }
    Ok(())
}

/// A symbol interner. **Seeded from the base generation's symbol table** so a base
/// entity's ids stay identity-valid — that is what lets the binary-dump fast path
/// byte-copy a base label/property/edge record without remapping the ids inside it.
/// Delta-born names append past the seeded range, in first-seen order, so a fixed
/// `(core, delta)` reproduces deterministically. (Dead symbols left by deletions
/// stay in the table; the builder tolerates unused symbols.)
struct Interner {
    names: Vec<String>,
    index: HashMap<String, u32>,
}

impl Interner {
    /// Seed from a base symbol table: `names[i]` keeps id `i`, so records already
    /// encoded against the base table need no remap.
    fn seeded(names: &[String]) -> Self {
        let index = names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u32))
            .collect();
        Self {
            names: names.to_vec(),
            index,
        }
    }

    fn intern(&mut self, name: &str) -> u32 {
        if let Some(&id) = self.index.get(name) {
            return id;
        }
        let id = self.names.len() as u32;
        self.names.push(name.to_string());
        self.index.insert(name.to_string(), id);
        id
    }

    fn into_names(self) -> Vec<String> {
        self.names
    }
}

/// The new dense id of a surviving node after tombstoned ids are elided: `old`
/// minus how many tombstoned ids sort below it. `tombs` is the sorted effective
/// tombstone set ([`DeltaSnapshot::effective_tombstoned_ids`]), so this renumbers
/// the surviving `0..node_count` span gaplessly in O(log #tombstones).
fn compact_id(tombs: &[u64], old: u64) -> u64 {
    old - tombs.partition_point(|&t| t < old) as u64
}

/// Fold a node/edge's named props into `(key_id, Value)` pairs against `keys`,
/// refusing a non-scalar value (a bug, never silent).
///
/// A `Value::Vector` here is an **unindexed** inline embedding — D12 routes an
/// *indexed* one out of the column store, so it never reaches this function. Inline
/// vectors are carried: `encode_props_record` encodes them, and the byte-copy fast
/// path ([`DumpWriter::append_node_raw`]) already preserves them verbatim, so dropping
/// them here (as this used to) meant a node lost its vector iff it happened to be
/// delta-patched — the same graph, two different answers depending on which path a
/// node took.
fn intern_props(
    props: &NamedProps,
    keys: &mut Interner,
    var: &str,
    out: &mut Vec<(u32, Value)>,
) -> Result<()> {
    out.clear();
    for (name, val) in props {
        match val_to_value(val) {
            Some(v) => out.push((keys.intern(name), v)),
            None => bail!("property {var}.{name} is not a scalar value"),
        }
    }
    Ok(())
}

/// Serialise `engine`'s merged view to a **binary** consolidation dump directory
/// ([`graph_format::consolidate_dump`]) — the fast path `slater-build` ingests
/// directly (skipping parse, node dedup, and endpoint resolution). The engine must
/// wrap the [`ReadView`] being dumped (a `MergedView` to capture the delta).
///
/// Unlike [`serialise_merge_dump`], node identity is not recovered from a business
/// key: dense ids are carried directly (compacted to elide tombstones), so a node
/// needs no range-indexed property, and endpoints are emitted as compacted ids with
/// no per-edge business-key lookup.
///
/// # Byte-copy fast path (Phase 0.5)
/// The symbol tables are **seeded from the base manifest**, so a base entity's label /
/// property / reltype ids are identity-valid in the dump. Every entity the delta does
/// not touch — the overwhelming majority during a consolidation — is then emitted by
/// byte-copying its raw `node_labels` / `node_props` / `edge_props` record straight
/// from the base stores (via the block cache), with no decode, no per-record `String`
/// allocation, and no re-encode. Only delta-born or delta-patched entities take the
/// decode + overlay + re-intern path. This turns the dump side from ~hundreds of
/// millions of `rel_record` allocations into a near-sequential block copy.
pub fn serialise_binary_dump<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    dir: &Path,
) -> Result<()> {
    // Seed the symbol tables from the base generation so base records byte-copy without
    // an id remap; delta-born names append past the seeded range.
    let manifest = view.manifest();
    let mut labels = Interner::seeded(&manifest.labels);
    let mut reltypes = Interner::seeded(&manifest.reltypes);
    let mut keys = Interner::seeded(&manifest.property_keys);
    let range_indexes: Vec<DumpRangeIndex> = manifest
        .range_indexes
        .iter()
        .map(|ri| DumpRangeIndex {
            entity: ri.entity,
            label_or_type: ri.label_or_type.clone(),
            property: ri.property.clone(),
        })
        .collect();

    let delta = view.delta();
    // The core stack (base + upper segments). A retarget consolidation runs over a *stacked*
    // set: the byte-copy fast paths below gate on the write-delta alone, but a segment can
    // patch or tombstone a base id too, and `raw_node_labels`/`raw_edge_props` read the base
    // block store — so a segment-touched base id must take the decode-through-stack slow path
    // (`node_record`/`rel_record` are already segment-aware). Only probed when `stacked`; a
    // singleton short-circuits to `None` with zero stack work, keeping a non-flushed
    // consolidation byte-for-byte unchanged.
    let stack = view.core_stack();
    let stacked = !stack.is_singleton();
    let core_nodes = view.core_generation().node_count();
    let core_edges = view.core_generation().edge_count();
    let n = view.node_count();
    let mut w = DumpWriter::create(dir)?;

    // The dense ids elided from the rebuild: every id the *delta* tombstones plus every id a
    // *segment* tombstones (a base row deleted into a segment, or a segment-born-then-deleted
    // id). Built in one pass as the node loop skips ids, so it stays ascending and
    // `compact_id` renumbers the survivors gaplessly — reclaiming born-then-deleted rows at the
    // dense-id level (the leanness Phase 5 deferred). For a singleton this is exactly
    // `delta.effective_tombstoned_ids()` (the stack contributes nothing).
    let mut combined_tombs: Vec<u64> = Vec::new();

    // Nodes in ascending compacted-id order (tombstoned ids elided). The append
    // position is the new dense id, matching `compact_id`.
    let mut label_ids: Vec<u32> = Vec::new();
    let mut prop_kv: Vec<(u32, Value)> = Vec::new();
    for old in 0..n {
        // A segment full row for this id, if any (an override of a base node, a segment-born
        // node, or a tombstone). Resolved once and reused by the fast-path gate below.
        let seg_row = if stacked {
            stack.resolve_node_row(old)?
        } else {
            None
        };
        if delta.is_tombstoned(old) || seg_row.as_ref().is_some_and(|r| r.tombstoned) {
            combined_tombs.push(old);
            continue;
        }
        // Fast path: a base node neither the delta NOR a segment touches — byte-copy its raw
        // label + property records straight from the base store.
        if old < core_nodes && delta.node_patch(old).is_none() && seg_row.is_none() {
            let lb = engine.raw_node_labels(old)?;
            let pb = engine.raw_node_props(old)?;
            w.append_node_raw(&lb, &pb)?;
            continue;
        }
        // Slow path: born, delta-patched, or segment-overridden — decode through the stack,
        // overlay the delta, and re-intern.
        let (lnames, props) = engine.node_record(old)?;
        label_ids.clear();
        for l in &lnames {
            label_ids.push(labels.intern(l));
        }
        intern_props(&props, &mut keys, "n", &mut prop_kv)?;
        w.append_node(&label_ids, &prop_kv)?;
    }

    // Edges, walked from each surviving source (so every edge is emitted once).
    // `outgoing_adj` is overlay-aware: it already drops tombstoned edges and edges to
    // tombstoned nodes and appends delta-born edges. `combined_tombs` is the exact set the
    // node loop skipped, so `compact_id` over it matches the node append positions.
    for old_src in 0..n {
        if combined_tombs.binary_search(&old_src).is_ok() {
            continue;
        }
        let new_src = compact_id(&combined_tombs, old_src);
        for adj in engine.outgoing_adj(old_src)? {
            let old_dst = adj.neighbour.0;
            // Belt-and-braces: a node tombstone (delta or segment) must never leak an edge.
            if combined_tombs.binary_search(&old_dst).is_ok() {
                continue;
            }
            let new_dst = compact_id(&combined_tombs, old_dst);
            let eid = adj.edge.0;
            // A segment full row for this edge, if any (a flushed core-edge patch).
            let seg_edge = if stacked {
                stack.resolve_edge_row(eid)?
            } else {
                None
            };
            // Fast path: a base edge neither the delta NOR a segment patches — byte-copy its
            // raw property record. `adj.reltype` is a base reltype id = dump id (seeded).
            if eid < core_edges && delta.edge_patches(eid).is_empty() && seg_edge.is_none() {
                let pb = engine.raw_edge_props(eid)?;
                w.append_edge_raw(new_src, new_dst, adj.reltype, &pb)?;
                continue;
            }
            // Slow path: born, delta-patched, or segment-patched edge — decode, overlay,
            // re-intern.
            let (rtype_name, eprops) = engine.rel_record(eid, adj.reltype)?;
            let rt = reltypes.intern(&rtype_name);
            intern_props(&eprops, &mut keys, "r", &mut prop_kv)?;
            w.append_edge(new_src, new_dst, rt, &prop_kv)?;
        }
    }

    // Vectors. An *indexed* embedding is routed out of the column store (D12), so the
    // node loop above cannot see it — it is simply not in the props record. Without
    // this pass the rebuild produces a generation with no embeddings and an empty
    // `vector_indexes`, silently: the dump would be well-formed, the build would exit
    // 0, and `db.idx.vector.queryNodes` would then report no such index.
    //
    // Vectors are keyed by *compacted* id, because the rebuild re-clusters and permutes
    // dense ids — anything keyed by the old id would be attached to the wrong node.
    // `compact_id` is monotone, so sorting on the new id also gives the ascending order
    // `append_vector` requires (and the builder merge-joins against).
    let mut vector_indexes: Vec<DumpVectorIndex> = Vec::new();
    let mut vectors: Vec<(u64, u32, Vec<f32>)> = Vec::new(); // (new_id, key_id, vector)
    for desc in &manifest.vector_indexes {
        let key_id = keys.intern(&desc.property);
        // The sealed base index, then the levels above it (delta patches and segment rows)
        // folded newest-wins: a node re-embedded since the build must carry its *new*
        // vector into the rebuild, and a node embedded for the first time since the build
        // has no base entry at all.
        //
        // The overlay is the small side (bounded by `memtableBytes` and `maxUpperSegments`)
        // and the base is the whole index, so the overlay drives the suppression and the
        // base still streams straight through — folding both into one map would put every
        // vector in the graph through a per-node insert to override a handful of them.
        //
        // `superseded` covers a **removal** as well as a re-embed: a node whose embedding was
        // taken away has no entry above the base to overwrite it with, so without suppressing
        // it here the rebuild would quietly restore the vector the user deleted.
        //
        // The levels are flattened newest-wins for the dump (a rebuild wants one vector per
        // node, not a scan of each level) — but from the *same* `VectorLevels` the KNN path
        // reads, so the two consumers cannot disagree about which level wins. A disagreement
        // would drop a vector on the floor, and only on consolidation.
        let levels = engine.vector_levels(desc)?;
        let superseded = levels.superseded();
        let push = |node_id: u64, vector: Vec<f32>, vectors: &mut Vec<_>| {
            // A node the delta or a segment deleted takes its embedding with it.
            if combined_tombs.binary_search(&node_id).is_ok() {
                return;
            }
            vectors.push((compact_id(&combined_tombs, node_id), key_id, vector));
        };
        for e in read_index_vectors(engine, view, desc)? {
            if superseded.contains(&e.node_id) {
                continue;
            }
            push(e.node_id, e.vector, &mut vectors);
        }
        for e in levels.into_effective_entries() {
            push(e.node_id, e.vector, &mut vectors);
        }
        vector_indexes.push(DumpVectorIndex {
            label: desc.label.clone(),
            property: desc.property.clone(),
            dim: desc.dim,
            metric: desc.metric,
        });
    }
    vectors.sort_by_key(|(id, key, _)| (*id, *key));
    for (id, key, v) in &vectors {
        w.append_vector(*id, *key, v)?;
    }

    w.finish(
        labels.into_names(),
        reltypes.into_names(),
        keys.into_names(),
        range_indexes,
        vector_indexes,
    )
    .context("finish binary consolidation dump")?;
    Ok(())
}

/// Every `(node_id, vector)` a vector index holds, read back out of the core.
///
/// The two arms keep their vectors in **different files**, and that is the whole trap
/// here (D31): a brute-force index's full-precision vectors live in `vectors.f32.blk`
/// at `[first_record, first_record + count)`, while a Vamana index's live in its own
/// `.vamana` blocks and are *not* in `vectors.f32.blk` at all — its `first_record` is
/// recorded as a meaningless `0`. Reading the wrong store for the mode yields an empty
/// group and hence a silently vector-less rebuild, which is exactly the failure this
/// path exists to prevent.
///
/// One inherent lossiness, worth knowing: a Vamana index stores its vectors
/// **L2-normalised** (the space its graph and PQ codebooks were built in — D29), so
/// the original magnitudes are gone from the core and cannot be recovered here. Vamana
/// is cosine-only (`vamana_eligible`) and cosine is scale-invariant, so every KNN score
/// round-trips exactly; only a vector's length is lost.
fn read_index_vectors<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    desc: &VectorIndexDesc,
) -> Result<Vec<VectorEntry>> {
    match desc.mode {
        AnnMode::BruteForce => engine.vector_group(desc.first_record, desc.count),
        AnnMode::Vamana { .. } => {
            let index = view
                .vamana_index(&desc.label, &desc.property)
                .with_context(|| {
                    format!(
                        "Vamana index files for (:{} {{{}}}) are not open; \
                         cannot carry its vectors through consolidation",
                        desc.label, desc.property
                    )
                })?;
            (0..desc.count as u32)
                .map(|i| {
                    let node = index.reader.node(i)?;
                    Ok(VectorEntry {
                        node_id: node.node_id,
                        vector: node.vector,
                    })
                })
                .collect()
        }
    }
}

/// `CREATE INDEX …;` for every range index, so the rebuilt generation carries the
/// same indexes forward (and business keys stay resolvable for later writes).
fn emit_index_ddl<V: ReadView>(view: &V, out: &mut impl Write) -> Result<()> {
    for ri in &view.manifest().range_indexes {
        let (lt, prop) = (quote_ident(&ri.label_or_type), quote_ident(&ri.property));
        match ri.entity {
            EntityKind::Node => writeln!(out, "CREATE INDEX FOR (n:{lt}) ON (n.{prop});")?,
            EntityKind::Edge => writeln!(out, "CREATE INDEX FOR ()-[r:{lt}]->() ON (r.{prop});")?,
        }
    }
    Ok(())
}

/// The recovered business identity of a node: its labels **ordered with the identity
/// label first** (the label carrying the range index on the present business key; the
/// remaining labels sorted by core label id for determinism), plus the key property and
/// value. A multi-label node round-trips as `MERGE (n:Ident:Other {key: v})`. Errors when
/// no label has a range-indexed property present — consolidation refuses rather than emit
/// an unidentifiable node.
fn node_identity(
    view: &impl ReadView,
    id: u64,
    labels: &[String],
    props: &NamedProps,
) -> Result<(Vec<String>, String, Value)> {
    // The identity label is the one with a range index on a property this node carries;
    // when several qualify, the lowest core label id wins (a deterministic tie-break).
    let mut best: Option<(u32, String, String, Value)> = None; // (label_id, label, key, value)
    for label in labels {
        for ri in &view.manifest().range_indexes {
            if ri.entity != EntityKind::Node || &ri.label_or_type != label {
                continue;
            }
            if let Some((_, val)) = props.iter().find(|(k, _)| k == &ri.property) {
                let value = val_to_value(val).with_context(|| {
                    format!(
                        "node {id} business key {}.{} is not a scalar",
                        label, ri.property
                    )
                })?;
                let lid = view.label_id(label).unwrap_or(u32::MAX);
                if best.as_ref().is_none_or(|(bid, ..)| lid < *bid) {
                    best = Some((lid, label.clone(), ri.property.clone(), value));
                }
            }
        }
    }
    let Some((_, ident_label, key, key_value)) = best else {
        bail!(
            "cannot consolidate node {id} (labels {labels:?}): no range-indexed business-key \
             property is set — add a range index on an identity property (nodes are identified \
             by an indexed key)"
        );
    };
    // Identity label first, then the rest ordered by core label id.
    let mut rest: Vec<(u32, &String)> = labels
        .iter()
        .filter(|l| **l != ident_label)
        .map(|l| (view.label_id(l).unwrap_or(u32::MAX), l))
        .collect();
    rest.sort();
    let mut ordered = Vec::with_capacity(labels.len());
    ordered.push(ident_label.clone());
    ordered.extend(rest.into_iter().map(|(_, l)| l.clone()));
    Ok((ordered, key, key_value))
}

/// `MERGE (n:Label {key: v}) SET n.p = v, …;` for one node — the key property
/// excluded from the `SET`, the rest sorted by name for determinism.
fn emit_node<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    id: u64,
    out: &mut impl Write,
) -> Result<()> {
    // A tombstoned node is deleted — the consolidated core must not carry it.
    if view.delta().is_tombstoned(id) {
        return Ok(());
    }
    let (labels, props) = engine.node_record(id)?;
    let (ident_labels, key, key_value) = node_identity(view, id, &labels, &props)?;
    // `MERGE (n:Ident:Other {key: v})` — all labels, identity first. The build MERGE
    // dialect matches on the leading (identity) label and writes the whole list. Every
    // identifier is quoted on the way out (HIK-84): a label or key is arbitrary text,
    // and un-quoted it would re-parse as *structure* in the rebuild.
    let label_str = ident_labels
        .iter()
        .map(|l| quote_ident(l))
        .collect::<Vec<_>>()
        .join(":");
    write!(
        out,
        "MERGE (n:{label_str} {{{}: {}}})",
        quote_ident(&key),
        literal(&key_value)
    )?;
    emit_set(&props, "n", Some(&key), out)?;
    writeln!(out, ";")?;
    Ok(())
}

/// Every outgoing edge of `src`, each as a business-key `MERGE` relationship. Edges
/// are emitted once, from the source, so the rebuild sees each exactly once.
fn emit_edges_from<V: ReadView>(
    engine: &Engine<'_, V>,
    view: &V,
    src: u64,
    out: &mut impl Write,
) -> Result<()> {
    // A deleted source node vanishes with its edges. (`outgoing_adj` is overlay-aware
    // as of Phase 3b: it already appends delta-born edges and drops tombstoned ones,
    // so this loop folds the topology delta for free.)
    if view.delta().is_tombstoned(src) {
        return Ok(());
    }
    let (slabels, sprops) = engine.node_record(src)?;
    // An edge endpoint is addressed by its identity label + key alone; the node's full
    // label set is written by its own node MERGE.
    let (sl, sk, sv) = {
        let (labels, k, v) = node_identity(view, src, &slabels, &sprops)?;
        (labels.into_iter().next().expect("identity label"), k, v)
    };
    for adj in engine.outgoing_adj(src)? {
        let dst = adj.neighbour.0;
        // Belt-and-braces: `outgoing_adj` already drops an edge to a tombstoned node,
        // but a node tombstone must never leak an edge into the rebuild.
        if view.delta().is_tombstoned(dst) {
            continue;
        }
        let (dlabels, dprops) = engine.node_record(dst)?;
        let (dl, dk, dv) = {
            let (labels, k, v) = node_identity(view, dst, &dlabels, &dprops)?;
            (labels.into_iter().next().expect("identity label"), k, v)
        };
        let (rtype, eprops) = engine.rel_record(adj.edge.0, adj.reltype)?;
        write!(
            out,
            "MERGE (a:{} {{{}: {}}})-[r:{}]->(b:{} {{{}: {}}})",
            quote_ident(&sl),
            quote_ident(&sk),
            literal(&sv),
            quote_ident(&rtype),
            quote_ident(&dl),
            quote_ident(&dk),
            literal(&dv)
        )?;
        emit_set(&eprops, "r", None, out)?;
        writeln!(out, ";")?;
    }
    Ok(())
}

/// Append ` SET <var>.<p> = <lit>, …` for every property except `exclude` (the
/// business key), sorted by name. Nothing is written when there is nothing to set.
fn emit_set(
    props: &NamedProps,
    var: &str,
    exclude: Option<&str>,
    out: &mut impl Write,
) -> Result<()> {
    let mut kept: Vec<(&String, Value)> = Vec::new();
    for (name, val) in props {
        if exclude == Some(name.as_str()) {
            continue;
        }
        let v = val_to_value(val)
            .with_context(|| format!("property {var}.{name} is not a scalar value"))?;
        kept.push((name, v));
    }
    kept.sort_by(|a, b| a.0.cmp(b.0));
    for (i, (name, v)) in kept.iter().enumerate() {
        let sep = if i == 0 { " SET" } else { "," };
        write!(out, "{sep} {var}.{} = {}", quote_ident(name), literal(v))?;
    }
    Ok(())
}

/// Render a stored scalar [`Value`] as a `slater-build`-dialect Cypher literal that
/// round-trips exactly through the builder's parser (`parse_string` unescaping,
/// `number`/`boolean`/`null`/`list` rules). Vectors are a `MERGE`-dump non-goal.
fn literal(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format_float(*f),
        Value::Str(s) => quote_str(s),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(literal).collect();
            format!("[{}]", inner.join(", "))
        }
        // A `vecf32(...)` prop cannot ride a MERGE dump (see the vectors non-goal);
        // node_props already routes embeddings out, so this is a belt-and-braces guard.
        Value::Vector(_) => "null".to_string(),
    }
}

/// Format an `f64` so it re-parses as a float, never an int: a value with no
/// fractional/exponent part gets a `.0` suffix (the `number` rule needs a `.` or
/// `e` to be a float). Non-finite values have no dump spelling and become `null`.
fn format_float(f: f64) -> String {
    if !f.is_finite() {
        return "null".to_string();
    }
    let s = format!("{f}");
    if s.bytes().any(|b| b == b'.' || b == b'e' || b == b'E') {
        s
    } else {
        format!("{s}.0")
    }
}

/// Render an **identifier** — a label, relationship type, property key or index
/// property — for the builder's `label` / `reltype` / `key` rules.
///
/// A name is emitted bare only when it is exactly what those rules accept bare:
/// non-empty and `[A-Za-z0-9_]` throughout. Anything else is backtick-quoted with
/// every inner backtick doubled (`` ` `` → ``` `` ```), which is openCypher's
/// `EscapedSymbolicName` and the exact inverse of `slater-build`'s
/// `parser::unquote_key`. The empty name spells `` `` ``.
///
/// This is a **security** boundary, not cosmetics (HIK-84): labels, reltypes and
/// property keys are arbitrary strings over Bolt, so an un-quoted name like
/// `` `x` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a `` would re-parse as *structure*
/// when an operator rebuilds the dump. Quoted, it is inert — the builder's statement
/// splitter is backtick-aware, so not even a `;` inside a name ends the statement.
pub(crate) fn quote_ident(s: &str) -> String {
    let bare = !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if bare {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + 2);
    out.push('`');
    for c in s.chars() {
        if c == '`' {
            out.push('`');
        }
        out.push(c);
    }
    out.push('`');
    out
}

/// Single-quote and escape a string for the builder's `sq_inner` rule, matching its
/// `parse_string` unescaping exactly (`\\`, `\'`, `\n`, `\t`, `\r`, `\0`).
fn quote_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\0' => out.push_str("\\0"),
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::BlockCache;
    use crate::generation::Generation;
    use crate::read_view::MergedView;
    use crate::testgen;
    use graph_format::columns::decode_props;
    use graph_format::consolidate_dump::DumpReader;
    use graph_format::nodelabels::decode_labels;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    /// Read a binary dump back into name-space: `(labels, props)` per node and
    /// `(src_idx, dst_idx, reltype, props)` per edge, resolving symbol ids through
    /// the dump's meta tables. Lets the tests assert on the merged, compacted graph.
    #[allow(clippy::type_complexity)]
    fn read_dump(
        dir: &Path,
    ) -> (
        Vec<(Vec<String>, Vec<(String, Value)>)>,
        Vec<(u64, u64, String, Vec<(String, Value)>)>,
    ) {
        let r = DumpReader::open(dir).unwrap();
        let m = r.meta();
        let (labels, reltypes, keys) = (
            m.labels.clone(),
            m.reltypes.clone(),
            m.property_keys.clone(),
        );
        let name_props = |pb: &[u8]| -> Vec<(String, Value)> {
            decode_props(pb)
                .unwrap()
                .into_iter()
                .map(|(k, v)| (keys[k as usize].clone(), v))
                .collect()
        };
        let mut nodes = Vec::new();
        r.for_each_node(|_, lb, pb| {
            let ls = decode_labels(lb, false)
                .unwrap()
                .into_iter()
                .map(|l| labels[l as usize].clone())
                .collect();
            nodes.push((ls, name_props(pb)));
            Ok(())
        })
        .unwrap();
        let mut edges = Vec::new();
        r.for_each_edge(|_, s, d, t, pb| {
            edges.push((s, d, reltypes[t as usize].clone(), name_props(pb)));
            Ok(())
        })
        .unwrap();
        (nodes, edges)
    }

    /// The property value of `name` in a node/edge's decoded props, if present.
    fn prop<'a>(props: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
        props.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }

    /// HIK-84: identifiers are bare only when the builder's `label`/`reltype`/`key`
    /// rules accept them bare; everything else is backtick-quoted with inner backticks
    /// doubled, so no name can re-parse as structure in a rebuild.
    #[test]
    fn identifiers_are_quoted_unless_bare_legal() {
        assert_eq!(quote_ident("Person"), "Person");
        assert_eq!(quote_ident("_id2"), "_id2");
        assert_eq!(quote_ident("Odd Label"), "`Odd Label`");
        assert_eq!(quote_ident("a-b"), "`a-b`");
        assert_eq!(quote_ident("café"), "`café`");
        // The escape-the-escape case: an inner backtick is doubled.
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert_eq!(quote_ident("`"), "````");
        // The empty name has a spelling too (`` `` `` — the grammar's `*`).
        assert_eq!(quote_ident(""), "``");
        // The finding's payload is inert once quoted: it is one name, not a statement.
        assert_eq!(
            quote_ident("`x` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a"),
            "```x`` = 'v'; MERGE (m:Owned {id:'atk'}) SET m.a`"
        );
    }

    #[test]
    fn literals_round_trip_and_escape() {
        assert_eq!(literal(&Value::Null), "null");
        assert_eq!(literal(&Value::Bool(true)), "true");
        assert_eq!(literal(&Value::Int(-7)), "-7");
        assert_eq!(literal(&Value::Float(2.5)), "2.5");
        // A whole-valued float keeps a decimal point so it re-parses as a float.
        assert_eq!(literal(&Value::Float(10.0)), "10.0");
        assert_eq!(format_float(f64::NAN), "null");
        assert_eq!(literal(&Value::Str("plain".into())), "'plain'");
        // Escapes match the builder's parse_string unescaping.
        assert_eq!(literal(&Value::Str("a'b\\c\nd".into())), "'a\\'b\\\\c\\nd'");
        assert_eq!(
            literal(&Value::List(vec![Value::Int(1), Value::Str("x".into())])),
            "[1, 'x']"
        );
    }

    #[test]
    fn serialise_refuses_unidentifiable_node() {
        // A node whose labels carry no range-indexed property has no business key, so the
        // `MERGE` dump must refuse rather than emit an unkeyed node. `write_meta` declares
        // no range indexes at all, so its very first node trips this.
        let (root, graph, _) = testgen::write_meta("consolidate_refuse");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);
        let mut buf = Vec::new();
        let err = serialise_merge_dump(&engine, &view, &mut buf).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("no range-indexed business-key"),
            "expected an unidentifiable-node refusal, got: {msg}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn merge_dump_refuses_a_graph_with_vector_indexes() {
        // The `MERGE` dialect has no spelling for an embedding (`merge_build` rejects a
        // vector literal), so a text dump of a vector-carrying graph would silently render
        // every embedding as `null` and rebuild the graph without them. Refuse instead.
        // The binary consolidation dump is the path that actually carries vectors.
        let (root, graph, _) = testgen::write_basic("consolidate_refuse_vectors");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);
        let mut buf = Vec::new();
        let err = serialise_merge_dump(&engine, &view, &mut buf).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("vector indexes") && msg.contains("(:Person {embedding})"),
            "expected a vector-index refusal naming the index, got: {msg}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn node_identity_selects_indexed_label_and_orders_it_first() {
        // A multi-label node's identity is the range-indexed label, and it leads the
        // emitted label list regardless of the order the labels are presented in.
        let (root, graph) = testgen::write_indexed_people("node_ident_multi");
        let gen = Generation::open(&root, &graph).unwrap();
        let view = MergedView::read_only(&gen);
        let props: NamedProps = vec![("name".to_string(), crate::exec::Val::Str("Alice".into()))];
        let (labels, key, value) =
            node_identity(&view, 0, &["VIP".to_string(), "Person".to_string()], &props).unwrap();
        assert_eq!(
            labels,
            vec!["Person".to_string(), "VIP".to_string()],
            "the indexed label (Person) is the identity and leads the list"
        );
        assert_eq!(key, "name");
        assert_eq!(value, Value::Str("Alice".into()));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_folds_delta_into_the_dump() {
        // A fully single-label, indexed fixture serialises cleanly, and overlaying a
        // delta patch changes the emitted node line (the dump is the merged state).
        let (root, graph) = testgen::write_indexed_people("consolidate_dump");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Pure core first.
        let core_view = MergedView::read_only(&gen);
        let mut core = Vec::new();
        serialise_merge_dump(&Engine::new(&core_view, &cache), &core_view, &mut core).unwrap();
        let core = String::from_utf8(core).unwrap();
        assert!(core.contains("CREATE INDEX FOR (n:Person) ON (n.name);"));
        assert!(
            core.contains("MERGE (n:Person {name: 'Alice'}) SET n.age = 30;"),
            "core dump:\n{core}"
        );
        // The one edge round-trips with both endpoints' business keys.
        assert!(
            core.contains("MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;"),
            "core dump:\n{core}"
        );

        // Now overlay a patch on Alice's age and re-serialise the merged view.
        let mut mem = Memtable::new();
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(0),
            [("age".to_string(), Value::Int(99))],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(
            out.contains("MERGE (n:Person {name: 'Alice'}) SET n.age = 99;"),
            "merged dump should carry the overlaid age:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_emits_a_delta_born_node() {
        // A node created only in the delta (MERGE on an absent key) must appear in the
        // consolidated dump so the rebuild carries it forward — with its business key
        // and its SET properties.
        let (root, graph) = testgen::write_indexed_people("consolidate_born");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::with_synthetic_base(gen.node_count());
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None, // delta-born: absent from the core
            [("age".to_string(), Value::Int(50))],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            out.contains("MERGE (n:Person {name: 'Dave'}) SET n.age = 50;"),
            "delta-born node must be emitted:\n{out}"
        );
        // The core people survive alongside it.
        assert!(
            out.contains("MERGE (n:Person {name: 'Alice'})"),
            "dump:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_drops_a_delta_born_then_deleted_node() {
        // A node created only in the delta and then deleted (MERGE then DELETE by key)
        // must NOT appear in the consolidated dump — the tombstone suppresses it in the
        // `0..node_count` emit loop, so the delete survives a rebuild.
        let (root, graph) = testgen::write_indexed_people("consolidate_born_deleted");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::with_synthetic_base(gen.node_count());
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None, // delta-born: absent from the core
            [("age".to_string(), Value::Int(50))],
        );
        // Tombstone the born node (resolved=None: the active memtable already holds its
        // synthetic id, so the tombstone lands on the born entry).
        mem.delete_node("Person", "name", Value::Str("Dave".into()), None);
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            !out.contains("'Dave'"),
            "born-then-deleted node must be dropped from the dump:\n{out}"
        );
        // The core people are untouched.
        assert!(
            out.contains("MERGE (n:Person {name: 'Alice'})"),
            "core nodes survive:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_emits_a_delta_born_edge() {
        // Edges created only in the delta (Phase 3) must appear in the consolidated
        // dump so a rebuild carries the topology forward — both a born edge between two
        // existing core nodes and one to a delta-born endpoint node.
        let (root, graph) = testgen::write_indexed_people("consolidate_born_edge");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Core: Alice(0), Bob(1), Carol(2); one core edge Alice-KNOWS->Bob.
        let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
        // Born edge between two core nodes: Bob-KNOWS->Carol.
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Bob".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Carol".into()),
            Some(1),
            Some(2),
            [],
        );
        // Born edge to a born endpoint: Carol-KNOWS->Dave (Dave absent from the core).
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Carol".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Dave".into()),
            Some(2),
            None,
            [],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        // The original core edge is still there.
        assert!(
            out.contains(
                "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 2020;"
            ),
            "core edge survives:\n{out}"
        );
        // Both born edges (no properties → no SET) round-trip.
        assert!(
            out.contains("MERGE (a:Person {name: 'Bob'})-[r:KNOWS]->(b:Person {name: 'Carol'});"),
            "born core→core edge emitted:\n{out}"
        );
        assert!(
            out.contains("MERGE (a:Person {name: 'Carol'})-[r:KNOWS]->(b:Person {name: 'Dave'});"),
            "born edge to a born endpoint emitted:\n{out}"
        );
        // The born endpoint node itself is emitted (so the rebuild can resolve it).
        assert!(
            out.contains("MERGE (n:Person {name: 'Dave'});"),
            "born endpoint node emitted:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_carries_delta_born_edge_properties() {
        // A born edge created with a property (edge-property overlay) must carry its
        // SET into the consolidated dump so a rebuild preserves it.
        let (root, graph) = testgen::write_indexed_people("consolidate_edge_props");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Core: Alice(0), Bob(1), Carol(2). Born edge Bob-KNOWS->Carol with `since`.
        let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Bob".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Carol".into()),
            Some(1),
            Some(2),
            [("since".to_string(), Value::Int(1999))],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            out.contains(
                "MERGE (a:Person {name: 'Bob'})-[r:KNOWS]->(b:Person {name: 'Carol'}) SET r.since = 1999;"
            ),
            "born edge property carried into the dump:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_carries_a_core_edge_patch() {
        // Patching a *core* edge's property in place (`SET r.since = 7` on the existing
        // Alice-KNOWS->Bob, `since = 2020`) must carry the new value into the dump so a
        // rebuild preserves it — like a patched core node.
        let (root, graph) = testgen::write_indexed_people("consolidate_core_edge_patch");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        // Resolve the core edge id of Alice(0)-KNOWS->Bob(1) over an empty-delta view.
        let empty = MergedView::read_only(&gen);
        let core_edge_id = Engine::new(&empty, &cache)
            .outgoing_adj(0)
            .unwrap()
            .iter()
            .find(|a| a.neighbour.0 == 1)
            .expect("core edge Alice-KNOWS->Bob")
            .edge
            .0;

        let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
        mem.patch_core_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            Some(0),
            Some(1),
            core_edge_id,
            [("since".to_string(), Value::Int(7))],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            out.contains(
                "MERGE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) SET r.since = 7;"
            ),
            "the core-edge patch is carried into the dump:\n{out}"
        );
        assert!(
            !out.contains("r.since = 2020"),
            "the stale core value must not appear:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_drops_a_deleted_edge() {
        // Deleting the core edge Alice-KNOWS->Bob must remove it from the dump while
        // keeping both endpoint nodes — otherwise a rebuild would resurrect the edge.
        let (root, graph) = testgen::write_indexed_people("consolidate_del_edge");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
        mem.delete_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            Some(0),
            Some(1),
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            !out.contains("-[r:KNOWS]->"),
            "the deleted edge must not appear:\n{out}"
        );
        // Both endpoint nodes survive the edge delete.
        assert!(
            out.contains("MERGE (n:Person {name: 'Alice'})"),
            "dump:\n{out}"
        );
        assert!(
            out.contains("MERGE (n:Person {name: 'Bob'})"),
            "dump:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn serialise_drops_a_tombstoned_node_and_its_edge() {
        // Deleting Alice must remove both her node line and the Alice-KNOWS->Bob edge
        // from the consolidated dump — otherwise consolidation would resurrect her.
        let (root, graph) = testgen::write_indexed_people("consolidate_tombstone");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::new();
        mem.delete_node("Person", "name", Value::Str("Alice".into()), Some(0));
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let mut out = Vec::new();
        serialise_merge_dump(&Engine::new(&merged, &cache), &merged, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();

        assert!(
            !out.contains("{name: 'Alice'}"),
            "tombstoned Alice must not appear (node or edge endpoint):\n{out}"
        );
        // Bob and Carol survive.
        assert!(
            out.contains("MERGE (n:Person {name: 'Bob'})"),
            "dump:\n{out}"
        );
        assert!(
            out.contains("MERGE (n:Person {name: 'Carol'})"),
            "dump:\n{out}"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    // ── binary dump (direct consolidation) ────────────────────────────────────

    fn dump_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("slater_bindump_{}_{}", std::process::id(), name))
    }

    #[test]
    fn binary_dump_folds_delta_and_carries_edges() {
        // The merged state (a core patch on Alice's age, a born node Dave, a born edge
        // Bob-KNOWS->Carol) must all appear in the binary dump.
        let (root, graph) = testgen::write_indexed_people("bindump_fold");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
        // Patch a core node.
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Alice".into()),
            Some(0),
            [("age".to_string(), Value::Int(99))],
        );
        // Born node.
        mem.upsert_node(
            "Person",
            "name",
            Value::Str("Dave".into()),
            None,
            [("age".to_string(), Value::Int(50))],
        );
        // Born edge between two core nodes.
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Bob".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Carol".into()),
            Some(1),
            Some(2),
            [],
        );
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let dir = dump_dir("fold");
        let _ = std::fs::remove_dir_all(&dir);
        serialise_binary_dump(&Engine::new(&merged, &cache), &merged, &dir).unwrap();

        let (nodes, edges) = read_dump(&dir);
        // Core Alice(0), Bob(1), Carol(2) + born Dave(3), all in id order.
        assert_eq!(nodes.len(), 4);
        let by_name = |name: &str| {
            nodes
                .iter()
                .find(|(_, p)| prop(p, "name") == Some(&Value::Str(name.into())))
                .unwrap_or_else(|| panic!("node {name} missing from dump"))
        };
        assert_eq!(prop(&by_name("Alice").1, "age"), Some(&Value::Int(99)));
        assert_eq!(prop(&by_name("Dave").1, "age"), Some(&Value::Int(50)));
        assert!(by_name("Alice").0.iter().any(|l| l == "Person"));

        // The core edge Alice->Bob and the born edge Bob->Carol both present.
        let name_of = |idx: u64| {
            prop(&nodes[idx as usize].1, "name")
                .and_then(|v| match v {
                    Value::Str(s) => Some(s.clone()),
                    _ => None,
                })
                .unwrap()
        };
        let edge_pairs: Vec<(String, String)> = edges
            .iter()
            .map(|(s, d, _, _)| (name_of(*s), name_of(*d)))
            .collect();
        assert!(edge_pairs.contains(&("Alice".into(), "Bob".into())));
        assert!(edge_pairs.contains(&("Bob".into(), "Carol".into())));
        assert_eq!(edges.len(), 2);

        std::fs::remove_dir_all(&root).ok();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn binary_dump_compacts_over_a_tombstone() {
        // Deleting Alice (core id 0) must drop her node and her edge, and renumber the
        // survivors gaplessly — Bob and Carol occupy compacted ids 0 and 1, and the
        // surviving edge (if any) references the new ids.
        let (root, graph) = testgen::write_indexed_people("bindump_tombstone");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);

        let mut mem = Memtable::new();
        mem.delete_node("Person", "name", Value::Str("Alice".into()), Some(0));
        let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let dir = dump_dir("tombstone");
        let _ = std::fs::remove_dir_all(&dir);
        serialise_binary_dump(&Engine::new(&merged, &cache), &merged, &dir).unwrap();

        let (nodes, edges) = read_dump(&dir);
        // Alice gone; Bob and Carol remain, at compacted ids 0 and 1.
        assert_eq!(nodes.len(), 2);
        let names: Vec<Value> = nodes
            .iter()
            .map(|(_, p)| prop(p, "name").unwrap().clone())
            .collect();
        assert!(!names.contains(&Value::Str("Alice".into())));
        assert!(names.contains(&Value::Str("Bob".into())));
        assert!(names.contains(&Value::Str("Carol".into())));
        // The only core edge (Alice->Bob) is gone with Alice.
        assert!(edges.is_empty());
        // Every surviving edge endpoint (none here) would be a valid compacted id.
        for (s, d, _, _) in &edges {
            assert!(*s < 2 && *d < 2);
        }

        std::fs::remove_dir_all(&root).ok();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fixed `(core, delta)` dumps byte-identically — the consolidation golden.
    #[test]
    fn binary_dump_is_byte_deterministic() {
        let (root, graph) = testgen::write_indexed_people("bindump_determinism");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let run = |dir: &Path| {
            let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
            mem.upsert_node(
                "Person",
                "name",
                Value::Str("Dave".into()),
                None,
                [("age".to_string(), Value::Int(50))],
            );
            let merged = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
            let _ = std::fs::remove_dir_all(dir);
            serialise_binary_dump(&Engine::new(&merged, &cache), &merged, dir).unwrap();
        };
        let a = dump_dir("det_a");
        let b = dump_dir("det_b");
        run(&a);
        run(&b);
        for f in ["nodes.blk", "edges.blk", "meta.json"] {
            assert_eq!(
                std::fs::read(a.join(f)).unwrap(),
                std::fs::read(b.join(f)).unwrap(),
                "dump file {f} differs across runs"
            );
        }
        std::fs::remove_dir_all(&root).ok();
        let _ = std::fs::remove_dir_all(&a);
        let _ = std::fs::remove_dir_all(&b);
    }
}
