// SPDX-License-Identifier: Apache-2.0
//! Overlay overwrites ("pass 1.9"): apply `MERGE|MATCH … SET …` patch statements.
//!
//! The dump dialect is otherwise CREATE-only. An *overlay* dump may carry overwrite
//! statements that change property values of nodes/edges created earlier in the SAME
//! build input. Matching is by **label + property key** (real-Cypher MERGE), so it
//! cannot be resolved while a shard is parsed (a target may live in any shard) — the
//! statements are stashed verbatim in each [`crate::buckets::ShardMeta`] and resolved
//! here once, after the global symbol tables exist.
//!
//! This stage builds, in bounded memory (overlays are small):
//!   * **node patches** — `prov node id → ordered set-prop lists`, folded into
//!     `node_props.blk` during emit (keyed by provisional id, before clustering);
//!   * **edge patches** — `(src_prov, dst_prov, reltype) → ordered set-prop lists`,
//!     folded into `edge_props.blk` during the resolve pass (which already has those
//!     ids); a patch matching no edge is an error (edge create-on-absent is not a v1
//!     feature);
//!   * **created nodes** — synthesised by a MERGE whose match found nothing; emitted
//!     after the node scan with provisional ids `base_node_count + i`.
//!
//! Set-prop "last-writer-wins" is per key: applying patches in statement order, a
//! later assignment to the same key overrides an earlier one; unnamed keys are kept.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Result};

use graph_format::columns::{decode_props, encode_props_record};
use graph_format::ids::Value;
use graph_format::nodelabels::{decode_labels, encode_labels_record};
use graph_format::wire::{read_uvarint, read_value, skip_value, write_value};

use crate::buckets::{self, NodeRec, ShardMeta, ShardRemap};
use crate::model::NodeMatch;
use crate::shared::Interner;

/// One edge's accumulated patches plus a hit flag (set when emit/resolve finds the
/// matching base edge). Interior-mutable so parallel resolve workers can mark it
/// through a shared `&Overlay`.
struct EdgePatch {
    patches: Vec<Vec<(u32, Value)>>,
    hit: AtomicBool,
}

/// Resolved overlay patches, ready to fold into the stores. Constructed by
/// [`Overlay::build`]; `None` when the dump carries no overwrite statements (so the
/// build pays nothing for the common CREATE-only case).
pub(crate) struct Overlay {
    /// `prov node id → ordered set-prop patches` (global key ids), last-wins.
    node: HashMap<u64, Vec<Vec<(u32, Value)>>>,
    /// `(src_prov, dst_prov, reltype_global) → patches + hit flag`.
    edge: HashMap<(u64, u64, u32), EdgePatch>,
    /// Nodes synthesised by a 0-match MERGE; label/prop blobs already in GLOBAL ids,
    /// assigned provisional ids `base_node_count + i` in order at emit time.
    pub(crate) created: Vec<NodeRec>,
}

/// A pending endpoint/node match: locate provisional ids of nodes with `label_gid`
/// carrying `key_gid == value`. `None` symbol ids mean the label/key is absent from
/// the graph entirely → zero matches.
struct MatchReq {
    label_gid: Option<u32>,
    key_gid: Option<u32>,
    value: Value,
    provs: Vec<u64>,
}

impl Overlay {
    /// Build the overlay from the shards' stashed overwrite statements. Extends the
    /// global `labels`/`keys` interners with any new names introduced by SET targets
    /// or MERGE-created nodes (so the manifest carries them). Scans the node buckets
    /// once to resolve label+property matches. Returns `None` if there are no
    /// overwrites at all.
    pub(crate) fn build(
        node_bkt: &Path,
        remaps: &[ShardRemap],
        metas: &[ShardMeta],
        labels: &mut Interner,
        keys: &mut Interner,
        reltypes: &Interner,
    ) -> Result<Option<Overlay>> {
        // Collect overwrites in global input order (shard order, then statement order).
        let node_ovr: Vec<&crate::model::NodeOverwriteStmt> = metas
            .iter()
            .flat_map(|m| m.node_overwrites.iter())
            .collect();
        let edge_ovr: Vec<&crate::model::EdgeOverwriteStmt> = metas
            .iter()
            .flat_map(|m| m.edge_overwrites.iter())
            .collect();
        if node_ovr.is_empty() && edge_ovr.is_empty() {
            return Ok(None);
        }

        // One match request per node overwrite and per edge endpoint. `reqs[i]` is
        // filled with matching provisional ids by the node scan below.
        let mut reqs: Vec<MatchReq> = Vec::new();
        let mut push_req = |m: &NodeMatch, labels: &Interner, keys: &Interner| -> usize {
            let idx = reqs.len();
            reqs.push(MatchReq {
                label_gid: labels.get(&m.label),
                key_gid: keys.get(&m.key),
                value: m.value.clone(),
                provs: Vec::new(),
            });
            idx
        };
        let node_req: Vec<usize> = node_ovr
            .iter()
            .map(|o| push_req(&o.match_, labels, keys))
            .collect();
        let edge_req: Vec<(usize, usize)> = edge_ovr
            .iter()
            .map(|o| {
                (
                    push_req(&o.src, labels, keys),
                    push_req(&o.dst, labels, keys),
                )
            })
            .collect();

        // Build a hash index `(label, key, value) → request indices`, plus the set of
        // distinct `(label, key)` probe pairs. The node scan then resolves each node by
        // an O(1) lookup per probe pair, instead of a linear compare against every
        // request — so cost is O(nodes × distinct-(label,key)-pairs), not
        // O(nodes × overwrites), letting large overlays scale. Matching is exact by
        // value AND type (an `Int` match value does not match a `Float` node value of
        // equal magnitude); overlay match keys are business identifiers.
        let mut index: HashMap<(u32, u32, VKey), Vec<usize>> = HashMap::new();
        let mut probe_pairs: Vec<(u32, u32)> = Vec::new();
        for (i, r) in reqs.iter().enumerate() {
            if let (Some(lg), Some(kg)) = (r.label_gid, r.key_gid) {
                if !probe_pairs.contains(&(lg, kg)) {
                    probe_pairs.push((lg, kg));
                }
                index.entry((lg, kg, vkey(&r.value))).or_default().push(i);
            }
        }

        // Single scan over all node buckets (global symbol ids), recording provs.
        if !probe_pairs.is_empty() {
            buckets::for_each_node_remapped(node_bkt, remaps, |prov, node| {
                let labels = decode_labels(&node.labels_blob)?;
                for &(lg, kg) in &probe_pairs {
                    if !labels.contains(&lg) {
                        continue;
                    }
                    let Some(v) = value_of_key(&node.props_blob, kg)? else {
                        continue;
                    };
                    if let Some(members) = index.get(&(lg, kg, vkey(&v))) {
                        for &ri in members {
                            reqs[ri].provs.push(prov);
                        }
                    }
                }
                Ok(())
            })?;
        }

        let mut overlay = Overlay {
            node: HashMap::new(),
            edge: HashMap::new(),
            created: Vec::new(),
        };

        // Resolve node overwrites.
        for (o, &ri) in node_ovr.iter().zip(&node_req) {
            let set_props = intern_set_props(&o.set_props, keys)?;
            let provs = std::mem::take(&mut reqs[ri].provs);
            if !provs.is_empty() {
                for p in provs {
                    overlay.node.entry(p).or_default().push(set_props.clone());
                }
            } else if o.is_merge {
                overlay
                    .created
                    .push(make_node(&o.match_, &set_props, labels, keys));
            } else {
                eprintln!(
                    "note: MATCH (:{} {{{}: {:?}}}) SET … matched no node — overlay no-op",
                    o.match_.label, o.match_.key, o.match_.value
                );
            }
        }

        // Resolve edge overwrites. Endpoints/reltype that resolve to nothing mean the
        // edge cannot exist → error (edge create-on-absent is not supported in v1).
        for (o, &(sri, dri)) in edge_ovr.iter().zip(&edge_req) {
            let set_props = intern_set_props(&o.set_props, keys)?;
            let reltype_gid = reltypes.get(&o.reltype);
            let src_provs = std::mem::take(&mut reqs[sri].provs);
            let dst_provs = std::mem::take(&mut reqs[dri].provs);
            let (Some(rt), false, false) =
                (reltype_gid, src_provs.is_empty(), dst_provs.is_empty())
            else {
                bail!(
                    "{} (:{} {{{}: {:?}}})-[:{}]->(:{} {{{}: {:?}}}) SET … matched no edge \
                     (unknown endpoint or relationship type); edge create-on-absent is not supported",
                    if o.is_merge { "MERGE" } else { "MATCH" },
                    o.src.label, o.src.key, o.src.value,
                    o.reltype,
                    o.dst.label, o.dst.key, o.dst.value,
                );
            };
            for &s in &src_provs {
                for &d in &dst_provs {
                    overlay
                        .edge
                        .entry((s, d, rt))
                        .or_insert_with(|| EdgePatch {
                            patches: Vec::new(),
                            hit: AtomicBool::new(false),
                        })
                        .patches
                        .push(set_props.clone());
                }
            }
        }

        Ok(Some(overlay))
    }

    pub(crate) fn has_node_patches(&self) -> bool {
        !self.node.is_empty()
    }
    pub(crate) fn has_edge_patches(&self) -> bool {
        !self.edge.is_empty()
    }

    /// Fold this node's set-prop patches onto `blob` (a global-id `node_props.blk`
    /// record). Returns the rewritten blob, or `None` if the node has no patch.
    pub(crate) fn fold_node(&self, prov: u64, blob: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.node.get(&prov) {
            Some(ps) => Ok(Some(apply_patches(blob, ps)?)),
            None => Ok(None),
        }
    }

    /// Fold an edge's set-prop patches onto `blob` for `(src, dst, reltype)`, marking
    /// the patch matched. Returns the rewritten blob, or `None` if no patch matches.
    pub(crate) fn fold_edge(
        &self,
        src: u64,
        dst: u64,
        reltype: u32,
        blob: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        match self.edge.get(&(src, dst, reltype)) {
            Some(ep) => {
                ep.hit.store(true, Ordering::Relaxed);
                Ok(Some(apply_patches(blob, &ep.patches)?))
            }
            None => Ok(None),
        }
    }

    /// `(src_prov, dst_prov, reltype)` of every edge patch that matched no edge during
    /// resolve — the caller bails (edge create-on-absent unsupported in v1).
    pub(crate) fn unmatched_edges(&self) -> Vec<(u64, u64, u32)> {
        self.edge
            .iter()
            .filter(|(_, ep)| !ep.hit.load(Ordering::Relaxed))
            .map(|(k, _)| *k)
            .collect()
    }
}

/// Apply ordered set-prop patches to a property record, per-key last-writer-wins:
/// each `(key, value)` overrides an existing key or appends a new one.
fn apply_patches(blob: &[u8], patches: &[Vec<(u32, Value)>]) -> Result<Vec<u8>> {
    let mut props = decode_props(blob)?;
    for patch in patches {
        for (k, v) in patch {
            if let Some(slot) = props.iter_mut().find(|(ek, _)| ek == k) {
                slot.1 = v.clone();
            } else {
                props.push((*k, v.clone()));
            }
        }
    }
    Ok(encode_props_record(&props))
}

/// Intern a SET clause's `(name, value)` pairs into global key ids. Rejects vector
/// values (SET on a vector-indexed property is unsupported in v1 — it would bypass
/// the vector store).
fn intern_set_props(
    set_props: &[(String, Value)],
    keys: &mut Interner,
) -> Result<Vec<(u32, Value)>> {
    let mut out = Vec::with_capacity(set_props.len());
    for (k, v) in set_props {
        if matches!(v, Value::Vector(_)) {
            bail!("SET {k} = vecf32(…): overwriting a vector property is not supported in v1");
        }
        out.push((keys.intern(k), v.clone()));
    }
    Ok(out)
}

/// Synthesise a node for a 0-match MERGE: it carries the match `label`, the match
/// `{key: value}`, and the SET props (last-wins if a SET targets the match key).
fn make_node(
    m: &NodeMatch,
    set_props: &[(u32, Value)],
    labels: &mut Interner,
    keys: &mut Interner,
) -> NodeRec {
    let label_gid = labels.intern(&m.label);
    let key_gid = keys.intern(&m.key);
    let mut props: Vec<(u32, Value)> = vec![(key_gid, m.value.clone())];
    for (k, v) in set_props {
        if let Some(slot) = props.iter_mut().find(|(ek, _)| ek == k) {
            slot.1 = v.clone();
        } else {
            props.push((*k, v.clone()));
        }
    }
    NodeRec {
        dump_id: None,
        labels_blob: encode_labels_record(&[label_gid]),
        props_blob: encode_props_record(&props),
        vec_props: Vec::new(),
    }
}

/// A hashable, exact-equality key for a [`Value`], so the match index can hash by
/// `(label, key, value)`. Floats hash by their raw bits (exact equality), which is
/// fine for identifier-typed match keys; `List`/`Vector` fall back to their wire
/// encoding.
#[derive(PartialEq, Eq, Hash)]
enum VKey {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    Str(String),
    Bytes(Vec<u8>),
}

fn vkey(v: &Value) -> VKey {
    match v {
        Value::Null => VKey::Null,
        Value::Bool(b) => VKey::Bool(*b),
        Value::Int(i) => VKey::Int(*i),
        Value::Float(f) => VKey::Float(f.to_bits()),
        Value::Str(s) => VKey::Str(s.clone()),
        other => {
            let mut b = Vec::new();
            write_value(&mut b, other);
            VKey::Bytes(b)
        }
    }
}

/// Read the value of `key_id` from a pre-encoded property record (no full decode).
fn value_of_key(props_blob: &[u8], key_id: u32) -> Result<Option<Value>> {
    let mut r = props_blob;
    let count = read_uvarint(&mut r)?;
    for _ in 0..count {
        let k = read_uvarint(&mut r)? as u32;
        if k == key_id {
            return Ok(Some(read_value(&mut r)?));
        }
        skip_value(&mut r)?;
    }
    Ok(None)
}
