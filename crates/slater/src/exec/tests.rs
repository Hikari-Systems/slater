// SPDX-License-Identifier: Apache-2.0
//! Unit tests for the parent module (extracted verbatim from the inline
//! `mod tests`; a pure relocation, no test logic changed).

use super::*;
use crate::generation::Generation;
use crate::parser;
use crate::testgen;
use graph_format::ids::Generation as GenId;

/// The writable-layer read overlay (Phase 1c): a delta patch on an existing
/// node's property overrides the core value last-writer-wins, a delta patch on
/// a *new* property name appears, and both the all-props path (`node_record` /
/// `properties()`) and the single-prop path (`n.key`) reflect it.
#[test]
fn delta_overlay_folds_node_property_patches() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("delta_overlay_unit");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Patch node 0 (Alice :Person, age=30): overwrite `age`, add new `rating`.
    let mut mem = Memtable::new();
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Alice".into()),
        Some(0),
        [
            ("age".to_string(), Value::Int(99)),
            ("rating".to_string(), Value::Str("AAA".into())),
        ],
    );
    let delta = DeltaSnapshot::from_memtable(Arc::new(mem));
    let view = MergedView::new(&gen, delta);

    // All-props path: node_record reflects the overwrite and the new property.
    let engine = Engine::new(&view, &cache);
    let (_labels, props) = engine.node_record(0).unwrap();
    let age = props.iter().find(|(k, _)| k == "age").map(|(_, v)| v);
    assert!(
        matches!(age, Some(Val::Int(99))),
        "age overwritten: {props:?}"
    );
    let rating = props.iter().find(|(k, _)| k == "rating").map(|(_, v)| v);
    assert!(
        matches!(rating, Some(Val::Str(s)) if s == "AAA"),
        "new property present: {props:?}"
    );
    // An unpatched node is untouched by the overlay.
    let (_l, p1) = engine.node_record(1).unwrap();
    let age1 = p1.iter().find(|(k, _)| k == "age").map(|(_, v)| v);
    assert!(
        matches!(age1, Some(Val::Int(25))),
        "node 1 untouched: {p1:?}"
    );

    // Single-prop path: `n.age` / `n.rating` read through the overlay too.
    let ast = parser::parse("MATCH (n:Person {name:'Alice'}) RETURN n.age, n.rating").unwrap();
    let res = Engine::new(&view, &cache).run(&ast).unwrap();
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(99)), "n.age via overlay");
    assert!(
        matches!(&res.rows[0][1], Val::Str(s) if s == "AAA"),
        "n.rating via overlay"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Stack a single upper core segment over a `write_basic` base and repoint `current`
/// at a set that lists it. The segment overrides base node 0 (full-row replace: keeps
/// `name`, changes `age` 30→99, adds a non-core-symbol prop `mood`, drops `city`/`team`),
/// tombstones base node 2, births node 5 (`:Person {name:'Zed', age:50}`) and edge 5
/// (`(0)-[:KNOWS {since:2099}]->(5)`). Returns `(root, graph, set_uuid)`.
fn write_basic_with_segment(tag: &str) -> (std::path::PathBuf, String, uuid::Uuid) {
    use graph_format::manifest::FileEntry;
    use graph_format::segindex::{write_index_fragments, IndexSpec};
    use graph_format::segmanifest::{
        DirtyIndex, SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION,
    };
    use graph_format::segment::{AdjEdge, EdgeRow, NodeRow, SegmentWriter};
    use graph_format::segpostings::{write_posting_fragments, PostingSpec};
    use graph_format::setmanifest::{SegmentRef, SetManifest};

    let (root, graph, base_uuid) = testgen::write_basic(tag);
    let seg_uuid = uuid::Uuid::from_u128(0x5_5e60_0000_0000_0000_0000_0000_0001);
    let set_uuid = uuid::Uuid::from_u128(0x5_5e70_0000_0000_0000_0000_0000_0001);

    let seg_dir = root
        .join(&graph)
        .join("segments")
        .join(seg_uuid.to_string());
    std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
    let mut w = SegmentWriter::create(&seg_dir, 0x22, 4096, 3).unwrap();
    // Nodes pushed in ascending dense-id order: override(0), tombstone(2), born(5).
    w.push_node(
        0,
        &NodeRow {
            labels: vec!["Person".into()],
            props: vec![
                ("name".into(), Value::Str("Alice".into())),
                ("age".into(), Value::Int(99)),
                ("mood".into(), Value::Str("calm".into())),
            ],
            tombstoned: false,
        },
    )
    .unwrap();
    w.push_node(2, &NodeRow::tombstone()).unwrap();
    w.push_node(
        5,
        &NodeRow {
            labels: vec!["Person".into()],
            props: vec![
                ("name".into(), Value::Str("Zed".into())),
                ("age".into(), Value::Int(50)),
            ],
            tombstoned: false,
        },
    )
    .unwrap();
    w.push_edge(
        5,
        &EdgeRow {
            src: 0,
            dst: 5,
            reltype: "KNOWS".into(),
            props: vec![("since".into(), Value::Int(2099))],
            tombstoned: false,
        },
    )
    .unwrap();
    // Adjacency fragments: born edge 5 (0→5 KNOWS) on both endpoints, and a removal of
    // base edge 4 (0→2 KNOWS) from node 0's outgoing list.
    w.push_adj_out(
        0,
        &[
            AdjEdge {
                other: 2,
                reltype: "KNOWS".into(),
                edge_id: 4,
                removed: true,
            },
            AdjEdge {
                other: 5,
                reltype: "KNOWS".into(),
                edge_id: 5,
                removed: false,
            },
        ],
    )
    .unwrap();
    w.push_adj_in(
        5,
        &[AdjEdge {
            other: 0,
            reltype: "KNOWS".into(),
            edge_id: 5,
            removed: false,
        }],
    )
    .unwrap();
    w.finish().unwrap();

    // Index fragments: the born/patched (value, id) pairs this segment carries, plus the
    // removal sidecar of base ids whose indexed value it supersedes (node 0's age moved
    // 30→99, node 2 tombstoned). name: node 0 keeps "Alice", so only Carol(2) is removed.
    write_index_fragments(
        &seg_dir,
        &[
            IndexSpec {
                label: "Person".into(),
                prop: "age".into(),
                entries: vec![(Value::Int(99), 0), (Value::Int(50), 5)],
                removals: vec![0, 2],
            },
            IndexSpec {
                label: "Person".into(),
                prop: "name".into(),
                entries: vec![(Value::Str("Zed".into()), 5)],
                removals: vec![2],
            },
        ],
        4096,
        3,
        None,
    )
    .unwrap();
    // Endpoint driving sets: the born edge 0-[:KNOWS]->5.
    write_posting_fragments(
        &seg_dir,
        &[PostingSpec {
            reltype: "KNOWS".into(),
            src_ids: vec![0],
            tgt_ids: vec![5],
        }],
    )
    .unwrap();

    let mut m = SegmentManifest {
        magic: SEGMENT_MAGIC.into(),
        version: SEGMENT_MANIFEST_VERSION,
        segment_uuid: GenId(seg_uuid),
        base: GenId(base_uuid),
        created_unix: 0,
        node_band: (5, 6), // one born node id
        edge_band: (5, 6), // one born edge id
        content_hash: String::new(),
        encryption: None,
        node_count_delta: 0, // +1 born (5), -1 tombstoned (2)
        edge_count_delta: 0, // +1 born (e5), -1 removed (e4)
        reltype_edge_deltas: vec![("KNOWS".into(), 0)], // KNOWS: +e5 -e4
        label_node_deltas: vec![("Person".into(), 0)],
        hub_degree_out_deltas: vec![],
        hub_degree_in_deltas: vec![],
        marginals_exact: true,
        dirty_vectors: vec![],
        dirty_indexes: vec![
            DirtyIndex {
                label: "Person".into(),
                property: "age".into(),
                fragment: "idx_0.isam".into(),
            },
            DirtyIndex {
                label: "Person".into(),
                property: "name".into(),
                fragment: "idx_1.isam".into(),
            },
        ],
        label_membership_touch: None,
        mac: None,
        files: vec![FileEntry {
            name: "node.blk".into(),
            bytes: 0,
            blake3: "aa".into(),
            sha256: None,
            crc32c: None,
        }],
    };
    m.set_content_hash();
    m.write_to_dir(&seg_dir).unwrap();

    let sets = root.join(&graph).join("sets");
    std::fs::create_dir_all(&sets).unwrap();
    let mut set = SetManifest::singleton(GenId(base_uuid), 0);
    set.set_uuid = GenId(set_uuid);
    set.segments = vec![SegmentRef::from_manifest(&m)];
    std::fs::write(
        sets.join(format!("{set_uuid}.json")),
        set.to_bytes().unwrap(),
    )
    .unwrap();
    std::fs::write(root.join(&graph).join("current"), set_uuid.to_string()).unwrap();
    (root, graph, set_uuid)
}

fn prop<'a>(props: &'a NamedProps, key: &str) -> Option<&'a Val> {
    props.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// Slice 1 parity oracle: the pre-streaming **materialised** adjacency fold
/// (core read → per-segment fragment fold → delta fold), reproduced verbatim so the
/// streaming [`for_each_adj_overlaid`] can be checked byte-for-byte against it. This is
/// the frozen behaviour of the old `read_adj_overlaid` before it became a `collect`.
#[cfg(test)]
fn materialised_adj_fold(
    gen: &dyn ReadView,
    cache: &BlockCache,
    node: u64,
    outgoing: bool,
) -> Vec<topology::Adj> {
    // core
    let mut core = if node >= gen.core_generation().node_count() {
        Vec::new()
    } else {
        let topo = gen.topology();
        let global = if outgoing {
            topo.outgoing_global(NodeId(node))
        } else {
            topo.incoming_global(NodeId(node))
        };
        let rec = cache
            .record(topo.inner(), gen.uuid(), FileKind::Topology, global)
            .unwrap();
        topology::decode_adj(&rec, outgoing).unwrap()
    };
    // per-segment fold, oldest→newest
    let stack = gen.core_stack();
    if !stack.is_singleton() {
        for seg in stack.segments() {
            let r = &seg.reader;
            let frag = if outgoing {
                if !r.may_hold_out_adj(node) {
                    continue;
                }
                r.out_adj(node).unwrap()
            } else {
                if !r.may_hold_in_adj(node) {
                    continue;
                }
                r.in_adj(node).unwrap()
            };
            if frag.is_empty() {
                continue;
            }
            let mut removed: HashSet<u64> = HashSet::new();
            let mut born: Vec<topology::Adj> = Vec::new();
            for e in frag {
                if e.removed {
                    removed.insert(e.edge_id);
                } else if let Some(rt) = gen.reltype_id(&e.reltype) {
                    born.push(topology::Adj {
                        reltype: rt,
                        neighbour: NodeId(e.other),
                        edge: EdgeId(e.edge_id),
                    });
                }
            }
            if !removed.is_empty() {
                core.retain(|a| !removed.contains(&a.edge.0));
            }
            core.extend(born);
        }
    }
    // delta fold
    let delta = gen.delta();
    if !delta.is_empty() {
        let deltas = if outgoing {
            delta.out_edges(node)
        } else {
            delta.in_edges(node)
        };
        let mut suppress: HashSet<(u32, u64)> = HashSet::new();
        let mut born: Vec<topology::Adj> = Vec::new();
        for e in deltas {
            let Some(rt) = gen.reltype_id(&e.reltype) else {
                continue;
            };
            if e.tombstoned {
                suppress.insert((rt, e.other));
            } else if let Some(eid) = e.edge_id {
                born.push(topology::Adj {
                    reltype: rt,
                    neighbour: NodeId(e.other),
                    edge: EdgeId(eid),
                });
            }
        }
        core.retain(|a| {
            !suppress.contains(&(a.reltype, a.neighbour.0)) && !delta.is_tombstoned(a.neighbour.0)
        });
        for a in born {
            if !delta.is_tombstoned(a.neighbour.0) {
                core.push(a);
            }
        }
    }
    core
}

/// Slice 1: the streaming [`for_each_adj_overlaid`] reproduces the materialised
/// core→segment→delta fold **byte-for-byte** — same edges, same order — across
/// core-only / segment / delta / tombstone / node-delete fixtures, and the result is
/// invariant to the emit `chunk` size (chunk boundaries never reorder or drop edges).
/// [`read_adj_overlaid`] (now a `collect`) is asserted equal to the oracle too.
#[test]
fn for_each_adj_overlaid_byte_parity() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    // Every node/direction: collect wrapper == oracle, and every chunk size streams the
    // same sequence with no empty/over-cap chunk.
    let check = |view: &MergedView, cache: &BlockCache, max_node: u64| {
        for node in 0..=max_node {
            for outgoing in [true, false] {
                let want = materialised_adj_fold(view, cache, node, outgoing);
                let got = read_adj_overlaid(view, cache, node, outgoing).unwrap();
                assert_eq!(got, want, "collect parity node={node} out={outgoing}");
                for chunk in [1usize, 2, 3, 8192] {
                    let mut streamed = Vec::new();
                    for_each_adj_overlaid(view, cache, node, outgoing, None, chunk, &mut |c| {
                        assert!(!c.is_empty(), "empty chunk node={node} chunk={chunk}");
                        assert!(c.len() <= chunk, "over-cap chunk node={node} chunk={chunk}");
                        streamed.extend_from_slice(c);
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(
                        streamed, want,
                        "stream parity node={node} out={outgoing} chunk={chunk}"
                    );
                }
            }
        }
    };

    // A: core-only — singleton stack + empty delta (both streaming fast paths).
    {
        let (root, graph, _) = testgen::write_basic("adj_stream_core");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        check(&view, &cache, 4);
        std::fs::remove_dir_all(&root).ok();
    }

    // B: one upper segment, empty delta — segment fold with a removed + born fragment.
    {
        let (root, graph, _) = write_basic_with_segment("adj_stream_seg");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        // Sanity: node 0's out list lost base e4 and gained segment e5 (fold is non-trivial).
        let out0 = read_adj_overlaid(&view, &cache, 0, true).unwrap();
        assert!(!out0.iter().any(|a| a.edge.0 == 4), "segment removed e4");
        assert!(out0.iter().any(|a| a.edge.0 == 5), "segment born e5");
        check(&view, &cache, 6);
        std::fs::remove_dir_all(&root).ok();
    }

    // C: segment + rich delta — born edge, edge suppression, and a node delete.
    {
        let (root, graph, _) = write_basic_with_segment("adj_stream_seg_delta");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let mut mem = Memtable::new();
        // Register both endpoints so the edge delete resolves core dense ids.
        mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
        mem.upsert_node("Person", "name", Value::Str("Bob".into()), Some(1), []);
        // Delta-born out-edge 0→3 (Acme) KNOWS.
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Company",
            "name",
            Value::Str("Acme".into()),
            Some(0),
            Some(3),
            [],
        );
        // Suppress base edge e0 (0→1 KNOWS).
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
        // Node delete: Globex (4) — drops any edge whose neighbour is 4.
        mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        // Sanity: the delta branches are actually live (else the oracle would trivially agree).
        assert!(view.delta().is_tombstoned(4), "node 4 tombstoned in delta");
        let knows = gen.reltype_id("KNOWS").unwrap();
        let out0 = read_adj_overlaid(&view, &cache, 0, true).unwrap();
        // e0 (0-[:KNOWS]->1) is delta-suppressed; the delta-born 0-[:KNOWS]->3 is present.
        // (Check by neighbour, not edge id — a bare Memtable numbers born ids from 0.)
        assert!(
            !out0
                .iter()
                .any(|a| a.reltype == knows && a.neighbour.0 == 1),
            "delta suppressed e0 (0->1 KNOWS)"
        );
        assert!(
            out0.iter()
                .any(|a| a.reltype == knows && a.neighbour.0 == 3),
            "delta-born 0->3 KNOWS present"
        );
        check(&view, &cache, 6);
        std::fs::remove_dir_all(&root).ok();
    }

    // D: delta only, no segments — empty stack fast path with a live delta + node delete.
    {
        let (root, graph, _) = testgen::write_basic("adj_stream_delta_only");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let mut mem = Memtable::new();
        mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Company",
            "name",
            Value::Str("Acme".into()),
            Some(0),
            Some(3),
            [],
        );
        mem.delete_node("Person", "name", Value::Str("Carol".into()), Some(2));
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        assert!(view.delta().is_tombstoned(2), "node 2 tombstoned in delta");
        check(&view, &cache, 4);
        std::fs::remove_dir_all(&root).ok();
    }
}

/// HIK-91: the write-path existence probe [`Engine::has_incident_edge`] short-circuits on
/// the **first** surviving edge instead of materialising the whole adjacency (the cost the
/// per-row plain-DELETE conformance check used to pay, once per row, for a hub). The
/// regression seam is [`ADJ_VISIT_COUNT`]: the probe walks O(1) edges; the materialising
/// `outgoing_adj` reader walks the node's full degree. Correctness is also pinned — a node
/// WITH relationships and one WITHOUT are both classified right, including edges that live
/// **only in the delta** (a delta-born edge counts; a delta-tombstoned core edge does not).
#[test]
fn has_incident_edge_short_circuits_and_is_overlay_exact() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let visits = || ADJ_VISIT_COUNT.with(|c| c.get());
    let reset = || ADJ_VISIT_COUNT.with(|c| c.set(0));

    let (root, graph, _) = testgen::write_basic("hik91_probe");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // --- Core-only view (empty delta). ---
    // Node 0 (Alice) is a mini-hub: out-edges e0(->1), e2(->3), e4(->2). The materialising
    // reader decodes all three; the probe stops at the first.
    {
        let view = MergedView::read_only(&gen);
        let engine = Engine::new(&view, &cache);

        reset();
        assert!(
            engine.has_incident_edge(0).unwrap(),
            "node 0 has outgoing relationships"
        );
        assert_eq!(
            visits(),
            1,
            "probe must short-circuit at the first edge, not walk the whole adjacency"
        );

        // Contrast: the full-materialise reader the pre-fix check used walks every out-edge.
        reset();
        let all = engine.outgoing_adj(0).unwrap();
        assert_eq!(all.len(), 3, "node 0 has three out-edges");
        assert_eq!(
            visits(),
            3,
            "materialising reader walks the whole list (the old cost)"
        );

        // Node 3 (Acme) has no out-edges but an incoming WORKS_AT (e2). The probe must check
        // both directions — it short-circuits on the single incoming edge (1 out-scan of 0
        // survivors + 1 in survivor).
        reset();
        assert!(
            engine.has_incident_edge(3).unwrap(),
            "node 3 has an incoming relationship"
        );
        assert_eq!(
            visits(),
            1,
            "incoming-only node: exactly one surviving edge visited"
        );
    }

    // --- Overlaid view: delta-born edge, delta-tombstoned core edge, isolated born node. ---
    {
        let knows = gen.reltype_id("KNOWS").unwrap();
        let works_at = gen.reltype_id("WORKS_AT").unwrap();
        let mut mem = Memtable::new();
        // A brand-new delta-born node (dense id 5) with a single born out-edge 5 -[:KNOWS]-> 1.
        mem.upsert_node("Person", "name", Value::Str("Dave".into()), Some(5), []);
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Dave".into()),
            "KNOWS",
            "Person",
            "name",
            Value::Str("Bob".into()),
            Some(5),
            Some(1),
            [],
        );
        // A second delta-born node (dense id 6) with NO edges — the "without relationships" case.
        mem.upsert_node("Person", "name", Value::Str("Eve".into()), Some(6), []);
        // Tombstone node 3 (Acme)'s only edge, the core WORKS_AT e2 (0->3), so node 3 becomes
        // relationship-free through the overlay even though the core carries an edge to it.
        mem.delete_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "WORKS_AT",
            "Company",
            "name",
            Value::Str("Acme".into()),
            Some(0),
            Some(3),
        );
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let engine = Engine::new(&view, &cache);

        // Delta-born node with an edge → has relationships (edge lives only in the delta).
        assert!(
            engine.has_incident_edge(5).unwrap(),
            "delta-born node 5 has a born out-edge"
        );
        // Delta-born node with no edges → no relationships.
        assert!(
            !engine.has_incident_edge(6).unwrap(),
            "delta-born node 6 is isolated"
        );
        // Core node whose sole edge is tombstoned by the delta → no relationships (a plain
        // DELETE of it must be allowed). This is why a core-only degree read is unsafe here.
        assert!(
            !engine.has_incident_edge(3).unwrap(),
            "node 3's only (core) edge is delta-tombstoned"
        );

        // find_outgoing_edge over the core-only view resolves the genuine core edge id and
        // short-circuits; a non-existent (reltype, dst) returns None.
        let core_view = MergedView::read_only(&gen);
        let core_engine = Engine::new(&core_view, &cache);
        reset();
        assert_eq!(
            core_engine.find_outgoing_edge(0, works_at, 3).unwrap(),
            Some(2),
            "0 -[:WORKS_AT]-> 3 is core edge e2"
        );
        assert_eq!(visits(), 1, "find stops at the matching edge");
        assert_eq!(
            core_engine.find_outgoing_edge(0, knows, 3).unwrap(),
            None,
            "there is no 0 -[:KNOWS]-> 3 edge"
        );
    }

    std::fs::remove_dir_all(&root).ok();
}

/// Slice 2: the streamed hop reader [`for_each_hop_overlaid`] yields the **same hops
/// in the same order** as the materialising [`hops_par`] — for every direction and a
/// range of type filters (untyped, a `:KNOWS` set, an empty set) — over core /
/// segment / segment+delta fixtures. This is the guarantee the hub routing rests on:
/// swapping a hub's materialise for a stream cannot change the traversal's result.
#[test]
fn for_each_hop_overlaid_matches_hops_par() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    // Hop has no PartialEq — compare by its full tuple projection.
    let key = |h: &Hop| (h.edge, h.neighbour, h.reltype, h.start, h.end);
    let check = |view: &MergedView, cache: &BlockCache, knows: u32, max_node: u64| {
        let tfs: Vec<Option<TypeFilter>> = vec![
            None,
            Some(TypeFilter::AnyOf(vec![knows])),
            Some(TypeFilter::AnyOf(vec![])),
        ];
        for node in 0..=max_node {
            for dir in [
                Direction::Outgoing,
                Direction::Incoming,
                Direction::Undirected,
            ] {
                for tf in &tfs {
                    let want = hops_par(view, cache, node, dir, tf.as_ref()).unwrap();
                    // A small chunk (3) forces multi-chunk streaming across boundaries.
                    let mut got = Vec::new();
                    for_each_hop_overlaid(view, cache, node, dir, tf.as_ref(), 3, &mut |c| {
                        got.extend_from_slice(c);
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(
                        got.iter().map(key).collect::<Vec<_>>(),
                        want.iter().map(key).collect::<Vec<_>>(),
                        "hop parity node={node} dir={dir:?} tf={tf:?}",
                        tf = tf.as_ref().map(|_| "some")
                    );
                }
            }
        }
    };

    // Core-only.
    {
        let (root, graph, _) = testgen::write_basic("hop_stream_core");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let knows = gen.reltype_id("KNOWS").unwrap();
        let view = MergedView::read_only(&gen);
        check(&view, &cache, knows, 4);
        std::fs::remove_dir_all(&root).ok();
    }
    // Segment + delta (born edge, edge-delete, node-delete) — the full overlay.
    {
        let (root, graph, _) = write_basic_with_segment("hop_stream_seg_delta");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let knows = gen.reltype_id("KNOWS").unwrap();
        let mut mem = Memtable::new();
        mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
        mem.upsert_node("Person", "name", Value::Str("Bob".into()), Some(1), []);
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Company",
            "name",
            Value::Str("Acme".into()),
            Some(0),
            Some(3),
            [],
        );
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
        mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        check(&view, &cache, knows, 6);
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Degree-sum terminal count fast path: a k-hop `count(endpoint)` answered by summing
/// effective degree over the penultimate frontier must equal the materialising walk —
/// across 1/2/3-hop, undirected, an anchor scan, and a live delta of edge writes — and
/// it must actually engage (not silently decline and pass via the walk). Node-deletes
/// and non-qualifying shapes decline to the walk, still correct.
#[test]
fn degree_terminal_count_matches_walk() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    fn pattern_of(q: &str) -> crate::parser::ast::Pattern {
        let ast = parser::parse(q).unwrap();
        let crate::parser::ast::Clause::Match(m) = &ast.head.reading[0] else {
            panic!("not a match: {q}");
        };
        m.patterns[0].clone()
    }
    let count = |view: &MergedView, cache: &BlockCache, q: &str| -> i64 {
        let ast = parser::parse(q).unwrap();
        match Engine::new(view, cache).run(&ast).unwrap().rows[0][0] {
            Val::Int(n) => n,
            ref v => panic!("count not int: {v:?}"),
        }
    };
    let rows = |view: &MergedView, cache: &BlockCache, q: &str| -> usize {
        let ast = parser::parse(q).unwrap();
        Engine::new(view, cache).run(&ast).unwrap().rows.len()
    };

    // Untyped final hops qualify even on write_basic's two-reltype graph (total degree
    // == matching count). Fast `count(m)` must equal the materialised `RETURN m` rows.
    let (root, graph, _) = testgen::write_basic("degterm");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    {
        let view = MergedView::read_only(&gen);
        let eng = Engine::new(&view, &cache);
        let cases = [
            (
                "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN count(m)",
                "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN m",
            ),
            (
                "MATCH (a:Person {name:'Alice'})-[]->()-[]->(m) RETURN count(m)",
                "MATCH (a:Person {name:'Alice'})-[]->()-[]->(m) RETURN m",
            ),
            (
                "MATCH (a:Person {name:'Alice'})-[]->()-[]->()-[]->(m) RETURN count(m)",
                "MATCH (a:Person {name:'Alice'})-[]->()-[]->()-[]->(m) RETURN m",
            ),
            (
                "MATCH (a:Person)-[]->(m) RETURN count(m)",
                "MATCH (a:Person)-[]->(m) RETURN m",
            ),
            (
                "MATCH (a:Person {name:'Alice'})-[]-(m) RETURN count(m)",
                "MATCH (a:Person {name:'Alice'})-[]-(m) RETURN m",
            ),
        ];
        for (fast, refq) in cases {
            assert!(
                eng.degree_terminal_dir(&pattern_of(fast)).is_some(),
                "degree terminal must engage for `{fast}`"
            );
            assert_eq!(
                count(&view, &cache, fast) as usize,
                rows(&view, &cache, refq),
                "count mismatch for `{fast}`"
            );
        }
        // Shapes that must decline (→ walk): typed final hop on a multi-reltype graph,
        // a filtered final node, a var-length hop, a path variable, a back-reference.
        for q in [
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(m) RETURN count(m)",
            "MATCH (a:Person {name:'Alice'})-[]->(m:Company) RETURN count(m)",
            "MATCH (a:Person {name:'Alice'})-[*1..2]->(m) RETURN count(m)",
            "MATCH p=(a:Person {name:'Alice'})-[]->(m) RETURN count(m)",
            "MATCH (a:Person {name:'Alice'})-[]->(a) RETURN count(a)",
        ] {
            assert!(
                eng.degree_terminal_dir(&pattern_of(q)).is_none(),
                "degree terminal must decline for `{q}`"
            );
        }
    }

    // Live delta of edge writes: the composed degree must reflect the born edges.
    {
        let mut mem = Memtable::new();
        for k in 0..3 {
            mem.upsert_edge(
                "Person",
                "name",
                Value::Str("Alice".into()),
                "KNOWS",
                "Person",
                "name",
                Value::Str(format!("newpal{k}")),
                Some(0),
                None,
                [],
            );
        }
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let eng = Engine::new(&view, &cache);
        let q = "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN count(m)";
        assert!(eng.degree_terminal_dir(&pattern_of(q)).is_some());
        assert_eq!(
            count(&view, &cache, q) as usize,
            rows(
                &view,
                &cache,
                "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN m"
            ),
            "delta-composed count must match the walk"
        );
    }

    // Pending node-delete ⇒ decline (non-local), but the walk still counts correctly.
    {
        let mut mem = Memtable::new();
        mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        let eng = Engine::new(&view, &cache);
        let q = "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN count(m)";
        assert!(
            eng.degree_terminal_dir(&pattern_of(q)).is_none(),
            "a pending node-delete must decline the degree terminal"
        );
        assert_eq!(
            count(&view, &cache, q) as usize,
            rows(
                &view,
                &cache,
                "MATCH (a:Person {name:'Alice'})-[]->(m) RETURN m"
            ),
        );
    }
    std::fs::remove_dir_all(&root).ok();
}

/// Slice 2: the hub routing probe [`Engine::effective_degree_ub`] is a **safe upper
/// bound** — it never under-counts a real hub, so no hub is ever mistaken for a normal
/// node and materialised. For every non-delta-tombstoned node the bound is ≥ the
/// actual overlaid degree (out+in for undirected); a delta-tombstoned node reports 0
/// (the documented "deleted, never expanded" contract).
#[test]
fn effective_degree_ub_never_undercounts() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let actual = |view: &MergedView, cache: &BlockCache, node: u64, dir: Direction| -> u64 {
        let deg = |outgoing: bool| {
            read_adj_overlaid(view, cache, node, outgoing)
                .unwrap()
                .len() as u64
        };
        match dir {
            Direction::Outgoing => deg(true),
            Direction::Incoming => deg(false),
            Direction::Undirected => deg(true) + deg(false),
        }
    };
    let check = |view: &MergedView, cache: &BlockCache, max_node: u64| {
        let engine = Engine::new(view, cache);
        for node in 0..=max_node {
            for dir in [
                Direction::Outgoing,
                Direction::Incoming,
                Direction::Undirected,
            ] {
                let ub = engine.effective_degree_ub(node, dir).unwrap();
                if view.delta().is_tombstoned(node) {
                    assert_eq!(ub, 0, "delta-tombstoned node {node} probes to 0");
                } else {
                    let got = actual(view, cache, node, dir);
                    assert!(
                        ub >= got,
                        "under-count node={node} dir={dir:?}: ub={ub} < actual={got}"
                    );
                }
            }
        }
    };

    // Core-only: the bound is exact (no deletions to over-count).
    {
        let (root, graph, _) = testgen::write_basic("ub_core");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let view = MergedView::read_only(&gen);
        check(&view, &cache, 4);
        std::fs::remove_dir_all(&root).ok();
    }
    // Core + delta with a born edge, an edge-delete, and a node-delete: core and delta
    // terms are exact, so the bound stays ≥ actual. (A *segment*-born edge below the
    // build floor is a documented, harmless under-count — the sidecar records only
    // `|Δ| >= floor` — so it is covered separately by
    // `segment_degree_delta_feeds_the_hub_probe`, not here.)
    {
        let (root, graph, _) = testgen::write_basic("ub_delta");
        let gen = Generation::open(&root, &graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let mut mem = Memtable::new();
        mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
        mem.upsert_node("Person", "name", Value::Str("Bob".into()), Some(1), []);
        mem.upsert_edge(
            "Person",
            "name",
            Value::Str("Alice".into()),
            "KNOWS",
            "Company",
            "name",
            Value::Str("Acme".into()),
            Some(0),
            Some(3),
            [],
        );
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
        mem.delete_node("Company", "name", Value::Str("Globex".into()), Some(4));
        let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
        assert!(view.delta().is_tombstoned(4));
        check(&view, &cache, 4);
        std::fs::remove_dir_all(&root).ok();
    }
}

/// Slice 3: with a hub-degree sidecar present, [`Engine::effective_degree_ub`] takes
/// its core term from the O(1) sidecar lookup — exact for a listed hub, `floor-1` for
/// a node below the floor — instead of reading the record's leading count. Attaches a
/// hand-written `hub_degrees.blk` to a `write_basic` fixture (node 0 out-degree 3;
/// node 2 in-degree 2) and re-seals the manifest, then checks the accessors and probe.
#[test]
fn effective_degree_ub_uses_hub_sidecar() {
    use crate::read_view::MergedView;
    use graph_format::integrity::{content_hash, hash_file};
    use graph_format::manifest::{FileEntry, HubDegreeDesc, Manifest};

    let (root, graph, uuid) = testgen::write_basic("hub_sidecar_reader");
    let gendir = root.join(&graph).join(uuid.to_string());
    // write_basic: node 0 out-edges e0→1, e2→3, e4→2 (out-degree 3); node 2 in-edges
    // e1(1→2), e4(0→2) (in-degree 2). Floor 2 ⇒ out-hub {0:3}, in-hub {2:2}.
    graph_format::hubdegree::write_hub_degrees(
        gendir.join("hub_degrees.blk"),
        &[(0, 3)],
        &[(2, 2)],
        4096,
        3,
        None,
    )
    .unwrap();

    // Re-seal the (plaintext, MAC-less) manifest: add the file to the inventory,
    // recompute the content hash, and record the descriptor.
    let mut m = Manifest::read_from_dir(&gendir).unwrap();
    let p = gendir.join("hub_degrees.blk");
    m.files.push(FileEntry {
        name: "hub_degrees.blk".into(),
        bytes: std::fs::metadata(&p).unwrap().len(),
        blake3: hash_file(&p).unwrap(),
        sha256: None,
        crc32c: None,
    });
    m.files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = m
        .files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    m.content_hash = content_hash(&inv);
    m.hub_degrees = Some(HubDegreeDesc {
        floor: 2,
        out_hubs: 1,
        in_hubs: 1,
    });
    m.write_to_dir(&gendir).unwrap();

    let gen = Generation::open(&root, &graph).unwrap();
    assert_eq!(gen.hub_degree_floor(), Some(2));
    assert_eq!(gen.core_out_degree_if_hub(0), Some(3));
    assert_eq!(gen.core_out_degree_if_hub(1), None, "out-degree 1 < floor");
    assert_eq!(gen.core_in_degree_if_hub(2), Some(2));
    assert_eq!(gen.core_in_degree_if_hub(0), None);

    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);
    // Empty delta/segments ⇒ the UB is exactly the sidecar core term.
    assert_eq!(
        engine.effective_degree_ub(0, Direction::Outgoing).unwrap(),
        3
    );
    // Node 1 is not listed out ⇒ UB = floor-1 = 1 (never under-counts its real 1).
    assert_eq!(
        engine.effective_degree_ub(1, Direction::Outgoing).unwrap(),
        1
    );
    assert_eq!(
        engine.effective_degree_ub(2, Direction::Incoming).unwrap(),
        2
    );
    std::fs::remove_dir_all(&root).ok();
}

/// Slice 5: `directed_edge_count` consults the pinned hub sidecar *before* the chunk-lazy
/// dense column, so a mega-hub's degree is answered from the resident sidecar and faults no
/// dense chunk. Builds a `write_basic` fixture with BOTH `hub_degrees.blk` (floor 2 ⇒ out-hub
/// {0:3}) and the dense `node_degrees.blk`, then asserts: a hub lookup returns the exact
/// degree with zero resident chunks; a non-hub lookup (below the floor) does fault its chunk.
#[test]
fn hub_lookup_skips_dense_chunk_fault() {
    use crate::read_view::MergedView;
    use graph_format::integrity::{content_hash, hash_file};
    use graph_format::manifest::{FileEntry, HubDegreeDesc, Manifest};

    let (root, graph, uuid) = testgen::write_basic("hub_before_dense");
    let gendir = root.join(&graph).join(uuid.to_string());
    // write_basic degrees: out=[3,1,1,0,0], in=[0,1,2,1,1] over 5 nodes.
    graph_format::hubdegree::write_hub_degrees(
        gendir.join("hub_degrees.blk"),
        &[(0, 3)],
        &[(2, 2)],
        4096,
        3,
        None,
    )
    .unwrap();
    graph_format::nodedegree::write_node_degrees(
        gendir.join("node_degrees.blk"),
        &[3, 1, 1, 0, 0],
        &[0, 1, 2, 1, 1],
        4096,
        graph_format::degree_ef::DegreeCodecOpts::default(),
        None,
    )
    .unwrap();

    // Re-seal the plaintext manifest: add both files to the inventory, record the sidecar
    // descriptor, and recompute the content hash.
    let mut m = Manifest::read_from_dir(&gendir).unwrap();
    for name in ["hub_degrees.blk", "node_degrees.blk"] {
        let p = gendir.join(name);
        m.files.push(FileEntry {
            name: name.into(),
            bytes: std::fs::metadata(&p).unwrap().len(),
            blake3: hash_file(&p).unwrap(),
            sha256: None,
            crc32c: None,
        });
    }
    m.files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = m
        .files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    m.content_hash = content_hash(&inv);
    m.hub_degrees = Some(HubDegreeDesc {
        floor: 2,
        out_hubs: 1,
        in_hubs: 1,
    });
    m.write_to_dir(&gendir).unwrap();

    let gen = Generation::open(&root, &graph).unwrap();
    assert_eq!(gen.degree_column_resident_chunks(), Some(0), "cold at open");
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);

    // Node 0 is an out-hub ⇒ answered by the sidecar, exact, no dense chunk faulted.
    assert_eq!(engine.directed_edge_count(0, true).unwrap(), 3);
    assert_eq!(
        gen.degree_column_resident_chunks(),
        Some(0),
        "hub answered from the sidecar must not fault a dense chunk"
    );

    // Node 1 (out-degree 1 < floor) is not a hub ⇒ falls through to the dense column,
    // which faults its chunk. Value is exact.
    assert_eq!(engine.directed_edge_count(1, true).unwrap(), 1);
    assert_eq!(
        gen.degree_column_resident_chunks(),
        Some(1),
        "a non-hub lookup faults the dense chunk"
    );
    // Node 2 in-degree 2 is an in-hub ⇒ sidecar again, no new (in-half) chunk faulted.
    assert_eq!(engine.directed_edge_count(2, false).unwrap(), 2);
    assert_eq!(gen.degree_column_resident_chunks(), Some(1));

    std::fs::remove_dir_all(&root).ok();
}

/// Slice 4: a flush that borns many edges from one node records that node's out-degree
/// delta in the segment manifest (`|Δ| >= floor`), the `CoreStack` fold sums it, and
/// `effective_degree_ub` adds it to the core term — the O(#segments) segment path of the
/// hub probe, end to end (write → flush → segment manifest → fold → probe).
#[test]
fn segment_degree_delta_feeds_the_hub_probe() {
    use crate::cache::VectorIndexCache;
    use crate::config::DeltaConfig;
    use crate::read_view::MergedView;
    use crate::server::{execute_edge_write, Graphs};
    use std::collections::HashMap;

    let floor = graph_format::hubdegree::DEFAULT_HUB_DEGREE_FLOOR as u64;
    let born = floor + 6; // 1030 born out-edges from Alice ⇒ Δ = 1030 >= floor
    let (root, graph, _) = testgen::write_basic("seg_degree_delta");
    let wal = root.join("_wal");
    let cfg = DeltaConfig {
        enabled: true,
        wal_dir: wal.to_string_lossy().into_owned(),
        memtable_bytes: 256 << 20,
        l0_compaction_trigger: 0,
        segment_flush_bytes: 0,
        max_upper_segments: 0,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: String::new(),
        builder_bin: "slater-build".to_string(),
        off_heap_l0: false,
        segment_gc_grace_secs: 0,
    };
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs.enable_writable_layer(&cfg, &root, None).unwrap();
    {
        let gen = graphs.get(&graph).unwrap();
        let writer = graphs.writer(&graph).unwrap();
        for k in 0..born {
            let q = format!(
                "MERGE (a:Person {{name:'Alice'}})-[:KNOWS]->(c:Person {{name:'hubleaf{k}'}})"
            );
            match parser::parse_statement(&q).unwrap() {
                parser::ast::Statement::WriteEdge(w) => {
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                other => panic!("expected an edge write, got {other:?}"),
            }
        }
    }
    graphs
        .flush_graph_to_segment(&graph, &vc, &root)
        .unwrap()
        .expect("a non-empty delta flushes to a segment");

    let gen = graphs.get(&graph).unwrap();
    assert_eq!(gen.stack().segments().len(), 1);
    // The segment manifest records Alice (node 0) with the exact out-degree delta.
    let out_deltas = &gen.stack().segments()[0].manifest.hub_degree_out_deltas;
    assert_eq!(
        out_deltas.iter().find(|(id, _)| *id == 0).map(|(_, d)| *d),
        Some(born as i64),
        "segment out-degree delta for Alice: {out_deltas:?}"
    );
    // The fold sums it; the probe adds it to the (block-peek) core term (no core sidecar
    // on this fixture): core out-degree 3 + segment Δ = 3 + born.
    assert_eq!(gen.stack().hub_out_degree_delta(0), born as i64);
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(gen.as_ref());
    let engine = Engine::new(&view, &cache);
    assert_eq!(
        engine.effective_degree_ub(0, Direction::Outgoing).unwrap(),
        3 + born,
    );
    // With a low stream threshold the node is now a hub via the segment delta alone.
    let hub_engine = Engine::new(&view, &cache).with_adj_stream_threshold(floor);
    assert!(hub_engine.is_hub(0, Direction::Outgoing).unwrap());
    std::fs::remove_dir_all(&root).ok();
}

/// A segment full row overrides/extends the base node reads it carries, births new
/// entities, and tombstones nodes — through both `node_record` (all-props) and the
/// single-property path. This is the read oracle for slice 3.2.
#[test]
fn segment_full_row_overrides_and_extends_reads() {
    use crate::read_view::MergedView;
    let (root, graph, set_uuid) = write_basic_with_segment("seg_full_row_reads");
    let gen = Generation::open(&root, &graph).unwrap();
    assert_eq!(gen.uuid(), GenId(set_uuid));
    assert_eq!(gen.stack().segments().len(), 1);
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);

    // Overridden node 0: full-row replace — age 30→99, new `mood`, and `city`/`team` gone.
    let (labels0, p0) = engine.node_record(0).unwrap();
    assert_eq!(labels0, vec!["Person".to_string()]);
    assert!(matches!(prop(&p0, "name"), Some(Val::Str(s)) if s == "Alice"));
    assert!(matches!(prop(&p0, "age"), Some(Val::Int(99))), "{p0:?}");
    assert!(matches!(prop(&p0, "mood"), Some(Val::Str(s)) if s == "calm"));
    assert!(
        prop(&p0, "city").is_none(),
        "full-row replace drops base props: {p0:?}"
    );
    assert!(prop(&p0, "team").is_none(), "{p0:?}");
    // Single-property path agrees, including the non-core-symbol key `mood`.
    assert!(matches!(engine.node_prop(0, "age").unwrap(), Val::Int(99)));
    assert!(matches!(engine.node_prop(0, "mood").unwrap(), Val::Str(s) if s == "calm"));
    assert!(matches!(engine.node_prop(0, "city").unwrap(), Val::Null));

    // Born node 5.
    let (labels5, p5) = engine.node_record(5).unwrap();
    assert_eq!(labels5, vec!["Person".to_string()]);
    assert!(matches!(prop(&p5, "name"), Some(Val::Str(s)) if s == "Zed"));
    assert!(matches!(engine.node_prop(5, "age").unwrap(), Val::Int(50)));

    // Tombstoned node 2: no labels, no props.
    let (labels2, p2) = engine.node_record(2).unwrap();
    assert!(
        labels2.is_empty() && p2.is_empty(),
        "tombstoned: {labels2:?} {p2:?}"
    );

    // Untouched base node 1 reads straight from the base.
    let (_l1, p1) = engine.node_record(1).unwrap();
    assert!(matches!(prop(&p1, "age"), Some(Val::Int(25))));
    assert!(matches!(prop(&p1, "city"), Some(Val::Str(s)) if s == "London"));

    // Born edge 5 resolves its full row; base edge 0 is untouched.
    let knows = gen.reltype_id("KNOWS").unwrap();
    let (t5, ep5) = engine.rel_record(5, knows).unwrap();
    assert_eq!(t5, "KNOWS");
    assert!(
        matches!(prop(&ep5, "since"), Some(Val::Int(2099))),
        "{ep5:?}"
    );
    assert!(matches!(
        engine.edge_prop(5, "since").unwrap(),
        Val::Int(2099)
    ));
    let (_t0, ep0) = engine.rel_record(0, knows).unwrap();
    assert!(
        matches!(prop(&ep0, "since"), Some(Val::Int(2020))),
        "{ep0:?}"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// The write-delta sits above the segment stack: a delta patch wins over a segment full
/// row (delta > segment > base), for both the all-props and single-property paths.
#[test]
fn delta_wins_over_segment_full_row() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = write_basic_with_segment("seg_delta_precedence");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Patch node 0 (already segment-overridden to age 99): the delta sets age 7.
    let mut mem = Memtable::new();
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Alice".into()),
        Some(0),
        [("age".to_string(), Value::Int(7))],
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let engine = Engine::new(&view, &cache);

    let (_l0, p0) = engine.node_record(0).unwrap();
    assert!(
        matches!(prop(&p0, "age"), Some(Val::Int(7))),
        "delta wins: {p0:?}"
    );
    // The segment's other props still show through where the delta is silent.
    assert!(matches!(prop(&p0, "mood"), Some(Val::Str(s)) if s == "calm"));
    assert!(matches!(engine.node_prop(0, "age").unwrap(), Val::Int(7)));
    std::fs::remove_dir_all(&root).ok();
}

/// A segment's adjacency fragments fold over the base neighbour list: a `removed` entry
/// suppresses a base edge, a born entry appends one, and an untouched node reads its base
/// adjacency unchanged (its fence skips the segment). The read oracle for slice 3.3.
#[test]
fn segment_adjacency_fragments_merge_over_base() {
    use crate::read_view::MergedView;
    let (root, graph, _) = write_basic_with_segment("seg_adjacency");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);
    let knows = gen.reltype_id("KNOWS").unwrap();
    let works = gen.reltype_id("WORKS_AT").unwrap();

    let triples = |adj: &[topology::Adj]| -> Vec<(u64, u32, u64)> {
        let mut v: Vec<_> = adj
            .iter()
            .map(|a| (a.neighbour.0, a.reltype, a.edge.0))
            .collect();
        v.sort();
        v
    };

    // Base node 0 out-edges: →1 (KNOWS e0), →3 (WORKS_AT e2), →2 (KNOWS e4). The segment
    // removes e4 and adds e5 (→5 KNOWS).
    assert_eq!(
        triples(&engine.outgoing(0).unwrap()),
        vec![(1, knows, 0), (3, works, 2), (5, knows, 5)],
    );
    // Incoming to born node 5 is the born edge alone (no base row for a synthetic id).
    assert_eq!(triples(&engine.incoming(5).unwrap()), vec![(0, knows, 5)]);
    // A node with no fragment in the segment reads its base adjacency unchanged.
    assert_eq!(
        triples(&engine.outgoing(1).unwrap()),
        vec![(2, knows, 1)], // base edge e1: 1→2 KNOWS
    );

    // Under a delta that adds one more out-edge from node 0, all three layers compose.
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;
    let mut mem = Memtable::new();
    mem.upsert_node("Person", "name", Value::Str("Alice".into()), Some(0), []);
    // A second, delta-born out-edge from node 0: 0→3 (Acme) KNOWS.
    mem.upsert_edge(
        "Person",
        "name",
        Value::Str("Alice".into()),
        "KNOWS",
        "Company",
        "name",
        Value::Str("Acme".into()),
        Some(0),
        Some(3),
        [],
    );
    let dview = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let deng = Engine::new(&dview, &cache);
    let out0 = deng.outgoing(0).unwrap();
    // base e0(→1), base e2(→3 WORKS_AT), segment e5(→5), delta born(→3 KNOWS); e4 gone.
    assert_eq!(out0.len(), 4, "{:?}", triples(&out0));
    assert!(out0
        .iter()
        .any(|a| a.neighbour.0 == 5 && a.reltype == knows));
    assert!(
        !out0.iter().any(|a| a.edge.0 == 4),
        "removed edge stays gone under a delta"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// The scan_candidates seam merges segment index fragments (base hits minus removals ∪
/// the segments' matching born/patched ids), recomputes label membership over segment
/// full rows, and unions endpoint postings — with tombstoned nodes suppressed. The read
/// oracle for slice 3.4.
#[test]
fn segment_index_label_and_reltype_scans_merge() {
    use crate::plan::NodeScan;
    use crate::read_view::MergedView;
    let (root, graph, _) = write_basic_with_segment("seg_scans");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);

    let eq = |age: i64| -> Vec<u64> {
        let mut v = engine
            .scan_candidates(&NodeScan::RangeEq {
                index: "node_Person_age".into(),
                key: Value::Int(age),
            })
            .unwrap();
        v.sort_unstable();
        v
    };
    // Node 0's age moved 30→99 (found at 99, gone at 30); node 5 born at 50; node 2
    // (age 40) tombstoned, so its stale base entry is suppressed by the removal sidecar.
    assert_eq!(eq(99), vec![0]);
    assert_eq!(eq(30), Vec::<u64>::new());
    assert_eq!(eq(50), vec![5]);
    assert_eq!(eq(40), Vec::<u64>::new());
    assert_eq!(eq(25), vec![1]); // untouched base node Bob

    // Range: age >= 45 → the moved node 0 (99) and born node 5 (50); base 30/25/40 excluded.
    let mut rng = engine
        .scan_candidates(&NodeScan::RangeRange {
            index: "node_Person_age".into(),
            lo: Some((Value::Int(45), true)),
            hi: None,
        })
        .unwrap();
    rng.sort_unstable();
    assert_eq!(rng, vec![0, 5]);

    // Label scan: Person = {Alice(0, overridden, still Person), Bob(1), Zed(5, born)};
    // Carol(2) tombstoned and dropped.
    let person = gen.label_id("Person").unwrap();
    let mut labs = engine
        .scan_candidates(&NodeScan::LabelScan { label_id: person })
        .unwrap();
    labs.sort_unstable();
    assert_eq!(labs, vec![0, 1, 5]);
    // (RelTypeScan's segment-posting union is exercised in
    // `segment_reltype_scan_unions_postings`, which uses a base fixture carrying the
    // endpoint postings a `RelTypeScan` requires.)

    std::fs::remove_dir_all(&root).ok();
}

/// Stack a **births-only** segment (no tombstones/removals, so its marginals are trivially
/// self-consistent) over a `write_basic` base: born node 5 (`:Person {name:'Zed'}`) and
/// born edge 5 (`(0)-[:KNOWS]->(5)`) with adjacency. Returns `(root, graph, seg_uuid)`.
fn write_basic_with_born_segment(tag: &str) -> (std::path::PathBuf, String, uuid::Uuid) {
    use graph_format::manifest::FileEntry;
    use graph_format::segmanifest::{SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION};
    use graph_format::segment::{AdjEdge, EdgeRow, NodeRow, SegmentWriter};
    use graph_format::setmanifest::{SegmentRef, SetManifest};

    let (root, graph, base_uuid) = testgen::write_basic(tag);
    let seg_uuid = uuid::Uuid::from_u128(0x5_5eb0_0000_0000_0000_0000_0000_0001);
    let set_uuid = uuid::Uuid::from_u128(0x5_5eb1_0000_0000_0000_0000_0000_0001);
    let seg_dir = root
        .join(&graph)
        .join("segments")
        .join(seg_uuid.to_string());
    std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
    let mut w = SegmentWriter::create(&seg_dir, 0x44, 4096, 3).unwrap();
    w.push_node(
        5,
        &NodeRow {
            labels: vec!["Person".into()],
            props: vec![("name".into(), Value::Str("Zed".into()))],
            tombstoned: false,
        },
    )
    .unwrap();
    w.push_adj_out(
        0,
        &[AdjEdge {
            other: 5,
            reltype: "KNOWS".into(),
            edge_id: 5,
            removed: false,
        }],
    )
    .unwrap();
    w.push_adj_in(
        5,
        &[AdjEdge {
            other: 0,
            reltype: "KNOWS".into(),
            edge_id: 5,
            removed: false,
        }],
    )
    .unwrap();
    w.push_edge(
        5,
        &EdgeRow {
            src: 0,
            dst: 5,
            reltype: "KNOWS".into(),
            props: vec![],
            tombstoned: false,
        },
    )
    .unwrap();
    w.finish().unwrap();

    let mut m = SegmentManifest {
        magic: SEGMENT_MAGIC.into(),
        version: SEGMENT_MANIFEST_VERSION,
        segment_uuid: GenId(seg_uuid),
        base: GenId(base_uuid),
        created_unix: 0,
        node_band: (5, 6),
        edge_band: (5, 6),
        content_hash: String::new(),
        encryption: None,
        node_count_delta: 1,
        edge_count_delta: 1,
        reltype_edge_deltas: vec![("KNOWS".into(), 1)],
        label_node_deltas: vec![("Person".into(), 1)],
        hub_degree_out_deltas: vec![],
        hub_degree_in_deltas: vec![],
        marginals_exact: true,
        dirty_vectors: vec![],
        dirty_indexes: vec![],
        label_membership_touch: None,
        mac: None,
        files: vec![FileEntry {
            name: "node.blk".into(),
            bytes: 0,
            blake3: "aa".into(),
            sha256: None,
            crc32c: None,
        }],
    };
    m.set_content_hash();
    m.write_to_dir(&seg_dir).unwrap();
    let sets = root.join(&graph).join("sets");
    std::fs::create_dir_all(&sets).unwrap();
    let mut set = SetManifest::singleton(GenId(base_uuid), 0);
    set.set_uuid = GenId(set_uuid);
    set.segments = vec![SegmentRef::from_manifest(&m)];
    std::fs::write(
        sets.join(format!("{set_uuid}.json")),
        set.to_bytes().unwrap(),
    )
    .unwrap();
    std::fs::write(root.join(&graph).join("current"), set_uuid.to_string()).unwrap();
    (root, graph, seg_uuid)
}

/// Whole-graph counts are answered from the summed segment marginals (node/label/edge/
/// reltype), and a segment whose marginals are not exact declines to full execution —
/// which is segment-aware and yields the same answer. The read oracle for slice 3.5.
#[test]
fn segment_marginals_answer_counts_and_decline_when_inexact() {
    use crate::read_view::MergedView;
    use graph_format::segmanifest::SegmentManifest;
    let (root, graph, seg_uuid) = write_basic_with_born_segment("seg_counts");
    let seg_dir = root
        .join(&graph)
        .join("segments")
        .join(seg_uuid.to_string());
    let cache = BlockCache::new(1 << 20);

    let count = |view: &MergedView, q: &str| -> i64 {
        let res = Engine::new(view, &cache)
            .run(&parser::parse(q).unwrap())
            .unwrap();
        match res.rows[0][0] {
            Val::Int(n) => n,
            ref v => panic!("expected Int, got {v:?}"),
        }
    };
    let reltype_groups = |view: &MergedView| -> Vec<(String, i64)> {
        let res = Engine::new(view, &cache)
            .run(&parser::parse("MATCH ()-[r]->() RETURN type(r), count(*)").unwrap())
            .unwrap();
        let mut g: Vec<(String, i64)> = res
            .rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Val::Str(s), Val::Int(c)) => (s.clone(), *c),
                other => panic!("{other:?}"),
            })
            .collect();
        g.sort();
        g
    };

    // Live estate = base 5 nodes + Zed(5); base 5 edges + e5. Answered from marginals.
    let gen = Generation::open(&root, &graph).unwrap();
    {
        let view = MergedView::read_only(&gen);
        assert_eq!(count(&view, "MATCH (n) RETURN count(*)"), 6);
        assert_eq!(count(&view, "MATCH (n:Person) RETURN count(*)"), 4); // + Zed
        assert_eq!(count(&view, "MATCH (n:Company) RETURN count(*)"), 2); // untouched
        assert_eq!(count(&view, "MATCH ()-[r]->() RETURN count(*)"), 6);
        // KNOWS = e0,e1,e4,e5 = 4; WORKS_AT = e2,e3 = 2.
        assert_eq!(
            reltype_groups(&view),
            vec![("KNOWS".to_string(), 4), ("WORKS_AT".to_string(), 2)]
        );
    }

    // Flip the segment's marginals to inexact: the count fast paths must decline and full
    // execution (segment-aware) must still return the same answers.
    let mut m = SegmentManifest::read_from_dir(&seg_dir).unwrap();
    m.marginals_exact = false;
    m.write_to_dir(&seg_dir).unwrap();
    let gen2 = Generation::open(&root, &graph).unwrap();
    let view2 = MergedView::read_only(&gen2);
    assert_eq!(
        count(&view2, "MATCH (n) RETURN count(*)"),
        6,
        "decline → full exec"
    );
    assert_eq!(count(&view2, "MATCH (n:Person) RETURN count(*)"), 4);
    assert_eq!(count(&view2, "MATCH ()-[r]->() RETURN count(*)"), 6);
    assert_eq!(
        reltype_groups(&view2),
        vec![("KNOWS".to_string(), 4), ("WORKS_AT".to_string(), 2)]
    );

    std::fs::remove_dir_all(&root).ok();
}

/// A `RelTypeScan` unions each segment's endpoint driving set over the base postings
/// (over-inclusion is safe — the first hop re-filters by reltype). Uses a base fixture
/// that carries the endpoint postings a `RelTypeScan` needs.
#[test]
fn segment_reltype_scan_unions_postings() {
    use crate::plan::NodeScan;
    use crate::read_view::MergedView;
    use graph_format::manifest::FileEntry;
    use graph_format::segmanifest::{SegmentManifest, SEGMENT_MAGIC, SEGMENT_MANIFEST_VERSION};
    use graph_format::segment::{NodeRow, SegmentWriter};
    use graph_format::segpostings::{write_posting_fragments, PostingSpec};
    use graph_format::setmanifest::{SegmentRef, SetManifest};

    let (root, graph) = testgen::write_rel_sparse("seg_reltype_scan");
    let base_uuid = Generation::current_uuid(&root, &graph).unwrap();
    let seg_uuid = uuid::Uuid::from_u128(0x5_5e60_0000_0000_0000_0000_0000_0009);
    let set_uuid = uuid::Uuid::from_u128(0x5_5e70_0000_0000_0000_0000_0000_0009);

    // A segment that births node 6 (:N) with a new outgoing T-edge, so its endpoint
    // posting adds node 6 to T's source driving set (base T sources are {0,1}).
    let seg_dir = root
        .join(&graph)
        .join("segments")
        .join(seg_uuid.to_string());
    std::fs::create_dir_all(seg_dir.parent().unwrap()).unwrap();
    let mut w = SegmentWriter::create(&seg_dir, 0x33, 4096, 3).unwrap();
    w.push_node(
        6,
        &NodeRow {
            labels: vec!["N".into()],
            props: vec![("name".into(), Value::Str("g".into()))],
            tombstoned: false,
        },
    )
    .unwrap();
    w.finish().unwrap();
    write_posting_fragments(
        &seg_dir,
        &[PostingSpec {
            reltype: "T".into(),
            src_ids: vec![6],
            tgt_ids: vec![],
        }],
    )
    .unwrap();

    let mut m = SegmentManifest {
        magic: SEGMENT_MAGIC.into(),
        version: SEGMENT_MANIFEST_VERSION,
        segment_uuid: GenId(seg_uuid),
        base: GenId(base_uuid),
        created_unix: 0,
        node_band: (6, 7),
        edge_band: (3, 3),
        content_hash: String::new(),
        encryption: None,
        node_count_delta: 1,
        edge_count_delta: 0,
        reltype_edge_deltas: vec![],
        label_node_deltas: vec![("N".into(), 1)],
        hub_degree_out_deltas: vec![],
        hub_degree_in_deltas: vec![],
        marginals_exact: true,
        dirty_vectors: vec![],
        dirty_indexes: vec![],
        label_membership_touch: None,
        mac: None,
        files: vec![FileEntry {
            name: "node.blk".into(),
            bytes: 0,
            blake3: "aa".into(),
            sha256: None,
            crc32c: None,
        }],
    };
    m.set_content_hash();
    m.write_to_dir(&seg_dir).unwrap();
    let sets = root.join(&graph).join("sets");
    std::fs::create_dir_all(&sets).unwrap();
    let mut set = SetManifest::singleton(GenId(base_uuid), 0);
    set.set_uuid = GenId(set_uuid);
    set.segments = vec![SegmentRef::from_manifest(&m)];
    std::fs::write(
        sets.join(format!("{set_uuid}.json")),
        set.to_bytes().unwrap(),
    )
    .unwrap();
    std::fs::write(root.join(&graph).join("current"), set_uuid.to_string()).unwrap();

    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);
    let t = gen.reltype_id("T").unwrap();
    let mut srcs = engine
        .scan_candidates(&NodeScan::RelTypeScan {
            reltype_ids: vec![t],
            side: RelEndpointSide::Source,
            guaranteed_label: None,
        })
        .unwrap();
    srcs.sort_unstable();
    assert_eq!(
        srcs,
        vec![0, 1, 6],
        "base T sources {{0,1}} ∪ segment {{6}}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// `algo.*` procedures build their subgraph view over the *effective* estate: the
/// label-filtered node set now includes a segment-born node carrying the label (it went
/// through the base label postings only before slice 3.6's fix). Regression guard for the
/// adversarial-review finding.
#[test]
fn algo_view_includes_segment_born_labelled_node() {
    use crate::read_view::MergedView;
    let (root, graph, _) = write_basic_with_born_segment("seg_algo_view");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);

    // Base :Person = {Alice, Bob, Carol}; the segment births Zed (:Person). The WCC view
    // over :Person must span all four, so the row count is 4, not the base-only 3.
    let res = engine
        .run(
            &parser::parse(
                "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
                     RETURN count(*)",
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        matches!(res.rows[0][0], Val::Int(4)),
        "{:?}",
        res.rows[0][0]
    );
    std::fs::remove_dir_all(&root).ok();
}

/// A stacked set opens and answers queries identically through a non-filesystem backend
/// (mem store), exercising the store-native segment reader path end-to-end (the segments
/// live on the same object store as the base). Conformance for slice 3.6.
#[test]
fn stacked_set_opens_and_reads_over_mem_store() {
    use crate::read_view::MergedView;
    use graph_format::store::mem::MemObjectStore;
    use graph_format::store::ObjectStore;

    fn load_tree(store: &MemObjectStore, root: &std::path::Path, dir: &std::path::Path) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                load_tree(store, root, &path);
            } else {
                let key = path
                    .strip_prefix(root)
                    .unwrap()
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                store
                    .put(&key, &std::fs::read(&path).unwrap(), None)
                    .unwrap();
            }
        }
    }

    let (root, graph, _) = write_basic_with_born_segment("seg_mem_store");
    let mem = MemObjectStore::new();
    load_tree(&mem, &root, &root);

    let gen = Generation::open_with_store(&mem, &graph, None).unwrap();
    assert_eq!(
        gen.stack().segments().len(),
        1,
        "segment loaded via the mem store"
    );
    let cache = BlockCache::new(1 << 20);
    let view = MergedView::read_only(&gen);
    let engine = Engine::new(&view, &cache);

    // Born node 5 reads its full row through the store; whole-graph count is marginal-summed.
    let (labels, props) = engine.node_record(5).unwrap();
    assert_eq!(labels, vec!["Person".to_string()]);
    assert!(matches!(prop(&props, "name"), Some(Val::Str(s)) if s == "Zed"));
    let res = engine
        .run(&parser::parse("MATCH (n) RETURN count(*)").unwrap())
        .unwrap();
    assert!(matches!(res.rows[0][0], Val::Int(6)));
    // Its born adjacency resolves too.
    let knows = gen.reltype_id("KNOWS").unwrap();
    assert!(engine
        .incoming(5)
        .unwrap()
        .iter()
        .any(|a| a.neighbour.0 == 0 && a.reltype == knows));

    std::fs::remove_dir_all(&root).ok();
}

/// Every pure scalar function delegated to `slater-scalar` must still be
/// advertised by `CALL dbms.functions()` (the registry the planner validates
/// against), so the extraction did not silently drop a name.
#[test]
fn pure_functions_are_advertised() {
    for name in slater_scalar::PURE_FUNCTIONS {
        assert!(
            IMPLEMENTED_FUNCTIONS.contains(name),
            "slater-scalar advertises `{name}` but IMPLEMENTED_FUNCTIONS does not"
        );
    }
}

/// Smoke-test the delegation path: a scalar call routes through `slater-scalar`
/// and a `coalesce` over a runtime-only `Val` still uses the local fallback.
#[test]
fn scalar_delegation_and_runtime_fallback() {
    let (root, graph, _) = testgen::write_basic("scalar_delegation");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(64 << 20);
    let eng = Engine::new(&gen, &cache);
    // delegated to slater-scalar (compare via to_display — Val is not PartialEq)
    assert_eq!(
        eng.call_function("toUpper", false, vec![Val::Str("ab".into())])
            .unwrap()
            .to_display(),
        "AB"
    );
    assert_eq!(
        eng.call_function("round", false, vec![Val::Float(2.5)])
            .unwrap()
            .to_display(),
        "3"
    );
    // coalesce with a runtime-only first arg keeps the local fallback (returns
    // the node, which has no `Value` projection)
    assert!(matches!(
        eng.call_function("coalesce", false, vec![Val::Node(7), Val::Null])
            .unwrap(),
        Val::Node(7)
    ));
}

/// Open the shared fixture and run `q`, returning the result.
fn run(root_tag: &str, q: &str) -> (std::path::PathBuf, QueryResult) {
    let (root, graph, _) = testgen::write_basic(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast).unwrap();
    (root, res)
}

/// Single-column results as a sorted Vec of display strings, for order-free
/// assertions.
fn col0(res: &QueryResult) -> Vec<String> {
    let mut v: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    v.sort();
    v
}

/// Did this evaluation fail with a typed [`ArithmeticOverflow`]?
///
/// Classified by *type*, never by message text (house rule).
fn overflowed(r: Result<Val>) -> bool {
    r.err()
        .is_some_and(|e| e.downcast_ref::<ArithmeticOverflow>().is_some())
}

/// Integer arithmetic that leaves `i64` **errors**; it never wraps, and never
/// panics.
///
/// Regression for HIK-73. `[profile.release]` sets no `overflow-checks`, so
/// before the fix `+`/`-`/`*` wrapped silently in production while panicking
/// under `cargo test` (a debug build) — the same query quietly lying in prod and
/// killing the process under test. These assertions pin the **release**
/// behaviour, not the debug panic: they demand an `Err`, so the pre-fix code
/// fails them in a release build by returning `Ok(<wrapped>)`, not merely by
/// failing to panic. Run under both profiles, they prove the two now agree.
#[test]
fn arith_int_overflow_is_a_typed_error_not_a_wrap() {
    assert!(overflowed(arith(
        BinOp::Add,
        Val::Int(i64::MAX),
        Val::Int(1)
    )));
    assert!(overflowed(arith(
        BinOp::Sub,
        Val::Int(i64::MIN),
        Val::Int(1)
    )));
    assert!(overflowed(arith(
        BinOp::Mul,
        Val::Int(i64::MAX),
        Val::Int(2)
    )));
    assert!(overflowed(arith(
        BinOp::Mul,
        Val::Int(i64::MIN),
        Val::Int(-1)
    )));
    // `i64::MIN / -1` and `i64::MIN % -1` are a harder bug than the wrap: Rust
    // checks division overflow in *every* profile, so these panicked in release
    // too — `RETURN -9223372036854775808 / -1` was a remote process kill, not a
    // wrong answer. Now a clean error.
    assert!(overflowed(arith(
        BinOp::Div,
        Val::Int(i64::MIN),
        Val::Int(-1)
    )));
    assert!(overflowed(arith(
        BinOp::Mod,
        Val::Int(i64::MIN),
        Val::Int(-1)
    )));

    // Representable arithmetic is untouched, including at the boundary.
    assert!(matches!(
        arith(BinOp::Add, Val::Int(2), Val::Int(3)).unwrap(),
        Val::Int(5)
    ));
    assert!(matches!(
        arith(BinOp::Sub, Val::Int(i64::MAX), Val::Int(1)).unwrap(),
        Val::Int(x) if x == i64::MAX - 1
    ));
    assert!(matches!(
        arith(BinOp::Div, Val::Int(i64::MIN), Val::Int(1)).unwrap(),
        Val::Int(i64::MIN)
    ));
    assert!(matches!(
        arith(BinOp::Mod, Val::Int(i64::MIN), Val::Int(2)).unwrap(),
        Val::Int(0)
    ));
    // Division / modulo by zero keep their own distinct errors — not overflows.
    assert!(!overflowed(arith(BinOp::Div, Val::Int(1), Val::Int(0))));
    assert!(arith(BinOp::Div, Val::Int(1), Val::Int(0)).is_err());
    assert!(!overflowed(arith(BinOp::Mod, Val::Int(1), Val::Int(0))));
    assert!(arith(BinOp::Mod, Val::Int(1), Val::Int(0)).is_err());
    // `^` yields a Float even for integer operands, so it cannot overflow i64.
    assert!(matches!(
        arith(BinOp::Pow, Val::Int(2), Val::Int(3)).unwrap(),
        Val::Float(f) if f == 8.0
    ));
    // A float operand still promotes and saturates to inf, as before.
    assert!(matches!(
        arith(BinOp::Mul, Val::Float(f64::MAX), Val::Float(2.0)).unwrap(),
        Val::Float(f) if f.is_infinite()
    ));
}

/// `sum()` over integers errors past `i64` rather than wrapping — and rather
/// than promoting to `f64` (FalkorDB promotes; Neo4j errors; we error).
///
/// Regression for HIK-73: `RETURN sum(n.big)` past `i64::MAX` returned a
/// *negative* total in release.
#[test]
fn sum_of_ints_past_i64_errors_rather_than_wrapping() {
    assert!(matches!(
        sum(&[Val::Int(1), Val::Int(2)]).unwrap(),
        Val::Int(3)
    ));
    assert!(matches!(
        sum(&[Val::Int(i64::MAX), Val::Int(0)]).unwrap(),
        Val::Int(x) if x == i64::MAX
    ));
    assert!(overflowed(sum(&[Val::Int(i64::MAX), Val::Int(1)])));
    assert!(overflowed(sum(&[Val::Int(i64::MAX), Val::Int(i64::MAX)])));
    assert!(overflowed(sum(&[Val::Int(i64::MIN), Val::Int(-1)])));
    // The overflow is detected mid-fold, not just on the final pair.
    assert!(overflowed(sum(&[
        Val::Int(i64::MAX),
        Val::Int(1),
        Val::Int(-1)
    ])));
    // A float in the column still sums as f64 (unchanged).
    assert!(matches!(
        sum(&[Val::Int(1), Val::Float(0.5)]).unwrap(),
        Val::Float(f) if f == 1.5
    ));
}

/// Unary `-` on `i64::MIN` errors instead of wrapping back to `i64::MIN`
/// (`-x == x`, a silent absurdity) — end-to-end through a real query, so the
/// `Expr::Neg` eval arm and the Bolt-visible failure are both covered.
#[test]
fn negating_i64_min_errors_rather_than_wrapping_to_itself() {
    let (root, graph, _) = testgen::write_basic("exec_neg_overflow");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let run = |q: &str| engine.run(&parser::parse(q).unwrap());

    // `i64::MIN`, spelled without an out-of-range literal: -(i64::MAX) - 1.
    let err = run("RETURN -(-9223372036854775807 - 1) AS v")
        .expect_err("negating i64::MIN must fail, not wrap");
    assert!(
        err.downcast_ref::<ArithmeticOverflow>().is_some(),
        "expected a typed ArithmeticOverflow, got: {err}"
    );
    // Same query shape one step inside the boundary still evaluates.
    let res = run("RETURN -(-9223372036854775807) AS v").unwrap();
    assert!(matches!(res.rows[0][0], Val::Int(x) if x == i64::MAX));

    // `i64::MIN / -1` — this one *panicked*, in release as well as debug, so
    // before the fix this query killed the server process. Now it is a clean,
    // per-query failure and the engine survives it.
    let err = run("RETURN (-9223372036854775807 - 1) / -1 AS v")
        .expect_err("i64::MIN / -1 must fail, not panic");
    assert!(
        err.downcast_ref::<ArithmeticOverflow>().is_some(),
        "expected a typed ArithmeticOverflow, got: {err}"
    );
    // The engine is still usable after the failed query.
    let res = run("RETURN 1 + 1 AS v").unwrap();
    assert!(matches!(res.rows[0][0], Val::Int(2)));

    let _ = std::fs::remove_dir_all(&root);
}

/// A `duration(…)` the engine cannot represent is a clean, typed query error
/// — end to end, through both spellings any authenticated client can send.
///
/// `duration_to_timet` did `years.trunc() as i64`, and Rust's float→int `as`
/// cast **saturates**: `1e19` became `i64::MAX` rather than erroring, and the
/// `years_int * 12` on the next line then overflowed. Debug (overflow-checks
/// on by default) panicked inside query execution, with no `catch_unwind` on
/// the query path. Release (overflow-checks off by default — the profile that
/// *ships*) wrapped and answered silently.
///
/// That asymmetry is why these assertions have to hold under `--release`
/// too: in debug the pre-fix code fails loudly for the wrong reason, so a
/// debug-only test would look like it was doing its job while the silent
/// wrong answer shipped. Every case is asserted on the *answer* — any `Ok` is
/// a failure, whatever it contains — and never on "not the known-wrong
/// value", which is a trap here: the silent release answers vary by input
/// (see `temporal::tests::ten_quintillion_years_is_rejected_not_silently_wrapped`),
/// so a test pinned to one of them passes against the unfixed code.
#[test]
fn absurd_duration_components_are_a_typed_error_not_a_silent_wrap() {
    let (root, graph, _) = testgen::write_basic("exec_duration_overflow");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let run = |q: &str| engine.run(&parser::parse(q).unwrap());

    // Classified by error *type*, never by message text (house rule).
    let bad = |q: &str| match run(q) {
        Ok(res) => panic!(
            "`{q}` must be a query error, but answered {}",
            res.rows[0][0].to_display()
        ),
        Err(e) => assert!(
            e.downcast_ref::<temporal::DurationOutOfRange>().is_some(),
            "expected a typed DurationOutOfRange from `{q}`, got: {e}"
        ),
    };

    // The reported reproductions — the string form (the `duration()` Str
    // arm) and the map form (`build_duration`). Pre-fix in release both
    // answered `P106751991166935DT15H30M7S` (measured, not the `P-1Y` the
    // report predicted — the `extra_days` residue dominates the wrapped
    // `-12` months).
    bad("RETURN duration('P9999999999999999999Y')");
    bad("RETURN duration({years: 1e19})");
    bad("RETURN toString(duration({years: 1e19}))");
    // `1e400` parses to f64 INFINITY (`parse::<f64>` never errors on
    // overflow), and `INFINITY as i64` saturates to `i64::MAX` identically.
    bad("RETURN duration({years: 1e400})");
    // Negative extremes: `-1e19` saturated to `i64::MIN`, which wraps too.
    bad("RETURN duration({years: -1e19})");
    bad("RETURN duration({years: -1e400})");
    bad("RETURN duration('P-9999999999999999999Y')");
    // `i64::MAX` as an integer literal: `as f64` rounds it *up* to 2^63,
    // which has no i64 counterpart at all. This is the **actual** minus-one-
    // year input — it is the only spelling that both saturates the cast and
    // leaves a zero fractional residue, so pre-fix in release it really did
    // answer `P-1Y` (verified: secs = -31_536_000 against v0.23.1).
    bad("RETURN duration({years: 9223372036854775807})");
    // Not just `years` — every component is user-supplied.
    bad("RETURN duration({months: 1e19})");
    bad("RETURN duration({days: 1e400})");
    bad("RETURN duration({seconds: 1e19})");
    // The seconds fold one line down, where representable components make an
    // unrepresentable `time_t` — and `base_time` is non-zero here, which is
    // what overflowed the add.
    bad("RETURN duration({years: 1, days: 1e18})");
    // A duration whose `time_t` leaves chrono's calendar: it decoded back as
    // ~1e14 *days*, so `localdatetime(…) + it` overflowed the `* 86_400`.
    // Refused at construction now, which is the only gate `Val::Duration`
    // has.
    bad("RETURN duration({days: 100000000000000})");
    bad("RETURN localdatetime({year:2000}) + duration({days: 100000000000000})");
    // `duration ± duration` re-encodes through the same fold.
    bad("RETURN duration({years: 1e19}) + duration({years: 1})");
    bad("RETURN duration({years: 1}) - duration({years: 1e19})");

    // Only the unrepresentable is refused: ordinary durations, a value just
    // inside the boundary, and a malformed string (→ NULL, FalkorDB parity)
    // are all unchanged.
    let res = run("RETURN toString(duration('P1Y2M3DT4H5M6S')) AS d").unwrap();
    assert_eq!(render(&res.rows[0][0]), "'P1Y2M3DT4H5M6S'");
    let res = run("RETURN toString(duration({years: 100000})) AS d").unwrap();
    assert_eq!(render(&res.rows[0][0]), "'P100000Y'");
    let res = run("RETURN duration('not a duration') AS d").unwrap();
    assert!(matches!(res.rows[0][0], Val::Null));
    let res = run("RETURN duration(null) AS d").unwrap();
    assert!(matches!(res.rows[0][0], Val::Null));

    // The engine is still usable after the failed queries.
    let res = run("RETURN 1 + 1 AS v").unwrap();
    assert!(matches!(res.rows[0][0], Val::Int(2)));

    let _ = std::fs::remove_dir_all(&root);
}

/// An extreme negative list index / slice bound is out of range, not a crash.
///
/// Same bug class as the rest of HIK-73, found by sweeping the file for
/// unchecked `i64`: `list_index` computed `len as i64 + i` and `slice_range`
/// took `start.abs()`, and `|i64::MIN|` is not an `i64`. Both **panicked in a
/// debug build** (`attempt to negate with overflow`) and wrapped in a release
/// one — so `RETURN [1,2,3][-9223372036854775808]` crashed any dev/test build.
#[test]
fn extreme_negative_list_bounds_are_out_of_range_not_an_overflow() {
    let xs = [1, 2, 3];
    // Slicing: an unreachably-negative start clamps to the whole list, exactly
    // as a merely-large negative one does.
    assert_eq!(slice_range(&xs, i64::MIN, 3), &xs[..]);
    assert_eq!(slice_range(&xs, -100, 3), &xs[..]);
    assert_eq!(slice_range(&xs, 0, i64::MIN), &[] as &[i32]);
    assert_eq!(slice_range(&xs, i64::MIN, i64::MAX), &xs[..]);
    // Ordinary slices are unaffected.
    assert_eq!(slice_range(&xs, 1, 3), &xs[1..]);
    assert_eq!(slice_range(&xs, -2, 3), &xs[1..]);

    // Indexing: out of range → None, not a wrapped (in-range!) index.
    assert_eq!(list_index(3, i64::MIN), None);
    assert_eq!(list_index(3, -4), None);
    assert_eq!(list_index(3, i64::MAX), None);
    assert_eq!(list_index(3, -1), Some(2));
    assert_eq!(list_index(3, 0), Some(0));
}

/// All rows as display strings, sorted, for order-free whole-result equality.
fn rows_disp(res: &QueryResult) -> Vec<Vec<String>> {
    let mut v: Vec<Vec<String>> = res
        .rows
        .iter()
        .map(|r| r.iter().map(|c| c.to_display()).collect())
        .collect();
    v.sort();
    v
}

#[test]
fn power_operator_and_float_literals_eval() {
    // `^` always yields a Float, even for integer operands (Neo4j semantics),
    // and the new float lexis (`1e3`, `.5`) evaluates to the right numbers.
    let (root, res) = run(
        "exec_pow",
        "RETURN 2 ^ 3 AS a, 2 ^ 10 AS b, -2 ^ 2 AS c, 2 ^ 3 ^ 2 AS d, \
             1e3 AS e, .5 AS f, 4 ^ 0.5 AS g",
    );
    let r = &res.rows[0];
    let f = |v: &Val| match v {
        Val::Float(x) => *x,
        other => panic!("expected Float, got {other:?}"),
    };
    assert_eq!(f(&r[0]), 8.0);
    assert_eq!(f(&r[1]), 1024.0);
    assert_eq!(f(&r[2]), 4.0); // (-2) ^ 2
    assert_eq!(f(&r[3]), 64.0); // (2 ^ 3) ^ 2, left-assoc
    assert_eq!(f(&r[4]), 1000.0);
    assert_eq!(f(&r[5]), 0.5);
    assert_eq!(f(&r[6]), 2.0); // 4 ^ 0.5 == sqrt(4)
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn trailing_semicolon_is_accepted() {
    let (root, res) = run("exec_semi", "MATCH (n) RETURN count(*) AS c;");
    assert!(matches!(res.rows[0][0], Val::Int(5)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn all_nodes_scan_counts() {
    let (root, res) = run("exec_count_all", "MATCH (n) RETURN count(*) AS c");
    assert_eq!(res.columns, vec!["c"]);
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(5)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn label_scan_with_projection() {
    let (root, res) = run("exec_label", "MATCH (n:Person) RETURN n.name AS name");
    assert_eq!(res.columns, vec!["name"]);
    assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn label_count_uses_fast_path() {
    // Stage 3: `MATCH (n:Person) RETURN count(*)` reads the label posting length
    // (3 Person nodes in the fixture) without materialising rows.
    let (root, res) = run("exec_count_label", "MATCH (n:Person) RETURN count(*) AS c");
    assert_eq!(res.columns, vec!["c"]);
    assert!(
        matches!(res.rows[0][0], Val::Int(3)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);

    // count(n) over the same pattern is identical.
    let (root, res) = run(
        "exec_count_label_n",
        "MATCH (n:Person) RETURN count(n) AS c",
    );
    assert!(matches!(res.rows[0][0], Val::Int(3)));
    let _ = std::fs::remove_dir_all(&root);

    // An unknown label counts zero (not an error, not a full scan).
    let (root, res) = run("exec_count_unknown", "MATCH (n:Nope) RETURN count(*) AS c");
    assert!(matches!(res.rows[0][0], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

// ---- whole-graph label/reltype metadata fast paths (Stage M) ----

/// Open the richer metadata fixture (multi-label node, no-label node, self-loop).
fn meta_gen(tag: &str) -> (std::path::PathBuf, Generation) {
    let (root, graph, _) = testgen::write_meta(tag);
    let gen = Generation::open(&root, &graph).unwrap();
    (root, gen)
}

#[test]
fn meta_reltype_enumeration_and_grouped_counts() {
    let (root, gen) = meta_gen("meta_reltype");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();

    // A1 — DISTINCT type(r): the reltype list.
    let a1 = run("MATCH ()-[r]->() RETURN DISTINCT type(r) AS t");
    assert_eq!(a1.columns, vec!["t"]);
    assert_eq!(col0(&a1), vec!["KNOWS", "OWNS", "WORKS_AT"]);

    // B1 — type(r), count(*): edges per reltype (KNOWS 2, WORKS_AT 2, OWNS 1).
    let b1 = run("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        rows_disp(&b1),
        vec![
            vec!["KNOWS".to_string(), "2".to_string()],
            vec!["OWNS".to_string(), "1".to_string()],
            vec!["WORKS_AT".to_string(), "2".to_string()],
        ]
    );

    // Reverse arrow gives the same totals; count(r) == count(*).
    let b1r = run("MATCH ()<-[r]-() RETURN type(r) AS t, count(r) AS c");
    assert_eq!(rows_disp(&b1r), rows_disp(&b1));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_first_label_enumeration_and_counts() {
    let (root, gen) = meta_gen("meta_label");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();

    // A2 — DISTINCT labels(n)[0]: includes the null bucket (the label-less node).
    let a2 = run("MATCH (n) RETURN DISTINCT labels(n)[0] AS l");
    assert_eq!(col0(&a2), vec!["Admin", "Company", "Person", "null"]);

    // B2 — labels(n)[0], count(*): Person 2 (Alice+Bob first-label), Admin 1
    // (Carol), Company 1 (Acme), null 1 (Ghost).
    let b2 = run("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c");
    assert_eq!(
        rows_disp(&b2),
        vec![
            vec!["Admin".to_string(), "1".to_string()],
            vec!["Company".to_string(), "1".to_string()],
            vec!["Person".to_string(), "2".to_string()],
            vec!["null".to_string(), "1".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_fast_paths_match_the_scan() {
    // Every fast-pathed form must equal the general matcher on the same query;
    // appending an always-true WHERE forces the matcher (its independent truth).
    let (root, gen) = meta_gen("meta_parity");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let parity = |fast: &str, slow: &str| {
        let f = eng.run(&parser::parse(fast).unwrap()).unwrap();
        let s = eng.run(&parser::parse(slow).unwrap()).unwrap();
        assert_eq!(f.columns, s.columns, "columns: {fast}");
        assert_eq!(rows_disp(&f), rows_disp(&s), "rows: {fast} vs {slow}");
    };
    // bare enumerations + counts, both arrow directions + undirected
    parity(
        "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t",
        "MATCH ()-[r]->() WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
    );
    parity(
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    // undirected: each edge matches in both orientations (self-loops counted
    // twice), so the fast path returns 2× the directed count — verified equal to
    // the matcher.
    parity(
        "MATCH ()-[r]-() RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]-() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()-[r]-() RETURN DISTINCT type(r) AS t",
        "MATCH ()-[r]-() WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
    );
    parity(
        "MATCH (n) RETURN DISTINCT labels(n)[0] AS l",
        "MATCH (n) WHERE 1 = 1 RETURN DISTINCT labels(n)[0] AS l",
    );
    parity(
        "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c",
        "MATCH (n) WHERE 1 = 1 RETURN labels(n)[0] AS l, count(*) AS c",
    );
    // labelled schema marginals: source-, target-, reverse-arrow-, multi-label.
    parity(
        "MATCH (:Person)-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()<-[r]-(:Person) RETURN type(r) AS t, count(*) AS c",
        "MATCH ()<-[r]-(:Person) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Admin)-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Admin)-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    // both-endpoints-labelled (the full schema-triple cube), grouped + fully
    // specified, including a multi-label endpoint.
    parity(
        "MATCH (:Person)-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Company)-[r]->(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Company)-[r]->(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Admin)-[r]->(:Company) RETURN DISTINCT type(r) AS t",
        "MATCH (:Admin)-[r]->(:Company) WHERE 1 = 1 RETURN DISTINCT type(r) AS t",
    );
    // undirected with a labelled endpoint — src+tgt marginal (one end) and
    // triple+mirror (both ends), verified equal to the matcher.
    parity(
        "MATCH (:Person)-[r]-() RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]-() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH ()-[r]-(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH ()-[r]-(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    parity(
        "MATCH (:Person)-[r]-(:Company) RETURN type(r) AS t, count(*) AS c",
        "MATCH (:Person)-[r]-(:Company) WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c",
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-77. A `DELETE` of a business key that exists nowhere is a no-op, and must
/// leave the **delta empty** — because an empty delta is what gates the metadata
/// fast paths. This asserts the fast path is *still taken* (the gated recogniser
/// returns `Some`, i.e. the answer comes from resident metadata with no block reads),
/// not merely that the count happens to be numerically right — the matcher would
/// return the same numbers while scanning the graph, which at 91.6M nodes is the
/// known OOM shape.
#[test]
fn noop_node_delete_keeps_the_metadata_fast_path_engaged() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, gen) = meta_gen("meta_noop_delete");
    let cache = BlockCache::new(1 << 20);
    // A labelled-endpoint schema-cube shape: `try_reltype_meta_fast_path` answers it
    // from the resident marginals over a pure core, and **declines** (⇒ full matcher)
    // the moment the delta is non-empty. So "did it return `Some`?" is exactly "was
    // the fast path taken?".
    let ast =
        parser::parse("MATCH (:Person)-[r]->() RETURN type(r) AS t, count(*) AS c").expect("parse");

    // Truth: the same query over the pure core, fast-pathed.
    let core_view = MergedView::read_only(&gen);
    let want = Engine::new(&core_view, &cache)
        .try_reltype_meta_fast_path(&ast.head)
        .unwrap()
        .expect("the fast path answers this shape over a pure core");

    // A delta holding *only* a delete of a key that exists nowhere.
    let mut mem = Memtable::new();
    mem.delete_node("Person", "name", Value::Str("Nobody".into()), None);
    let (mem_empty, mem_deltas) = (mem.is_empty(), mem.node_delta_count());
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let eng = Engine::new(&view, &cache);

    // The load-bearing assertion: the recogniser still answers, so the query is still
    // served from resident metadata rather than falling through to the matcher.
    let got = eng
        .try_reltype_meta_fast_path(&ast.head)
        .unwrap()
        .expect("the metadata fast path must still be engaged after a no-op DELETE");
    assert_eq!(rows_disp(&got), rows_disp(&want), "same resident answer");
    // Why it stays engaged: the no-op tombstone stored nothing, so the delta — the
    // reader's fast-path predicate — is still empty.
    assert!(mem_empty, "a no-op tombstone leaves the memtable empty");
    assert_eq!(mem_deltas, 0, "…and stores no phantom node entry");
    assert!(
        view.delta().is_empty(),
        "the reader's fast-path predicate still holds after a no-op DELETE"
    );
    // …and the query as a whole still answers correctly.
    assert_eq!(rows_disp(&eng.run(&ast).unwrap()), rows_disp(&want));

    // Control: a delete that *does* resolve populates the delta, and the same
    // recogniser then declines — so the assertion above is genuinely sensitive to
    // delta emptiness (it fails on the pre-fix `delete_node`, which stored a phantom
    // entry for `Nobody`). The real delete also still tombstones its node, so the
    // topology overlay keeps suppressing its incident edges.
    let mut mem = Memtable::new();
    mem.delete_node("Admin", "name", Value::Str("Carol".into()), Some(2));
    let live = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        !live.delta().is_empty(),
        "a real delete populates the delta"
    );
    assert!(live.delta().is_tombstoned(2), "and suppresses node 2");
    assert!(
        Engine::new(&live, &cache)
            .try_reltype_meta_fast_path(&ast.head)
            .unwrap()
            .is_none(),
        "over a live delta the labelled-endpoint cube declines — the gate this test guards"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_order_by_skip_limit() {
    // A trailing ORDER BY / SKIP / LIMIT is applied to the finished metadata rows,
    // order-identically to the matcher (compared without re-sorting).
    let (root, gen) = meta_gen("meta_order");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let run = |q: &str| eng.run(&parser::parse(q).unwrap()).unwrap();
    let disp = |res: &QueryResult| -> Vec<Vec<String>> {
        res.rows
            .iter()
            .map(|r| r.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    // Total order (c desc, then key) so ties are deterministic across paths.
    let ordered_parity = |fast: &str, slow: &str| {
        let f = run(fast);
        let s = run(slow);
        assert_eq!(f.columns, s.columns, "cols: {fast}");
        assert_eq!(disp(&f), disp(&s), "ordered rows: {fast} vs {slow}");
    };
    ordered_parity(
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t",
        "MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t",
    );
    // LIMIT truncates after ordering: the single largest group.
    assert_eq!(
        disp(&run(
            "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c ORDER BY c DESC, t LIMIT 1"
        )),
        vec![vec!["KNOWS".to_string(), "2".to_string()]],
    );
    // SKIP + LIMIT on the label side.
    ordered_parity(
            "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c ORDER BY c DESC, l SKIP 1 LIMIT 2",
            "MATCH (n) WHERE 1 = 1 RETURN labels(n)[0] AS l, count(*) AS c ORDER BY c DESC, l SKIP 1 LIMIT 2",
        );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_fast_path_reads_no_blocks_under_tiny_budget() {
    // The regression guard: with `maxIntermediate` far below the edge count the
    // metadata queries still SUCCEED (no materialisation), read zero blocks, and
    // charge no budget — while the scanning form of the same question trips.
    let (root, gen) = meta_gen("meta_perf");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache).with_max_intermediate(1);
    for q in [
        "MATCH ()-[r]->() RETURN DISTINCT type(r) AS t",
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
        "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c",
    ] {
        let before = cache.metrics().misses;
        let res = eng.run(&parser::parse(q).unwrap()).unwrap();
        assert!(!res.rows.is_empty(), "empty result for {q}");
        assert_eq!(cache.metrics().misses, before, "fast path read blocks: {q}");
        assert_eq!(eng.cost(), 0, "fast path charged budget: {q}");
    }
    // The materialising form of the same question DOES trip the tiny budget —
    // exactly the failure the fast path removes.
    let scan = eng.run(
        &parser::parse("MATCH ()-[r]->() WHERE 1 = 1 RETURN type(r) AS t, count(*) AS c").unwrap(),
    );
    assert!(scan.is_err(), "scan should trip maxIntermediate=1");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_declines_still_correct() {
    // Each "do NOT fast-path" shape falls back to the matcher and stays correct.
    let (root, gen) = meta_gen("meta_decline");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let rows = |q: &str| rows_disp(&eng.run(&parser::parse(q).unwrap()).unwrap());

    // rel-type filter.
    assert_eq!(
        rows("MATCH ()-[r:KNOWS]->() RETURN type(r) AS t, count(*) AS c"),
        vec![vec!["KNOWS".to_string(), "2".to_string()]],
    );
    // WHERE predicate.
    assert_eq!(
        rows("MATCH ()-[r]->() WHERE type(r) = 'KNOWS' RETURN type(r) AS t, count(*) AS c"),
        vec![vec!["KNOWS".to_string(), "2".to_string()]],
    );
    // count(DISTINCT …) — declines; here it equals count(*) (all edges distinct).
    assert_eq!(
        rows("MATCH ()-[r]->() RETURN type(r) AS t, count(DISTINCT r) AS c"),
        rows("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c"),
    );
    // a node variable reused on both endpoints `(a)-[r]->(a)` constrains a
    // self-loop, so the whole-graph counts must NOT be used — it declines and the
    // matcher returns only the self-loop (OWNS: Acme→Acme).
    assert_eq!(
        rows("MATCH (a)-[r]->(a) RETURN type(r) AS t, count(*) AS c"),
        vec![vec!["OWNS".to_string(), "1".to_string()]],
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn meta_where_clause_is_not_ignored() {
    // A WHERE narrows the match, so the whole-graph metadata counts would be
    // WRONG — the fast path must decline and the matcher return the *filtered*
    // answer. Each case is chosen so the correct answer DIFFERS from the
    // metadata count, proving the resident counts are not reused.
    let (root, gen) = meta_gen("meta_where");
    let cache = BlockCache::new(1 << 20);
    let eng = Engine::new(&gen, &cache);
    let rows = |q: &str| rows_disp(&eng.run(&parser::parse(q).unwrap()).unwrap());

    // Whole-graph baseline (fast path): KNOWS 2, WORKS_AT 2, OWNS 1.
    let base = rows("MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        base,
        vec![
            vec!["KNOWS".to_string(), "2".to_string()],
            vec!["OWNS".to_string(), "1".to_string()],
            vec!["WORKS_AT".to_string(), "2".to_string()],
        ]
    );

    // WHERE on a source property → only Alice's out-edges (KNOWS 1, WORKS_AT 1).
    let by_src =
        rows("MATCH (a)-[r]->() WHERE a.name = 'Alice' RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        by_src,
        vec![
            vec!["KNOWS".to_string(), "1".to_string()],
            vec!["WORKS_AT".to_string(), "1".to_string()],
        ]
    );
    assert_ne!(
        by_src, base,
        "WHERE on source property must change the counts"
    );

    // WHERE that prunes an entire reltype group — OWNS must disappear, not be
    // reported with its metadata count of 1.
    let pruned =
        rows("MATCH ()-[r]->() WHERE type(r) <> 'OWNS' RETURN type(r) AS t, count(*) AS c");
    assert_eq!(
        pruned,
        vec![
            vec!["KNOWS".to_string(), "2".to_string()],
            vec!["WORKS_AT".to_string(), "2".to_string()],
        ]
    );
    assert!(
        !pruned.iter().any(|r| r[0] == "OWNS"),
        "WHERE must prune the OWNS group entirely"
    );

    // WHERE that matches nothing → zero rows, NOT the metadata counts.
    let none =
        rows("MATCH ()-[r]->() WHERE r.no_such_prop = 99 RETURN type(r) AS t, count(*) AS c");
    assert!(
        none.is_empty(),
        "a WHERE matching no edges must yield no rows"
    );

    // Label side: a WHERE on a node property → only the matching node's first
    // label (Bob → Person 1), not the whole-graph Person count of 2.
    let base_l = rows("MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c");
    let one = rows("MATCH (n) WHERE n.name = 'Bob' RETURN labels(n)[0] AS l, count(*) AS c");
    assert_eq!(one, vec![vec!["Person".to_string(), "1".to_string()]]);
    assert_ne!(
        one, base_l,
        "WHERE on a node property must change the counts"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn count_with_constant_extra_projection_fast_path() {
    // The benchmark appends `… , $k AS k` (a constant grouping key) to bust the
    // result cache. That is still a single group, so the fast path fires and the
    // extra column is carried through in order.
    let (root, res) = run(
        "exec_count_tag",
        "MATCH (n:Person) RETURN count(*) AS c, 7 AS k",
    );
    assert_eq!(res.columns, vec!["c", "k"]);
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(3)));
    assert!(matches!(res.rows[0][1], Val::Int(7)));
    let _ = std::fs::remove_dir_all(&root);

    // Order preserved when the tag precedes the count.
    let (root, res) = run("exec_count_tag2", "MATCH (n) RETURN 9 AS k, count(n) AS c");
    assert_eq!(res.columns, vec!["k", "c"]);
    assert!(matches!(res.rows[0][0], Val::Int(9)));
    assert!(matches!(res.rows[0][1], Val::Int(5)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn count_with_non_constant_extra_projection_falls_back() {
    // A second item that reads node data is a real grouping key — must NOT take
    // the fast path; group-by-city over the 3 Person nodes yields 2 rows.
    let (root, res) = run(
        "exec_count_group",
        "MATCH (n:Person) RETURN n.city AS city, count(*) AS c",
    );
    assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn count_with_where_still_correct() {
    // A residual WHERE disables the fast path; the answer must still be right
    // (2 of the 3 Person nodes have age >= 30 in the fixture: Alice 30, Carol 40;
    // Bob is 25).
    let (root, res) = run(
        "exec_count_where",
        "MATCH (n:Person) WHERE n.age >= 30 RETURN count(*) AS c",
    );
    assert!(
        matches!(res.rows[0][0], Val::Int(2)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn streaming_scan_where_and_property_projection() {
    // Stage 5: a single node-only MATCH streams without per-row HashMaps. A
    // WHERE filter that reads a property (city = 'London') keeps Alice + Bob,
    // and the projected property comes back correctly.
    let (root, res) = run(
        "exec_stream_where",
        "MATCH (n:Person) WHERE n.city = 'London' RETURN n.name AS name",
    );
    assert_eq!(res.columns, vec!["name"]);
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn streaming_scan_group_by_property_aggregation() {
    // Aggregation over the streamed rows: group the 3 Person nodes by city
    // (London → 2, Paris → 1). Exercises the streaming match feeding
    // project_aggregated with a per-row property read.
    let (root, res) = run(
        "exec_stream_agg",
        "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC",
    );
    assert_eq!(res.columns, vec!["city", "c"]);
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][0].to_display(), "London");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    assert_eq!(res.rows[1][0].to_display(), "Paris");
    assert!(matches!(res.rows[1][1], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn streaming_scan_inline_prop_filter() {
    // An inline property on the anchor (handled by node_ok in the streaming
    // path, not a residual WHERE) selects the single matching node.
    let (root, res) = run(
        "exec_stream_inline",
        "MATCH (n:Person {city: 'Paris'}) RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_distinct_count_fast_path() {
    // Stage 7: `count(DISTINCT n.p)` over an indexed property is the number of
    // distinct index keys. age has 3 distinct values; team has one ('Red'),
    // and the index omits Carol (no team) — DISTINCT also excludes null.
    let (root, res) = run(
        "exec_g_distinct_age",
        "MATCH (n:Person) RETURN count(DISTINCT n.age) AS c",
    );
    assert_eq!(res.columns, vec!["c"]);
    assert!(
        matches!(res.rows[0][0], Val::Int(3)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);

    // With the cache-busting constant tail, and a single distinct value.
    let (root, res) = run(
        "exec_g_distinct_team",
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c, 7 AS k",
    );
    assert_eq!(res.columns, vec!["c", "k"]);
    assert!(
        matches!(res.rows[0][0], Val::Int(1)),
        "{:?}",
        res.rows[0][0]
    );
    assert!(matches!(res.rows[0][1], Val::Int(7)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_group_by_fast_path() {
    // Stage 7: group-by an indexed property reads (key, count) from the index.
    // team: Alice/Bob 'Red' (2) and Carol's missing team becomes a null group
    // (1). ORDER BY c DESC puts the larger group first.
    let (root, res) = run(
        "exec_g_groupby_team",
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c ORDER BY c DESC",
    );
    assert_eq!(res.columns, vec!["t", "c"]);
    assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
    assert_eq!(res.rows[0][0].to_display(), "Red");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    assert!(matches!(res.rows[1][0], Val::Null), "{:?}", res.rows[1][0]);
    assert!(matches!(res.rows[1][1], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);

    // All-distinct indexed property: one group of 1 per value (no null group,
    // every Person has an age). `count(n)` behaves like `count(*)` here.
    let (root, res) = run(
        "exec_g_groupby_age",
        "MATCH (n:Person) RETURN n.age AS a, count(n) AS c",
    );
    assert_eq!(
        rows_disp(&res),
        vec![
            vec!["25".to_string(), "1".to_string()],
            vec!["30".to_string(), "1".to_string()],
            vec!["40".to_string(), "1".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_matches_general_path() {
    // The fast path must return exactly what the general (materialise + group)
    // path does. A residual WHERE that keeps every row forces the general path;
    // both group-by team (incl. the null group) and distinct-count must agree.
    let (root, fast) = run(
        "exec_g_cmp_fast",
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
    );
    let _ = std::fs::remove_dir_all(&root);
    let (root, general) = run(
        "exec_g_cmp_gen",
        "MATCH (n:Person) WHERE n.age >= 0 RETURN n.team AS t, count(*) AS c",
    );
    assert_eq!(rows_disp(&fast), rows_disp(&general));
    let _ = std::fs::remove_dir_all(&root);

    let (root, fast) = run(
        "exec_g_cmp_fast_d",
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c",
    );
    let _ = std::fs::remove_dir_all(&root);
    let (root, general) = run(
        "exec_g_cmp_gen_d",
        "MATCH (n:Person) WHERE n.age >= 0 RETURN count(DISTINCT n.team) AS c",
    );
    assert_eq!(rows_disp(&fast), rows_disp(&general));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_fast_path_guards() {
    // Shapes the fast path must decline, each still answered correctly by the
    // general path.

    // (a) Residual WHERE: age >= 30 keeps Alice (Red) and Carol (null).
    let (root, res) = run(
        "exec_g_guard_where",
        "MATCH (n:Person) WHERE n.age >= 30 RETURN n.team AS t, count(*) AS c",
    );
    assert_eq!(
        rows_disp(&res),
        vec![
            vec!["Red".to_string(), "1".to_string()],
            vec!["null".to_string(), "1".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);

    // (b) A non-count aggregate (sum) over the grouping property.
    let (root, res) = run(
        "exec_g_guard_sum",
        "MATCH (n:Person) RETURN n.team AS t, sum(n.age) AS s",
    );
    // Red = Alice 30 + Bob 25 = 55; null group = Carol 40.
    assert_eq!(
        rows_disp(&res),
        vec![
            vec!["Red".to_string(), "55".to_string()],
            vec!["null".to_string(), "40".to_string()],
        ]
    );
    let _ = std::fs::remove_dir_all(&root);

    // (c) Two grouping keys (the second `node.prop` trips the >1-key guard).
    let (root, res) = run(
        "exec_g_guard_twokeys",
        "MATCH (n:Person) RETURN n.team AS t, n.city AS city, count(*) AS c",
    );
    // (Red, London) Alice+Bob = 2; (null, Paris) Carol = 1.
    assert_eq!(res.rows.len(), 2, "{:?}", res.rows);
    let _ = std::fs::remove_dir_all(&root);

    // (d) A non-indexed grouping property (city) — must fall back, still right.
    let (root, res) = run(
        "exec_g_guard_noindex",
        "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC",
    );
    assert_eq!(res.rows[0][0].to_display(), "London");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    assert_eq!(res.rows[1][0].to_display(), "Paris");
    assert!(matches!(res.rows[1][1], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn grouped_index_fast_path_fires_without_scanning() {
    // Proof the fast path actually *fires* (rather than just agreeing with the
    // general path): the index walk charges nothing to the intermediate budget,
    // so a budget far too small for a per-row scan still succeeds. The control —
    // the same query forced onto the general path by a residual WHERE — exhausts
    // that budget scanning the 3 Person rows.
    //
    // The `count(DISTINCT n.p)` shape also exercises the parser quirk where the
    // inner DISTINCT sets `ret.distinct`; the fast path must not be fooled into
    // declining.
    let res = run_budgeted(
        "exec_g_fire_distinct",
        2,
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c, 7 AS k",
    )
    .expect("distinct-count fast path must not scan");
    assert!(
        matches!(res.rows[0][0], Val::Int(1)),
        "{:?}",
        res.rows[0][0]
    );

    let res = run_budgeted(
        "exec_g_fire_group",
        2,
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
    )
    .expect("group-by fast path must not scan");
    assert_eq!(res.rows.len(), 2);

    // Control: forced onto the general (scanning) path, the same budget trips.
    let err = run_budgeted(
        "exec_g_fire_control",
        2,
        "MATCH (n:Person) WHERE n.age >= 0 RETURN count(DISTINCT n.team) AS c",
    );
    assert!(
        err.is_err(),
        "the general path must exhaust the tiny budget (proving the fast path \
             above genuinely avoided the scan)"
    );
}

#[test]
fn grouped_index_histogram_matches_scan() {
    // Level-1 precompute correctness: a histogram-ON generation answers
    // group-by / count(DISTINCT) from `prop_hist.blk`; an otherwise-identical
    // histogram-OFF generation answers them by walking the ISAM. Every query
    // must return identical rows AND identical row order.
    let ordered = |res: &QueryResult| -> Vec<Vec<String>> {
        res.rows
            .iter()
            .map(|r| r.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let exec = |root: &std::path::Path, graph: &str, q: &str| -> QueryResult {
        let gen = Generation::open(root, graph).unwrap();
        let cache = BlockCache::new(1 << 20);
        let out = Engine::new(&gen, &cache)
            .run(&parser::parse(q).unwrap())
            .unwrap();
        out
    };

    let queries = [
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c ORDER BY c DESC",
        "MATCH (n:Person) RETURN n.team AS t, count(*) AS c",
        "MATCH (n:Person) RETURN count(DISTINCT n.team) AS c",
        "MATCH (n:Person) RETURN n.age AS a, count(*) AS c ORDER BY a ASC",
        "MATCH (n:Person) RETURN count(DISTINCT n.age) AS c, 7 AS k",
    ];
    for (i, q) in queries.iter().enumerate() {
        let (root_off, g_off, _) = testgen::write_basic(&format!("exec_hist_off_{i}"));
        // The OFF generation carries no histogram → fallback (index walk).
        let gen_off = Generation::open(&root_off, &g_off).unwrap();
        assert!(gen_off.property_histogram("node_Person_team").is_none());
        drop(gen_off);
        let off = exec(&root_off, &g_off, q);
        let _ = std::fs::remove_dir_all(&root_off);

        let (root_on, g_on, _) = testgen::write_basic_with_histograms(&format!("exec_hist_on_{i}"));
        // The ON generation's histogram is byte-identical to the walk it replaces.
        let gen_on = Generation::open(&root_on, &g_on).unwrap();
        let hist = gen_on
            .property_histogram("node_Person_team")
            .expect("histogram present in the ON generation");
        let walk = gen_on
            .range_index("node_Person_team")
            .unwrap()
            .distinct_key_counts()
            .unwrap();
        assert_eq!(hist, walk.as_slice(), "histogram must equal the index walk");
        drop(gen_on);
        let on = exec(&root_on, &g_on, q);
        let _ = std::fs::remove_dir_all(&root_on);

        assert_eq!(on.columns, off.columns, "columns differ for `{q}`");
        assert_eq!(ordered(&on), ordered(&off), "rows/order differ for `{q}`");
    }
}

#[test]
fn param_indexed_equality_count_fast_path() {
    // Stage 1 + 3: `{name: $n}` selects the name index and the count comes from
    // its `lookup_eq` length, not a label scan + materialise.
    let (root, graph, _) = testgen::write_basic("exec_count_param_idx");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let mut params = HashMap::new();
    params.insert("n".to_string(), Val::Str("Carol".into()));
    let engine = Engine::new(&gen, &cache).with_params(params);
    let ast = parser::parse("MATCH (n:Person {name: $n}) RETURN count(*) AS c").unwrap();
    let res = engine.run(&ast).unwrap();
    assert!(
        matches!(res.rows[0][0], Val::Int(1)),
        "{:?}",
        res.rows[0][0]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn range_index_equality_lookup() {
    let (root, res) = run(
        "exec_rangeeq",
        "MATCH (n:Person {name: 'Bob'}) RETURN n.age AS age",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(25)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn where_range_filter_and_order() {
    let (root, res) = run(
        "exec_range",
        "MATCH (n:Person) WHERE n.age >= 30 RETURN n.name AS name ORDER BY n.age DESC",
    );
    // Carol (40) then Alice (30).
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(names, vec!["Carol", "Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn relationship_pattern_traversal() {
    let (root, res) = run(
        "exec_rel",
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b",
    );
    let mut pairs: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Alice".into(), "Bob".into()),
            ("Alice".into(), "Carol".into()),
            ("Bob".into(), "Carol".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn relationship_value_carries_type_and_stored_endpoints() {
    // Outgoing walk: r is the stored Alice(0)-[:KNOWS]->Bob(1) edge.
    let (root, res) = run(
            "exec_reltype",
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN type(r) AS t, r AS rel",
        );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "KNOWS");
    match res.rows[0][1] {
        Val::Rel {
            start,
            end,
            reltype,
            ..
        } => {
            assert_eq!((start, end, reltype), (0, 1, 0));
        }
        ref other => panic!("expected a relationship, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);

    // Walking the SAME edge incoming must report the same stored direction
    // (start→end is src→dst, not the traversal direction).
    let (root, res) = run(
        "exec_reltype_in",
        "MATCH (b:Person {name: 'Bob'})<-[r:KNOWS]-(a) RETURN r AS rel",
    );
    assert_eq!(res.rows.len(), 1);
    match res.rows[0][0] {
        Val::Rel { start, end, .. } => assert_eq!((start, end), (0, 1)),
        ref other => panic!("expected a relationship, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn incoming_direction_traversal() {
    let (root, res) = run(
        "exec_incoming",
        "MATCH (a:Person)<-[:KNOWS]-(b:Person) RETURN a.name AS a, b.name AS b",
    );
    // Reverse of the KNOWS edges: Bob<-Alice, Carol<-Bob, Carol<-Alice.
    let mut pairs: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Bob".into(), "Alice".into()),
            ("Carol".into(), "Alice".into()),
            ("Carol".into(), "Bob".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn relationship_property_predicate() {
    let (root, res) = run(
        "exec_relprop",
        "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN a.name AS a, b.name AS b",
    );
    // Only the Alice-[:KNOWS {since:2020}]->Bob edge carries the property.
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Alice");
    assert_eq!(res.rows[0][1].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

// Inline property maps whose value is bound earlier (by a `WITH` or an earlier
// node/rel) must resolve against the current scope — `(b {id: x})` behaves like
// `(b) WHERE b.id = x`. This was the last eu-ai-act-data-service parity gap.

#[test]
fn inline_node_prop_resolves_variable_from_with() {
    // The exact reported gap: a WITH-bound value feeding a later inline map.
    let (root, res) = run(
        "exec_inline_with",
        "MATCH (n:Person {name:'Bob'}) WITH n.name AS who \
             MATCH (m:Person {name: who}) RETURN m.age AS age",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(25)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_node_prop_joins_across_matches() {
    // baseId-style join: carry one node's property into another node's inline map.
    let (root, res) = run(
        "exec_inline_join",
        "MATCH (a:Person {name:'Alice'}) WITH a.city AS c \
             MATCH (p:Person {city: c}) RETURN p.name AS n",
    );
    // Alice and Bob are both in London.
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_rel_prop_resolves_variable() {
    // Variable value on a relationship inline map.
    let (root, res) = run(
        "exec_inline_rel",
        "WITH 2020 AS yr MATCH (a)-[r:KNOWS {since: yr}]->(b) \
             RETURN a.name AS a, b.name AS b",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Alice");
    assert_eq!(res.rows[0][1].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_node_prop_resolves_property_access() {
    // The value is a property access (`a.name`), not just a bare variable.
    let (root, res) = run(
        "exec_inline_propaccess",
        "MATCH (a:Person {name:'Bob'}) \
             MATCH (m:Person {name: a.name}) RETURN m.name AS n",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn inline_node_prop_literal_still_works() {
    // Regression guard: literal inline maps must keep matching after the change.
    let (root, res) = run(
        "exec_inline_literal",
        "MATCH (n:Person {name:'Bob'}) RETURN n.age AS age",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(25)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn variable_length_expansion() {
    let (root, res) = run(
        "exec_varlen",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name AS name",
    );
    // 1 hop: Bob, Carol. 2 hops: Alice→Bob→Carol = Carol again.
    assert_eq!(col0(&res), vec!["Bob", "Carol", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn type_alternation() {
    let (root, res) = run(
        "exec_altern",
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS name",
    );
    // Alice KNOWS Bob, KNOWS Carol, WORKS_AT Acme.
    assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn with_aggregation_group_and_having() {
    let (root, res) = run(
        "exec_with",
        "MATCH (n:Person) WITH n.city AS city, count(*) AS c WHERE c > 1 RETURN city, c",
    );
    // London has 2 (Alice, Bob); Paris has 1 (filtered out).
    assert_eq!(res.columns, vec!["city", "c"]);
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "London");
    assert!(matches!(res.rows[0][1], Val::Int(2)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn distinct_and_aggregate_functions() {
    let (root, res) = run(
            "exec_aggs",
            "MATCH (n:Person) RETURN count(n) AS c, sum(n.age) AS total, avg(n.age) AS mean, min(n.age) AS lo, max(n.age) AS hi, collect(DISTINCT n.city) AS cities",
        );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Int(3)));
    assert!(matches!(r[1], Val::Int(95))); // 30+25+40
    assert!(matches!(r[2], Val::Float(f) if (f - 95.0 / 3.0).abs() < 1e-9));
    assert!(matches!(r[3], Val::Int(25)));
    assert!(matches!(r[4], Val::Int(40)));
    match &r[5] {
        Val::List(xs) => {
            let mut cities: Vec<String> = xs.iter().map(|v| v.to_display()).collect();
            cities.sort();
            assert_eq!(cities, vec!["London", "Paris"]);
        }
        other => panic!("expected a list, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn distinct_projection() {
    let (root, res) = run(
        "exec_distinct",
        "MATCH (n:Person) RETURN DISTINCT n.city AS city",
    );
    assert_eq!(col0(&res), vec!["London", "Paris"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn skip_and_limit() {
    let (root, res) = run(
        "exec_skiplimit",
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name SKIP 1 LIMIT 1",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Bob");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn map_projection() {
    let (root, res) = run(
        "exec_mapproj",
        "MATCH (n:Person {name: 'Alice'}) RETURN n {.name, .age} AS m",
    );
    match &res.rows[0][0] {
        Val::Map(m) => {
            assert_eq!(m[0].0, "name");
            assert_eq!(m[0].1.to_display(), "Alice");
            assert_eq!(m[1].0, "age");
            assert!(matches!(m[1].1, Val::Int(30)));
        }
        other => panic!("expected a map, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn case_and_list_predicate_and_in() {
    let (root, res) = run(
            "exec_case",
            "MATCH (n:Person) RETURN n.name AS name, CASE WHEN n.age >= 30 THEN 'senior' ELSE 'junior' END AS band ORDER BY n.name",
        );
    let bands: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(
        bands,
        vec![
            ("Alice".into(), "senior".into()),
            ("Bob".into(), "junior".into()),
            ("Carol".into(), "senior".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn where_in_and_string_ops() {
    let (root, res) = run(
        "exec_in",
        "MATCH (n:Person) WHERE n.age IN [25, 40] AND n.name STARTS WITH 'C' RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn union_distinct_and_all() {
    let (root, res) = run(
        "exec_union",
        "MATCH (n:Person) RETURN n.name AS x UNION MATCH (c:Company) RETURN c.name AS x",
    );
    assert_eq!(res.columns, vec!["x"]);
    assert_eq!(col0(&res), vec!["Acme", "Alice", "Bob", "Carol", "Globex"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn optional_match_yields_nulls() {
    // Companies have no outgoing KNOWS, so the optional rel is null.
    let (root, res) = run(
            "exec_optional",
            "MATCH (n:Company) OPTIONAL MATCH (n)-[:KNOWS]->(m) RETURN n.name AS name, m AS friend ORDER BY n.name",
        );
    assert_eq!(res.rows.len(), 2);
    for r in &res.rows {
        assert!(matches!(r[1], Val::Null));
    }
    assert_eq!(res.rows[0][0].to_display(), "Acme");
    let _ = std::fs::remove_dir_all(&root);
}

// ── Stage 6 — traversal-frame characterization ───────────────────────────
// These lock the exact result set of the multi-hop / variable-length walk so
// the mutate-in-place binding frame (replacing the per-hop `binding.clone()`)
// is provably result-preserving. They pass on the pre-Stage-6 code and must
// still pass byte-for-byte after the rewrite.

#[test]
fn frame_two_hop_chain_exact_rows() {
    // KNOWS Person→Person edges: Alice→Bob, Bob→Carol, Alice→Carol. The only
    // length-2 KNOWS chain is Alice→Bob→Carol (Carol has no outgoing KNOWS).
    let (root, res) = run(
        "exec_frame_2hop",
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             RETURN a.name AS a, b.name AS b, c.name AS c",
    );
    let rows: Vec<(String, String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display(), r[2].to_display()))
        .collect();
    assert_eq!(rows, vec![("Alice".into(), "Bob".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_three_hop_chain_exact_rows() {
    // Headline-shaped 3-hop: KNOWS, KNOWS, WORKS_AT. The only walk is
    // Alice→Bob→Carol→Globex (Carol WORKS_AT Globex).
    let (root, res) = run(
        "exec_frame_3hop",
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:WORKS_AT]->(d) \
             RETURN a.name AS a, b.name AS b, c.name AS c, d.name AS d",
    );
    let rows: Vec<(String, String, String, String)> = res
        .rows
        .iter()
        .map(|r| {
            (
                r[0].to_display(),
                r[1].to_display(),
                r[2].to_display(),
                r[3].to_display(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![(
            "Alice".into(),
            "Bob".into(),
            "Carol".into(),
            "Globex".into()
        )]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_sibling_branch_binding_isolation() {
    // The specific frame risk: Alice has TWO KNOWS siblings (Bob, Carol). Only
    // the Bob branch extends (Bob→Carol); the Carol branch dead-ends. If a
    // sibling fails to restore the mid binding `b` on backtrack, the Carol
    // branch would leak `b = Bob` and fabricate rows. Exactly one row proves
    // each branch is isolated.
    let (root, res) = run(
        "exec_frame_sibling",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN b.name AS b, c.name AS c",
    );
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(rows, vec![("Bob".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_same_end_node_via_two_paths() {
    // Carol is reachable from Alice by two distinct KNOWS paths — direct
    // (Alice→Carol) and via Bob (Alice→Bob→Carol). Both must survive as
    // separate rows; the frame must not collapse or duplicate them.
    let (root, res) = run(
        "exec_frame_twopaths",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(c:Person {name:'Carol'}) \
             RETURN c.name AS c",
    );
    assert_eq!(col0(&res), vec!["Carol", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_undirected_traversal() {
    // Bob's KNOWS edges: incoming from Alice (e0), outgoing to Carol (e1).
    // Undirected sees both.
    let (root, res) = run(
        "exec_frame_undirected",
        "MATCH (a:Person {name:'Bob'})-[:KNOWS]-(x) RETURN x.name AS x",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_where_references_mid_pattern_var() {
    // A WHERE on the mid node `b` (evaluated against the full row scope) keeps
    // only the chain through Bob.
    let (root, res) = run(
        "exec_frame_midwhere",
        "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE b.name = 'Bob' RETURN a.name AS a, c.name AS c",
    );
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(rows, vec![("Alice".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_multipattern_comma_join_shared_var() {
    // Two comma-joined patterns sharing `b`: pattern 1 binds b∈{Bob,Carol};
    // pattern 2 (b)-[:KNOWS]->(c) only extends from Bob.
    let (root, res) = run(
        "exec_frame_comma",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b), (b)-[:KNOWS]->(c) \
             RETURN b.name AS b, c.name AS c",
    );
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(rows, vec![("Bob".into(), "Carol".into())]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_varlen_zero_length_includes_self() {
    // `*0..1`: zero hops binds the anchor itself (Alice); one hop adds its
    // KNOWS neighbours.
    let (root, res) = run(
        "exec_frame_varlen0",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*0..1]->(b) RETURN b.name AS b",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_varlen_relationship_uniqueness() {
    // Undirected `*2..2` from Bob must not reuse an edge within a path: the
    // walks are Bob-e0-Alice-e4-Carol and Bob-e1-Carol-e4-Alice. Reusing e0/e1
    // would step back to Bob — so a "Bob" in the result would mean uniqueness
    // is broken.
    let (root, res) = run(
        "exec_frame_unique",
        "MATCH (a:Person {name:'Bob'})-[:KNOWS*2..2]-(x) RETURN x.name AS x",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn frame_path_var_walk_order() {
    // The path scratch buffer must yield nodes/relationships in walk order
    // (Alice→Bob→Carol = ids 0,1,2; edges e0,e1 = ids 0,1) after the frame
    // push/pop rewrite.
    let (root, res) = run(
        "exec_frame_pathorder",
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN [n IN nodes(p) | id(n)] AS ns, [r IN relationships(p) | id(r)] AS rs",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(render(&res.rows[0][0]), "[0,1,2]");
    assert_eq!(render(&res.rows[0][1]), "[0,1]");
    let _ = std::fs::remove_dir_all(&root);
}

// ── GQL quantified path patterns ─────────────────────────────────────────
// Graph (write_basic): KNOWS = Alice→Bob, Bob→Carol, Alice→Carol;
// WORKS_AT = Alice→Acme, Carol→Globex.

/// Run a query against the basic fixture, returning the result or the error
/// string (and always cleaning the fixture up).
fn run_result(tag: &str, q: &str) -> std::result::Result<QueryResult, String> {
    let (root, graph, _) = testgen::write_basic(tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let out = parser::parse(q)
        .map_err(|e| e.to_string())
        .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()));
    let _ = std::fs::remove_dir_all(&root);
    out
}

/// Sorted first-column display strings for a query that must succeed.
fn gql_col0(tag: &str, q: &str) -> Vec<String> {
    let mut v: Vec<String> = run_result(tag, q)
        .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"))
        .rows
        .iter()
        .map(|r| r[0].to_display())
        .collect();
    v.sort();
    v
}

#[test]
fn quantified_path_equals_varlength() {
    // The GQL group `((x)-[:KNOWS]->(y)){1,2}` is the cross-dialect equivalent
    // of Cypher's `-[:KNOWS*1..2]->`; both must yield the same multiset of end
    // nodes (Bob, Carol via 1 hop; Carol again via Alice→Bob→Carol).
    let gql = gql_col0(
        "exec_gql_q_vs_vl_g",
        "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){1,2} (b:Person) RETURN b.name AS b",
    );
    let cypher = gql_col0(
        "exec_gql_q_vs_vl_c",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(b:Person) RETURN b.name AS b",
    );
    assert_eq!(gql, vec!["Bob", "Carol", "Carol"]);
    assert_eq!(gql, cypher, "GQL quantifier must match Cypher var-length");
}

#[test]
fn quantified_exact_equals_fixed_varlength() {
    // `{2}` is exactly `*2..2`: the only 2-hop KNOWS path from Alice ends at Carol.
    let gql = gql_col0(
        "exec_gql_exact_g",
        "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){2} (b) RETURN b.name AS b",
    );
    let cypher = gql_col0(
        "exec_gql_exact_c",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*2..2]->(b) RETURN b.name AS b",
    );
    assert_eq!(gql, vec!["Carol"]);
    assert_eq!(gql, cypher);
}

#[test]
fn quantified_multi_hop_inner_matches_unrolled() {
    // A two-relationship inner sub-path repeated once equals the unrolled Cypher
    // chain `-[:KNOWS]->()-[:WORKS_AT]->()`: Alice→Carol→Globex (Bob has no
    // WORKS_AT edge).
    let gql = gql_col0(
            "exec_gql_multi_g",
            "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)-[:WORKS_AT]->(z)){1} (b) RETURN b.name AS b",
        );
    let cypher = gql_col0(
        "exec_gql_multi_c",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->()-[:WORKS_AT]->(b) RETURN b.name AS b",
    );
    assert_eq!(gql, vec!["Globex"]);
    assert_eq!(gql, cypher);
}

#[test]
fn quantified_dialect_switch_across_union() {
    // One query, two dialects: a Cypher branch UNIONed with a GQL branch. The
    // Cypher branch returns Alice's direct KNOWS (Bob, Carol); the GQL `{2}`
    // branch returns the 2-hop end (Carol); UNION de-dups to {Bob, Carol}.
    let rows = gql_col0(
        "exec_gql_union",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(b) RETURN b.name AS b \
             UNION \
             MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){2} (b) RETURN b.name AS b",
    );
    assert_eq!(rows, vec!["Bob", "Carol"]);
}

#[test]
fn quantified_mixed_with_plain_hop() {
    // A plain Cypher hop and a GQL group in the SAME pattern: Alice -KNOWS-> m
    // then one more KNOWS to b. Only Alice→Bob→Carol qualifies (Carol has no
    // outgoing KNOWS), so b = Carol — same as the unrolled 2-hop Cypher chain.
    let gql = gql_col0(
            "exec_gql_mixed_g",
            "MATCH (a:Person {name:'Alice'})-[:KNOWS]->(m) ((x)-[:KNOWS]->(y)){1} (b) RETURN b.name AS b",
        );
    let cypher = gql_col0(
        "exec_gql_mixed_c",
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]->()-[:KNOWS]->(b) RETURN b.name AS b",
    );
    assert_eq!(gql, vec!["Carol"]);
    assert_eq!(gql, cypher);
}

#[test]
fn quantified_count_bypasses_fast_path() {
    // `count(*)` over a quantified pattern must NOT take the single-node count
    // fast path (which keys off empty `rels`); the segments guard routes it to
    // the general matcher, counting all three matching paths.
    let res = run_result(
        "exec_gql_count",
        "MATCH (a:Person {name:'Alice'}) ((x)-[:KNOWS]->(y)){1,2} (b) RETURN count(*) AS c",
    )
    .unwrap();
    assert!(
        matches!(res.rows[0][0], Val::Int(3)),
        "{:?}",
        res.rows[0][0]
    );
}

#[test]
fn quantified_unbounded_rejected() {
    for q in [
        "MATCH (a) ((x)-[:KNOWS]->(y))+ (b) RETURN b",
        "MATCH (a) ((x)-[:KNOWS]->(y))* (b) RETURN b",
        "MATCH (a) ((x)-[:KNOWS]->(y)){1,} (b) RETURN b",
    ] {
        let e = run_result("exec_gql_unbounded", q).unwrap_err();
        assert!(
            e.contains("unbounded") || e.contains("lower bound"),
            "{q}: {e}"
        );
    }
}

#[test]
fn quantified_zero_lower_bound_rejected() {
    let e = run_result(
        "exec_gql_zero",
        "MATCH (a) ((x)-[:KNOWS]->(y)){0,2} (b) RETURN b",
    )
    .unwrap_err();
    assert!(e.contains("lower bound below 1"), "{e}");
}

// ── GQL path restrictors (PR 2) ──────────────────────────────────────────
// Run over the cyclic fixture (testgen::write_cycle): a→b→c→a triangle plus a
// c→b chord. Over `(s{name:'a'})-[:R*1..4]->(x)` the four modes yield a distinct
// number of paths — WALK 6, TRAIL 4, SIMPLE 3, ACYCLIC 2 — which is exactly what
// sets them apart (see the fixture doc-comment for the per-length enumeration).

/// Parse + run `q` against a fresh cycle fixture, returning the result or the
/// error string, and always cleaning the fixture up.
fn cycle_result(tag: &str, q: &str) -> std::result::Result<QueryResult, String> {
    let (root, graph) = testgen::write_cycle(tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let out = parser::parse(q)
        .map_err(|e| e.to_string())
        .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()));
    let _ = std::fs::remove_dir_all(&root);
    out
}

/// Sorted end-node names of `(s{name:'a'})-[<restrictor>:R*1..4]->(x)`, one entry
/// per matched path (duplicates kept), for the given restrictor prefix.
fn cycle_ends(tag: &str, restrictor: &str) -> Vec<String> {
    let q = format!("MATCH {restrictor} (s {{name:'a'}})-[:R*1..4]->(x) RETURN x.name AS n");
    let mut v: Vec<String> = cycle_result(tag, &q)
        .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"))
        .rows
        .iter()
        .map(|r| r[0].to_display())
        .collect();
    v.sort();
    v
}

#[test]
fn restrictors_distinguish_modes_on_cycle() {
    // The headline: each mode produces a different path multiset on the cycle.
    let walk = cycle_ends("exec_gql_r_walk", "WALK");
    let trail = cycle_ends("exec_gql_r_trail", "TRAIL");
    let simple = cycle_ends("exec_gql_r_simple", "SIMPLE");
    let acyclic = cycle_ends("exec_gql_r_acyclic", "ACYCLIC");

    // WALK reuses edges and nodes freely: every walk of length 1..4.
    assert_eq!(walk, vec!["a", "b", "b", "b", "c", "c"], "WALK");
    // TRAIL forbids edge reuse: drops the two length-4 walks that repeat an edge.
    assert_eq!(trail, vec!["a", "b", "b", "c"], "TRAIL");
    // SIMPLE forbids interior node repeats but lets the walk close at its start
    // `a`; the second visit to `b` (via the chord) is excluded.
    assert_eq!(simple, vec!["a", "b", "c"], "SIMPLE");
    // ACYCLIC forbids every node repeat, so the closing return to `a` is gone too.
    assert_eq!(acyclic, vec!["b", "c"], "ACYCLIC");

    // …and the counts are all distinct (6, 4, 3, 2).
    assert_eq!(
        (walk.len(), trail.len(), simple.len(), acyclic.len()),
        (6, 4, 3, 2)
    );
}

#[test]
fn bare_star_equals_trail() {
    // Parity: a bare `*` (no restrictor) must be byte-for-byte today's behaviour,
    // which is edge-unique = TRAIL. So absence of a restrictor ≡ explicit TRAIL.
    let bare = cycle_ends("exec_gql_r_bare", "");
    let trail = cycle_ends("exec_gql_r_bare_trail", "TRAIL");
    assert_eq!(bare, trail, "bare * must equal explicit TRAIL");
    assert_eq!(bare, vec!["a", "b", "b", "c"]);
}

#[test]
fn acyclic_excludes_start_that_simple_keeps() {
    // The one place SIMPLE and ACYCLIC differ on this graph is the cycle-closing
    // path a→b→c→a: SIMPLE keeps it (endpoints may coincide), ACYCLIC drops it.
    let simple = cycle_ends("exec_gql_r_se_simple", "SIMPLE");
    let acyclic = cycle_ends("exec_gql_r_se_acyclic", "ACYCLIC");
    assert!(
        simple.contains(&"a".to_string()),
        "SIMPLE keeps the closed cycle"
    );
    assert!(
        !acyclic.contains(&"a".to_string()),
        "ACYCLIC drops the closed cycle"
    );
}

#[test]
fn restrictor_requires_variable_length() {
    // A restrictor is honoured only where `varlen` owns the uniqueness scope.
    // On a fixed hop or a node-only pattern it is rejected, not silently ignored.
    for q in [
        "MATCH TRAIL (s {name:'a'})-[:R]->(x) RETURN x",
        "MATCH WALK (n) RETURN n",
    ] {
        let e = cycle_result("exec_gql_r_novar", q).unwrap_err();
        assert!(e.contains("variable-length relationship"), "{q}: {e}");
    }
}

#[test]
fn restrictor_over_quantified_group_rejected() {
    // The grammar accepts `TRAIL ((…)){m,n}` but lowering rejects it: the group
    // desugars into separate expansions that cannot share one uniqueness scope.
    let e = cycle_result(
        "exec_gql_r_quant",
        "MATCH TRAIL (s {name:'a'}) ((x)-[:R]->(y)){1,2} (z) RETURN z",
    )
    .unwrap_err();
    assert!(e.contains("restrictor") && e.contains("quantified"), "{e}");
}

// ── GQL shortest-path selectors (PR 3) ───────────────────────────────────
// ANY/ALL SHORTEST and SHORTEST k share the BFS core `select_paths` with
// `shortestPath()`. Parity is checked on the basic fixture; the multi-path
// behaviours run over the diamond fixture (testgen::write_diamond), which has two
// length-2 `s→t` paths (via `a`, via `b`) plus a length-3 detour `s→a→c→t`.

/// Parse + run `q` against a fresh diamond fixture, returning the result or the
/// error string, and always cleaning the fixture up.
fn diamond_result(tag: &str, q: &str) -> std::result::Result<QueryResult, String> {
    let (root, graph) = testgen::write_diamond(tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let out = parser::parse(q)
        .map_err(|e| e.to_string())
        .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()));
    let _ = std::fs::remove_dir_all(&root);
    out
}

/// Sorted path lengths (`size(r)` per row) for a diamond query that must succeed.
fn diamond_lengths(tag: &str, q: &str) -> Vec<i64> {
    let mut v: Vec<i64> = diamond_result(tag, q)
        .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"))
        .rows
        .iter()
        .map(|r| match r[0] {
            Val::Int(i) => i,
            ref o => panic!("expected Int length, got {o:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn any_shortest_parity_with_shortest_path() {
    // ANY SHORTEST over a MATCH pattern agrees with the shortestPath() function on
    // the same endpoints: the single shortest KNOWS path Alice→Carol is the direct
    // 1-hop edge, and its node sequence is [Alice, Carol].
    let sel = run_result(
        "exec_gql_any_parity",
        "MATCH ANY SHORTEST p = (a:Person {name:'Alice'})-[:KNOWS*]->(c:Person {name:'Carol'}) \
             RETURN size(relationships(p)) AS l, [n IN nodes(p) | n.name] AS names",
    )
    .unwrap();
    assert_eq!(sel.rows.len(), 1, "one shortest path for the single pair");
    assert!(
        matches!(sel.rows[0][0], Val::Int(1)),
        "{:?}",
        sel.rows[0][0]
    );
    assert_eq!(render(&sel.rows[0][1]), "['Alice','Carol']");

    // The shortestPath() function returns the identical length on the same pair.
    let func = run_result(
        "exec_gql_any_parity_fn",
        "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Carol'}) \
             RETURN length(shortestPath((a)-[:KNOWS*]->(c))) AS l",
    )
    .unwrap();
    assert!(matches!(func.rows[0][0], Val::Int(1)));
}

#[test]
fn any_shortest_picks_one_of_the_ties() {
    // On the diamond, ANY SHORTEST returns exactly one s→t path, of length 2.
    let lens = diamond_lengths(
        "exec_gql_any_one",
        "MATCH ANY SHORTEST (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
    );
    assert_eq!(lens, vec![2], "a single shortest path");
}

#[test]
fn all_shortest_returns_all_ties() {
    // ALL SHORTEST returns both length-2 paths (via `a`, via `b`) and not the
    // length-3 detour — every path of the minimum length, no more.
    let lens = diamond_lengths(
        "exec_gql_all_ties",
        "MATCH ALL SHORTEST (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
    );
    assert_eq!(lens, vec![2, 2], "two length-2 ties");

    // The two paths are distinct: their interior node is `a` in one, `b` in the
    // other.
    let res = diamond_result(
        "exec_gql_all_ties_nodes",
        "MATCH ALL SHORTEST p = (s {name:'s'})-[:R*]->(t {name:'t'}) \
             RETURN [n IN nodes(p) | n.name] AS names",
    )
    .unwrap();
    let mut names: Vec<String> = res.rows.iter().map(|r| render(&r[0])).collect();
    names.sort();
    assert_eq!(names, vec!["['s','a','t']", "['s','b','t']"]);
}

#[test]
fn shortest_k_returns_k_in_length_order() {
    // SHORTEST 2 → the two length-2 ties.
    assert_eq!(
        diamond_lengths(
            "exec_gql_k2",
            "MATCH SHORTEST 2 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
        ),
        vec![2, 2],
    );
    // SHORTEST 3 → the two ties plus the length-3 detour (k can pull in a longer
    // path once the shortest ones are spent).
    assert_eq!(
        diamond_lengths(
            "exec_gql_k3",
            "MATCH SHORTEST 3 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
        ),
        vec![2, 2, 3],
    );
    // SHORTEST 4 cannot exceed the three loopless paths that exist.
    assert_eq!(
        diamond_lengths(
            "exec_gql_k4",
            "MATCH SHORTEST 4 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
        ),
        vec![2, 2, 3],
    );
    // SHORTEST 1 ≡ ANY SHORTEST: a single shortest path.
    assert_eq!(
        diamond_lengths(
            "exec_gql_k1",
            "MATCH SHORTEST 1 (s {name:'s'})-[r:R*]->(t {name:'t'}) RETURN size(r) AS l",
        ),
        vec![2],
    );
}

#[test]
fn selector_applies_where_after_selection() {
    // Free endpoints ranging over every node, narrowed by a WHERE on their names:
    // only the s→t pairing survives, yielding the two shortest paths. This proves
    // the clause WHERE is applied per produced path, across the endpoint product.
    let lens = diamond_lengths(
        "exec_gql_sel_where",
        "MATCH ALL SHORTEST (x)-[r:R*]->(y) WHERE x.name = 's' AND y.name = 't' \
             RETURN size(r) AS l",
    );
    assert_eq!(lens, vec![2, 2]);

    // A WHERE that excludes every endpoint pair yields no rows.
    let none = diamond_result(
        "exec_gql_sel_where_empty",
        "MATCH ANY SHORTEST (x)-[r:R*]->(y) WHERE x.name = 't' AND y.name = 's' \
             RETURN size(r) AS l",
    )
    .unwrap();
    assert!(none.rows.is_empty(), "no t→s path exists");
}

#[test]
fn selector_optional_emits_null_when_no_path() {
    // OPTIONAL MATCH with a selector keeps the driving row and null-fills when no
    // path connects the endpoints (t cannot reach s).
    let res = diamond_result(
        "exec_gql_sel_optional",
        "MATCH (a {name:'t'}) OPTIONAL MATCH ANY SHORTEST (a)-[r:R*]->(z {name:'s'}) \
             RETURN a.name AS a, r IS NULL AS no_path",
    )
    .unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "t");
    assert!(matches!(res.rows[0][1], Val::Bool(true)));
}

#[test]
fn selector_rejections() {
    // A multi-relationship selected pattern is out of scope (PR 3 covers a single
    // relationship, like shortestPath()).
    let e = diamond_result(
        "exec_gql_sel_multi",
        "MATCH ANY SHORTEST (s {name:'s'})-[:R]->(m)-[:R*]->(t {name:'t'}) RETURN t",
    )
    .unwrap_err();
    assert!(e.contains("single relationship"), "{e}");

    // A selector combined with a restrictor is not yet supported.
    let e = diamond_result(
        "exec_gql_sel_restr",
        "MATCH ANY SHORTEST TRAIL (s {name:'s'})-[:R*]->(t {name:'t'}) RETURN t",
    )
    .unwrap_err();
    assert!(e.contains("restrictor"), "{e}");

    // A selector over a quantified group is rejected at lowering.
    let e = diamond_result(
        "exec_gql_sel_quant",
        "MATCH ALL SHORTEST (s {name:'s'}) ((x)-[:R]->(y)){1,2} (t) RETURN t",
    )
    .unwrap_err();
    assert!(e.contains("selector") && e.contains("quantified"), "{e}");

    // A selector cannot share its clause with a comma-joined pattern.
    let e = diamond_result(
        "exec_gql_sel_multipat",
        "MATCH ANY SHORTEST (s {name:'s'})-[:R*]->(t {name:'t'}), (u) RETURN t",
    )
    .unwrap_err();
    assert!(e.contains("only") && e.contains("pattern"), "{e}");
}

// ── GQL label boolean expressions (PR 4) ─────────────────────────────────
// The basic fixture has disjoint labels :Person (Alice, Bob, Carol) and
// :Company (Acme, Globex), and rel-types KNOWS / WORKS_AT — enough to tell the
// boolean forms apart on both nodes and relationships.

#[test]
fn label_boolean_node_cardinalities() {
    // OR unions the two label sets (all 5), NOT-Person leaves the 2 companies,
    // and AND is empty (no node carries both labels) — three distinct sets.
    assert_eq!(
        gql_col0(
            "exec_gql_label_or",
            "MATCH (n:Person|Company) RETURN n.name AS n"
        ),
        vec!["Acme", "Alice", "Bob", "Carol", "Globex"],
    );
    assert_eq!(
        gql_col0("exec_gql_label_not", "MATCH (n:!Person) RETURN n.name AS n"),
        vec!["Acme", "Globex"],
    );
    assert!(
        gql_col0(
            "exec_gql_label_and",
            "MATCH (n:Person&Company) RETURN n.name AS n"
        )
        .is_empty(),
        "no node carries both labels",
    );
}

#[test]
fn colon_chain_lowers_to_and_not_or() {
    // Parity: `:Person:Company` is AND sugar, so it must give the SAME (empty)
    // result as `:Person&Company` — NOT the 5-row OR result. A regression that
    // lowered the colon chain to OR would surface here.
    let colon = gql_col0(
        "exec_gql_colon_and",
        "MATCH (n:Person:Company) RETURN n.name AS n",
    );
    let amp = gql_col0(
        "exec_gql_amp_and",
        "MATCH (n:Person&Company) RETURN n.name AS n",
    );
    assert!(colon.is_empty());
    assert_eq!(colon, amp);
}

#[test]
fn label_boolean_reltype_cardinalities() {
    // Alice's out-edges: KNOWS→Bob, KNOWS→Carol, WORKS_AT→Acme. OR keeps all
    // three neighbours, NOT-KNOWS keeps just the WORKS_AT target, AND is empty
    // (an edge carries exactly one type).
    assert_eq!(
        gql_col0(
            "exec_gql_rel_or",
            "MATCH (a {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b",
        ),
        vec!["Acme", "Bob", "Carol"],
    );
    assert_eq!(
        gql_col0(
            "exec_gql_rel_not",
            "MATCH (a {name:'Alice'})-[:!KNOWS]->(b) RETURN b.name AS b",
        ),
        vec!["Acme"],
    );
    assert!(
        gql_col0(
            "exec_gql_rel_and",
            "MATCH (a {name:'Alice'})-[:KNOWS&WORKS_AT]->(b) RETURN b.name AS b",
        )
        .is_empty(),
        "an edge carries exactly one type",
    );
}

#[test]
fn reltype_alternation_parity_with_single_types() {
    // `:KNOWS|WORKS_AT` (now an Or expression) must equal the union of the two
    // single-type traversals — the pre-GQL alternation behaviour, unchanged.
    let alt = gql_col0(
        "exec_gql_rel_alt",
        "MATCH (a {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b",
    );
    let knows = gql_col0(
        "exec_gql_rel_knows",
        "MATCH (a {name:'Alice'})-[:KNOWS]->(b) RETURN b.name AS b",
    );
    let works = gql_col0(
        "exec_gql_rel_works",
        "MATCH (a {name:'Alice'})-[:WORKS_AT]->(b) RETURN b.name AS b",
    );
    let mut union = [knows, works].concat();
    union.sort();
    assert_eq!(alt, union);
}

// ── GQL PR 5 — `FOR` is UNWIND ────────────────────────────────────────────

#[test]
fn for_and_unwind_produce_identical_rows() {
    // `FOR x IN list` lowers onto the same UnwindClause as `UNWIND list AS x`,
    // so the two must emit byte-for-byte identical result rows — confirming the
    // lowering reaches the unchanged executor path.
    let by_for = gql_col0("exec_gql_for", "FOR x IN [3, 1, 2] RETURN x ORDER BY x");
    let by_unwind = gql_col0(
        "exec_gql_unwind",
        "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x",
    );
    assert_eq!(by_for, by_unwind);
    assert_eq!(by_for, vec!["1", "2", "3"]);

    // FOR over a MATCH-produced list behaves exactly like UNWIND too — one row
    // per matched `b` (Alice KNOWS both Bob and Carol in the basic fixture).
    let for_match = gql_col0(
        "exec_gql_for_match",
        "MATCH (a {name:'Alice'})-[:KNOWS]->(b) FOR n IN [b.name] RETURN n",
    );
    assert_eq!(for_match, vec!["Bob", "Carol"]);
}

#[test]
fn cast_executes_as_the_conversion_function() {
    // CAST lowers onto the to*/temporal functions, so it must compute exactly
    // what those functions do — confirming the lowering reaches the real path.
    assert_eq!(
        gql_col0("exec_gql_cast_int", "RETURN CAST('42' AS INTEGER) AS v"),
        gql_col0("exec_gql_toint", "RETURN toInteger('42') AS v"),
    );
    assert_eq!(
        gql_col0("exec_gql_cast_int2", "RETURN CAST('42' AS INTEGER) AS v"),
        vec!["42"],
    );
    // Float, string and boolean spellings all round-trip through their function.
    assert_eq!(
        gql_col0("exec_gql_cast_float", "RETURN CAST(3 AS FLOAT) AS v"),
        gql_col0("exec_gql_tofloat", "RETURN toFloat(3) AS v"),
    );
    assert_eq!(
        gql_col0("exec_gql_cast_bool", "RETURN CAST('true' AS BOOLEAN) AS v"),
        vec!["true"],
    );
    // A non-convertible value yields NULL, exactly like toInteger.
    assert_eq!(
        gql_col0("exec_gql_cast_null", "RETURN CAST('nope' AS INTEGER) AS v"),
        gql_col0("exec_gql_toint_null", "RETURN toInteger('nope') AS v"),
    );
}

// ── Stage 6 — LIMIT pushdown (early-stop) ────────────────────────────────
// Pushing the LIMIT into the match must return the SAME prefix of rows (in
// match-emit order) that buffering-then-truncating did — early-stop changes
// *when* matching halts, never *which* rows come first.

/// All rows of `q` as `(a, b)` display-string pairs, plus fixture cleanup.
fn pairs(tag: &str, q: &str) -> Vec<(String, String)> {
    let (root, res) = run(tag, q);
    let v = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    let _ = std::fs::remove_dir_all(&root);
    v
}

#[test]
fn limit_pushdown_traversal_returns_order_preserving_prefix() {
    let full = pairs(
        "exec_limit_full",
        "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b",
    );
    assert!(full.len() >= 3, "{full:?}"); // Alice→Bob, Alice→Carol, Bob→Carol
    let limited = pairs(
        "exec_limit_2",
        "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b LIMIT 2",
    );
    assert_eq!(limited.len(), 2);
    assert_eq!(limited.as_slice(), &full[..2]);
}

#[test]
fn limit_pushdown_with_skip() {
    // SKIP s LIMIT n caps the match at s+n, then the projection drops s — the
    // single returned row must equal the unlimited row at index s.
    let full = pairs(
        "exec_skiplim_full",
        "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b",
    );
    let limited = pairs(
        "exec_skiplim",
        "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS a, b.name AS b SKIP 1 LIMIT 1",
    );
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0], full[1]);
}

#[test]
fn limit_pushdown_streaming_scan_prefix() {
    // The node-only streaming path (try_stream_match) honours the cap too.
    let (root, full) = run(
        "exec_limit_stream_full",
        "MATCH (n:Person) RETURN n.name AS name",
    );
    let names_full = col0(&full); // sorted; just need the count
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(names_full.len(), 3);
    let (root, lim) = run(
        "exec_limit_stream",
        "MATCH (n:Person) RETURN n.name AS name LIMIT 2",
    );
    assert_eq!(lim.rows.len(), 2);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn limit_does_not_break_aggregation_or_order() {
    // The cap MUST be `None` when the projection aggregates or orders: the LIMIT
    // applies after the full group + sort, so all 3 Person rows must be seen.
    let (root, res) = run(
        "exec_limit_agg_guard",
        "MATCH (n:Person) RETURN n.city AS city, count(*) AS c ORDER BY c DESC LIMIT 1",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "London");
    assert!(
        matches!(res.rows[0][1], Val::Int(2)),
        "{:?}",
        res.rows[0][1]
    );
    let _ = std::fs::remove_dir_all(&root);

    // ORDER BY without aggregation also needs the full set before truncating.
    let (root, res) = run(
        "exec_limit_order_guard",
        "MATCH (n:Person) RETURN n.name AS name ORDER BY n.age DESC LIMIT 1",
    );
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Carol"); // oldest at 40
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn reads_route_through_the_block_cache() {
    // A second identical run over the same cache must be served from resident
    // blocks (no new misses), proving the executor reads through the cache.
    let (root, graph, _) = testgen::write_basic("exec_cache");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name").unwrap();

    engine.run(&ast).unwrap();
    let after_first = cache.metrics();
    assert!(
        after_first.misses > 0,
        "first run should populate the cache"
    );
    engine.run(&ast).unwrap();
    let after_second = cache.metrics();
    assert_eq!(
        after_second.misses, after_first.misses,
        "second run should hit the cache for every block"
    );
    assert!(after_second.hits > after_first.hits);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn parameter_substitution() {
    let (root, graph, _) = testgen::write_basic("exec_param");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let mut params = HashMap::new();
    params.insert("name".to_string(), Val::Str("Carol".into()));
    let engine = Engine::new(&gen, &cache).with_params(params);
    let ast = parser::parse("MATCH (n:Person) WHERE n.name = $name RETURN n.age AS age").unwrap();
    let res = engine.run(&ast).unwrap();
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(40)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn range_refuses_unbounded_span() {
    let (root, graph, _) = testgen::write_basic("exec_range_cap");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);

    // A full-i64 span would allocate until OOM, and the old unchecked `i += step`
    // wrapped past i64::MAX into an infinite loop. The element-count guard now
    // refuses it before allocating — a single cheap query no longer downs the server.
    let ast = parser::parse("RETURN range(0, 9223372036854775807)").unwrap();
    let err = engine
        .run(&ast)
        .expect_err("an unbounded range must be refused");
    assert!(
        format!("{err:#}").contains("range()"),
        "expected a range() limit error, got: {err:#}"
    );

    // A bounded range still materialises exactly.
    let ast = parser::parse("RETURN range(1, 5)").unwrap();
    let res = engine.run(&ast).unwrap();
    match &res.rows[0][0] {
        Val::List(xs) => assert_eq!(xs.len(), 5),
        other => panic!("expected a list, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn max_rows_limit_is_enforced() {
    let (root, graph, _) = testgen::write_basic("exec_maxrows");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache).with_max_rows(2);
    let ast = parser::parse("MATCH (n) RETURN n.name").unwrap();
    assert!(
        engine.run(&ast).is_err(),
        "5 rows should exceed the cap of 2"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── Regex limits + per-query intermediate budget (Tier-2 hardening) ──────

/// Open the shared fixture with an intermediate-element budget set.
fn budgeted_engine(
    root_tag: &str,
    budget: u64,
) -> (std::path::PathBuf, Generation, BlockCache, u64) {
    let (root, graph, _) = testgen::write_basic(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    (root, gen, cache, budget)
}

/// Run `q` against the fixture with the given budget, returning the result.
fn run_budgeted(root_tag: &str, budget: u64, q: &str) -> Result<QueryResult> {
    let (root, gen, cache, budget) = budgeted_engine(root_tag, budget);
    let engine = Engine::new(&gen, &cache).with_max_intermediate(budget);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast);
    let _ = std::fs::remove_dir_all(&root);
    res
}

/// Run `q` with the per-query budget OFF and a server-wide budget set. Asserts
/// the universal invariant — every query refunds its whole global charge, so
/// the live counter returns to zero — and returns `(result, peak_charge)`.
fn run_global(root_tag: &str, global: u64, q: &str) -> (Result<QueryResult>, u64) {
    let (root, graph, _) = testgen::write_basic(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(global);
    let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast);
    let peak = budget.peak();
    assert_eq!(
        budget.in_use(),
        0,
        "every query must refund its whole global charge"
    );
    let _ = std::fs::remove_dir_all(&root);
    (res, peak)
}

/// Run `q` with BOTH the per-query and the server-wide budget set, so a test
/// can assert which guard trips first. Also asserts the global refund invariant.
fn run_both(root_tag: &str, per_query: u64, global: u64, q: &str) -> Result<QueryResult> {
    let (root, graph, _) = testgen::write_basic(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(global);
    let engine = Engine::new(&gen, &cache)
        .with_max_intermediate(per_query)
        .with_global_budget(&budget);
    let ast = parser::parse(q).unwrap();
    let res = engine.run(&ast);
    assert_eq!(
        budget.in_use(),
        0,
        "query must refund its whole global charge"
    );
    let _ = std::fs::remove_dir_all(&root);
    res
}

/// True if `res` is the per-query budget error.
fn is_per_query_budget_err(res: &Result<QueryResult>) -> bool {
    res.as_ref().err().is_some_and(|e| {
        format!("{e:#}").contains("intermediate result budget")
            && !format!("{e:#}").contains("server-wide")
    })
}

/// True if `res` is the server-wide budget error.
fn is_global_budget_err(res: &Result<QueryResult>) -> bool {
    res.as_ref()
        .err()
        .is_some_and(|e| format!("{e:#}").contains("server-wide intermediate budget"))
}

/// True if `res` is the transient walk-work (`query.maxScan`) error — the budget a
/// count-pushdown traversal charges instead of the retained `maxIntermediate`.
fn is_scan_budget_err(res: &Result<QueryResult>) -> bool {
    res.as_ref()
        .err()
        .is_some_and(|e| format!("{e:#}").contains("scan budget"))
}

#[test]
fn regex_pattern_length_is_capped() {
    // A pattern past MAX_REGEX_PATTERN_BYTES is refused before compilation.
    let long = "a".repeat(2 * MAX_REGEX_PATTERN_BYTES);
    let err = run_err("exec_regex_len", &format!("RETURN 'a' =~ '{long}'"));
    assert!(
        err.contains("regex pattern is"),
        "expected the pattern-length error, got: {err}"
    );
}

#[test]
fn regex_size_limit_is_enforced() {
    // Well under the length cap in source bytes, but the compiled automaton
    // (a^100M via nested bounded repetition) blows the NFA size limit.
    let err = run_err(
        "exec_regex_size",
        "RETURN 'a' =~ '((((a{100}){100}){100}){100})'",
    );
    assert!(
        err.contains("Invalid regex"),
        "expected a size-limit compile error, got: {err}"
    );
}

#[test]
fn regex_cache_compiles_once_per_query() {
    let (root, graph, _) = testgen::write_basic("exec_regex_cache");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    // `=~` evaluates once per Person row; the pattern must compile once.
    let ast = parser::parse("MATCH (n:Person) WHERE n.name =~ 'A.*' RETURN n.name").unwrap();
    let res = engine.run(&ast).unwrap();
    assert_eq!(col0(&res), vec!["Alice"]);
    assert_eq!(
        engine.regex_cache.borrow().len(),
        1,
        "one constant pattern should occupy exactly one cache slot"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn intermediate_budget_caps_comprehension() {
    // range(0, 100000) charges ~100k; the comprehension's output charges
    // another ~100k, so a 150k budget trips inside the comprehension itself.
    let err = run_budgeted(
        "exec_budget_comp",
        150_000,
        "RETURN [x IN range(0, 100000) | x]",
    )
    .expect_err("the comprehension must exceed the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
}

#[test]
fn intermediate_budget_caps_concat_doubling() {
    // acc + acc doubles per iteration; charging every temp trips the budget
    // after ~12 iterations instead of allocating 2^30 elements.
    let err = run_budgeted(
        "exec_budget_concat",
        10_000,
        "RETURN size(reduce(acc = [0], x IN range(1, 30) | acc + acc))",
    )
    .expect_err("geometric list growth must exceed the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
}

#[test]
fn intermediate_budget_caps_unwind() {
    // range(0, 1000) charges ~1k and fits; the UNWIND'd rows charge ~1k more
    // and trip a 1.5k budget inside apply_unwind.
    let err = run_budgeted(
        "exec_budget_unwind",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN count(x)",
    )
    .expect_err("the unwound rows must exceed the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
}

#[test]
fn global_budget_bounds_concurrent_aggregate() {
    // The mechanism the per-query cap cannot provide: two "in-flight" queries
    // charging against one shared budget. Each is individually fine, but their
    // sum trips the ceiling — and the charge is held until each query refunds.
    let b = GlobalIntermediateBudget::new(1_000);
    assert!(b.try_charge(600), "query A within the ceiling");
    assert!(!b.try_charge(600), "query A+B exceed the ceiling");
    assert_eq!(b.in_use(), 1_200, "both charges live until refunded");
    b.release(600);
    assert_eq!(b.in_use(), 600);
    b.release(600);
    assert_eq!(b.in_use(), 0, "all refunded");
    assert_eq!(b.peak(), 1_200, "peak records the high-water");
}

#[test]
fn global_budget_zero_disables() {
    let b = GlobalIntermediateBudget::new(0);
    assert!(b.try_charge(10_000_000), "a 0 limit never rejects");
    assert_eq!(b.in_use(), 0, "a disabled guard never accumulates");
}

#[test]
fn global_budget_trips_with_per_query_off() {
    // Per-query budget disabled (0), but the server-wide guard still bounds the
    // query — and the distinct error names the global knob.
    let (root, graph, _) = testgen::write_basic("exec_global_solo");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(1_500);
    let engine = Engine::new(&gen, &cache)
        .with_max_intermediate(0)
        .with_global_budget(&budget);
    let ast = parser::parse("UNWIND range(0, 1000) AS x RETURN count(x)").unwrap();
    let err = engine
        .run(&ast)
        .expect_err("the global budget must trip with the per-query budget off");
    assert!(
        format!("{err:#}").contains("server-wide intermediate budget"),
        "expected the global-budget error, got: {err:#}"
    );
    assert_eq!(
        budget.in_use(),
        0,
        "a failed query refunds its whole charge"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn global_budget_refunds_after_successful_run() {
    let (root, graph, _) = testgen::write_basic("exec_global_refund");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(10_000);
    let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
    let ast = parser::parse("UNWIND range(0, 100) AS x RETURN count(x)").unwrap();
    engine.run(&ast).expect("well within the budget");
    assert_eq!(budget.in_use(), 0, "a finished query holds no charge");
    assert!(budget.peak() > 0, "it did draw on the budget mid-run");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn global_budget_rises_during_run_and_falls_after() {
    // Observe the live gauge from a second thread *while* a query executes: the
    // global charge must climb above zero during the run and return to zero
    // when it completes (the shared in-flight accounting, end to end).
    use std::sync::atomic::{AtomicBool, Ordering};
    let (root, graph, _) = testgen::write_basic("exec_global_inflight");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    // Generous ceiling so the query never trips the guard; it still charges
    // ~900k elements and holds them for the whole run, so the reader can see it
    // climb. (range() itself caps at 1M elements, so stay under that here.)
    let budget = GlobalIntermediateBudget::new(100_000_000);
    let done = AtomicBool::new(false);
    let mut max_live = 0u64;
    std::thread::scope(|s| {
        s.spawn(|| {
            let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
            let ast = parser::parse("UNWIND range(0, 900000) AS x RETURN count(x)").unwrap();
            engine.run(&ast).expect("within the budget");
            done.store(true, Ordering::Release);
        });
        // Sample the live gauge until the query thread signals completion,
        // yielding each iteration so the worker is not starved (the sampler
        // must not monopolise a constrained scheduler). The deadline is a
        // safety net so a stuck query fails the test rather than hanging it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !done.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            max_live = max_live.max(budget.in_use());
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    });
    assert!(
        max_live > 0,
        "the global charge must be observable above zero while the query runs"
    );
    assert_eq!(
        budget.in_use(),
        0,
        "the charge must fall back to zero once the query completes"
    );
    assert!(budget.peak() >= max_live, "peak tracks the live high-water");
    let _ = std::fs::remove_dir_all(&root);
}

// ── Per-query budget across every materialising operation ────────────────

#[test]
fn intermediate_budget_caps_collect() {
    // collect() buffers all inputs; charging the buffer trips a tight budget.
    let err = run_budgeted(
        "exec_budget_collect",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    )
    .expect_err("the collect buffer must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_count_distinct() {
    // count(DISTINCT x) holds a `seen` set; charging it trips the budget.
    let err = run_budgeted(
        "exec_budget_distinct",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN count(DISTINCT x)",
    )
    .expect_err("the DISTINCT seen-set must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_order_by() {
    // ORDER BY clones every row plus its sort key into a buffer (charged).
    let err = run_budgeted(
        "exec_budget_order",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x ORDER BY x",
    )
    .expect_err("the ORDER BY buffer must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_group_by() {
    // A distinct grouping key per row creates ~N groups; charging each group
    // (plus the unwound rows) trips the budget.
    let err = run_budgeted(
        "exec_budget_group",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x AS g, count(*) AS n",
    )
    .expect_err("the group table must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_caps_union() {
    // A UNION accumulates both branches (and a DISTINCT seen-set); a tight
    // budget trips while building it.
    let err = run_budgeted(
        "exec_budget_union",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x \
             UNION UNWIND range(0, 1000) AS y RETURN y",
    )
    .expect_err("the UNION buildup must exceed the budget");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

#[test]
fn intermediate_budget_zero_disables_the_cap() {
    // A 0 budget means unlimited: a large materialisation completes.
    let res = run_budgeted(
        "exec_budget_zero",
        0,
        "UNWIND range(0, 200000) AS x RETURN count(x)",
    )
    .expect("a 0 budget must not cap anything");
    assert_eq!(res.rows.len(), 1);
}

#[test]
fn intermediate_budget_allows_within_limit() {
    // Comfortably under the cap → the query succeeds.
    let res = run_budgeted(
        "exec_budget_within",
        100_000,
        "UNWIND range(0, 1000) AS x RETURN count(x)",
    )
    .expect("a query within the budget must succeed");
    assert_eq!(res.rows.len(), 1);
}

#[test]
fn intermediate_budget_threshold_passes_then_trips() {
    // The same materialisation passes under a generous cap and trips under a
    // tight one — the budget actually gates on the charged element count.
    run_budgeted(
        "exec_budget_thresh_ok",
        50_000,
        "RETURN [x IN range(0, 1000) | x]",
    )
    .expect("generous budget passes");
    let err = run_budgeted(
        "exec_budget_thresh_no",
        1_500,
        "RETURN [x IN range(0, 1000) | x]",
    )
    .expect_err("tight budget trips");
    assert!(format!("{err:#}").contains("intermediate result budget"));
}

// ── Server-wide budget across the same operations ────────────────────────

#[test]
fn global_budget_trips_on_comprehension() {
    let (res, _) = run_global("exec_g_comp", 1_500, "RETURN [x IN range(0, 1000) | x]");
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_collect() {
    let (res, _) = run_global(
        "exec_g_collect",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_count_distinct() {
    let (res, _) = run_global(
        "exec_g_distinct",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN count(DISTINCT x)",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_order_by() {
    let (res, _) = run_global(
        "exec_g_order",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x ORDER BY x",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_on_union() {
    let (res, _) = run_global(
        "exec_g_union",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN x UNION UNWIND range(0, 1000) AS y RETURN y",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_allows_small_query() {
    let (res, peak) = run_global("exec_g_small", 100_000, "RETURN [x IN range(0, 50) | x]");
    assert!(res.is_ok(), "a small query must not trip: {res:?}");
    assert!(peak > 0, "it still drew on the budget");
}

#[test]
fn global_budget_zero_completes_large() {
    // Per-query off and global 0 → no cap; a large materialisation completes
    // and the (disabled) counter never accumulates.
    let (res, peak) = run_global(
        "exec_g_zero",
        0,
        "UNWIND range(0, 200000) AS x RETURN count(x)",
    );
    assert!(res.is_ok(), "0 disables the guard: {res:?}");
    assert_eq!(peak, 0, "a disabled guard never accumulates");
}

#[test]
fn global_budget_refunds_after_a_trip() {
    // run_global already asserts in_use == 0; make the failure path explicit.
    let (res, _) = run_global(
        "exec_g_refund_fail",
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_global_budget_err(&res), "expected a trip: {res:?}");
}

// ── Interaction of the two budgets ───────────────────────────────────────

#[test]
fn per_query_budget_trips_first_when_tighter() {
    // Tighter per-query cap (1500) beneath a roomy global (10M) → the per-query
    // guard fires, named by its own error.
    let res = run_both(
        "exec_both_pq",
        1_500,
        10_000_000,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_per_query_budget_err(&res), "got: {res:?}");
}

#[test]
fn global_budget_trips_first_when_tighter() {
    // Tighter global (1500) beneath a roomy per-query cap (10M) → the
    // server-wide guard fires, named by its own error.
    let res = run_both(
        "exec_both_g",
        10_000_000,
        1_500,
        "UNWIND range(0, 1000) AS x RETURN collect(x)",
    );
    assert!(is_global_budget_err(&res), "got: {res:?}");
}

#[test]
fn both_budgets_off_completes_large() {
    let res = run_both(
        "exec_both_off",
        0,
        0,
        "UNWIND range(0, 200000) AS x RETURN count(x)",
    );
    assert!(res.is_ok(), "both budgets off → no cap: {res:?}");
}

// ── Expansion charge: a hub read must trip the budget (root cause 2b) ─────

/// Few-thousand-edge hub; comfortably clears `EXPAND_PAR_MIN` (64) so the pooled
/// reader fans out, and small enough to build in well under a millisecond.
const HUB_N: u64 = 3_000;
/// Far below `HUB_N`, so a single hub expansion (which charges ~`HUB_N`) trips it.
const HUB_TIGHT: u64 = 100;
/// Far above the whole star's cumulative charge (~a few × `HUB_N`), so a full
/// expansion completes — the guard must bound hubs without over-charging.
const HUB_GENEROUS: u64 = 10_000_000;

/// Run `q` against an `n`-leaf hub fixture (see [`testgen::write_hub`]) with the
/// given per-query and server-wide budgets (0 disables either), optionally behind
/// a fanout pool so the parallel `expand_chain_par` path is exercised. Asserts the
/// universal refund invariant and returns `(result, global_peak)`.
fn run_hub(
    tag: &str,
    n: u64,
    per_query: u64,
    scan: u64,
    global: u64,
    with_pool: bool,
    q: &str,
) -> (Result<QueryResult>, u64) {
    let (root, graph) = testgen::write_hub(tag, n);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(global);
    let mut engine = Engine::new(&gen, &cache)
        .with_max_intermediate(per_query)
        .with_max_scan(scan)
        .with_global_budget(&budget);
    if with_pool {
        engine = engine.with_fanout_pool(Some(std::sync::Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap(),
        )));
    }
    let res = engine.run(&parser::parse(q).unwrap());
    let peak = budget.peak();
    assert_eq!(
        budget.in_use(),
        0,
        "every query must refund its whole global charge"
    );
    let _ = std::fs::remove_dir_all(&root);
    (res, peak)
}

// ── Per-query-type budget routing (the retention split) ───────────────────
// The same hub adjacency read is charged against a *different* budget depending on
// what the query does with the rows. `RETURN count(*)` is count-pushdown — it
// retains nothing, so its reads charge the transient `maxScan` budget and never the
// retained `maxIntermediate` nor the server-wide aggregate. A row-returning or
// var-length traversal materialises, so the same reads charge `maxIntermediate`
// (and the global budget). run_hub args: (tag, n, maxIntermediate, maxScan, global).

#[test]
fn hub_count_one_hop_answered_by_degree_terminal() {
    // The degree-sum terminal answers a 1-hop `count(neighbour)` from the hub's stored
    // out-degree in O(1) — it never walks the `HUB_N`-edge adjacency, so the tight scan
    // cap the old row-by-row walk tripped is no longer even approached. (The 2-hop
    // variant below still trips: building its penultimate frontier reads the hub.)
    let (res, _) = run_hub(
        "exec_hub_1hop_degterm",
        HUB_N,
        0,
        HUB_TIGHT,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x)",
    );
    let r = res.expect("degree terminal answers a 1-hop hub count without tripping maxScan");
    assert!(
        matches!(r.rows[0][0], Val::Int(n) if n == HUB_N as i64),
        "1-hop count == hub out-degree: {:?}",
        r.rows[0][0]
    );
}

#[test]
fn hub_count_two_hop_trips_scan_budget() {
    let (res, _) = run_hub(
        "exec_hub_2hop_scan",
        HUB_N,
        0,
        HUB_TIGHT,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN count(y)",
    );
    assert!(is_scan_budget_err(&res), "got: {res:?}");
}

#[test]
fn hub_count_filtered_trips_scan_with_zero_rows() {
    // 2b for counts: `:Hub` matches only the centre, so every neighbour is rejected
    // and ZERO rows complete — yet the adjacency read still charges scan and trips.
    let (res, _) = run_hub(
        "exec_hub_filt_scan",
        HUB_N,
        0,
        HUB_TIGHT,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x:Hub) RETURN count(x)",
    );
    assert!(
        is_scan_budget_err(&res),
        "a filtered count read (no rows complete) must still trip maxScan: {res:?}"
    );
}

#[test]
fn hub_count_ignores_retained_and_global_budgets() {
    // The crux of the split: with the retained *and* global budgets tight (well
    // below `HUB_N`) but scan generous, the count still completes with the right
    // answer — it draws neither — and never charges the server-wide aggregate.
    let (res, peak) = run_hub(
        "exec_hub_count_iso",
        HUB_N,
        HUB_TIGHT,
        HUB_GENEROUS,
        HUB_TIGHT,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x) AS n",
    );
    let res = res.expect("a count must not draw the retained/global budgets");
    assert_eq!(col0(&res), vec![HUB_N.to_string()]);
    assert!(
        peak < HUB_N,
        "count-pushdown must not charge the per-edge reads to the server-wide \
             aggregate: peak={peak}"
    );
}

#[test]
fn hub_materialize_one_hop_trips_per_query_budget() {
    // Row-returning: the same read materialises, so it charges the retained budget.
    let (res, _) = run_hub(
        "exec_hub_1hop_pq",
        HUB_N,
        HUB_TIGHT,
        0,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
    );
    assert!(
        is_per_query_budget_err(&res),
        "a materialising hub read must trip maxIntermediate: {res:?}"
    );
}

#[test]
fn hub_materialize_one_hop_trips_global_budget() {
    let (res, _) = run_hub(
        "exec_hub_1hop_g",
        HUB_N,
        0,
        0,
        HUB_TIGHT,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
    );
    assert!(
        is_global_budget_err(&res),
        "a materialising hub read must trip the server-wide budget: {res:?}"
    );
}

#[test]
fn hub_materialize_two_hop_trips_per_query_budget() {
    let (res, _) = run_hub(
        "exec_hub_2hop_pq",
        HUB_N,
        HUB_TIGHT,
        0,
        0,
        false,
        "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN y",
    );
    assert!(is_per_query_budget_err(&res), "got: {res:?}");
}

#[test]
fn hub_varlen_count_charges_retained_not_scan() {
    // The two-regime nuance the sweep found: a *var-length* `count(*)` still
    // materialises its per-node path set, so even under count-pushdown it charges
    // the retained budget (and trips it) — unlike a fixed-hop count, which is pure
    // scan. With scan disabled, the trip can only be the retained path materialise.
    let (res, _) = run_hub(
        "exec_hub_varlen_count",
        HUB_N,
        HUB_TIGHT,
        0,
        0,
        false,
        "MATCH (c:Hub)-[:LINK*1..2]->(x) RETURN count(*)",
    );
    assert!(
        is_per_query_budget_err(&res),
        "a var-length count materialises paths and must trip maxIntermediate: {res:?}"
    );
}

#[test]
fn frame_get_flatten_shadowing() {
    // Pins the shadowing convention that makes the parallel walk match the
    // sequential LIFO oracle: a child frame shadows its parent, the last write in
    // a layer wins, and `flatten` (root-first) reproduces both.
    use std::sync::Arc;
    let mut base = HashMap::new();
    base.insert("a".to_string(), Val::Int(1));
    base.insert("b".to_string(), Val::Int(2));
    let root = Frame::root(&base);
    let child = Arc::new(Frame {
        parent: Some(root),
        delta: vec![("b".into(), Val::Int(20))],
    });
    let grand = Arc::new(Frame {
        parent: Some(child),
        delta: vec![("a".into(), Val::Int(100)), ("a".into(), Val::Int(101))],
    });
    assert!(
        matches!(grand.get("b"), Some(Val::Int(20))),
        "child shadows parent"
    );
    assert!(
        matches!(grand.get("a"), Some(Val::Int(101))),
        "last delta wins"
    );
    assert!(grand.get("c").is_none());
    let flat = grand.flatten();
    assert_eq!(flat.len(), 2);
    assert!(matches!(flat.get("a"), Some(Val::Int(101))));
    assert!(matches!(flat.get("b"), Some(Val::Int(20))));
}

#[test]
fn count_pushdown_matches_materialized() {
    // The pushed-down `count(*)`/`count(v)` must equal the row count the
    // materialising path produces — across 1/2/3-hop, a constant co-item, and an
    // empty match. (write_basic: KNOWS Alice->Bob, Bob->Carol.)
    let count_of = |tag: &str, q: &str| -> i64 {
        match &run_budgeted(tag, 1_000_000, q).unwrap().rows[0][0] {
            Val::Int(n) => *n,
            o => panic!("count is not an Int: {o:?}"),
        }
    };
    let rows_of =
        |tag: &str, q: &str| -> usize { run_budgeted(tag, 1_000_000, q).unwrap().rows.len() };
    let cases = [
        (
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*) AS c",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name AS b",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(c) AS c",
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name AS x",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN count(*) AS c, 7 AS k",
            "MATCH (a:Person)-[:KNOWS]->(b) RETURN b.name AS b",
        ),
        (
            // empty: 3-hop KNOWS dead-ends at Carol.
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN count(*) AS c",
            "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(d) RETURN d.name AS d",
        ),
    ];
    for (cq, rq) in cases {
        assert_eq!(
            count_of("cpd_eq", cq) as usize,
            rows_of("cpd_eq", rq),
            "`{cq}`"
        );
    }
}

#[test]
fn count_pushdown_falls_back_but_correct() {
    // Shapes that must NOT push down still return the correct count via the
    // materialising path.
    let count_of = |q: &str| -> i64 {
        match &run_budgeted("cpd_fb", 1_000_000, q).unwrap().rows[0][0] {
            Val::Int(n) => *n,
            o => panic!("count is not an Int: {o:?}"),
        }
    };
    // count(DISTINCT) — KNOWS targets {Bob, Carol} = 2 distinct (not pushed: needs
    // the value set), vs 3 total KNOWS edges (Alice->Bob, Bob->Carol, Alice->Carol).
    assert_eq!(
        count_of("MATCH (a:Person)-[:KNOWS]->(b) RETURN count(DISTINCT b) AS c"),
        2
    );
    // WHERE survivor filter — only Alice->Bob of the 3 KNOWS edges (falls back to
    // the materialising path, which applies WHERE).
    assert_eq!(
        count_of("MATCH (a:Person)-[:KNOWS]->(b) WHERE b.name = 'Bob' RETURN count(*) AS c"),
        1
    );
}

#[test]
fn hub_small_expansion_succeeds_under_a_generous_budget() {
    // The guard must bound hubs without over-charging: a generous scan budget lets
    // the whole star expand and return the right count. A materialising run of the
    // same shape really draws the server-wide aggregate (≥ one charge per edge read).
    let (res, _) = run_hub(
        "exec_hub_small_ok",
        HUB_N,
        HUB_GENEROUS,
        HUB_GENEROUS,
        HUB_GENEROUS,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN count(x) AS n",
    );
    let res = res.expect("a generous budget must let the hub expand");
    assert_eq!(col0(&res), vec![HUB_N.to_string()]);
    let (mat, peak) = run_hub(
        "exec_hub_small_mat",
        HUB_N,
        HUB_GENEROUS,
        0,
        HUB_GENEROUS,
        false,
        "MATCH (c:Hub)-[:LINK]->(x) RETURN x",
    );
    mat.expect("materialise under a generous budget");
    assert!(
        peak >= HUB_N,
        "a materialising expansion must charge the aggregate ≥ once per edge read: peak={peak}"
    );
}

#[test]
fn hub_expansion_charge_on_parallel_path() {
    // The fanout pool routes a fixed multi-hop chain through `expand_chain_par`,
    // whose adjacency reads gather on rayon — where the per-query `Cell` charge
    // state cannot be touched. The charge is applied on the calling thread once the
    // buffer lands, so the pooled walk routes to the SAME budget as the sequential
    // one: a count trips `maxScan`, a materialising walk trips `maxIntermediate` /
    // the global budget, and under generous budgets both return the sequential
    // result. The hop-1 frontier (`HUB_N` leaves) clears `EXPAND_PAR_MIN`, so the
    // pooled reader truly fans out rather than degrading to a sequential read.
    let cq = "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN count(y) AS n";
    let mq = "MATCH (c:Hub)-[:LINK]->(x)-[:LINK]->(y) RETURN y";
    // count-pushdown on the pooled path → scan budget.
    let (scan, _) = run_hub("exec_hub_par_scan", HUB_N, 0, HUB_TIGHT, 0, true, cq);
    assert!(
        is_scan_budget_err(&scan),
        "the pooled count must trip maxScan: {scan:?}"
    );
    // materialising on the pooled path → retained + global budgets.
    let (pq, _) = run_hub("exec_hub_par_pq", HUB_N, HUB_TIGHT, 0, 0, true, mq);
    assert!(
        is_per_query_budget_err(&pq),
        "the pooled materialising walk must trip maxIntermediate: {pq:?}"
    );
    let (g, _) = run_hub("exec_hub_par_g", HUB_N, 0, 0, HUB_TIGHT, true, mq);
    assert!(
        is_global_budget_err(&g),
        "the pooled materialising walk must trip the server-wide budget: {g:?}"
    );
    // Generous budgets: pooled and sequential counts agree exactly.
    let (par, _) = run_hub(
        "exec_hub_par_ok",
        HUB_N,
        HUB_GENEROUS,
        HUB_GENEROUS,
        HUB_GENEROUS,
        true,
        cq,
    );
    let (seq, _) = run_hub(
        "exec_hub_seq_ok",
        HUB_N,
        HUB_GENEROUS,
        HUB_GENEROUS,
        HUB_GENEROUS,
        false,
        cq,
    );
    let par = par.expect("pooled generous run");
    let seq = seq.expect("sequential generous run");
    assert_eq!(
        col0(&par),
        col0(&seq),
        "pooled and sequential expansions must agree"
    );
    assert_eq!(col0(&par), vec![HUB_N.to_string()]);
}

#[test]
fn engine_is_not_sync_rayon_invariant() {
    // Compile-time guard-rail for the rayon-safety invariant. The entire argument
    // that `par_gather`/`par_walk` are race-free rests on `&Engine` never crossing a
    // thread boundary: the `Sync + Send` bound on `par_gather`'s closure can only
    // reject a closure that captures `&self` *because* `Engine` is `!Sync` (its
    // per-query `Cell`/`RefCell` charge state — `budget_used`, `scan_used`,
    // `count_acc`, `global_charged`, `regex_cache`). If a future change makes that
    // state `Sync` (e.g. swapping a `Cell` for an `Atomic` to "charge in parallel"),
    // the `AmbiguousIfSync` resolution below becomes ambiguous and this stops
    // compiling — forcing a deliberate re-read of `charge_walk` and the `par_gather`
    // contract before the invariant is weakened.
    trait AmbiguousIfSync<A> {
        fn _f() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    impl<T: ?Sized + Sync> AmbiguousIfSync<u8> for T {}
    // Resolves to the blanket `()` impl unambiguously iff `Engine` is NOT `Sync`.
    let _ = <Engine<'static, Generation> as AmbiguousIfSync<_>>::_f;
}

// ── Engine reuse: the charge resets and refunds per run ───────────────────

#[test]
fn global_charge_resets_between_runs_on_a_reused_engine() {
    let (root, graph, _) = testgen::write_basic("exec_g_reuse");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let budget = GlobalIntermediateBudget::new(100_000);
    let engine = Engine::new(&gen, &cache).with_global_budget(&budget);
    let ast = parser::parse("UNWIND range(0, 500) AS x RETURN count(x)").unwrap();
    for _ in 0..5 {
        engine.run(&ast).expect("within the budget");
        assert_eq!(budget.in_use(), 0, "each run fully refunds before the next");
    }
    // A reused engine that has succeeded many times still trips correctly when a
    // single run exceeds the budget (no stale carry-over inflating the charge).
    let big = parser::parse("UNWIND range(0, 200000) AS x RETURN collect(x)").unwrap();
    assert!(
        engine.run(&big).is_err(),
        "the oversized run must still trip"
    );
    assert_eq!(budget.in_use(), 0, "the tripped run also refunds");
    let _ = std::fs::remove_dir_all(&root);
}

// ── GlobalIntermediateBudget mechanics ───────────────────────────────────

#[test]
fn global_budget_starts_at_zero() {
    let b = GlobalIntermediateBudget::new(1_000);
    assert_eq!(b.in_use(), 0);
    assert_eq!(b.peak(), 0);
    assert_eq!(b.limit(), 1_000);
}

#[test]
fn global_budget_charge_to_exact_limit_then_trips() {
    let b = GlobalIntermediateBudget::new(1_000);
    assert!(
        b.try_charge(1_000),
        "charging exactly to the limit is allowed"
    );
    assert_eq!(b.in_use(), 1_000);
    assert!(!b.try_charge(1), "one element past the limit trips");
    b.release(1_001);
    assert_eq!(b.in_use(), 0);
}

#[test]
fn global_budget_release_cycles_return_to_zero() {
    let b = GlobalIntermediateBudget::new(10_000);
    for _ in 0..1_000 {
        assert!(b.try_charge(7));
        b.release(7);
    }
    assert_eq!(b.in_use(), 0, "balanced charge/release nets to zero");
    assert!(b.peak() >= 7, "peak captured the per-cycle high-water");
}

#[test]
fn varlen_bounds_inverted_range_stays_empty() {
    // `*5..3`: an explicit max below min is an empty range. It must NOT be clamped
    // to `5..5` (which would wrongly match exactly-length-5 walks). Every consumer
    // treats `max < min` as "no path", so the raw inverted bounds are correct.
    let vl = VarLength {
        min: Some(5),
        max: Some(3),
    };
    let (min, max) = varlen_bounds(&vl);
    assert_eq!((min, max), (5, 3));
    assert!(
        max < min,
        "inverted range must stay inverted (empty), not clamp"
    );

    // A normal range is unaffected.
    assert_eq!(
        varlen_bounds(&VarLength {
            min: Some(2),
            max: Some(4)
        }),
        (2, 4)
    );
    // An open `*` still spans 1..=MAX_VARLEN_HOPS.
    assert_eq!(
        varlen_bounds(&VarLength {
            min: None,
            max: None
        }),
        (1, MAX_VARLEN_HOPS)
    );
}

#[test]
fn varlen_charges_intermediate_budget() {
    // A tiny budget trips while materialising variable-length paths…
    let err = run_budgeted(
        "exec_budget_varlen_tiny",
        2,
        "MATCH (a)-[*1..3]->(b) RETURN count(*)",
    )
    .expect_err("varlen paths must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    // …and a generous budget leaves the same query untouched (no over-charge).
    let res = run_budgeted(
        "exec_budget_varlen_ok",
        1_000_000,
        "MATCH (a)-[*1..3]->(b) RETURN count(*)",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 1);
}

#[test]
fn correlated_unwind_seek_returns_right_rows() {
    // `UNWIND … AS w MATCH (n:Person {name:w})` keys the anchor off the per-row
    // scalar `w`. The planner now resolves it to a `node_Person_name` index seek
    // (see plan.rs `bound_scalar_*` tests); this proves the seek path is sound
    // end-to-end — the right rows, no more, no fewer.
    let (root, res) = run(
        "exec_correlated_unwind",
        "UNWIND ['Alice', 'Bob', 'Nobody'] AS w \
             MATCH (n:Person {name: w}) RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn correlated_where_seek_returns_right_rows() {
    // The `WHERE n.name = w` spelling resolves to the same per-row seek.
    let (root, res) = run(
        "exec_correlated_where",
        "UNWIND ['Carol', 'Bob'] AS w \
             MATCH (n:Person) WHERE n.name = w RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn distinct_charges_intermediate_budget() {
    // The `seen` set behind `RETURN DISTINCT` is charged: a budget that admits
    // the 3-row match (3) but not the DISTINCT pass (+3) trips; 1M is untouched.
    let err = run_budgeted(
        "exec_budget_distinct_tiny",
        5,
        "MATCH (n:Person) RETURN DISTINCT n.city",
    )
    .expect_err("DISTINCT must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_distinct_ok",
        1_000_000,
        "MATCH (n:Person) RETURN DISTINCT n.city",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 2); // London, Paris
}

#[test]
fn order_by_charges_intermediate_budget() {
    // The `keyed` sort buffer clones every row; charged before it is built.
    let err = run_budgeted(
        "exec_budget_order_tiny",
        5,
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
    )
    .expect_err("ORDER BY must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_order_ok",
        1_000_000,
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 3);
}

#[test]
fn group_by_charges_intermediate_budget() {
    // Each distinct group costs one element; a budget that admits the match (3)
    // and the first group but not the second (Paris) trips.
    let err = run_budgeted(
        "exec_budget_group_tiny",
        4,
        "MATCH (n:Person) RETURN n.city, count(*)",
    )
    .expect_err("GROUP BY must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_group_ok",
        1_000_000,
        "MATCH (n:Person) RETURN n.city, count(*)",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 2); // {London: 2}, {Paris: 1}
}

#[test]
fn all_shortest_frontier_charges_intermediate_budget() {
    // `ALL SHORTEST`/`SHORTEST k` keep the cloned-per-branch simple-path search
    // (the number of shortest paths can be exponential), whose BFS frontier is
    // charged per expansion layer so a hub-dense graph trips the budget mid-search
    // instead of ballooning RSS. The destination (a Company) is unreachable over
    // `:KNOWS`, so no *result* is ever charged — only the frontier — yet a tiny
    // budget still trips.
    let q = "MATCH (a:Person {name:'Alice'}), (z:Company {name:'Acme'}) \
                 MATCH ALL SHORTEST (a)-[:KNOWS*]->(z) RETURN count(*) AS c";
    let err = run_budgeted("exec_budget_allsp_tiny", 3, q)
        .expect_err("the ALL SHORTEST frontier must charge the budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted("exec_budget_allsp_ok", 1_000_000, q)
        .expect("a generous budget must not affect the query");
    assert_eq!(col0(&res), vec!["0"]); // no KNOWS path Person→Company
}

#[test]
fn all_shortest_charges_frontier_branches_proportional_to_depth() {
    // Each live shortest-path branch clones a `Vec<Hop>` + `HashSet` whose size grows with
    // its depth, but the frontier charge used a fixed `charge(1)`, under-counting a deep
    // branch by a factor of its depth. On a length-12 chain the total charge is now
    // ≈ L(L+1)/2 = 78 (proportional), where the old fixed accounting was ≈ 2L-1 = 23. A
    // budget of 40 sits between them: it admitted the old accounting but must trip the new.
    let (root, graph) = testgen::write_chain("exec_chain_depth_charge", 12);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let q = "MATCH ALL SHORTEST (a {name:'n0'})-[r:R*]->(b {name:'n12'}) RETURN size(r) AS l";
    let ast = parser::parse(q).unwrap();

    // A generous budget completes: the single length-12 path.
    let ok = Engine::new(&gen, &cache)
        .with_max_intermediate(10_000)
        .run(&ast)
        .expect("a generous budget completes the chain search");
    assert!(matches!(ok.rows[0][0], Val::Int(12)), "{:?}", ok.rows[0][0]);

    // The depth-proportional charge trips at 40; the old fixed `charge(1)` would not.
    let err = Engine::new(&gen, &cache)
        .with_max_intermediate(40)
        .run(&ast)
        .expect_err("the depth-proportional frontier charge must trip at 40");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn two_free_endpoint_selector_charges_the_search_product() {
    // A selector with two free endpoints scans all candidates on each side and launches a
    // shortest-path search for every (src, dst) pair — quadratic in the id space. On the
    // isolated fixture (8 nodes, no edges) each search does ~0 frontier work, so the old
    // code sailed under a small budget however many pairs it ran; the fix charges the 8×8
    // product up front, tripping before the fan-out runs.
    let (root, graph) = testgen::write_isolated("exec_2free_product", 8);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let two_free = "MATCH ALL SHORTEST (a)-[r:R*]->(b) RETURN count(*) AS c";
    let ast = parser::parse(two_free).unwrap();
    let err = Engine::new(&gen, &cache)
        .with_max_intermediate(20)
        .run(&ast)
        .expect_err("two free endpoints must charge |srcs|×|dsts|");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    // A generous budget still completes (no edges ⇒ no path ⇒ count 0).
    let ok = Engine::new(&gen, &cache)
        .with_max_intermediate(10_000)
        .run(&ast)
        .expect("a generous budget completes");
    assert_eq!(col0(&ok), vec!["0"]);

    // Constraining one endpoint to a single candidate drops the product to 1×8 = 8, which
    // the same budget admits — the charge bites only the pathological two-free case.
    let one_bound = "MATCH ALL SHORTEST (a {name:'n0'})-[r:R*]->(b) RETURN count(*) AS c";
    let ast2 = parser::parse(one_bound).unwrap();
    let ok2 = Engine::new(&gen, &cache)
        .with_max_intermediate(20)
        .run(&ast2)
        .expect("one constrained endpoint stays under the budget");
    assert_eq!(col0(&ok2), vec!["0"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn shortest_path_any_succeeds_under_tiny_budget() {
    // `shortestPath()`/`ANY SHORTEST` now runs a single global-`visited` BFS that
    // enqueues each node at most once and charges no frontier, so it succeeds in
    // `O(V+E)` under a budget the old cloned-per-branch search would trip on (3 is
    // below where the frontier charge fired for `all_shortest_frontier_*`). The
    // unreachable-Company probe returns NULL cheaply; a reachable pair returns its
    // length.
    let unreachable = "MATCH (a:Person {name:'Alice'}), (z:Company {name:'Acme'}) \
                           RETURN shortestPath((a)-[:KNOWS*]->(z)) IS NULL AS np";
    let res = run_budgeted("exec_budget_anysp_unreach", 3, unreachable)
        .expect("the global-visited BFS must not charge the frontier");
    assert_eq!(col0(&res), vec!["true"]); // no KNOWS path Person→Company

    let reachable = "MATCH (a:Person {name:'Alice'}), (z:Person {name:'Carol'}) \
                         RETURN length(shortestPath((a)-[:KNOWS*]->(z))) AS l";
    let res = run_budgeted("exec_budget_anysp_reach", 3, reachable)
        .expect("the global-visited BFS must not charge the frontier");
    assert_eq!(col0(&res), vec!["1"]); // Alice-[:KNOWS]->Carol directly (e4)
}

#[test]
fn shortest_path_explore_cap_bounds_the_bfs() {
    // The dedicated `maxShortestPathExplore` cap bounds the global-visited BFS
    // independently of `maxIntermediate`: the reachable pair the unlimited BFS
    // finds above fails *cleanly* (no panic, no OOM) once the discovery count
    // exceeds the cap, while the default (0 = unlimited) still succeeds and the
    // re-derived path keeps its correct length.
    let q = "MATCH (a:Person {name:'Alice'}), (z:Person {name:'Carol'}) \
                 RETURN length(shortestPath((a)-[:KNOWS*]->(z))) AS l";
    let (root, gen, cache, _) = budgeted_engine("exec_sp_explore_cap", 1_000_000);
    let err = Engine::new(&gen, &cache)
        .with_max_shortest_path_explore(1)
        .run(&parser::parse(q).unwrap())
        .expect_err("the explore cap must bound the BFS");
    assert!(
        format!("{err:#}").contains("maxShortestPathExplore"),
        "expected the explore-cap error, got: {err:#}"
    );
    let res = Engine::new(&gen, &cache)
        .with_max_shortest_path_explore(0)
        .run(&parser::parse(q).unwrap())
        .expect("the default unlimited cap must succeed");
    assert_eq!(col0(&res), vec!["1"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn shortest_path_meets_in_the_middle() {
    // A length-2 shortest path exercises the bidirectional search's meet-in-middle
    // and reconstruction *across* the meeting node — the endpoints share no direct
    // edge; the path is Acme -WORKS_AT- Alice -KNOWS- Bob (undirected, mixed type).
    let q = "MATCH (a:Company {name:'Acme'}), (b:Person {name:'Bob'}) \
                 RETURN length(shortestPath((a)-[*..6]-(b))) AS l";
    let res = run_budgeted("exec_sp_midmeet", 1_000_000, q).expect("a length-2 path exists");
    assert_eq!(col0(&res), vec!["2"]);
}

#[test]
fn shortest_path_with_pool_is_correct() {
    // A pool-configured engine must return identical results to the sequential one
    // (the parallel frontier gather shares the same neighbour logic; the full-graph
    // benchmark exercises the large-frontier rayon branch).
    let (root, gen, cache, _) = budgeted_engine("exec_sp_pool", 1_000_000);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap(),
    );
    let q = "MATCH (a:Company {name:'Acme'}), (b:Person {name:'Bob'}) \
                 RETURN length(shortestPath((a)-[*..6]-(b))) AS l";
    let res = Engine::new(&gen, &cache)
        .with_fanout_pool(Some(pool))
        .run(&parser::parse(q).unwrap())
        .expect("pool-configured shortestPath runs");
    assert_eq!(col0(&res), vec!["2"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// Slice 2 integration: routing a hub through the streaming reader must return
/// **identical** results to materialising it — for both the sequential (`expand_chain`)
/// and pooled (`par_walk`) engines, over count / ordered-rows / undirected / path-var /
/// relationship-property (`rel_ok`) shapes. Driven by a low `adj_stream_threshold` (2)
/// so `write_basic`'s degree-3 anchor (Alice) streams while its degree-1 neighbours
/// materialise — a genuine hub/normal mix in one frontier. Each query is run four ways
/// (seq/pool × stream/materialise); all four must agree byte-for-byte.
#[test]
fn hub_streaming_matches_materialise() {
    let (root, gen, cache, _) = budgeted_engine("exec_hub_stream", 1_000_000);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let queries = [
        // 2-hop count — the count-pushdown terminal over a streamed hub frontier.
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN count(*)",
        // 2-hop ordered rows with rel + node vars bound (Alice is the streamed hub).
        "MATCH (a:Person)-[r1:KNOWS]->(b)-[r2:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b, c.name AS c ORDER BY a, b, c",
        // Type-alternation one-hop from the hub anchor (mixes KNOWS + WORKS_AT out-edges).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b ORDER BY b",
        // Same, UNORDERED — locks row-order preservation (streamed hop order must equal
        // the materialised `hops_par` order, not merely the same set).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b",
        // Undirected one-hop from the hub anchor (outgoing-then-incoming stream order).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS]-(x) RETURN x.name AS x ORDER BY x",
        // Path variable: the reconstructed path must match streamed vs materialised.
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p) AS len, nodes(p) AS ns",
        // Relationship-property predicate: gated OUT of the parallel path (has props),
        // so this exercises `expand_chain`'s hub arm applying `rel_ok` per streamed hop.
        "MATCH (a:Person {name:'Alice'})-[:KNOWS {since:2020}]->(b) RETURN b.name AS b ORDER BY b",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let run = |pool: Option<std::sync::Arc<rayon::ThreadPool>>, threshold: u64| {
            let mut e = Engine::new(&gen, &cache).with_adj_stream_threshold(threshold);
            if let Some(p) = pool {
                e = e.with_fanout_pool(Some(p));
            }
            e.run(&ast)
                .unwrap_or_else(|err| panic!("`{q}` (threshold {threshold}) failed: {err:#}"))
        };
        // Materialise baseline (threshold beyond any degree) on the sequential engine.
        let base = run(None, u64::MAX);
        let variants = [
            ("seq+stream", run(None, 2)),
            ("pool+materialise", run(Some(pool.clone()), u64::MAX)),
            ("pool+stream", run(Some(pool.clone()), 2)),
        ];
        for (tag, v) in &variants {
            assert_eq!(base.columns, v.columns, "columns differ ({tag}) for `{q}`");
            assert_eq!(disp(&base), disp(v), "rows differ ({tag}) for `{q}`");
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn multi_hop_with_pool_matches_sequential() {
    // The parallel breadth-first chain expansion (`expand_chain_par`) must return
    // exactly the rows — and in the same order — as the sequential depth-first
    // walk, across fixed multi-hop chains, a path variable, a pushed LIMIT, and a
    // tight intermediate budget. The fixture frontier is below `EXPAND_PAR_MIN`, so
    // `par_gather` reads sequentially here; this pins `expand_chain_par`'s merge
    // (node_ok / next-var / charge / cap / path binding) against the DFS path,
    // while the full-Wikidata benchmark exercises the wide-frontier rayon branch.
    let (root, gen, cache, _) = budgeted_engine("exec_multihop_pool", 1_000_000);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    // Var-length is gated OUT of the parallel path, so a `*1..2` query is the
    // sequential walk under both engines — still asserted identical to lock the gate.
    let queries = [
        // 2-hop, ordered, with both rel and node vars bound.
        "MATCH (a:Person)-[r1:KNOWS]->(b)-[r2:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b, c.name AS c ORDER BY a, b, c",
        // 3-hop mixed types ending in WORKS_AT.
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:WORKS_AT]->(d) \
             RETURN a.name AS a, d.name AS d ORDER BY a, d",
        // Undirected one-hop from a pinned anchor (outgoing-then-incoming order).
        "MATCH (a:Person {name:'Bob'})-[:KNOWS]-(x) RETURN x.name AS x ORDER BY x",
        // Path variable: the bound path must reconstruct identically.
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p) AS len, nodes(p) AS ns",
        // Type alternation + an anchor with no LIMIT/ORDER (pushed-cap off).
        "MATCH (a:Person {name:'Alice'})-[:KNOWS|WORKS_AT]->(b) RETURN b.name AS b ORDER BY b",
        // Inline property on a non-anchor node — exercises `node_ok` reading the
        // shared `Scope::Frame` on the parallel walk (Bob KNOWS Carol).
        "MATCH (a:Person)-[:KNOWS]->(b {name:'Carol'}) RETURN a.name AS a ORDER BY a",
        // Pushed LIMIT on a 2-hop — gated to the sequential early-exit path under
        // both engines (a capped chain must not breadth-first over-read).
        "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name AS c LIMIT 1",
        // Variable-length — gated to the sequential path under both engines.
        "MATCH (a:Person {name:'Alice'})-[:KNOWS*1..2]->(b) RETURN b.name AS b ORDER BY b",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        // Whole-result equality preserving row order — the parallel walk must be
        // byte-for-byte identical, not merely the same set.
        let disp = |r: &QueryResult| -> Vec<Vec<String>> {
            r.rows
                .iter()
                .map(|row| row.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    }
    // A tight intermediate budget must trip at the same point under both engines:
    // the 2-hop chain emits 1 row (Alice→Bob→Carol), so a budget of 1 fits and 0
    // (with the count terminal) is irrelevant — use a chain that overflows a small
    // budget identically. Alice→Bob→Carol is the lone 2-hop KNOWS path; a budget
    // that the cross-pattern terminal also charges trips both engines alike.
    let q = "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a.name, c.name";
    let ast = parser::parse(q).unwrap();
    let seq = Engine::new(&gen, &cache).with_max_intermediate(1).run(&ast);
    let par = Engine::new(&gen, &cache)
        .with_max_intermediate(1)
        .with_fanout_pool(Some(pool.clone()))
        .run(&ast);
    match (&seq, &par) {
        (Ok(s), Ok(p)) => assert_eq!(s.rows.len(), p.rows.len(), "budget row count differs"),
        (Err(_), Err(_)) => {} // both trip the budget — consistent
        _ => panic!("budget behaviour differs: seq={seq:?}, par={par:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn fixed_chain_enforces_relationship_uniqueness() {
    // HIK-81: a fixed-length chain must not traverse the same relationship twice
    // within one MATCH (openCypher relationship-isomorphism / relationship-uniqueness).
    // `write_cycle` is a directed triangle a→b→c→a with an extra chord c→b, i.e. edges
    //   e0: a→b   e1: b→c   e2: c→a   e3: c→b
    // so b and c are joined by TWO distinct undirected edges (e1, e3) — a genuine
    // 2-cycle — while every node also offers a same-edge bounce-back. Every expected
    // row below is derived by hand from that edge list, not from another slater path.
    let (root, graph) = testgen::write_cycle("exec_reluniq");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    // Sequential walk (`expand_chain`).
    let run_seq = |q: &str| {
        let ast = parser::parse(q).unwrap();
        disp(
            &Engine::new(&gen, &cache)
                .run(&ast)
                .unwrap_or_else(|e| panic!("seq `{q}` failed: {e:#}")),
        )
    };
    // Parallel breadth-first walk (`expand_chain_par` / `walk_merge_hop`).
    let run_par = |q: &str| {
        let ast = parser::parse(q).unwrap();
        disp(
            &Engine::new(&gen, &cache)
                .with_fanout_pool(Some(pool.clone()))
                .run(&ast)
                .unwrap_or_else(|e| panic!("par `{q}` failed: {e:#}")),
        )
    };
    // Hub-streaming arm of the sequential walk (threshold 1 makes every anchor stream).
    let run_hub = |q: &str| {
        let ast = parser::parse(q).unwrap();
        disp(
            &Engine::new(&gen, &cache)
                .with_adj_stream_threshold(1)
                .run(&ast)
                .unwrap_or_else(|e| panic!("hub `{q}` failed: {e:#}")),
        )
    };

    // (1) Undirected closed 2-walk anchored at b, binding both edges. Undirected
    //     neighbours of b are {a via e0, c via e1, c via e3}. Returning to b:
    //       via a: only e0 connects a–b ⇒ r2 == r1 ⇒ rejected (same edge).
    //       via c: e1 and e3 both connect b–c ⇒ the two DISTINCT-edge pairings survive.
    //     Expected (ORDER BY e1, e2): [c,1,3] and [c,3,1]. The three degenerate
    //     bounce-backs (e0/e0, e1/e1, e3/e3) must NOT appear — pre-fix they did, so
    //     this assertion fails without the fix (5 rows, incl. r1 == r2).
    let q = "MATCH (x {name:'b'})-[r1]-(y)-[r2]-(x) \
                 RETURN y.name AS y, id(r1) AS e1, id(r2) AS e2 ORDER BY e1, e2";
    let expected = vec![
        vec!["c".to_string(), "1".to_string(), "3".to_string()],
        vec!["c".to_string(), "3".to_string(), "1".to_string()],
    ];
    assert_eq!(
        run_seq(q),
        expected,
        "sequential must drop bounce-backs, keep the true 2-cycle"
    );
    assert_eq!(
        run_par(q),
        expected,
        "parallel (expand_chain_par) must match"
    );
    assert_eq!(
        run_hub(q),
        expected,
        "hub-streaming arm must enforce uniqueness too"
    );
    for row in run_seq(q) {
        assert_ne!(
            row[1], row[2],
            "a surviving row must bind two distinct edges"
        );
    }

    // (2) count(*) of the same closed walk = 2 (the two genuine 2-cycles), NOT 5
    //     (the pre-fix bounce-back inflation). `degree_terminal_dir` declines here
    //     because the closing node reuses the start variable, so the count flows
    //     through the per-hop merge that now enforces uniqueness.
    let cq = "MATCH (x {name:'b'})-[r1]-(y)-[r2]-(x) RETURN count(*)";
    assert_eq!(
        run_seq(cq),
        vec![vec!["2".to_string()]],
        "count(*) must exclude bounce-backs"
    );
    assert_eq!(run_par(cq), vec![vec!["2".to_string()]]);

    // (3) The rule binds ANONYMOUS relationship elements too, not only named vars:
    //     the same closed walk with no rel variables still yields exactly 2.
    let aq = "MATCH (x {name:'b'})-[]-(y)-[]-(x) RETURN count(*)";
    assert_eq!(
        run_seq(aq),
        vec![vec!["2".to_string()]],
        "anonymous rels are unique too"
    );
    assert_eq!(run_par(aq), vec![vec!["2".to_string()]]);

    // (4) Positive control — nodes may repeat, only relationships are unique, and a
    //     legitimately distinct-edge directed 3-hop chain from a is unaffected.
    //     Directed edges out of a: a-e0->b-e1->c-{e2->a, e3->b}. Both length-3 paths
    //     use three distinct edges, so both survive (one revisits node a).
    //       a→b→c→a  (e0,e1,e2)  and  a→b→c→b  (e0,e1,e3)
    let q3 = "MATCH (a {name:'a'})-[r1]->(m)-[r2]->(n)-[r3]->(z) \
                  RETURN z.name AS z, id(r1) AS e1, id(r2) AS e2, id(r3) AS e3 ORDER BY z";
    let expected3 = vec![
        vec![
            "a".to_string(),
            "0".to_string(),
            "1".to_string(),
            "2".to_string(),
        ],
        vec![
            "b".to_string(),
            "0".to_string(),
            "1".to_string(),
            "3".to_string(),
        ],
    ];
    assert_eq!(
        run_seq(q3),
        expected3,
        "distinct-edge 3-hop chains must still be returned"
    );
    assert_eq!(run_par(q3), expected3);

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn aggregation_with_pool_matches_sequential() {
    // The parallel group-by / count(DISTINCT) precompute (Task 12) must produce the
    // same grouped output — same row order, same values — as the sequential per-row
    // eval. The wide fixture has 200 nodes (≥ AGG_PAR_MIN) with `team` ∈ {Red, Blue,
    // null} and unique `name`, so the pooled engine truly fans the property reads out
    // while the grouping/reduction stays single-threaded.
    let (root, graph) = testgen::write_wide("exec_aggregation", 200);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let queries = [
        // Group-by a property + count(*) — the canonical shape.
        "MATCH (n) RETURN n.team AS t, count(*) AS c ORDER BY t",
        // count(DISTINCT n.p) — single row, no grouping item; nulls excluded.
        "MATCH (n) RETURN count(DISTINCT n.team) AS c",
        // Multiple aggregates over a group, incl. order-sensitive collect().
        "MATCH (n) RETURN n.team AS t, count(*) AS c, collect(n.name) AS names ORDER BY t",
        // min/max over a group (uses the cmp_total reduce path).
        "MATCH (n) RETURN n.team AS t, min(n.name) AS lo, max(n.name) AS hi ORDER BY t",
        // No grouping item, single-arg aggregate over the whole table.
        "MATCH (n) RETURN count(n.team) AS c",
        // A constant grouping item alongside the aggregate.
        "MATCH (n) RETURN n.team AS t, count(*) AS c, 1 AS one ORDER BY t",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    }

    // A `$param` grouping key exercises the Param arm of `eval_simple`.
    {
        let q = "MATCH (n) RETURN n.team AS t, count(*) AS c, $k AS k ORDER BY t";
        let ast = parser::parse(q).unwrap();
        let params = HashMap::from([("k".to_string(), Val::Int(7))]);
        let seq = Engine::new(&gen, &cache)
            .with_params(params.clone())
            .run(&ast)
            .unwrap();
        let par = Engine::new(&gen, &cache)
            .with_params(params)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap();
        assert_eq!(disp(&seq), disp(&par), "param rows differ for `{q}`");
    }

    // A tight intermediate budget must trip (or fit) at the same point under both
    // engines — the parallel path charges each new group and each aggregated value
    // in the same order as the sequential merge.
    let q = "MATCH (n) RETURN n.team AS t, count(*) AS c";
    let ast = parser::parse(q).unwrap();
    for budget in [1u64, 2, 3] {
        let seq = Engine::new(&gen, &cache)
            .with_max_intermediate(budget)
            .run(&ast);
        let par = Engine::new(&gen, &cache)
            .with_max_intermediate(budget)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast);
        match (&seq, &par) {
            (Ok(s), Ok(p)) => assert_eq!(disp(s), disp(p), "budget={budget} rows differ"),
            (Err(_), Err(_)) => {}
            _ => panic!("budget={budget} behaviour differs: seq={seq:?}, par={par:?}"),
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-78: an anchor scan must be a **stream**, not one `Vec<u64>` over the whole id
/// space built before the first row is produced. The observable is
/// `anchor_ids_scanned()` — the ids the scan actually walked, counted at the one place
/// they are produced.
///
/// * The **control**: a full-width scan that matches nothing must still walk the whole
///   id space, on both the `AllNodes` and the `LabelScan` sweep. Without this, "the
///   capped scan walked few ids" would be vacuous (a counter that always reads 0 would
///   satisfy it).
/// * The **claim**: the *same* scans under `LIMIT 1` must walk no more than the first
///   window — a few hundredths of the id space — not all of it. An eager scan fails
///   this even if it counts honestly, because a pushed `LIMIT` could only truncate the
///   row loop *after* every id had already been produced and held. (Verified: with the
///   sweeps reverted to `(0..node_count).collect()` / `collect_nodes_with_label`, this
///   assertion reports 20 000 walked ids and fails.)
#[test]
fn anchor_scan_streams_and_limit_short_circuits() {
    const N: u64 = 20_000;
    let (root, graph) = testgen::write_wide("exec_scan_stream", N);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let run = |q: &str| -> (usize, u64) {
        let ast = parser::parse(q).unwrap();
        let eng = Engine::new(&gen, &cache);
        let out = eng
            .run(&ast)
            .unwrap_or_else(|e| panic!("`{q}` failed: {e:#}"));
        (out.rows.len(), eng.anchor_ids_scanned())
    };

    // Control: an uncapped scan walks the whole id space (and the fixture has no index,
    // so `WHERE n.name = …` really is a full sweep, not a seek).
    for q in [
        "MATCH (n) WHERE n.name = 'nobody' RETURN n", // AllNodes
        "MATCH (n:Person {team:'Green'}) RETURN n",   // LabelScan
    ] {
        let (rows, walked) = run(q);
        assert_eq!(rows, 0, "`{q}` matches nothing");
        assert_eq!(walked, N, "`{q}` must walk the whole id space");
    }

    // The claim: `LIMIT 1` stops the scan inside its first window instead of producing
    // 20 000 ids up front. That window is also the scan's entire resident footprint.
    let ceiling = CAND_WINDOW_MIN;
    for q in [
        "MATCH (n) RETURN n LIMIT 1",        // AllNodes
        "MATCH (n:Person) RETURN n LIMIT 1", // LabelScan
    ] {
        let (rows, walked) = run(q);
        assert_eq!(rows, 1, "`{q}` returns one row");
        assert!(
            walked <= ceiling,
            "`{q}` walked {walked} ids (> {ceiling}) of a {N}-id space — LIMIT did not \
                 short-circuit the scan"
        );
    }

    // The stream still yields exactly the ids the eager sweep did.
    let (rows, walked) = run("MATCH (n:Person) RETURN n");
    assert_eq!(rows, N as usize / 2, "every :Person still matches");
    assert_eq!(walked, N, "…and the label sweep still walks the id space");
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-104: the *two arms HIK-78 left eager* — a `LabelScan` under a write delta and every
/// `RelTypeScan` — must also stream, via the order-preserving k-way merge. Same observable
/// as HIK-78 (`anchor_ids_scanned()` = the id space the scan actually walked); the extra
/// obligation is that the merge reproduce the eager `sort`+`dedup` **union across sources**
/// (base ∪ delta/segment overlay) exactly — order, dedup and tombstone suppression.
///
/// A rows-only test would pass without the fix (the eager path returned the same rows); the
/// claim is proven only against the *walked* count, under a delta and for a reltype scan.
#[test]
fn merged_and_reltype_scans_stream_and_limit_short_circuits() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    // ── LabelScan under a write delta (the case this ticket exists for) ──────────────
    const N: u64 = 20_000;
    let (root, graph) = testgen::write_wide("hik104_labelscan_delta", N);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // A live delta: two born :Person nodes + one deleted base :Person (node 2). This forces
    // the *merge* arm — a plain `LabelScan` is lazy only over a pure core with an empty
    // delta — and gives the merge a non-empty overlay plus a tombstone to suppress.
    let mut mem = Memtable::new();
    mem.upsert_node("Person", "name", Value::Str("newp0".into()), None, []);
    mem.upsert_node("Person", "name", Value::Str("newp1".into()), None, []);
    mem.delete_node("Person", "name", Value::Str("node0002".into()), Some(2));
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        view.delta().is_tombstoned(2),
        "node 2 tombstoned in the delta"
    );
    let nc = view.node_count();
    assert!(nc > N, "delta-born ids extend the scan bound");

    let person = view.label_id("Person").unwrap();
    let scan = NodeScan::LabelScan { label_id: person };

    // Correctness vs independently-derived truth: the merged candidate set is exactly the
    // base :Person ids (evens, from the fixture invariant) minus the tombstone, unioned
    // with the delta's born :Person ids — ascending and deduped, one window at a time.
    let born: Vec<u64> = view.delta().born_ids_with_label("Person");
    assert_eq!(born.len(), 2, "two born :Person nodes in the delta");
    let mut expected: Vec<u64> = (0..N).step_by(2).filter(|&i| i != 2).collect();
    expected.extend(&born);
    expected.sort_unstable();
    expected.dedup();

    let eng = Engine::new(&view, &cache);
    let got = eng.scan_candidates(&scan).unwrap();
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "merged label scan must be strictly ascending + deduped, got {got:?}"
    );
    assert_eq!(
        got, expected,
        "merged label scan = base ∪ overlay, minus tombstones"
    );
    // The uncapped drain walked the whole id space (control: the claim below is non-vacuous).
    assert_eq!(
        eng.anchor_ids_scanned(),
        nc,
        "uncapped merge walks the whole id space"
    );

    // The claim: under `LIMIT 1` the merge stops inside its first window (≤ `CAND_WINDOW_MIN`
    // of a 20 000-id space) instead of materialising the union up front.
    let eng2 = Engine::new(&view, &cache);
    let out = eng2
        .run(&parser::parse("MATCH (n:Person) RETURN n LIMIT 1").unwrap())
        .unwrap();
    assert_eq!(out.rows.len(), 1, "LIMIT 1 returns one row");
    assert!(
        eng2.anchor_ids_scanned() <= CAND_WINDOW_MIN,
        "LIMIT 1 walked {} ids (> {CAND_WINDOW_MIN}) under a delta — merge did not \
             short-circuit",
        eng2.anchor_ids_scanned()
    );
    let _ = std::fs::remove_dir_all(&root);

    // ── RelTypeScan over a dense endpoint posting (the 733 MB-class base) ────────────
    const M: u64 = 5_000;
    let (root, graph) = testgen::write_rel_chain("hik104_reltype", M);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // A delta that deletes one endpoint (node 100) — proves the merge's per-window
    // suppression runs for a reltype scan under a write delta too.
    let mut mem = Memtable::new();
    mem.delete_node("N", "name", Value::Str("node0100".into()), Some(100));
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        view.delta().is_tombstoned(100),
        "node 100 tombstoned in the delta"
    );
    let mnc = view.node_count();

    let t = view.reltype_id("T").unwrap();
    let scan = NodeScan::RelTypeScan {
        reltype_ids: vec![t],
        side: RelEndpointSide::Source,
        guaranteed_label: None,
    };
    // Every node but the last is a T source; the tombstone drops node 100.
    let expected: Vec<u64> = (0..M - 1).filter(|&i| i != 100).collect();

    let eng = Engine::new(&view, &cache);
    let got = eng.scan_candidates(&scan).unwrap();
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "merged reltype scan must be strictly ascending + deduped"
    );
    assert_eq!(
        got, expected,
        "reltype scan = dense posting minus the tombstone"
    );
    assert_eq!(
        eng.anchor_ids_scanned(),
        mnc,
        "uncapped reltype merge walks the whole id space"
    );

    // Short-circuit: pulling a single window walks no more than the first window, even
    // though the base posting covers ~all M nodes.
    let eng2 = Engine::new(&view, &cache);
    let mut s = eng2.candidate_stream(&scan).unwrap();
    let first = eng2.next_candidates(&mut s).unwrap();
    assert!(first.is_some(), "the first window yields candidates");
    assert!(
        eng2.anchor_ids_scanned() <= CAND_WINDOW_MIN,
        "one window walked {} ids (> {CAND_WINDOW_MIN}) of a {M}-id space — reltype merge \
             did not short-circuit",
        eng2.anchor_ids_scanned()
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn anchor_filter_with_pool_matches_sequential() {
    // The parallel anchor `node_ok` prefilter (Task 10) must keep exactly the
    // candidates — in the same order — that the sequential inline filter keeps,
    // across the shapes that make `node_ok` actually read a record: a label scan
    // with an inline property, a boolean label expression (full scan), an inline
    // property bound from a parameter, and a tight intermediate budget. The wide
    // fixture has 200 nodes (100 :Person / 100 :Company) so the candidate set
    // clears `SCAN_PAR_MIN` and the pooled engine truly fans the filter out.
    let (root, graph) = testgen::write_wide("exec_anchor_filter", 200);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let queries = [
        // Label scan (Person guaranteed) + inline prop → node_ok reads `team`.
        "MATCH (n:Person {team:'Red'}) RETURN n.name AS name ORDER BY name",
        // Boolean label expr → full scan + per-candidate label decode.
        "MATCH (n:Person|Company) RETURN n.name AS name ORDER BY name",
        // Negated label → full scan, keeps only the :Company half.
        "MATCH (n:!Person) RETURN n.name AS name ORDER BY name",
        // Inline prop with no matching value → every candidate rejected.
        "MATCH (n:Person {team:'Green'}) RETURN n.name AS name ORDER BY name",
        // Aggregate over the filtered set (uncapped, the prefilter's home turf).
        "MATCH (n:Person {team:'Blue'}) RETURN count(*) AS c",
    ];
    for q in queries {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(&gen, &cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        let disp = |r: &QueryResult| -> Vec<Vec<String>> {
            r.rows
                .iter()
                .map(|row| row.iter().map(|c| c.to_display()).collect())
                .collect()
        };
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    }
    // A tight intermediate budget must trip (or fit) at the same point under both
    // engines — the prefilter doesn't charge, so the single-threaded merge/terminal
    // still governs the budget identically.
    let q = "MATCH (n:Person|Company) RETURN n.name";
    let ast = parser::parse(q).unwrap();
    let seq = Engine::new(&gen, &cache)
        .with_max_intermediate(10)
        .run(&ast);
    let par = Engine::new(&gen, &cache)
        .with_max_intermediate(10)
        .with_fanout_pool(Some(pool.clone()))
        .run(&ast);
    match (&seq, &par) {
        (Ok(s), Ok(p)) => assert_eq!(s.rows.len(), p.rows.len(), "budget row count differs"),
        (Err(_), Err(_)) => {}
        _ => panic!("budget behaviour differs: seq={seq:?}, par={par:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn build_view_with_pool_matches_sequential() {
    // The parallel `algo.*` subgraph build (`build_view`, Task 11) must produce the
    // same view — hence identical algorithm output — as the sequential build. The
    // per-node adjacency reads gather on the pool while the pos-mapping/select merge
    // stays single-threaded, so node list + 0-based `out` are byte-for-byte identical.
    // Two fixtures: the small edge-bearing `write_basic` graph pins the merge with
    // real adjacency (below `BUILD_VIEW_PAR_MIN`, so `par_gather` reads sequentially),
    // and the 200-node `write_wide` graph clears the threshold so the pooled engine
    // truly fans the reads out (no edges → exercises the parallel read + empty merge).
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap(),
    );
    let disp = |r: &QueryResult| -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_display()).collect())
            .collect()
    };
    let assert_par_eq = |gen: &Generation, cache: &BlockCache, q: &str| {
        let ast = parser::parse(q).unwrap();
        let seq = Engine::new(gen, cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("sequential `{q}` failed: {e:#}"));
        let par = Engine::new(gen, cache)
            .with_fanout_pool(Some(pool.clone()))
            .run(&ast)
            .unwrap_or_else(|e| panic!("pooled `{q}` failed: {e:#}"));
        assert_eq!(seq.columns, par.columns, "columns differ for `{q}`");
        assert_eq!(disp(&seq), disp(&par), "rows differ for `{q}`");
    };

    // Edge-bearing fixture: every algo proc shape, incl. rel-type and label filters.
    let (root, graph, _) = testgen::write_basic("exec_build_view_pool");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let queries = [
        "CALL algo.WCC() YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
        "CALL algo.WCC({relationshipTypes: ['KNOWS']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
        "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
        "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
        "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
        "CALL algo.betweenness() YIELD node, score RETURN node.name AS name, score ORDER BY name",
        "CALL algo.HarmonicCentrality({nodeLabels: ['Person'], relationshipTypes: ['KNOWS']}) \
             YIELD node, score, reachable RETURN node.name AS name, score, reachable ORDER BY name",
        "CALL algo.labelPropagation({relationshipTypes: ['KNOWS']}) YIELD node, communityId \
             RETURN node.name AS name, communityId ORDER BY name",
    ];
    for q in queries {
        assert_par_eq(&gen, &cache, q);
    }
    let _ = std::fs::remove_dir_all(&root);

    // Wide fixture (200 nodes ≥ BUILD_VIEW_PAR_MIN): the pooled build fans the
    // adjacency reads across rayon; pool and sequential must still match exactly.
    let (wroot, wgraph) = testgen::write_wide("exec_build_view_pool_wide", 200);
    let wgen = Generation::open(&wroot, &wgraph).unwrap();
    let wcache = BlockCache::new(1 << 20);
    assert_par_eq(
        &wgen,
        &wcache,
        "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score ORDER BY name",
    );
    assert_par_eq(
        &wgen,
        &wcache,
        "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node, componentId \
             RETURN node.name AS name, componentId ORDER BY name",
    );
    let _ = std::fs::remove_dir_all(&wroot);
}

#[test]
fn rel_match_buffer_charges_intermediate_budget() {
    // `match_single_pattern` buffers a *materialising* relationship pattern's whole
    // result set before the cross-pattern terminal charges it; without charging the
    // buffer a dense expansion (every `:LINK` edge over a 1M-node graph) OOMs the
    // process. A row-returning query (not count-pushdown — that retains nothing and
    // is bounded by `maxScan`) exercises this retained buffer: the fixture's 3 KNOWS
    // edges trip a retained budget of 2 and pass at 1M.
    let err = run_budgeted(
        "exec_budget_relmatch_tiny",
        2,
        "MATCH (a)-[:KNOWS]->(b) RETURN b.name AS b",
    )
    .expect_err("the relationship-match buffer must charge the retained budget");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the budget error, got: {err:#}"
    );
    let res = run_budgeted(
        "exec_budget_relmatch_ok",
        1_000_000,
        "MATCH (a)-[:KNOWS]->(b) RETURN b.name AS b",
    )
    .expect("a generous budget must not affect the query");
    assert_eq!(res.rows.len(), 3, "3 KNOWS edges materialise 3 rows");
}

#[test]
fn budget_resets_between_runs() {
    let (root, gen, cache, _) = budgeted_engine("exec_budget_reset", 0);
    let engine = Engine::new(&gen, &cache).with_max_intermediate(1_500);
    // Each run charges ~1k; without the per-run reset the second would trip.
    let ast = parser::parse("RETURN size(range(0, 1000))").unwrap();
    engine.run(&ast).expect("first run fits the budget");
    engine
        .run(&ast)
        .expect("the budget must reset between runs");
    let _ = std::fs::remove_dir_all(&root);
}

// ── Vector KNN (M5) ──────────────────────────────────────────────────────

/// The three Person embeddings in the fixture (see `testgen`), by node id.
const FIXTURE_VECS: [(u64, [f32; 3]); 3] = [
    (0, [0.1, 0.2, 0.3]), // Alice
    (1, [0.2, 0.1, 0.0]), // Bob
    (2, [0.9, 0.8, 0.7]), // Carol
];

/// Brute-force reference: cosine-distance to `query`, ascending, tie-break id.
fn reference_knn(query: &[f32], k: usize) -> Vec<(u64, f64)> {
    let mut r: Vec<(u64, f64)> = FIXTURE_VECS
        .iter()
        .map(|(id, v)| (*id, 1.0 - vector::cosine_similarity(query, v)))
        .collect();
    r.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    r.truncate(k);
    r
}

#[test]
fn vector_knn_returns_k_nearest_ordered_with_reference_scores() {
    // Query equals Alice's vector, so Alice (distance 0) is first, then Carol,
    // then Bob — exactly the brute-force reference order and scores.
    let (root, res) = run(
        "exec_knn_ref",
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id, score",
    );
    assert_eq!(res.columns, vec!["id", "score"]);
    let want = reference_knn(&[0.1, 0.2, 0.3], 2);
    assert_eq!(res.rows.len(), want.len());
    for (got, (wid, wscore)) in res.rows.iter().zip(&want) {
        let Val::Int(id) = got[0] else {
            panic!("id should be an integer, got {:?}", got[0]);
        };
        let Val::Float(score) = got[1] else {
            panic!("score should be a float, got {:?}", got[1]);
        };
        assert_eq!(id as u64, *wid);
        assert!(
            (score - wscore).abs() < 1e-6,
            "score {score} vs reference {wscore}"
        );
    }
    // First hit is the exact match: distance ~0.
    assert!(matches!(res.rows[0][0], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_with_pool_is_correct() {
    // A pool-configured engine returns the identical (id, score) kNN rows as the
    // sequential engine. The fixture group is tiny (below KNN_PAR_MIN), so this
    // pins the pool wiring + sequential-fallback path end to end; the `vector`
    // unit test exercises the rayon chunked read/score branch directly.
    let (root, graph, _) = testgen::write_basic("exec_knn_pool");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let pool = std::sync::Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap(),
    );
    let q = "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
                 YIELD node, score RETURN id(node) AS id, score";
    let res = Engine::new(&gen, &cache)
        .with_fanout_pool(Some(pool))
        .run(&parser::parse(q).unwrap())
        .expect("pool-configured kNN runs");
    let want = reference_knn(&[0.1, 0.2, 0.3], 3);
    assert_eq!(res.rows.len(), want.len());
    for (got, (wid, wscore)) in res.rows.iter().zip(&want) {
        let Val::Int(id) = got[0] else {
            panic!("id should be an integer, got {:?}", got[0]);
        };
        let Val::Float(score) = got[1] else {
            panic!("score should be a float, got {:?}", got[1]);
        };
        assert_eq!(id as u64, *wid);
        assert!(
            (score - wscore).abs() < 1e-6,
            "score {score} vs reference {wscore}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_yield_alias_and_node_projection() {
    // Carol's own vector → Carol is the single nearest neighbour; the yielded
    // node is a real Node we can project a property off.
    let (root, res) = run(
        "exec_knn_alias",
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 1, vecf32([0.9, 0.8, 0.7])) \
             YIELD node AS n, score AS s RETURN n.name AS name, s",
    );
    assert_eq!(res.columns, vec!["name", "s"]);
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_display(), "Carol");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_yield_where_filters_rows() {
    // Ask for all three but keep only the (near-)exact match via YIELD ... WHERE.
    let (root, res) = run(
        "exec_knn_where",
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score WHERE score < 0.0001 RETURN id(node) AS id",
    );
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_unknown_index_is_an_error() {
    let (root, graph, _) = testgen::write_basic("exec_knn_noindex");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Company', 'embedding', 1, vecf32([0.1, 0.2, 0.3])) \
             YIELD node RETURN node",
    )
    .unwrap();
    let err = engine.run(&ast).err().unwrap();
    assert!(err.to_string().contains("no vector index"), "got: {err}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_dimension_mismatch_is_an_error() {
    let (root, graph, _) = testgen::write_basic("exec_knn_dim");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    // A 2-dim query against the 3-dim index.
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 1, vecf32([0.1, 0.2])) \
             YIELD node RETURN node",
    )
    .unwrap();
    let err = engine.run(&ast).err().unwrap();
    assert!(err.to_string().contains("dimension"), "got: {err}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_query_vector_from_parameter() {
    let (root, graph, _) = testgen::write_basic("exec_knn_param");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let mut params = HashMap::new();
    // A $param query vector arrives as a list of numbers.
    params.insert(
        "q".to_string(),
        Val::List(vec![Val::Float(0.9), Val::Float(0.8), Val::Float(0.7)]),
    );
    params.insert("k".to_string(), Val::Int(1));
    let engine = Engine::new(&gen, &cache).with_params(params);
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', $k, $q) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    let res = engine.run(&ast).unwrap();
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(2)), "Carol is nearest");
    let _ = std::fs::remove_dir_all(&root);
}

/// Did this run fail with the typed [`graph_format::pq::NonFiniteEmbedding`]?
/// Classified by *type*, never by message text (house rule) — a NaN that merely
/// trips some unrelated arity/type check would not satisfy this.
fn rejected_nonfinite(r: Result<QueryResult>) -> bool {
    r.err().is_some_and(|e| {
        e.downcast_ref::<graph_format::pq::NonFiniteEmbedding>()
            .is_some()
    })
}

#[test]
fn vecf32_rejects_a_nonfinite_component_at_write_ingest() {
    // The organic HIK-134 reproduction: log(-1.0) → NaN by slater's FalkorDB IEEE
    // semantics. vecf32 must reject it with a TYPED finiteness error *before* it becomes
    // a Vector that `SET n.embedding = …` would write into the index. Pre-fix this
    // returned Ok(a NaN-bearing Vector); a NaN slipping an arity check would not match
    // the typed error asserted here.
    let (root, graph, _) = testgen::write_basic("exec_vecf32_write_nan");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let ast = parser::parse("RETURN vecf32([log(-1.0), 0.2, 0.3]) AS v").unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).run(&ast)),
        "a NaN vecf32 component must be a typed finiteness error"
    );
    // Index uncorrupted: a subsequent clean KNN over the same fixture still returns the
    // reference nearest neighbour (the rejected write never touched the index).
    let ok = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    let res = Engine::new(&gen, &cache).run(&ok).unwrap();
    assert!(
        matches!(res.rows[0][0], Val::Int(0)),
        "nearest is Alice (exact match) — index uncorrupted"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vecf32_rejects_an_infinite_literal_via_the_parse_fold() {
    // vecf32([1e400, …]) — the f64 literal is finite but `as f32` saturates to +inf. The
    // parse-time constant fold must NOT bake it into a Vector literal (which would skip
    // the runtime gate); the runtime vecf32 gate then rejects it. Covers `±inf` *and* the
    // fold-bypass entry point in one shot.
    let (root, graph, _) = testgen::write_basic("exec_vecf32_inf_literal");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let ast = parser::parse("RETURN vecf32([1e400, 0.2, 0.3]) AS v").unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).run(&ast)),
        "a +inf vecf32 component must be a typed finiteness error"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn query_vector_nonfinite_is_rejected_against_a_clean_index() {
    // The load-bearing case (HIK-134): a NaN QUERY needs no write at all. Against the
    // clean fixture index, both the inline vecf32() form and a `$param` numeric-list form
    // (the distinct `eval_query_vector` gate) must be rejected with the typed finiteness
    // error — NOT answered with a `total_cmp`-ordered garbage result set.
    let (root, graph, _) = testgen::write_basic("exec_query_vec_nan");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Form A: inline vecf32([log(-1.0), …]) → the vecf32 ingest gate.
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([log(-1.0), 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).run(&ast)),
        "a clean index + vecf32(NaN) query must be a typed error, not a garbage result"
    );

    // Form B: a $param list carrying a NaN → the eval_query_vector List arm.
    let mut params = HashMap::new();
    params.insert(
        "q".to_string(),
        Val::List(vec![Val::Float(f64::NAN), Val::Float(0.2), Val::Float(0.3)]),
    );
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, $q) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    assert!(
        rejected_nonfinite(Engine::new(&gen, &cache).with_params(params).run(&ast)),
        "a clean index + $param NaN query must be a typed error, not a garbage result"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn vector_knn_reads_route_through_the_block_cache() {
    let (root, graph, _) = testgen::write_basic("exec_knn_cache");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node)",
    )
    .unwrap();
    engine.run(&ast).unwrap();
    let after_first = cache.metrics();
    assert!(after_first.misses > 0, "first run populates the cache");
    engine.run(&ast).unwrap();
    let after_second = cache.metrics();
    assert_eq!(
        after_second.misses, after_first.misses,
        "the vector group should be served from resident blocks on the second run"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A vector index is built over the base generation and is immutable, so a node
/// deleted afterwards is still *in* it. Every other read path suppresses a
/// tombstoned node; before this fix the KNN path did not, and handed the deleted
/// node back as a live `Val::Node` — the vector arm was the one hole.
#[test]
fn vector_knn_suppresses_delta_deleted_nodes() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("exec_knn_delete");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    // Carol's own embedding as the query, so she is the exact match (distance 0)
    // and must come back first — this is what makes her absence below meaningful.
    let ast = parser::parse(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 3, vecf32([0.9, 0.8, 0.7])) \
             YIELD node, score RETURN id(node) AS id",
    )
    .unwrap();
    let ids = |res: &QueryResult| -> Vec<i64> {
        res.rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(i) => i,
                ref other => panic!("id should be an integer, got {other:?}"),
            })
            .collect()
    };

    // Baseline: with no delta, Carol (2) is the nearest hit.
    let before = Engine::new(&MergedView::read_only(&gen), &cache)
        .run(&ast)
        .unwrap();
    assert_eq!(
        ids(&before),
        vec![2, 0, 1],
        "the exact match must lead on a pure-core read"
    );

    // Delete Carol. Her vector is still in the sealed index, so only the delta
    // tombstone can keep her out of the results.
    let mut mem = Memtable::new();
    mem.delete_node("Person", "name", Value::Str("Carol".into()), Some(2));
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    assert!(
        view.delta().is_tombstoned(2),
        "Carol tombstoned in the delta"
    );

    let after = Engine::new(&view, &cache).run(&ast).unwrap();
    let got = ids(&after);
    assert!(
        !got.contains(&2),
        "a deleted node must not be returned by KNN, got {got:?}"
    );
    assert_eq!(
        got,
        vec![0, 1],
        "the two live Person embeddings remain, still exact-ranked"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A vector written to the delta is immediately KNN-visible, with **exact** rank (the
/// overlay is brute-forced, not approximated).
///
/// The sharp part is the re-embed. A node whose vector a newer level supersedes still
/// sits in the sealed base index with its *stale* vector, and `TopK` does not dedup by
/// node id — so a merge that did not suppress the base entry would return that node
/// **twice**, at two different scores, and the stale copy could take one of the `k`
/// slots and evict a live candidate. Both the "appears once" and the "old vector no
/// longer matches" assertions below fail if the suppression is dropped.
#[test]
fn a_delta_written_vector_is_knn_visible_and_supersedes_the_base() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("exec_knn_delta_write");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let knn = |view: &MergedView, q: &str| -> Vec<(i64, f64)> {
        let ast = parser::parse(&format!(
            "CALL db.idx.vector.queryNodes('Person', 'embedding', 5, vecf32({q})) \
                 YIELD node, score RETURN id(node) AS id, score"
        ))
        .unwrap();
        Engine::new(view, &cache)
            .run(&ast)
            .unwrap()
            .rows
            .iter()
            .map(|r| match (&r[0], &r[1]) {
                (Val::Int(i), Val::Float(s)) => (*i, *s),
                other => panic!("unexpected KNN row {other:?}"),
            })
            .collect()
    };

    // Alice (0)'s original embedding, from the fixture.
    let base = MergedView::read_only(&gen);
    let old = knn(&base, "[0.1, 0.2, 0.3]");
    assert_eq!(old[0].0, 0, "Alice is the exact match for her own vector");
    assert!(old[0].1.abs() < 1e-6, "…at distance ~0");

    // Re-embed Alice onto a vector orthogonal to her old one, and add a brand-new
    // node carrying an embedding of its own.
    // Seeded with the core's counts, so a born node's synthetic id cannot collide
    // with a core dense id (`Memtable::new()` bases both at 0 — fine for a
    // patch-only delta, wrong the moment a node is born).
    let mut mem = Memtable::with_bases(gen.node_count(), gen.edge_count());
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Alice".into()),
        Some(0),
        [("embedding".to_string(), Value::Vector(vec![0.0, 0.0, 1.0]))],
    );
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Zoe".into()),
        None, // delta-born: no core row at all
        [("embedding".to_string(), Value::Vector(vec![1.0, 0.0, 0.0]))],
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));

    // Alice's NEW vector: she is now the exact match, and appears exactly once.
    let fresh = knn(&view, "[0.0, 0.0, 1.0]");
    assert_eq!(
        fresh[0].0, 0,
        "the delta's vector must win over the base's stale one"
    );
    assert!(
        fresh[0].1.abs() < 1e-6,
        "…at distance ~0, got {}",
        fresh[0].1
    );
    assert_eq!(
        fresh.iter().filter(|(id, _)| *id == 0).count(),
        1,
        "Alice must appear exactly once — the base's stale entry has to be suppressed \
             in the scan, not merged away afterwards; got {fresh:?}"
    );

    // Her OLD vector must no longer be an exact match for her — proof the stale base
    // entry is genuinely gone from the candidate set rather than merely outranked.
    let stale = knn(&view, "[0.1, 0.2, 0.3]");
    let alice = stale
        .iter()
        .find(|(id, _)| *id == 0)
        .expect("Alice is still live, just re-embedded");
    assert!(
        alice.1 > 1e-3,
        "Alice's stale base vector must not still be scoring ~0 against her old query; \
             got {alice:?}"
    );

    // The delta-born node is visible, exactly ranked, on its own vector. Its synthetic
    // id is the first past the core's dense range.
    let zoe = gen.node_count() as i64;
    let born = knn(&view, "[1.0, 0.0, 0.0]");
    assert_eq!(
        born[0].0, zoe,
        "a node born in the delta with an embedding must be KNN-visible; got {born:?}"
    );
    assert!(born[0].1.abs() < 1e-6, "…at distance ~0, got {}", born[0].1);
    let _ = std::fs::remove_dir_all(&root);
}

// ── The RW-index over the delta (HIK-112) ────────────────────────────────────────────
//
// These drive a **real** `DeltaWriter` (real WAL, real seal), because the two properties
// that matter are lifecycle properties: an index rebuilt from a replayed delta must answer
// what the delta says, and no vector may go missing across a seal. A hand-built `Memtable`
// cannot express either.

/// A `RwIndexConfig` with the floors removed, so the tiny fixtures below actually take the
/// index path instead of silently falling back to the brute force (which would make every
/// assertion here vacuous — the fallback is the *old* code, and it passes by construction).
#[cfg(test)]
fn rw_cfg_no_floor() -> crate::rwindex::RwIndexConfig {
    crate::rwindex::RwIndexConfig {
        enabled: true,
        min_vectors: 0,
        max_vectors: 1 << 20,
    }
}

/// The fixture's business-key resolver: Alice/Bob/Carol are core dense ids 0/1/2, and any
/// other name is a delta-born node.
#[cfg(test)]
fn basic_resolve(op: &slater_delta::WalOp) -> slater_delta::OpResolution {
    use slater_delta::{OpResolution, WalOp};
    let value = match op {
        WalOp::UpsertNode { value, .. }
        | WalOp::DeleteNode { value, .. }
        | WalOp::RemoveNodeProps { value, .. }
        | WalOp::ReplaceNode { value, .. }
        | WalOp::SetNodeLabels { value, .. } => value,
        _ => return OpResolution::Node(None),
    };
    OpResolution::Node(match value {
        Value::Str(s) if s == "Alice" => Some(0),
        Value::Str(s) if s == "Bob" => Some(1),
        Value::Str(s) if s == "Carol" => Some(2),
        _ => None,
    })
}

#[cfg(test)]
fn upsert_vec(name: &str, v: Vec<f32>) -> slater_delta::WalOp {
    slater_delta::WalOp::UpsertNode {
        label: "Person".into(),
        key: "name".into(),
        value: Value::Str(name.into()),
        patches: vec![("embedding".into(), Value::Vector(v))],
    }
}

/// The KNN top-`k` a query must produce, derived **independently** of the index: a plain
/// scan of the effective (id, vector) set with `vector::distance`, in the D26 total order.
/// This is the ground truth every RW-index test below is measured against — never "the
/// index agrees with the brute-force walk", which is the parity test that would pass even
/// if both shared a misunderstanding.
#[cfg(test)]
fn expected_topk(live: &[(u64, Vec<f32>)], q: &[f32], k: usize) -> Vec<(i64, f64)> {
    let mut scored: Vec<(f64, u64)> = live
        .iter()
        .map(|(id, v)| {
            (
                vector::distance(graph_format::manifest::Metric::Cosine, q, v),
                *id,
            )
        })
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    scored
        .into_iter()
        .take(k)
        .map(|(s, id)| (id as i64, s))
        .collect()
}

/// Run the KNN with the RW-index arm wired, and report whether the index actually served
/// the delta (rather than falling back), so a test cannot pass vacuously.
#[cfg(test)]
#[allow(clippy::type_complexity)]
fn knn_with_rw(
    gen: &Generation,
    cache: &BlockCache,
    writer: &crate::delta_writer::DeltaWriter,
    rw: &crate::rwindex::RwIndexCache,
    q: &str,
    k: usize,
) -> (Vec<(i64, f64)>, bool) {
    use crate::read_view::MergedView;
    // The delta and its epoch, in ONE atomic read — the pair the index is cut at.
    let published = writer.delta_snapshot_at();
    let epoch = published.epoch;
    let view = MergedView::new(gen, published.delta);
    let ast = parser::parse(&format!(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', {k}, vecf32({q})) \
             YIELD node, score RETURN id(node) AS id, score"
    ))
    .unwrap();
    let rows = Engine::new(&view, cache)
        .with_rw_index(rw, writer.touched_journal(), epoch, rw_cfg_no_floor())
        .run(&ast)
        .unwrap()
        .rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Val::Int(i), Val::Float(s)) => (*i, *s),
            other => panic!("unexpected KNN row {other:?}"),
        })
        .collect();
    // Did the index serve it? It serves iff it stands at exactly the query's epoch.
    let served = rw.index_epoch(gen.uuid(), "Person", "embedding") == Some(epoch);
    (rows, served)
}

/// **The RW-index is a cache of the delta; the delta is the durable thing.** Nothing is
/// persisted, so the recovery story is: replay the WAL, rebuild the index from the replayed
/// delta.
///
/// Drives the *real* WAL: write embeddings (a re-embed of a core node, two born nodes, and
/// a `REMOVE n.embedding` that un-embeds one), drop the writer without any clean shutdown,
/// reopen (which replays), and assert the KNN answers what a brute force over the
/// **replayed** delta says — not what the pre-crash query happened to return. Truth is the
/// state on disk, not the state in the dead process's memory.
#[test]
fn rw_index_rebuilds_from_wal_replay() {
    use crate::delta_writer::DeltaWriter;
    use crate::rwindex::RwIndexCache;
    use slater_delta::WalOp;

    let (root, graph, _) = testgen::write_basic("exec_rw_replay");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let wal = root.join("wal_rw_replay");
    let _ = std::fs::remove_dir_all(&wal);
    let core_n = gen.node_count();

    let open = || {
        DeltaWriter::open(
            &wal,
            &graph,
            gen.uuid(),
            gen.node_count(),
            gen.edge_count(),
            basic_resolve,
        )
        .unwrap()
    };

    {
        let w = open();
        // Alice (core id 0) is re-embedded onto the +z axis.
        w.write(
            upsert_vec("Alice", vec![0.0, 0.0, 1.0]),
            basic_resolve(&upsert_vec("Alice", vec![])),
        )
        .unwrap();
        // Two delta-born Persons (synthetic ids core_n, core_n + 1).
        w.write(
            upsert_vec("Zoe", vec![1.0, 0.0, 0.0]),
            slater_delta::OpResolution::Node(None),
        )
        .unwrap();
        w.write(
            upsert_vec("Yan", vec![0.0, 1.0, 0.0]),
            slater_delta::OpResolution::Node(None),
        )
        .unwrap();
        // Bob (core id 1) is UN-embedded. Absence cannot express this (D12 keeps an indexed
        // embedding out of the props record), so it rides its own channel — and if the
        // rebuilt index loses it, Bob's *stale base vector* silently starts scoring again.
        w.write(
            WalOp::RemoveNodeProps {
                label: "Person".into(),
                key: "name".into(),
                value: Value::Str("Bob".into()),
                props: vec!["embedding".into()],
            },
            slater_delta::OpResolution::Node(Some(1)),
        )
        .unwrap();
        // No flush, no clean close: just drop it. The WAL is fsynced per commit.
    }

    // Reopen — `DeltaWriter::open` replays the WAL dir into a fresh memtable — and rebuild
    // the index from the replayed delta. A brand-new `RwIndexCache`: nothing survived.
    let w = open();
    let rw = RwIndexCache::new();

    // The effective live set after the replay, derived by hand from the writes above:
    //   0 Alice → the delta's new vector      (the base's [0.1,0.2,0.3] is superseded)
    //   1 Bob   → GONE                        (un-embedded; the base's [0.2,0.1,0.0] must
    //                                          NOT come back)
    //   2 Carol → the base's [0.9,0.8,0.7]    (the delta says nothing about her)
    //   3 Zoe, 4 Yan → born, from the delta
    let live: Vec<(u64, Vec<f32>)> = vec![
        (0, vec![0.0, 0.0, 1.0]),
        (2, vec![0.9, 0.8, 0.7]),
        (core_n, vec![1.0, 0.0, 0.0]),
        (core_n + 1, vec![0.0, 1.0, 0.0]),
    ];

    for q in [
        (vec![0.0f32, 0.0, 1.0], "[0.0, 0.0, 1.0]"),
        (vec![1.0, 0.0, 0.0], "[1.0, 0.0, 0.0]"),
        // Bob's OLD (base) vector. He is un-embedded, so he must not come back AT ALL —
        // let alone lead, which is what a lost removal would do.
        (vec![0.2, 0.1, 0.0], "[0.2, 0.1, 0.0]"),
    ] {
        let (got, served) = knn_with_rw(&gen, &cache, &w, &rw, q.1, 5);
        assert!(
            served,
            "the RW-index must have served the query, not fallen back"
        );
        let want = expected_topk(&live, &q.0, 5);
        assert_eq!(
            got.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            want.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            "query {} — the index rebuilt from the replayed WAL must answer what the \
                 delta on disk says; got {got:?}, want {want:?}",
            q.1
        );
        for (g, e) in got.iter().zip(&want) {
            assert!((g.1 - e.1).abs() < 1e-5, "score {g:?} vs {e:?}");
        }
        assert!(
            !got.iter().any(|(id, _)| *id == 1),
            "Bob was un-embedded; his stale BASE vector must not score. Got {got:?}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// **No vector goes missing across a seal.**
///
/// `flush_to_l0` moves the whole active memtable into a sealed L0 level and resets the
/// memtable. HIK-112 warns that an index tied to the *active memtable* alone would be
/// cleared here, while the L0's core segment is not yet published — and the vectors in it
/// would vanish from KNN with nothing to say so.
///
/// This index is over `mem ⊕ L0`, which a seal does not change, so the seal journals an
/// empty touched set and the index is not touched at all. The test proves the *observable*:
/// the same ids, at the same scores, before and after. The mutation that matters is a
/// clear-on-seal — the **born** nodes below have no base entry at all, so if the delta arm
/// lost them they would disappear outright rather than merely re-rank.
#[test]
fn rw_index_ladder_survives_a_seal() {
    use crate::delta_writer::DeltaWriter;
    use crate::rwindex::RwIndexCache;

    let (root, graph, _) = testgen::write_basic("exec_rw_seal");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let wal = root.join("wal_rw_seal");
    let _ = std::fs::remove_dir_all(&wal);
    let core_n = gen.node_count();

    let w = DeltaWriter::open(
        &wal,
        &graph,
        gen.uuid(),
        gen.node_count(),
        gen.edge_count(),
        basic_resolve,
    )
    .unwrap();
    let rw = RwIndexCache::new();

    // Eight born Persons on a fan of directions in the x–y plane, plus a re-embed of Alice.
    let born: Vec<(u64, Vec<f32>)> = (0..8u64)
        .map(|i| {
            let a = i as f32 * 0.19;
            (core_n + i, vec![a.cos(), a.sin(), 0.0])
        })
        .collect();
    for (i, (_, v)) in born.iter().enumerate() {
        w.write(
            upsert_vec(&format!("N{i}"), v.clone()),
            slater_delta::OpResolution::Node(None),
        )
        .unwrap();
    }
    w.write(
        upsert_vec("Alice", vec![0.0, 0.0, 1.0]),
        slater_delta::OpResolution::Node(Some(0)),
    )
    .unwrap();

    let mut live: Vec<(u64, Vec<f32>)> = born.clone();
    live.push((0, vec![0.0, 0.0, 1.0])); // Alice, re-embedded
    live.push((1, vec![0.2, 0.1, 0.0])); // Bob, from the base (the delta never touches him)
    live.push((2, vec![0.9, 0.8, 0.7])); // Carol, from the base

    let q = "[1.0, 0.15, 0.0]";
    let qv = vec![1.0f32, 0.15, 0.0];
    let want = expected_topk(&live, &qv, 6);

    let (before, served) = knn_with_rw(&gen, &cache, &w, &rw, q, 6);
    assert!(served, "the RW-index must serve the pre-seal query");
    assert_eq!(
        before.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        want.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        "pre-seal: got {before:?}, want {want:?}"
    );

    // ── SEAL ──────────────────────────────────────────────────────────────────────────
    assert!(w.flush_to_l0().unwrap(), "the memtable had writes to seal");
    assert_eq!(w.l0_len(), 1, "the writes are in a sealed L0 level now");
    assert!(
        w.snapshot().is_empty(),
        "…and the ACTIVE memtable really is empty — without this the test would not \
             actually cross the seal, and would pass whatever the index did"
    );
    // The seal published a new epoch, so the index has to advance across it.
    assert!(
        w.delta_snapshot_at().epoch > 1,
        "the seal must have bumped the epoch"
    );

    let (after, served) = knn_with_rw(&gen, &cache, &w, &rw, q, 6);
    assert!(
        served,
        "the RW-index must still serve after the seal — a journal gap here would silently \
             force a rebuild, which is correct but hides the bug this test exists for"
    );
    assert_eq!(
        after.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        want.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        "A VECTOR WENT MISSING ACROSS THE SEAL. Before {before:?}, after {after:?}, \
             truth {want:?}"
    );
    for (a, b) in after.iter().zip(&before) {
        assert!(
            (a.1 - b.1).abs() < 1e-9,
            "score moved across the seal: {a:?} vs {b:?}"
        );
    }

    // And the born nodes — the ones with no base entry, which a cleared index would lose
    // outright — are still there.
    let born_ids: Vec<i64> = after
        .iter()
        .map(|(i, _)| *i)
        .filter(|i| *i >= core_n as i64)
        .collect();
    assert!(
        born_ids.len() >= 4,
        "the delta-born embeddings must survive the seal; got {after:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// D12 read parity. An *indexed* embedding is routed out of the column store at build
/// time, so a core node's `n.embedding` reads as `Null`. A delta-written embedding is
/// deliberately left in the node's property map (that map carries it to the flush and
/// the rebuild), so without an explicit suppression the same query would answer `Null`
/// for a core-resident node and a vector for a freshly-written one.
///
/// An *unindexed* vector property is not covered by D12 and must still read back — it
/// is an ordinary inline value, exactly as it is in the core.
#[test]
fn a_delta_written_indexed_vector_reads_as_null_like_the_core() {
    use crate::read_view::MergedView;
    use slater_delta::{DeltaSnapshot, Memtable};
    use std::sync::Arc;

    let (root, graph, _) = testgen::write_basic("exec_d12_delta_vector");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    // Alice (0) is core-resident and Person.embedding is vector-indexed, so her
    // embedding already reads as Null — the behaviour the delta must match.
    let base = Engine::new(&MergedView::read_only(&gen), &cache)
        .run(&parser::parse("MATCH (n:Person {name: 'Alice'}) RETURN n.embedding").unwrap())
        .unwrap();
    assert!(
        matches!(base.rows[0][0], Val::Null),
        "a core node's indexed embedding reads as Null (D12), got {:?}",
        base.rows[0][0]
    );

    // Re-embed Alice, and give her an unindexed vector property alongside.
    let mut mem = Memtable::new();
    mem.upsert_node(
        "Person",
        "name",
        Value::Str("Alice".into()),
        Some(0),
        [
            ("embedding".to_string(), Value::Vector(vec![0.1, 0.2, 0.3])),
            ("shadow".to_string(), Value::Vector(vec![0.4, 0.5])),
        ],
    );
    let view = MergedView::new(&gen, DeltaSnapshot::from_memtable(Arc::new(mem)));
    let eng = Engine::new(&view, &cache);

    let got = eng
        .run(
            &parser::parse(
                "MATCH (n:Person {name: 'Alice'}) RETURN n.embedding AS e, n.shadow AS s",
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        matches!(got.rows[0][0], Val::Null),
        "a delta-written *indexed* embedding must read as Null too, or the same graph \
             answers two ways depending on which level the node lives in; got {:?}",
        got.rows[0][0]
    );
    assert!(
        matches!(&got.rows[0][1], Val::Vector(v) if v == &[0.4, 0.5]),
        "an unindexed vector property is not routed out, so it must read back verbatim; \
             got {:?}",
        got.rows[0][1]
    );

    // The whole-map read (`RETURN n` / properties(n)) must agree with the column read.
    let all = eng
        .run(&parser::parse("MATCH (n:Person {name: 'Alice'}) RETURN properties(n) AS p").unwrap())
        .unwrap();
    let Val::Map(props) = &all.rows[0][0] else {
        panic!("properties() should yield a map");
    };
    assert!(
        !props.iter().any(|(k, _)| k == "embedding"),
        "the indexed embedding must be absent from properties(n), got {props:?}"
    );
    assert!(
        props.iter().any(|(k, _)| k == "shadow"),
        "the unindexed vector must survive properties(n), got {props:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn similarity_and_vecf32_scalar_functions() {
    let (root, res) = run(
        "exec_similarity",
        "RETURN similarity(vecf32([1.0, 0.0]), vecf32([1.0, 0.0])) AS same, \
             similarity(vecf32([1.0, 0.0]), vecf32([0.0, 1.0])) AS orth",
    );
    let Val::Float(same) = res.rows[0][0] else {
        panic!("expected float");
    };
    let Val::Float(orth) = res.rows[0][1] else {
        panic!("expected float");
    };
    assert!((same - 1.0).abs() < 1e-9);
    assert!(orth.abs() < 1e-9);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase8_vector_distance_functions() {
    // Vectors ported from FalkorDB tests/flow/test_vecsim.py::test01_vector_distance.
    // euclidean([1,2],[2,3]) = sqrt(2); cosine = 1 - 8/sqrt(65).
    let (root, res) = run(
        "exec_p8_dist",
        "RETURN vec.euclideanDistance(vecf32([1.0, 2.0]), vecf32([2.0, 3.0])) AS e, \
             vec.cosineDistance(vecf32([1.0, 2.0]), vecf32([2.0, 3.0])) AS c, \
             vec.euclideanDistance(vecf32([1.0, 1.0]), vecf32([1.0, 1.0])) AS esame, \
             vec.cosineDistance(vecf32([1.0, 1.0]), vecf32([1.0, 1.0])) AS csame",
    );
    assert_float(&res.rows[0][0], 2.0_f64.sqrt());
    assert_float(&res.rows[0][1], 1.0 - 8.0 / 65.0_f64.sqrt());
    assert_float(&res.rows[0][2], 0.0);
    assert_float(&res.rows[0][3], 0.0);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase8_vector_distance_null_propagates() {
    // A NULL operand → NULL (either side), for both functions.
    let (root, res) = run(
        "exec_p8_null",
        "RETURN vec.euclideanDistance(null, vecf32([1.0, 1.0])) AS a, \
             vec.euclideanDistance(vecf32([1.0, 1.0]), null) AS b, \
             vec.cosineDistance(null, null) AS c",
    );
    assert!(matches!(res.rows[0][0], Val::Null));
    assert!(matches!(res.rows[0][1], Val::Null));
    assert!(matches!(res.rows[0][2], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase8_vector_distance_errors() {
    // Dimension mismatch is an error (FalkorDB: "Vector dimension mismatch").
    let e = run_err(
        "exec_p8_dim",
        "RETURN vec.euclideanDistance(vecf32([1.0, 1.0]), vecf32([2.0, 2.0, 3.0])) AS d",
    );
    assert!(e.contains("dimension mismatch"), "got: {e}");
    // A non-vector operand is an error (FalkorDB: "Type mismatch"). Pass a
    // string directly (vecf32() would reject it first; the distance arm coerces
    // via as_vector and rejects a non-numeric scalar).
    let e = run_err(
        "exec_p8_type",
        "RETURN vec.cosineDistance([1.0, 1.0], 'foo') AS d",
    );
    assert!(e.contains("vectors"), "got: {e}");
}

// ── Phase 9 — Val::Point, point()/distance(), coordinate reads ────────────

// point() construction + coordinate property reads (test_point.py
// test_point_coordinates). FalkorDB stores f32; coordinates are asserted to
// 1e-5. An unknown coordinate key yields NULL.
#[test]
fn phase9_point_construction_and_coordinates() {
    let (root, res) = run(
        "exec_p9_coords",
        "WITH point({latitude: 32.070794860, longitude: 34.820751118}) AS p \
             RETURN p.latitude AS lat, p.longitude AS lon, p.v AS missing, typeOf(p) AS t",
    );
    let r = &res.rows[0];
    match r[0] {
        Val::Float(x) => assert!((x - 32.070794860).abs() < 1e-5, "lat {x}"),
        ref o => panic!("expected float latitude, got {o:?}"),
    }
    match r[1] {
        Val::Float(x) => assert!((x - 34.820751118).abs() < 1e-5, "lon {x}"),
        ref o => panic!("expected float longitude, got {o:?}"),
    }
    assert!(matches!(r[2], Val::Null), "unknown key → NULL");
    assert_eq!(render(&r[3]), "'Point'");
    let _ = std::fs::remove_dir_all(&root);
}

// distance() haversine, in metres (test_point.py test_point_distance). The
// FalkorDB suite tolerates 10% error; we assert the same vectors well within it.
#[test]
fn phase9_point_distance() {
    let (root, res) = run(
        "exec_p9_dist",
        "WITH point({latitude:32.070794860, longitude:34.820751118}) AS a, \
                  point({latitude:32.070109656, longitude:34.822351298}) AS b, \
                  point({latitude:30.621734079, longitude:-96.33775507}) AS c \
             RETURN distance(a, a) AS d0, distance(a, b) AS d160, distance(a, c) AS d_far",
    );
    let r = &res.rows[0];
    let f = |v: &Val| match v {
        Val::Float(x) => *x,
        o => panic!("expected float, got {o:?}"),
    };
    assert!(f(&r[0]).abs() < 1e-6, "same point → 0, got {}", f(&r[0]));
    let within =
        |got: f64, want: f64| assert!((got - want).abs() <= 0.1 * want, "got {got}, want ~{want}");
    within(f(&r[1]), 160.0);
    within(f(&r[2]), 11_352_120.0);
    let _ = std::fs::remove_dir_all(&root);
}

// Coordinate range validation + bad-key errors (test_point.py test_point_values).
#[test]
fn phase9_point_validation_errors() {
    for (tag, q, needle) in [
        (
            "exec_p9_lat_hi",
            "RETURN point({latitude:90.1, longitude:20}) AS p",
            "latitude should be within",
        ),
        (
            "exec_p9_lat_lo",
            "RETURN point({latitude:-90.1, longitude:20}) AS p",
            "latitude should be within",
        ),
        (
            "exec_p9_lon_hi",
            "RETURN point({latitude:10, longitude:180.1}) AS p",
            "longitude should be within",
        ),
        (
            "exec_p9_lon_lo",
            "RETURN point({latitude:10, longitude:-180.1}) AS p",
            "longitude should be within",
        ),
        (
            "exec_p9_one_key",
            "RETURN point({latitude:10}) AS p",
            "should have 2 elements",
        ),
        (
            "exec_p9_no_lat",
            "RETURN point({x:1, y:2}) AS p",
            "Did not find 'latitude'",
        ),
    ] {
        let e = run_err(tag, q);
        assert!(e.contains(needle), "query `{q}` → `{e}` (want `{needle}`)");
    }
}

// Ordering + equality. FalkorDB orders points by longitude then latitude
// (test_point.py test_nested_point ORDER BY p), and equal points are `=`.
#[test]
fn phase9_point_ordering_and_equality() {
    let (root, res) = run(
        "exec_p9_order",
        "UNWIND [point({latitude:33, longitude:35}), \
                     point({latitude:32, longitude:31}), \
                     point({latitude:32, longitude:32}), \
                     point({latitude:31, longitude:32}), \
                     point({latitude:29, longitude:36})] AS p \
             WITH p ORDER BY p RETURN p.longitude AS lon, p.latitude AS lat",
    );
    let lons: Vec<f64> = res
        .rows
        .iter()
        .map(|r| match r[0] {
            Val::Float(x) => x,
            ref o => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(lons, vec![31.0, 32.0, 32.0, 35.0, 36.0]);
    // The lon-32 tie breaks on latitude ascending (31 before 32).
    assert!(matches!(res.rows[1][1], Val::Float(x) if (x - 31.0).abs() < 1e-9));
    assert!(matches!(res.rows[2][1], Val::Float(x) if (x - 32.0).abs() < 1e-9));

    let (root2, eq) = run(
        "exec_p9_eq",
        "WITH point({latitude:32, longitude:34}) AS a, \
                  point({latitude:32, longitude:34}) AS b, \
                  point({latitude:32, longitude:35}) AS c \
             RETURN a = b AS same, a = c AS diff",
    );
    assert!(matches!(eq.rows[0][0], Val::Bool(true)));
    assert!(matches!(eq.rows[0][1], Val::Bool(false)));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

// NULL propagation + toString rendering (%f, 6 decimals — test_nested_point).
#[test]
fn phase9_point_null_and_tostring() {
    let (root, res) = run(
        "exec_p9_null_str",
        "RETURN point(null) AS np, distance(null, point({latitude:1, longitude:2})) AS nd, \
             toString(point({latitude:32, longitude:34})) AS s",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Null));
    assert!(matches!(r[1], Val::Null));
    assert_eq!(
        render(&r[2]),
        "'point({latitude: 32.000000, longitude: 34.000000})'"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase1_trig_and_angle_functions() {
    let (root, res) = run(
        "exec_p1_trig",
        "RETURN sin(0.0) AS s, cos(0.0) AS c, tan(0.0) AS t, \
             cot(0.7853981633974483) AS cot, asin(1.0) AS asin, acos(1.0) AS acos, \
             atan(1.0) AS atan, atan2(1.0, 1.0) AS atan2, \
             degrees(3.141592653589793) AS deg, radians(180.0) AS rad, \
             haversin(0.0) AS hav",
    );
    let f = |i: usize| match res.rows[0][i] {
        Val::Float(x) => x,
        _ => panic!("expected float at col {i}"),
    };
    let close = |a: f64, b: f64| assert!((a - b).abs() < 1e-9, "{a} != {b}");
    close(f(0), 0.0); // sin 0
    close(f(1), 1.0); // cos 0
    close(f(2), 0.0); // tan 0
    close(f(3), 1.0); // cot(pi/4)
    close(f(4), std::f64::consts::FRAC_PI_2); // asin 1
    close(f(5), 0.0); // acos 1
    close(f(6), std::f64::consts::FRAC_PI_4); // atan 1
    close(f(7), std::f64::consts::FRAC_PI_4); // atan2(1,1)
    close(f(8), 180.0); // degrees(pi)
    close(f(9), std::f64::consts::PI); // radians(180)
    close(f(10), 0.0); // haversin 0
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase1_left_right_and_isempty_typeof() {
    let (root, res) = run(
        "exec_p1_str",
        "RETURN left('muchacho', 4) AS l, right('muchacho', 4) AS r, \
             left('hi', 9) AS lover, right('hi', 9) AS rover, \
             isEmpty('') AS e1, isEmpty('x') AS e2, isEmpty([]) AS e3, \
             typeOf(1) AS t1, typeOf(1.5) AS t2, typeOf('a') AS t3, \
             typeOf(true) AS t4, typeOf([1]) AS t5, typeOf(null) AS t6",
    );
    let row = &res.rows[0];
    assert!(matches!(&row[0], Val::Str(s) if s == "much"));
    assert!(matches!(&row[1], Val::Str(s) if s == "acho"));
    assert!(matches!(&row[2], Val::Str(s) if s == "hi"));
    assert!(matches!(&row[3], Val::Str(s) if s == "hi"));
    assert!(matches!(row[4], Val::Bool(true)));
    assert!(matches!(row[5], Val::Bool(false)));
    assert!(matches!(row[6], Val::Bool(true)));
    assert!(matches!(&row[7], Val::Str(s) if s == "Integer"));
    assert!(matches!(&row[8], Val::Str(s) if s == "Float"));
    assert!(matches!(&row[9], Val::Str(s) if s == "String"));
    assert!(matches!(&row[10], Val::Str(s) if s == "Boolean"));
    assert!(matches!(&row[11], Val::Str(s) if s == "List"));
    assert!(matches!(&row[12], Val::Str(s) if s == "Null"));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase1_ornull_conversions() {
    let (root, res) = run(
        "exec_p1_ornull",
        "RETURN toIntegerOrNull('7') AS i, toIntegerOrNull('x') AS i2, \
             toFloatOrNull('1.5') AS f, toFloatOrNull('x') AS f2, \
             toBooleanOrNull('true') AS b, toBooleanOrNull('x') AS b2, \
             toStringOrNull(42) AS s, toStringOrNull(null) AS s2",
    );
    let row = &res.rows[0];
    assert!(matches!(row[0], Val::Int(7)));
    assert!(matches!(row[1], Val::Null));
    assert!(matches!(row[2], Val::Float(x) if (x - 1.5).abs() < 1e-9));
    assert!(matches!(row[3], Val::Null));
    assert!(matches!(row[4], Val::Bool(true)));
    assert!(matches!(row[5], Val::Null));
    assert!(matches!(&row[6], Val::Str(s) if s == "42"));
    assert!(matches!(row[7], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

/// Canonical render of a value for order-sensitive list assertions (the
/// `Val` enum derives no `PartialEq`). Mirrors Cypher literal syntax closely
/// enough to read the expectations off the FalkorDB test vectors.
#[cfg(test)]
fn render(v: &Val) -> String {
    match v {
        Val::Null => "null".into(),
        Val::Bool(b) => b.to_string(),
        Val::Int(i) => i.to_string(),
        Val::Float(f) => f.to_string(),
        Val::Str(s) => format!("'{s}'"),
        Val::List(xs) => {
            let inner: Vec<String> = xs.iter().map(render).collect();
            format!("[{}]", inner.join(","))
        }
        other => format!("{other:?}"),
    }
}

// Phase 2 — list functions tail / list.* and the to*List family.
#[test]
fn phase2_tail_dedup_sort() {
    let (root, res) = run(
        "exec_p2_list_a",
        "RETURN tail([1,2,3]) AS t, tail([7]) AS t1, tail([]) AS te, \
             list.dedup([1,2,1,3,3,2]) AS d, list.dedup([3,[1,2],3,[1],[1,2]]) AS dn, \
             list.sort([3,1,2]) AS s, list.sort([1,3,2], false) AS sd, \
             list.sort([[4,5,6],[1,2,3]]) AS sl",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[2,3]");
    assert_eq!(render(&r[1]), "[]");
    assert_eq!(render(&r[2]), "[]");
    assert_eq!(render(&r[3]), "[1,2,3]");
    assert_eq!(render(&r[4]), "[3,[1,2],[1]]");
    assert_eq!(render(&r[5]), "[1,2,3]");
    assert_eq!(render(&r[6]), "[3,2,1]");
    assert_eq!(render(&r[7]), "[[1,2,3],[4,5,6]]");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_list_remove() {
    // Vectors ported from FalkorDB tests/flow/test_list.py test09_remove.
    let (root, res) = run(
        "exec_p2_remove",
        "RETURN list.remove([1,2,3], 1, 2) AS a, list.remove([1,2,3,4], 1, 2) AS b, \
             list.remove([1,2,3], 2) AS c, list.remove([1,2,3,4], -1, 1) AS d, \
             list.remove([1,2,3,4], -4, 1) AS e, list.remove([1,2,3,4], -3, 5) AS f, \
             list.remove([1,2,3,4], -5, 5) AS g, list.remove([1,2,3,4], 4, 5) AS h, \
             list.remove([1,2,3], 1, 0) AS i, list.remove(null, 2) AS j",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[1]");
    assert_eq!(render(&r[1]), "[1,4]");
    assert_eq!(render(&r[2]), "[1,2]");
    assert_eq!(render(&r[3]), "[1,2,3]");
    assert_eq!(render(&r[4]), "[2,3,4]");
    assert_eq!(render(&r[5]), "[1]");
    assert_eq!(render(&r[6]), "[1,2,3,4]"); // out-of-bound index → unchanged
    assert_eq!(render(&r[7]), "[1,2,3,4]");
    assert_eq!(render(&r[8]), "[1,2,3]"); // count 0 → unchanged
    assert_eq!(render(&r[9]), "null");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_list_insert_and_insert_elements() {
    // Vectors ported from FalkorDB test_list.py test11_insert / test12.
    let (root, res) = run(
        "exec_p2_insert",
        "RETURN list.insert([1,2,3], 0, 4) AS a, list.insert([1,2,3], 3, 4) AS b, \
             list.insert([1,2,3], -1, 4) AS c, list.insert([1,2,3], -3, 4) AS d, \
             list.insert([], 0, 4) AS e, list.insert(null, 2, 3) AS f, \
             list.insert([1,2,3], 0, 2, false) AS g, \
             list.insertListElements([1,2,3], [4,5,6], 0) AS h, \
             list.insertListElements([1,2,3], [4], -1) AS i, \
             list.insertListElements([1,2,3], [9,3,2,7], 0, false) AS j, \
             list.insertListElements([1,2,3], null, 1) AS k",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[4,1,2,3]");
    assert_eq!(render(&r[1]), "[1,2,3,4]");
    assert_eq!(render(&r[2]), "[1,2,3,4]");
    assert_eq!(render(&r[3]), "[1,4,2,3]");
    assert_eq!(render(&r[4]), "[4]");
    assert_eq!(render(&r[5]), "null");
    assert_eq!(render(&r[6]), "[1,2,3]"); // dups=false + 2 already present → unchanged
    assert_eq!(render(&r[7]), "[4,5,6,1,2,3]");
    assert_eq!(render(&r[8]), "[1,2,3,4]"); // idx -1 with inclusive bounds → append
    assert_eq!(render(&r[9]), "[9,7,1,2,3]"); // dups dropped vs list1
    assert_eq!(render(&r[10]), "[1,2,3]"); // null list2 → unchanged
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_to_type_lists() {
    // Vectors ported from FalkorDB test_list.py test06–09.
    let (root, res) = run(
        "exec_p2_tolists",
        "RETURN toBooleanList(null) AS a, toBooleanList([null, null]) AS b, \
             toBooleanList(['abc', true, 'false', null, ['a','b']]) AS c, \
             toFloatList(['abc', 1.5, 7.0578, null, ['a','b']]) AS d, \
             toIntegerList(['abc', 7, '5', null, ['a','b']]) AS e, \
             toStringList([1, 2.5, 'x', null]) AS f",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "null");
    assert_eq!(render(&r[1]), "[null,null]");
    assert_eq!(render(&r[2]), "[null,true,false,null,null]");
    assert_eq!(render(&r[3]), "[null,1.5,7.0578,null,null]");
    assert_eq!(render(&r[4]), "[null,7,5,null,null]");
    assert_eq!(render(&r[5]), "['1','2.5','x',null]");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase2_entity_haslabels_and_degree() {
    // Fixture: Alice -KNOWS-> Bob, -WORKS_AT-> Acme, -KNOWS-> Carol;
    //          Bob -KNOWS-> Carol; Carol -WORKS_AT-> Globex.
    let (root, res) = run(
        "exec_p2_entity",
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}), \
                   (k:Company {name: 'Acme'}) \
             RETURN hasLabels(a, ['Person']) AS h1, hasLabels(a, ['Company']) AS h2, \
                    hasLabels(a, ['Person','Foo']) AS h3, hasLabels(k, ['Company']) AS h4, \
                    outdegree(a) AS od, outdegree(a, 'KNOWS') AS odk, \
                    outdegree(a, 'WORKS_AT') AS odw, outdegree(a, ['KNOWS','WORKS_AT']) AS oda, \
                    indegree(a) AS ai, indegree(c) AS ci, indegree(c, 'KNOWS') AS cik, \
                    indegree(c, 'WORKS_AT') AS ciw",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Bool(true)));
    assert!(matches!(r[1], Val::Bool(false)));
    assert!(matches!(r[2], Val::Bool(false)));
    assert!(matches!(r[3], Val::Bool(true)));
    assert!(matches!(r[4], Val::Int(3)));
    assert!(matches!(r[5], Val::Int(2)));
    assert!(matches!(r[6], Val::Int(1)));
    assert!(matches!(r[7], Val::Int(3)));
    assert!(matches!(r[8], Val::Int(0)));
    assert!(matches!(r[9], Val::Int(2)));
    assert!(matches!(r[10], Val::Int(2)));
    assert!(matches!(r[11], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

// ── Phase 3: statistical aggregations ────────────────────────────────────

/// A `Val::Float` close to `want` (FalkorDB returns doubles for these aggs).
fn assert_float(v: &Val, want: f64) {
    match v {
        Val::Float(x) => assert!((x - want).abs() < 1e-9, "expected ~{want}, got {x}"),
        other => panic!("expected Float({want}), got {other:?}"),
    }
}

#[test]
fn phase3_stdev_sample_and_population() {
    // Vectors ported from FalkorDB tests/flow/test_aggregation.py::test06_StDev.
    // Edge case: a single value has zero sample deviation.
    let (root, res) = run("exec_p3_stdev1", "RETURN stDev(5.1) AS s");
    assert_float(&res.rows[0][0], 0.0);
    let _ = std::fs::remove_dir_all(&root);

    // 1..10: sample variance = 82.5/9, population variance = 82.5/10.
    let (root, res) = run(
        "exec_p3_stdev2",
        "UNWIND [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] AS x \
             RETURN stDev(x) AS s, stDevP(x) AS sp",
    );
    assert_float(&res.rows[0][0], (82.5_f64 / 9.0).sqrt());
    assert_float(&res.rows[0][1], (82.5_f64 / 10.0).sqrt());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase3_percentile_cont() {
    // FalkorDB test04_percentileCont: linear interpolation over [2,4,6,8,10].
    let cases = [
        (0.0, 2.0),
        (0.1, 2.8),
        (0.33, 4.64),
        (0.5, 6.0),
        (1.0, 10.0),
    ];
    for (i, (p, want)) in cases.iter().enumerate() {
        let (root, res) = run(
            &format!("exec_p3_pcont_{i}"),
            &format!("UNWIND [2, 4, 6, 8, 10] AS x RETURN percentileCont(x, {p}) AS r"),
        );
        assert_float(&res.rows[0][0], *want);
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[test]
fn phase3_percentile_disc() {
    // FalkorDB test05_percentileDisc: nearest-rank over [2,4,6,8,10].
    let cases = [(0.0, 2.0), (0.1, 2.0), (0.33, 4.0), (0.5, 6.0), (1.0, 10.0)];
    for (i, (p, want)) in cases.iter().enumerate() {
        let (root, res) = run(
            &format!("exec_p3_pdisc_{i}"),
            &format!("UNWIND [2, 4, 6, 8, 10] AS x RETURN percentileDisc(x, {p}) AS r"),
        );
        assert_float(&res.rows[0][0], *want);
        let _ = std::fs::remove_dir_all(&root);
    }
    // p == 0 takes index 0 of the sorted values, regardless of input order.
    let (root, res) = run(
        "exec_p3_pdisc_zero",
        "UNWIND [0.5, 0, 1] AS x RETURN percentileDisc(x, 0) AS r",
    );
    assert_float(&res.rows[0][0], 0.0);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase3_empty_aggregation_defaults() {
    // FalkorDB test01_empty_aggregation: with no rows and no grouping key, the
    // statistical aggregates still emit one row — stDev/stDevP→0, percentiles→null.
    let (root, res) = run(
        "exec_p3_empty",
        "MATCH (n) WHERE n.name = 'noneExisting' \
             RETURN stDev(n.v) AS a, stDevP(n.v) AS b, \
                    percentileDisc(n.v, 0.5) AS c, percentileCont(n.v, 0.5) AS d",
    );
    assert_eq!(res.rows.len(), 1);
    let r = &res.rows[0];
    assert_float(&r[0], 0.0);
    assert_float(&r[1], 0.0);
    assert!(matches!(r[2], Val::Null));
    assert!(matches!(r[3], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

// log/log10/exp/e/pi/pow — the camelid §1 gap (TF-IDF scoring needs `log`).
#[test]
fn numeric_log_family_functions() {
    let (root, res) = run(
        "exec_logfns",
        "RETURN log(2.718281828459045) AS ln, log10(1000.0) AS l10, \
             exp(0.0) AS ex, e() AS e, pi() AS pi, pow(2.0, 10.0) AS p",
    );
    let f = |v: &Val| match v {
        Val::Float(x) => *x,
        other => panic!("expected float, got {other:?}"),
    };
    let r = &res.rows[0];
    assert!((f(&r[0]) - 1.0).abs() < 1e-12);
    assert!((f(&r[1]) - 3.0).abs() < 1e-12);
    assert!((f(&r[2]) - 1.0).abs() < 1e-12);
    assert!((f(&r[3]) - std::f64::consts::E).abs() < 1e-12);
    assert!((f(&r[4]) - std::f64::consts::PI).abs() < 1e-12);
    assert!((f(&r[5]) - 1024.0).abs() < 1e-9);
    let _ = std::fs::remove_dir_all(&root);
}

// FalkorDB parity: a non-positive argument to log yields the IEEE result
// (-inf / NaN), not an error; NULL propagates as NULL.
#[test]
fn log_domain_and_null_match_falkordb() {
    let (root, res) = run(
        "exec_log_domain",
        "RETURN log(0.0) AS zero, log(-1.0) AS neg, log(null) AS nul",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Float(x) if x == f64::NEG_INFINITY));
    assert!(matches!(r[1], Val::Float(x) if x.is_nan()));
    assert!(matches!(r[2], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

// eu-ai-act §P1: a relationship whose target node is already bound from a prior
// MATCH must lead with that bound node (reverse adjacency), not full-scan the
// start label once per bound row. We assert correctness here; the reroot in
// `maybe_reroot` removes the O(|start-label|)-per-row blow-up.
#[test]
fn reverse_traversal_to_bound_node() {
    // Bob is reached by Alice and Carol via KNOWS. Bind Bob first, then match
    // the incoming KNOWS with the *source* unbound — the planner should reroot
    // to lead with Bob and walk reverse adjacency.
    let (root, res) = run(
        "exec_bound_end_reroot",
        "MATCH (b:Person {name:'Bob'}) \
             MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS nm ORDER BY nm",
    );
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(names, vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-147 execution guard: a **parameterised** id lookup must do seek-sized work,
/// not scan-sized work. The plan-level assertions live in `plan.rs`; this one runs
/// the whole engine and measures the intermediate charge, so a future regression in
/// either the id-seek walker or the re-root fails the suite instead of merely making
/// production slow.
///
/// The star fixture makes the two plans differ by construction: leading with `m`
/// scans every node and expands all `2n` LINK edges, whereas seeking the id-anchored
/// `n` and walking one reverse edge touches exactly one. We assert both the peak
/// intermediate charge (bounded by a constant, not `n`) and that a budget far below
/// `n` still completes — the un-rerooted plan blows it, which is precisely the
/// `query.maxIntermediate` failure reported on the 10M sample.
#[test]
fn parameterised_id_lookup_does_seek_sized_work_not_scan_sized() {
    const N: u64 = 2_000;
    let (root, graph) = testgen::write_hub("exec_param_id_seek", N);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);

    let params: HashMap<String, Val> = [("x".to_string(), Val::Int(7))].into_iter().collect();
    let run_with = |budget: u64| {
        let global = GlobalIntermediateBudget::new(0);
        let engine = Engine::new(&gen, &cache)
            .with_max_intermediate(budget)
            .with_global_budget(&global)
            .with_params(params.clone());
        let res = engine.run(
            &parser::parse("MATCH (m)-[:LINK]->(n) WHERE id(n) = $x RETURN m.name AS nm").unwrap(),
        );
        (res, global.peak())
    };

    // A budget two orders of magnitude below the star's edge count. The seek plan
    // needs a handful of elements; the scan plan needs ~2n.
    let (res, peak) = run_with(N / 100);
    let res = res.expect("a parameterised id lookup must not scan the whole star");
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    assert_eq!(names, vec!["hub"], "only the hub links to leaf 7");
    assert!(
        peak < N / 100,
        "seek-sized work expected, peak charge was {peak} against {N} leaves"
    );

    // Same query, unbounded — the answer must not depend on the budget, and the
    // literal spelling must agree with the parameterised one.
    let (unbounded, _) = run_with(0);
    assert_eq!(unbounded.unwrap().rows.len(), 1);
    let engine = Engine::new(&gen, &cache);
    let literal = engine
        .run(&parser::parse("MATCH (m)-[:LINK]->(n) WHERE id(n) = 7 RETURN m.name AS nm").unwrap())
        .unwrap();
    assert_eq!(literal.rows.len(), 1);
    assert_eq!(literal.rows[0][0].to_display(), "hub");
    let _ = std::fs::remove_dir_all(&root);
}

/// The **Vamana arm** of the HIK-122 rescue read. The consolidation tests exercise this
/// through the real builder, but a fixture small enough to run there is always below
/// `ann_threshold` and so always brute-force — this is the only place the other arm is
/// reached, and it has to return the *raw* embedding the user wrote, not the ANN-space
/// point the graph navigates on (`build_vamana_index` transforms for search and stores raw;
/// a rescue that returned the transformed point would write a silently wrong vector into
/// the rebuilt column store).
#[test]
fn base_index_vectors_reads_raw_embeddings_from_a_vamana_index() {
    let fix = testgen::VamanaFixture {
        n: 200,
        dim: 16,
        r: 12,
        alpha: 1.2,
        pq_subspaces: 4,
        pq_bits: 8,
        vector_block_size: 4096,
    };
    let (root, graph, raw) = testgen::write_vamana("exec_rescue_vamana", &fix);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let desc = gen.manifest().vector_indexes[0].clone();
    assert!(
        matches!(desc.mode, AnnMode::Vamana { .. }),
        "the fixture must be above the ANN threshold, or this test proves nothing"
    );

    // A scattered handful, plus an id the index does not hold.
    let wanted: HashSet<u64> = [3u64, 7, 101, 199, 5_000].into_iter().collect();
    let got = engine.base_index_vectors(&desc, &wanted).unwrap();

    assert_eq!(
        got.len(),
        4,
        "every wanted id the base indexes must come back, and only those: id 5000 is not \
             in the index"
    );
    for id in [3u64, 7, 101, 199] {
        assert_eq!(
            got.get(&id),
            Some(&raw[id as usize]),
            "node {id}'s rescued vector must be the raw embedding, byte-for-byte"
        );
    }
    // An empty request must not read the index at all — the common case is no candidates.
    assert!(engine
        .base_index_vectors(&desc, &HashSet::new())
        .unwrap()
        .is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

/// The headline M7 test: a synthetic index far above the ANN threshold **and**
/// far larger than the vector cache budget. The Vamana/PQ arm recovers most of
/// the brute-force top-k while the vector-index pool stays bounded (resident PQ
/// codes + only a handful of paged-in Vamana blocks — never the whole store).
#[test]
fn vamana_knn_matches_brute_force_with_bounded_vector_cache() {
    let fix = testgen::VamanaFixture {
        n: 2000,
        dim: 32,
        r: 24,
        alpha: 1.2,
        pq_subspaces: 8,
        pq_bits: 8,
        vector_block_size: 8192,
    };
    let (root, graph, raw) = testgen::write_vamana("exec_vamana_recall", &fix);
    let gen = Generation::open(&root, &graph).unwrap();
    let block_cache = BlockCache::new(1 << 20);

    // Budget = resident PQ codes + room for only ~8 of the 8 KiB Vamana blocks,
    // far below the full store, so the pool must page during the walk.
    let (ord, pq_bytes, blocks_total) = {
        let vi = gen.vamana_index("Doc", "embedding").unwrap();
        (
            vi.ord,
            vi.pq.resident_bytes(),
            vi.reader.inner().num_blocks(),
        )
    };
    let budget = pq_bytes + 64 * 1024;
    let vec_cache = VectorIndexCache::new(budget);
    vec_cache.pin(
        gen.uuid(),
        ord,
        gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
    );

    let k = 10;
    let queries = 20;
    let mut recall_sum = 0.0f64;
    for qi in 0..queries {
        // A query near a stored vector, lightly perturbed.
        let mut q = raw[(qi * 97) % fix.n].clone();
        q[0] += 0.05;

        // Brute-force ground truth (cosine over the raw vectors).
        let mut truth: Vec<(f64, u64)> = raw
            .iter()
            .enumerate()
            .map(|(i, v)| (1.0 - vector::cosine_similarity(&q, v), i as u64))
            .collect();
        truth.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let truth_k: std::collections::HashSet<u64> =
            truth.iter().take(k).map(|(_, id)| *id).collect();

        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            Val::List(q.iter().map(|x| Val::Float(*x as f64)).collect()),
        );
        let engine = Engine::new(&gen, &block_cache)
            .with_vector_cache(&vec_cache, 96)
            .with_params(params);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', 10, $q) \
                 YIELD node, score RETURN id(node) AS id, score",
        )
        .unwrap();
        let res = engine.run(&ast).unwrap();
        assert!(res.rows.len() <= k);
        // Scores are ascending cosine distances (the brute-force contract).
        let mut prev = f64::NEG_INFINITY;
        let got: std::collections::HashSet<u64> = res
            .rows
            .iter()
            .map(|r| {
                if let Val::Float(s) = r[1] {
                    assert!(s + 1e-6 >= prev, "scores must be ascending");
                    prev = s;
                }
                match r[0] {
                    Val::Int(n) => n as u64,
                    _ => panic!("id(node) should be an integer"),
                }
            })
            .collect();
        let found = truth_k.iter().filter(|id| got.contains(id)).count();
        recall_sum += found as f64 / k as f64;
    }
    let recall = recall_sum / queries as f64;
    assert!(
        recall >= 0.8,
        "Vamana recall@{k} was {recall:.3}, expected ≥ 0.8"
    );

    // Bounded memory: the pool never grew past its budget (+ at most one
    // oversized block), and held only a fraction of the store's blocks.
    assert!(
        vec_cache.bytes() <= budget + fix.vector_block_size,
        "vector pool {} exceeded budget {}",
        vec_cache.bytes(),
        budget
    );
    assert!(
        vec_cache.block_count() < blocks_total,
        "paged in {} of {} blocks — the whole store should never be resident",
        vec_cache.block_count(),
        blocks_total
    );
    assert!(
        blocks_total > 16,
        "test needs the store to span many blocks"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// **The hole contract (v8).** `.pq` `node_ids[i] == HOLE` ⇒ layout ordinal `i` is a
/// tombstoned record: **never emitted, still navigated through**.
///
/// The two halves fail in opposite directions, and both are silent:
///  * emit a hole ⇒ a deleted vector comes back as a live node;
///  * *prune* a hole from the walk instead of just from the results ⇒ whatever lies
///    behind it becomes unreachable and recall on the **live** nodes quietly drops.
///
/// So this holes the **medoid** — the fixed entry point of every beam search — along
/// with the query's own nearest neighbours. If a hole were dropped from navigation, a
/// holed medoid would isolate the entry point and recall for the whole index would go
/// to **zero**, which is precisely the failure mode `AnnMode::Vamana::medoid` warns
/// about. Passing at ≥ 0.9 recall *over the live set* is therefore a direct assertion
/// that a hole is still a waypoint.
#[test]
fn vamana_hole_is_a_waypoint_but_never_emitted() {
    let fix = testgen::VamanaFixture {
        n: 2000,
        dim: 32,
        r: 24,
        alpha: 1.2,
        pq_subspaces: 8,
        pq_bits: 8,
        vector_block_size: 8192,
    };
    let k = 10;
    let queries = 20;
    // The queries this test will actually issue.
    let query_of = |qi: usize, raw: &[Vec<f32>]| -> Vec<f32> {
        let mut q = raw[(qi * 97) % fix.n].clone();
        q[0] += 0.05;
        q
    };
    let rank_by_distance = |q: &[f32], raw: &[Vec<f32>]| -> Vec<(f64, u64)> {
        let mut v: Vec<(f64, u64)> = raw
            .iter()
            .enumerate()
            .map(|(i, x)| (1.0 - vector::cosine_similarity(q, x), i as u64))
            .collect();
        v.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        v
    };

    // Pick the victims: the **true top-2 of every query the loop below issues**. That
    // choice is load-bearing. Holing nodes that no query would have returned anyway
    // makes the suppression assertion vacuous — it passes whether or not the sentinel
    // is honoured, because the hole was never a top-k candidate in the first place.
    // These are the two nodes each query *must* have surfaced, so a hole that leaks
    // has nowhere to hide. (Deriving them needs the vectors, and the fixture only
    // yields them once written — so build once, choose, rebuild with them holed.)
    let (probe_root, _probe_graph, raw) = testgen::write_vamana("exec_vamana_hole_probe", &fix);
    let victims: HashSet<u64> = (0..queries)
        .flat_map(|qi| {
            rank_by_distance(&query_of(qi, &raw), &raw)
                .into_iter()
                .take(2)
                .map(|(_, id)| id)
                .collect::<Vec<_>>()
        })
        .collect();
    let _ = std::fs::remove_dir_all(&probe_root);

    // Rebuild with those, **and the medoid**, holed.
    let holed_extra = victims.clone();
    let (root, graph, raw2, medoid_node_id) =
        testgen::write_vamana_holed("exec_vamana_hole", &fix, move |id, is_medoid| {
            is_medoid || holed_extra.contains(&id)
        });
    assert_eq!(raw2, raw, "the fixture must be deterministic across builds");
    let mut holed: HashSet<u64> = victims.clone();
    holed.insert(medoid_node_id);

    let gen = Generation::open(&root, &graph).unwrap();
    let vi_desc = &gen.manifest().vector_indexes[0];
    // `count` is the RECORD count — holes included. It is what bounds a neighbour
    // ordinal, so it must not shrink when records are tombstoned.
    assert_eq!(vi_desc.count, fix.n as u64);
    assert_eq!(vi_desc.live_count(), (fix.n - holed.len()) as u64);
    assert!((vi_desc.dead_ratio() - holed.len() as f64 / fix.n as f64).abs() < 1e-12);

    let block_cache = BlockCache::new(1 << 20);
    let (ord, pq_bytes) = {
        let vi = gen.vamana_index("Doc", "embedding").unwrap();
        (vi.ord, vi.pq.resident_bytes())
    };
    assert_eq!(
        gen.vamana_index("Doc", "embedding")
            .unwrap()
            .pq
            .live_count(),
        fix.n - holed.len()
    );
    let vec_cache = VectorIndexCache::new(pq_bytes + 64 * 1024);
    vec_cache.pin(
        gen.uuid(),
        ord,
        gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
    );

    // Ground truth: brute force over the **live** set only. A hole is deleted, so the
    // truth a correct index must reproduce is the truth without it.
    let mut recall_sum = 0.0f64;
    for qi in 0..queries {
        let q = query_of(qi, &raw);
        let ranked = rank_by_distance(&q, &raw);
        // Premise check: this query really is dominated by holed nodes — its true
        // nearest neighbour is one. Without this the emit assertion below proves
        // nothing.
        assert!(holed.contains(&ranked[0].1));
        let truth_k: HashSet<u64> = ranked
            .iter()
            .filter(|(_, id)| !holed.contains(id))
            .take(k)
            .map(|(_, id)| *id)
            .collect();

        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            Val::List(q.iter().map(|x| Val::Float(*x as f64)).collect()),
        );
        let engine = Engine::new(&gen, &block_cache)
            .with_vector_cache(&vec_cache, 96)
            .with_params(params);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', 10, $q) \
                 YIELD node, score RETURN id(node) AS id, score",
        )
        .unwrap();
        let res = engine.run(&ast).unwrap();

        let got: HashSet<u64> = res
            .rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(n) => n as u64,
                _ => panic!("id(node) should be an integer"),
            })
            .collect();
        // (a) A hole is never emitted — not the medoid, not the query's own nearest.
        for id in &holed {
            assert!(
                !got.contains(id),
                "holed node {id} was emitted (medoid = {medoid_node_id})"
            );
        }
        // And the sentinel itself must never leak out as a node id.
        assert!(!got.contains(&graph_format::pq::HOLE));

        let found = truth_k.iter().filter(|id| got.contains(id)).count();
        recall_sum += found as f64 / k as f64;
    }
    // (b) A hole is still a waypoint. The entry point of every search is holed; if
    // holes were pruned from the walk rather than only from the results, this would be
    // 0.0, not ≥ 0.9.
    let recall = recall_sum / queries as f64;
    assert!(
        recall >= 0.9,
        "recall@{k} over the live set was {recall:.3} with the medoid holed — a hole \
             must stay navigable, not just unemitted"
    );

    let _ = std::fs::remove_dir_all(&root);
}

// ── Delete consolidation (FreshDiskANN S5) ────────────────────────────────
//
// A hole is navigable but never emitted, so every search that *expands* one pays a
// block read for a record that can never be returned — forever, and more of them as the
// dead fraction grows. `graph_format::vamana_delete` patches the holes out of the
// adjacency; afterwards no reachable node names one and the dead records cost **zero**
// query IO. These two tests are the ones that decide whether that is true.

/// The shape of one delete-consolidation probe: `n`, dim and the block size are held
/// fixed across every fixture so the IO numbers are comparable.
fn s5_fixture() -> testgen::VamanaFixture {
    testgen::VamanaFixture {
        n: 2000,
        dim: 32,
        r: 24,
        alpha: 1.2,
        pq_subspaces: 8,
        pq_bits: 8,
        // A handful of records per block. The block reads a query pays then track the
        // records it *expands*, which is the quantity a hole inflates. Fat blocks would
        // make the measurement vacuous: with the whole store in a few blocks, every walk
        // faults all of them whether it expands the holes or not.
        vector_block_size: 512,
    }
}

/// Run `queries` KNN queries against a Vamana generation and return `(block reads **per
/// query**, mean recall@k over the live set)`.
///
/// The vector cache is **fresh for each query** and its budget is far larger than the
/// store, so no block is ever evicted and none is inherited from the previous query:
/// `metrics().misses` is then exactly the number of distinct blocks *that one query*
/// faulted — the IO it pays. That is the quantity the DoD bounds, and a cache shared
/// across the run would not measure it (the union of blocks touched by 20 queries
/// saturates at "the whole store" long before the dead fraction can move it).
///
/// Ground truth is an exact brute force over the raw vectors of the nodes that are *not*
/// holed. (Independently derived; nothing here compares one implementation to another.)
fn s5_probe(
    root: &std::path::Path,
    graph: &str,
    raw: &[Vec<f32>],
    holed: &HashSet<u64>,
    queries: usize,
    k: usize,
) -> (f64, f64) {
    s5_probe_at(root, graph, raw, holed, queries, k, 96)
}

#[allow(clippy::too_many_arguments)]
fn s5_probe_at(
    root: &std::path::Path,
    graph: &str,
    raw: &[Vec<f32>],
    holed: &HashSet<u64>,
    queries: usize,
    k: usize,
    beam_width: usize,
) -> (f64, f64) {
    let gen = Generation::open(root, graph).unwrap();
    let block_cache = BlockCache::new(1 << 20);
    let (ord, pq_bytes) = {
        let vi = gen.vamana_index("Doc", "embedding").unwrap();
        (vi.ord, vi.pq.resident_bytes())
    };

    let mut recall_sum = 0.0f64;
    let mut misses = 0u64;
    for qi in 0..queries {
        let vec_cache = VectorIndexCache::new(pq_bytes + (64 << 20));
        vec_cache.pin(
            gen.uuid(),
            ord,
            gen.vamana_index("Doc", "embedding").unwrap().pq.clone(),
        );

        let mut q = raw[(qi * 97) % raw.len()].clone();
        q[0] += 0.05;

        let mut ranked: Vec<(f64, u64)> = raw
            .iter()
            .enumerate()
            .map(|(i, v)| (1.0 - vector::cosine_similarity(&q, v), i as u64))
            .collect();
        ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let truth_k: HashSet<u64> = ranked
            .iter()
            .filter(|(_, id)| !holed.contains(id))
            .take(k)
            .map(|(_, id)| *id)
            .collect();

        let mut params = HashMap::new();
        params.insert(
            "q".to_string(),
            Val::List(q.iter().map(|x| Val::Float(*x as f64)).collect()),
        );
        let engine = Engine::new(&gen, &block_cache)
            .with_vector_cache(&vec_cache, beam_width)
            .with_params(params);
        let ast = parser::parse(
            "CALL db.idx.vector.queryNodes('Doc', 'embedding', 10, $q) \
                 YIELD node, score RETURN id(node) AS id, score",
        )
        .unwrap();
        let res = engine.run(&ast).unwrap();
        let got: HashSet<u64> = res
            .rows
            .iter()
            .map(|r| match r[0] {
                Val::Int(n) => n as u64,
                _ => panic!("id(node) should be an integer"),
            })
            .collect();
        for id in &got {
            assert!(!holed.contains(id), "a hole must never be emitted");
        }
        recall_sum += truth_k.iter().filter(|id| got.contains(id)).count() as f64 / k as f64;

        let m = vec_cache.metrics();
        assert_eq!(
            m.evictions, 0,
            "the budget must be big enough that a miss means a first touch, not a re-fault"
        );
        misses += m.misses;
    }
    (misses as f64 / queries as f64, recall_sum / queries as f64)
}

/// Hole every `step`-th node id.
fn s5_holed_set(n: usize, step: usize) -> HashSet<u64> {
    (0..n as u64).filter(|id| id % step as u64 == 0).collect()
}

/// The beam widths a query is tried at, ascending. `s5_io_at_recall` returns the IO at
/// the first one that clears the recall bar.
const S5_BEAMS: [usize; 6] = [16, 24, 32, 48, 64, 96];

/// The block reads a query must pay on this index to reach `target` recall@k **over the
/// live set** — i.e. the IO cost of *actually answering the query*, not of some fixed
/// beam width. Returns `(beam_width, block reads per query, recall)`.
fn s5_io_at_recall(
    root: &std::path::Path,
    graph: &str,
    raw: &[Vec<f32>],
    holed: &HashSet<u64>,
    queries: usize,
    k: usize,
    target: f64,
) -> (usize, f64, f64) {
    for beam in S5_BEAMS {
        let (io, recall) = s5_probe_at(root, graph, raw, holed, queries, k, beam);
        if recall >= target {
            return (beam, io, recall);
        }
    }
    panic!(
        "this index cannot reach recall {target} at any beam width up to {} — that is not \
             an IO regression, it is a broken graph",
        S5_BEAMS[S5_BEAMS.len() - 1]
    );
}

/// **The test that proves the slice works.** Query IO must not grow with the deleted
/// fraction.
///
/// # What "IO per query" has to mean here, and why the obvious measurement is vacuous
///
/// Measure `misses` at a **fixed beam width** and a lazily-deleted index costs *exactly*
/// the same IO as a healthy one — not approximately, exactly. It has to: tombstoning a
/// record rewrites the `.pq` id column and **nothing else**, so the `.vamana` is byte
/// identical, the PQ estimates are identical, and `beam_search` therefore walks the
/// identical nodes in the identical order. `emit` returns `None` for the holes and that
/// is the *whole* difference. (Measured: 18.6 / 33.0 / 62.1 / 89.4 block reads at beams
/// 16 / 32 / 64 / 96 — the same three significant figures at 0%, 50%, 67% and 80% dead.)
///
/// So a fixed-beam miss-count assertion would pass with **no consolidation whatsoever**.
/// It cannot fail, and a test that cannot fail is worse than none.
///
/// What a hole actually costs is a **beam slot**: it is expanded, it occupies one of the
/// `L` slots, and it returns nothing. Recall over the live set falls, and the only way to
/// get it back is to **widen the beam** — which is where the IO finally lands. So the
/// honest measure is **IO at iso-recall**: the block reads a query needs to reach the
/// same recall@10 over the live set. Measured that way the cost is exactly what the slice
/// claims, and it grows with the dead fraction:
///
/// ```text
///  dead    IO to reach recall@10 ≥ 0.8 over the live set
///          lazily deleted        delete-consolidated
///    0%       18.6                   18.6
///   50%       33.0                   17.8
///   67%       62.1                   17.6      ← 3.5× the IO, for the same answer
///   80%       62.1                   17.3
/// ```
#[test]
fn delete_consolidation_does_not_grow_query_io_with_the_dead_fraction() {
    let fix = s5_fixture();
    let (k, queries, target) = (10, 20, 0.8);

    // The IO baseline: a healthy index with nothing deleted.
    let (root0, graph0, raw0) = testgen::write_vamana("s5_io_clean", &fix);
    let (beam_clean, io_clean, _) =
        s5_io_at_recall(&root0, &graph0, &raw0, &HashSet::new(), queries, k, target);
    let _ = std::fs::remove_dir_all(&root0);

    // Two thirds deleted — well past the 20% consolidation trigger. First lazily (the
    // holes left in the adjacency, today's behaviour), then delete-consolidated. Same
    // vectors, same graph, same queries, same holes: the *only* difference is the pass.
    let holed: HashSet<u64> = (0..fix.n as u64)
        .filter(|id| !id.is_multiple_of(3))
        .collect();
    let h = holed.clone();
    let (root1, graph1, raw1, _) =
        testgen::write_vamana_holed("s5_io_lazy", &fix, move |id, _| h.contains(&id));
    let (beam_lazy, io_lazy, _) =
        s5_io_at_recall(&root1, &graph1, &raw1, &holed, queries, k, target);
    let _ = std::fs::remove_dir_all(&root1);

    let h = holed.clone();
    let (root2, graph2, raw2, _) =
        testgen::write_vamana_holed_consolidated("s5_io_done", &fix, move |id, _| h.contains(&id));
    assert_eq!(raw2, raw1, "the fixtures must differ only by the pass");
    let (beam_done, io_done, recall_done) =
        s5_io_at_recall(&root2, &graph2, &raw2, &holed, queries, k, target);
    let _ = std::fs::remove_dir_all(&root2);

    assert!(
        io_done <= io_clean * 1.1,
        "query IO GREW with the dead fraction: {io_done:.1} block reads per query (beam \
             {beam_done}) at 67% dead vs {io_clean:.1} (beam {beam_clean}) with nothing \
             deleted, for the same recall. The holes are still costing beam slots — the \
             consolidation did not patch them out of the adjacency."
    );
    assert!(
        io_done * 1.5 <= io_lazy,
        "the consolidation removed no IO: {io_done:.1} block reads per query (beam \
             {beam_done}) vs {io_lazy:.1} (beam {beam_lazy}) for the same index, the same \
             deletes and the same recall, with the holes left in the adjacency. A pass that \
             is functionally correct but does not reduce block reads has FAILED — that is the \
             entire point of the slice."
    );
    assert!(recall_done >= target);
}

/// Recall@10 over the **live** set, against an exact brute force over the live set, must
/// clear the 0.8 bar — and must not be materially below what the *same* live set gets
/// from an index with no deletes in it at all. Patching the dead nodes out of the
/// adjacency re-points their in-edges at what lay behind them (the splice); if that
/// splice were wrong, the graph would be quietly less navigable and recall would sag
/// even though every structural invariant still held.
#[test]
fn delete_consolidation_keeps_recall_over_the_live_set() {
    let fix = s5_fixture();
    let k = 10;
    let queries = 20;
    // A third of the index deleted, INCLUDING the medoid — the entry point of every
    // search. If the pass ever splices the medoid's own out-edges away, recall here is
    // not "a bit low", it is zero.
    let holed_ids = s5_holed_set(fix.n, 3);

    // The "before": the same index, the same graph, the same queries — with nothing
    // deleted, scored against the exact top-k of everything it holds. That is this
    // Vamana's intrinsic recall, and it is the bar the deleted-and-consolidated index
    // must still clear over *its* live set. (Scoring the undeleted index against the
    // live-set truth would be nonsense: it would be marked down for correctly returning
    // nodes that have not been deleted in it.)
    let (root0, graph0, raw0) = testgen::write_vamana("s5_recall_before", &fix);
    let (_, recall_before) = s5_probe(&root0, &graph0, &raw0, &HashSet::new(), queries, k);
    let _ = std::fs::remove_dir_all(&root0);

    let h = holed_ids.clone();
    let (root1, graph1, raw1, medoid_id) =
        testgen::write_vamana_holed_consolidated("s5_recall_after", &fix, move |id, is_medoid| {
            is_medoid || h.contains(&id)
        });
    assert_eq!(
        raw1, raw0,
        "the fixture must be deterministic across builds"
    );
    let mut holed = holed_ids.clone();
    holed.insert(medoid_id);
    let (_, recall_after) = s5_probe(&root1, &graph1, &raw1, &holed, queries, k);

    // The generation still opens — which is itself an assertion: `validate_vamana_index`
    // refuses an index whose medoid has no out-edges, so a pass that orphaned the entry
    // point could not have got this far. And the medoid really was holed.
    let gen = Generation::open(&root1, &graph1).unwrap();
    let vi = gen.vamana_index("Doc", "embedding").unwrap();
    assert!(vi.pq.is_hole(match gen.manifest().vector_indexes[0].mode {
        AnnMode::Vamana { medoid, .. } => medoid as usize,
        _ => panic!("expected a Vamana index"),
    }));
    drop(gen);
    let _ = std::fs::remove_dir_all(&root1);

    assert!(
        recall_after >= 0.8,
        "recall@{k} over the live set after the delete consolidation was \
             {recall_after:.3}, expected ≥ 0.8"
    );
    assert!(
        recall_after >= recall_before - 0.05,
        "the consolidation cost recall on the LIVE set: {recall_after:.3} after vs \
             {recall_before:.3} before the deletes. The splice is meant to preserve \
             navigability through the region the dead nodes bridged."
    );
}

// ── id() seek pushdown — end-to-end correctness ────────────────────────────
// Fixture ids: [0]Alice [1]Bob [2]Carol (Person), [3]Acme [4]Globex (Company).
// Edges: Alice-KNOWS->Bob, Bob-KNOWS->Carol, Alice-WORKS_AT->Acme,
//        Carol-WORKS_AT->Globex, Alice-KNOWS->Carol.

#[test]
fn id_seek_returns_the_one_node() {
    let (root, res) = run(
        "exec_id_seek",
        "MATCH (n) WHERE id(n) = 1 RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_drives_expansion_without_full_scan() {
    // Lab's neighbourhood-expansion shape. Anchor `n` is seeked to Alice(0),
    // then expanded — the result is exactly Alice's out-neighbours.
    let (root, res) = run(
        "exec_id_seek_expand",
        "MATCH (n)-[r]->(m) WHERE id(n) = 0 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_still_enforces_label() {
    // Node 0 is Alice (Person), not a Company → the residual label check on the
    // seeked candidate yields nothing.
    let (root, res) = run(
        "exec_id_seek_label",
        "MATCH (n:Company) WHERE id(n) = 0 RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_still_enforces_extra_predicate() {
    // id(n)=0 seeks Alice, but the AND-ed name predicate is for Bob → empty.
    let (root, res) = run(
        "exec_id_seek_pred_no",
        "MATCH (n) WHERE id(n) = 0 AND n.name = 'Bob' RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    // The matching companion: same id, the right name → one row.
    let (root2, res2) = run(
        "exec_id_seek_pred_yes",
        "MATCH (n) WHERE id(n) = 0 AND n.name = 'Alice' RETURN n.name AS name",
    );
    assert_eq!(col0(&res2), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

#[test]
fn id_under_or_returns_all_disjuncts() {
    // THE wrong-results guard: if the seek wrongly fired on the OR it would
    // return only one node. Both must come back.
    let (root, res) = run(
        "exec_id_or",
        "MATCH (n) WHERE id(n) = 0 OR id(n) = 2 RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_in_list_returns_each() {
    let (root, res) = run(
        "exec_id_in",
        "MATCH (n) WHERE id(n) IN [0, 2, 99] RETURN n.name AS name",
    );
    // 99 is out of range and contributes nothing.
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_out_of_range_returns_empty() {
    let (root, res) = run(
        "exec_id_oor",
        "MATCH (n) WHERE id(n) = 999 RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_negative_returns_empty() {
    let (root, res) = run(
        "exec_id_neg",
        "MATCH (n) WHERE id(n) = -5 RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_seek_with_disjunction_companion_predicate() {
    // `id(n) = 0 AND (name='Alice' OR name='Zzz')`: the seek narrows to Alice,
    // the parenthesised OR is re-checked as a residual → Alice stays.
    let (root, res) = run(
        "exec_id_and_or",
        "MATCH (n) WHERE id(n) = 0 AND (n.name = 'Alice' OR n.name = 'Zzz') RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── id() seek with anchor re-rooting (id on the far end of the traversal) ───

#[test]
fn id_on_end_reroots_outgoing_expansion() {
    // `(m)-[r]->(n) WHERE id(n)=1`: id is on the END node n (Bob). Re-rooting
    // seeks Bob and walks the edge backwards → m is whoever points to Bob: Alice.
    let (root, res) = run(
        "exec_reroot_out",
        "MATCH (m)-[r]->(n) WHERE id(n) = 1 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn id_on_end_reroots_incoming_expansion() {
    // `(m)<-[r]-(n) WHERE id(n)=0`: n is Alice; m is each of Alice's
    // out-neighbours (Bob, Acme, Carol) — same as a forward expansion from her.
    let (root, res) = run(
        "exec_reroot_in",
        "MATCH (m)<-[r]-(n) WHERE id(n) = 0 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn reroot_matches_unrerooted_result_set() {
    // Both Bob and Alice point to Carol(2); re-rooting must find both.
    let (root, res) = run(
        "exec_reroot_multi",
        "MATCH (m)-[r]->(n) WHERE id(n) = 2 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn reroot_still_enforces_end_label() {
    // Acme(3) is a Company reached from Alice via WORKS_AT → one row.
    let (root, res) = run(
        "exec_reroot_label_ok",
        "MATCH (m)-[r]->(n:Company) WHERE id(n) = 3 RETURN m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    // Bob(1) is a Person, so the :Company constraint on the seeked end empties it.
    let (root2, res2) = run(
        "exec_reroot_label_no",
        "MATCH (m)-[r]->(n:Company) WHERE id(n) = 1 RETURN m.name AS name",
    );
    assert!(res2.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

#[test]
fn varlength_end_id_is_not_rerooted_but_correct() {
    // A `*` hop is excluded from re-rooting (order of a returned rel-list could
    // change); the result must still be correct via the normal scan. Paths
    // ending at Carol(2): Bob→Carol, Alice→Carol, Alice→Bob→Carol ⇒ {Alice,Bob}.
    let (root, res) = run(
        "exec_reroot_varlen",
        "MATCH (m)-[r*1..2]->(n) WHERE id(n) = 2 RETURN DISTINCT m.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── §1 list comprehension ──────────────────────────────────────────────

/// Display a single-row, single-column list result as a Vec of display strings.
fn list0(res: &QueryResult) -> Vec<String> {
    assert_eq!(res.rows.len(), 1, "expected exactly one row");
    match &res.rows[0][0] {
        Val::List(xs) => xs.iter().map(|v| v.to_display()).collect(),
        other => panic!("expected a list, got {other:?}"),
    }
}

#[test]
fn list_comprehension_filter_keeps_non_null() {
    let (root, res) = run(
        "exec_listcomp_filter",
        "RETURN [x IN [1, null, 2] WHERE x IS NOT NULL] AS r",
    );
    assert_eq!(list0(&res), vec!["1", "2"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn list_comprehension_projection_only() {
    let (root, res) = run("exec_listcomp_map", "RETURN [x IN [1, 2, 3] | x * 2] AS r");
    assert_eq!(list0(&res), vec!["2", "4", "6"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn list_comprehension_filter_and_projection() {
    let (root, res) = run(
        "exec_listcomp_both",
        "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS r",
    );
    assert_eq!(list0(&res), vec!["4", "6"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn list_comprehension_then_index() {
    // The primary call site: extract the first non-`Concept` label.
    let (root, res) = run(
        "exec_listcomp_index",
        "RETURN [l IN ['Concept', 'Person'] WHERE l <> 'Concept'][0] AS r",
    );
    assert_eq!(res.rows[0][0].to_display(), "Person");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn list_comprehension_null_source_is_null() {
    let (root, res) = run(
        "exec_listcomp_null",
        "RETURN [x IN null WHERE x > 1 | x] AS r",
    );
    assert!(matches!(res.rows[0][0], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn list_comprehension_nested() {
    // Inner builds [0,2,4,6] (evens 0..6); outer keeps those whose double is
    // ≥ 4 and doubles them: 2→4, 4→8, 6→12.
    let (root, res) = run(
        "exec_listcomp_nested",
        "RETURN [e IN [n IN [0,1,2,3,4,5,6] WHERE n % 2 = 0] WHERE e * 2 >= 4 | e * 2] AS r",
    );
    assert_eq!(list0(&res), vec!["4", "8", "12"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn bare_membership_list_still_parses_as_list_literal() {
    // `[x IN list]` (no WHERE/`|`) must remain a one-element list literal whose
    // element is the membership test — NOT a comprehension.
    let (root, res) = run("exec_membership_literal", "RETURN [2 IN [1, 2, 3]] AS r");
    match &res.rows[0][0] {
        Val::List(xs) => {
            assert_eq!(xs.len(), 1);
            assert!(matches!(xs[0], Val::Bool(true)));
        }
        other => panic!("expected a one-element list, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

// ── §2 pattern comprehension ────────────────────────────────────────────

#[test]
fn pattern_comprehension_degree_via_size() {
    // size([(n)-[:KNOWS]->(:Person) | 1]) — outgoing KNOWS degree per person.
    // Alice→{Bob,Carol}=2, Bob→{Carol}=1, Carol→{}=0.
    let (root, res) = run(
            "exec_patcomp_size",
            "MATCH (n:Person) RETURN n.name AS name, size([(n)-[:KNOWS]->(:Person) | 1]) AS deg ORDER BY name",
        );
    let got: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("Alice".into(), "2".into()),
            ("Bob".into(), "1".into()),
            ("Carol".into(), "0".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn pattern_comprehension_collects_neighbour_props() {
    // Alice knows Bob and Carol; the projection collects their names.
    let (root, res) = run(
        "exec_patcomp_names",
        "MATCH (n:Person {name: 'Alice'}) RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
    );
    let mut friends = list0(&res);
    friends.sort();
    assert_eq!(friends, vec!["Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn pattern_comprehension_empty_match_is_empty_list() {
    // Carol has no outgoing KNOWS edge → an empty list, not null.
    let (root, res) = run(
        "exec_patcomp_empty",
        "MATCH (n:Person {name: 'Carol'}) RETURN [(n)-[:KNOWS]->(m) | m.name] AS friends",
    );
    match &res.rows[0][0] {
        Val::List(xs) => assert!(xs.is_empty()),
        other => panic!("expected an empty list, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

// ── §3 UNWIND ───────────────────────────────────────────────────────────

#[test]
fn unwind_list_emits_one_row_per_element() {
    let (root, res) = run("exec_unwind_list", "UNWIND [1, 2, 3] AS x RETURN x");
    assert_eq!(col0(&res), vec!["1", "2", "3"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn unwind_empty_and_null_emit_zero_rows() {
    let (root, res) = run("exec_unwind_empty", "UNWIND [] AS x RETURN x");
    assert!(res.rows.is_empty());
    let (root2, res2) = run("exec_unwind_null", "UNWIND null AS x RETURN x");
    assert!(res2.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

#[test]
fn unwind_scalar_wraps_as_single_row() {
    // FalkorDB divergence from Neo4j: a scalar unwinds to one row.
    let (root, res) = run("exec_unwind_scalar", "UNWIND 5 AS q RETURN q");
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(5)));
    let (root2, res2) = run("exec_unwind_scalar_str", "UNWIND 'abc' AS q RETURN q");
    assert_eq!(res2.rows.len(), 1);
    assert_eq!(res2.rows[0][0].to_display(), "abc");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

#[test]
fn unwind_null_element_is_a_real_row() {
    let (root, res) = run("exec_unwind_null_elem", "UNWIND [1, null, 2] AS x RETURN x");
    assert_eq!(res.rows.len(), 3);
    assert!(matches!(res.rows[1][0], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn unwind_preserves_upstream_context() {
    // The original `l` column survives alongside the unwound `x` (TCK scenario:
    // UNWIND does not prune context).
    let (root, res) = run(
        "exec_unwind_ctx",
        "WITH [1, 2] AS l UNWIND l AS x RETURN l, x ORDER BY x",
    );
    assert_eq!(res.rows.len(), 2);
    // Each row keeps the full list in column 0 and one element in column 1.
    assert!(matches!(&res.rows[0][0], Val::List(xs) if xs.len() == 2));
    assert!(matches!(res.rows[0][1], Val::Int(1)));
    assert!(matches!(res.rows[1][1], Val::Int(2)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn unwind_variable_length_relationship_list() {
    // §3+§4 combined: unwind a collected edge list, then read its endpoints.
    let (root, res) = run(
        "exec_unwind_rels",
        "MATCH (a)-[r*1..2]->(b) WITH r LIMIT 1 UNWIND r AS e RETURN type(e) AS t",
    );
    assert!(res
        .rows
        .iter()
        .all(|row| row[0].to_display() == "KNOWS" || row[0].to_display() == "WORKS_AT"));
    assert!(!res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

// ── §4 startNode / endNode ──────────────────────────────────────────────

#[test]
fn start_and_end_node_match_walked_endpoints() {
    // For every KNOWS edge, startNode(e)==a and endNode(e)==b.
    let (root, res) = run(
            "exec_startend",
            "MATCH (a)-[e:KNOWS]->(b) RETURN a.name AS an, startNode(e).name AS sn, b.name AS bn, endNode(e).name AS en",
        );
    assert!(!res.rows.is_empty());
    for r in &res.rows {
        assert_eq!(r[0].to_display(), r[1].to_display(), "startNode mismatch");
        assert_eq!(r[2].to_display(), r[3].to_display(), "endNode mismatch");
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn start_node_of_null_is_null() {
    let (root, res) = run(
        "exec_startnull",
        "OPTIONAL MATCH (a:Person)-[e:NONEXISTENT]->(b) RETURN startNode(e) AS s LIMIT 1",
    );
    assert!(matches!(res.rows[0][0], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

/// Parse + run `q` expecting an engine error; returns the error text.
fn run_err(root_tag: &str, q: &str) -> String {
    let (root, graph, _) = testgen::write_basic(root_tag);
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse(q).unwrap();
    let err = engine.run(&ast).expect_err("expected query error");
    let _ = std::fs::remove_dir_all(&root);
    err.to_string()
}

// Phase 4 — regex `=~` full-match operator (openCypher / FalkorDB
// `str_MatchRegex`: the whole subject must match, anchored at both ends).
#[test]
fn phase4_regex_match_operator() {
    let (root, res) = run(
        "exec_p4_regex",
        "RETURN 'abc' =~ 'a.c' AS m1, 'abc' =~ 'a' AS m2, 'abc' =~ 'ab.*' AS m3, \
             'Hello World' =~ '.*World' AS m4, 'A' =~ 'a' AS m5, \
             null =~ 'a' AS m6, 'foo' =~ null AS m7",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "true"); // full match
    assert_eq!(render(&r[1]), "false"); // 'a' is not the whole 'abc'
    assert_eq!(render(&r[2]), "true");
    assert_eq!(render(&r[3]), "true");
    assert_eq!(render(&r[4]), "false"); // case-sensitive
    assert_eq!(render(&r[5]), "null"); // null subject -> null
    assert_eq!(render(&r[6]), "null"); // null pattern -> null
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase4_regex_invalid_pattern_errors() {
    let msg = run_err("exec_p4_badregex", "RETURN 'aa' =~ '('");
    assert!(msg.contains("Invalid regex"), "got: {msg}");
}

// Phase 4 — string.join (vectors ported from test_function_calls.py test89).
#[test]
fn phase4_string_join() {
    let (root, res) = run(
        "exec_p4_join",
        "RETURN string.join(['HELL','OW']) AS a, string.join(['HELL','OW'], ' ') AS b, \
             string.join(['HELL'], ' ') AS c, string.join(['HELL','OW','NOW'], ' ') AS d, \
             string.join([]) AS e, string.join([], '|') AS f, string.join(null, '') AS g",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'HELLOW'");
    assert_eq!(render(&r[1]), "'HELL OW'");
    assert_eq!(render(&r[2]), "'HELL'");
    assert_eq!(render(&r[3]), "'HELL OW NOW'");
    assert_eq!(render(&r[4]), "''");
    assert_eq!(render(&r[5]), "''");
    assert_eq!(render(&r[6]), "null");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase4_string_join_type_mismatch_errors() {
    let msg = run_err("exec_p4_join_err", "RETURN string.join(['HELL', 2], ' ')");
    assert!(
        msg.contains("Type mismatch") && msg.contains("Integer"),
        "got: {msg}"
    );
}

// Phase 4 — string.matchRegEx (vectors ported from test_function_calls.py
// test91). Unanchored scan; each match is [full, group1, …]; null -> [].
#[test]
fn phase4_string_matchregex() {
    let (root, res) = run(
        "exec_p4_matchregex",
        r"RETURN
                string.matchRegEx('blabla <header h1>txt1</header>', '<header (\w+)>(\w+)</header>') AS a,
                string.matchRegEx('blabla <header h1>txt1</header> blabla <header h2>txt2</header>', '<header (\w+)>(\w+)</header>') AS b,
                string.matchRegEx('aba', 'a') AS c,
                string.matchRegEx('', 'a') AS d,
                string.matchRegEx('bla', '(bla)(bal)') AS e,
                string.matchRegEx('bla9', '(bla)[(bal)9]') AS f,
                string.matchRegEx(null, 'bla') AS g,
                string.matchRegEx('bla', null) AS h",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[['<header h1>txt1</header>','h1','txt1']]");
    assert_eq!(
        render(&r[1]),
        "[['<header h1>txt1</header>','h1','txt1'],['<header h2>txt2</header>','h2','txt2']]"
    );
    assert_eq!(render(&r[2]), "[['a'],['a']]");
    assert_eq!(render(&r[3]), "[]");
    assert_eq!(render(&r[4]), "[]");
    assert_eq!(render(&r[5]), "[['bla9','bla']]");
    assert_eq!(render(&r[6]), "[]");
    assert_eq!(render(&r[7]), "[]");
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 4 — string.replaceRegEx (vectors ported from test_function_calls.py
// test92). Literal replacement (no `$group` expansion); null operand -> null.
#[test]
fn phase4_string_replaceregex() {
    let (root, res) = run(
        "exec_p4_replaceregex",
        r"RETURN
                string.replaceRegEx('blabla <header h1>txt1</header>', '<header (\w+)>(\w+)</header>', 'hellow') AS a,
                string.replaceRegEx('blabla <header h1>txt1</header> blabla <header h2>txt2</header>', '<header (\w+)>(\w+)</header>', 'hellow') AS b,
                string.replaceRegEx('abc', '[b]') AS c,
                string.replaceRegEx('abc', '[b]', '55') AS d,
                string.replaceRegEx('abcb', '[b]', '') AS e,
                string.replaceRegEx('bbla', '[b]', 'bla') AS f,
                string.replaceRegEx('', '[b]', 'bla') AS g,
                string.replaceRegEx(null, 'bla') AS h,
                string.replaceRegEx('bla', null) AS i,
                string.replaceRegEx('bla', 'bla', null) AS j",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'blabla hellow'");
    assert_eq!(render(&r[1]), "'blabla hellow blabla hellow'");
    assert_eq!(render(&r[2]), "'ac'");
    assert_eq!(render(&r[3]), "'a55c'");
    assert_eq!(render(&r[4]), "'ac'");
    assert_eq!(render(&r[5]), "'blablala'");
    assert_eq!(render(&r[6]), "''");
    assert_eq!(render(&r[7]), "null");
    assert_eq!(render(&r[8]), "null");
    assert_eq!(render(&r[9]), "null");
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 5 — list slice `[i..j]` (vectors ported from TCK List2.feature and
// FalkorDB `AR_SLICE`). Open ends, negative indices, empty/exceeding ranges.
#[test]
fn phase5_list_slice() {
    let (root, res) = run(
        "exec_p5_slice",
        "WITH [1,2,3,4,5] AS l5, [1,2,3] AS l3 RETURN \
             l5[1..3] AS a, l3[1..] AS b, l3[..2] AS c, l3[0..1] AS d, \
             l3[0..0] AS e, l3[-3..-1] AS f, l3[3..1] AS g, l3[-5..5] AS h, \
             l3[..] AS i",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "[2,3]");
    assert_eq!(render(&r[1]), "[2,3]");
    assert_eq!(render(&r[2]), "[1,2]");
    assert_eq!(render(&r[3]), "[1]");
    assert_eq!(render(&r[4]), "[]");
    assert_eq!(render(&r[5]), "[1,2]");
    assert_eq!(render(&r[6]), "[]");
    assert_eq!(render(&r[7]), "[1,2,3]");
    assert_eq!(render(&r[8]), "[1,2,3]");
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 5 — slice null handling (test_list.py test03 + TCK List2 [9]): a NULL
// list or any NULL bound yields NULL.
#[test]
fn phase5_slice_null() {
    let (root, res) = run(
        "exec_p5_slice_null",
        "WITH null AS n, [1,2,3] AS l RETURN \
             n[0..5] AS a, l[0..null] AS b, l[null..2] AS c, l[null..] AS d, n[..] AS e",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "null");
    assert_eq!(render(&r[1]), "null");
    assert_eq!(render(&r[2]), "null");
    assert_eq!(render(&r[3]), "null");
    assert_eq!(render(&r[4]), "null");
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 5 — string slicing (Slater extension beyond FalkorDB's array-only
// slice; slices by Unicode scalar value).
#[test]
fn phase5_string_slice() {
    let (root, res) = run(
        "exec_p5_str_slice",
        "WITH 'hello' AS s RETURN s[1..3] AS a, s[..2] AS b, s[2..] AS c, s[-2..] AS d",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'el'");
    assert_eq!(render(&r[1]), "'he'");
    assert_eq!(render(&r[2]), "'llo'");
    assert_eq!(render(&r[3]), "'lo'");
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 5 — reduce (vectors ported from FalkorDB test_reduce.py).
#[test]
fn phase5_reduce() {
    let (root, res) = run(
        "exec_p5_reduce",
        "RETURN \
             reduce(sum = 0, n in [1,2,3] | sum + n) AS a, \
             reduce(sum = 0, n in [1,2,3] | sum - n) AS b, \
             reduce(sum = 0, n in ['1','2','3'] | sum + toInteger(n)) AS c, \
             reduce(last = 0, n in [1,2,3] | n) AS d, \
             reduce(msg = 'hello ', c in ['w','o','r','l','d'] | msg + c) AS e, \
             reduce(arr = [1,2], n in [2,3] | arr + n) AS f, \
             reduce(sum = 1, n in [] | sum + n) AS g",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "6");
    assert_eq!(render(&r[1]), "-6");
    assert_eq!(render(&r[2]), "6");
    assert_eq!(render(&r[3]), "3");
    assert_eq!(render(&r[4]), "'hello world'");
    assert_eq!(render(&r[5]), "[1,2,2,3]");
    assert_eq!(render(&r[6]), "1");
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 5 — reduce with carried/outer variables and nesting (test_reduce.py
// test_variable_reduction / test_nested_reduction / test_multiple_reductions).
#[test]
fn phase5_reduce_variables_and_nesting() {
    let (root, res) = run(
        "exec_p5_reduce_vars",
        "WITH 1 AS base, [1,2,3] AS arr, -1 AS bias \
             RETURN reduce(sum = base, n in arr | sum + n + bias) AS a, \
             reduce(sum = reduce(x = 1, n in [1] | x + n), \
                    n in reduce(arr = [1], n in [2] | arr + n) | sum + n) AS b",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "4");
    assert_eq!(render(&r[1]), "5");
    let _ = std::fs::remove_dir_all(&root);

    let (root, res) = run(
        "exec_p5_reduce_multi",
        "UNWIND [[1,2,3],[4,5,6]] AS arr RETURN reduce(sum = 1, n in arr | sum + n) AS s",
    );
    assert_eq!(col0(&res), vec!["16", "7"]);
    let _ = std::fs::remove_dir_all(&root);
}

// Phase 5 — reduce null/error handling (test_reduce.py test_null_reduction /
// test_type_missmatch_reduction).
#[test]
fn phase5_reduce_null_and_errors() {
    let (root, res) = run(
        "exec_p5_reduce_null",
        "RETURN reduce(sum = null, n in [1,2,3] | sum + n) AS a, \
             reduce(sum = 1, n in null | sum + n) AS b, \
             reduce(sum = 1, n in [1,2,3] | sum + n + null) AS c",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "null");
    assert_eq!(render(&r[1]), "null");
    assert_eq!(render(&r[2]), "null");
    let _ = std::fs::remove_dir_all(&root);

    // 'a' * 1 is an invalid operation; '2' is not a list.
    assert!(run_err(
        "exec_p5_reduce_e1",
        "RETURN reduce(sum = 'a', n in [1,2,3] | sum * n)"
    )
    .contains("cannot apply arithmetic"));
    assert!(run_err(
        "exec_p5_reduce_e2",
        "RETURN reduce(sum = 1, n in 2 | sum + n)"
    )
    .contains("needs a list"));
    // A reduce missing its `| body` is a plain function call over the
    // would-be accumulator binding `sum`, which is unbound -> runtime error.
    assert!(run_err("exec_p5_reduce_e3", "RETURN reduce(sum = 0, n in [1,2,3])").contains("'sum'"));
    let _ = std::fs::remove_dir_all(&root);
}

// ── Phase 6 — pattern predicates & EXISTS { } ──────────────────────────────
//
// Vectors are adapted from FalkorDB/TCK `expressions/pattern/Pattern1.feature`
// and `existentialSubqueries/ExistentialSubquery1.feature` onto the shared
// read-only fixture (those scenarios use CREATE setup we cannot replay).
// Fixture topology:
//   Alice -KNOWS-> Bob, Bob -KNOWS-> Carol, Alice -KNOWS-> Carol,
//   Alice -WORKS_AT-> Acme, Carol -WORKS_AT-> Globex.

// Pattern1 [1]/[4]/[6]: any / typed-outgoing / typed-incoming connection.
#[test]
fn phase6_pattern_predicate_directions() {
    // Any outgoing edge — everyone with an out-edge (not the two companies).
    let (root, res) = run(
        "exec_p6_any_out",
        "MATCH (n) WHERE (n)-->() RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);

    // Outgoing KNOWS only (Carol's sole out-edge is WORKS_AT).
    let (root, res) = run(
        "exec_p6_knows_out",
        "MATCH (n) WHERE (n)-[:KNOWS]->() RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);

    // Incoming KNOWS.
    let (root, res) = run(
        "exec_p6_knows_in",
        "MATCH (n) WHERE (n)<-[:KNOWS]-() RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

// Pattern1 [5]: undirected connection sees the edge from either end.
#[test]
fn phase6_pattern_predicate_undirected_and_label() {
    let (root, res) = run(
        "exec_p6_undirected",
        "MATCH (n) WHERE (n)-[:WORKS_AT]-() RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Alice", "Carol", "Globex"]);
    let _ = std::fs::remove_dir_all(&root);

    // A label predicate on the far node restricts the match.
    let (root, res) = run(
        "exec_p6_label",
        "MATCH (n) WHERE (n)-[:WORKS_AT]->(:Company) RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

// Pattern1 [19]/[20]/[21]: negation, conjunction, disjunction of predicates.
#[test]
fn phase6_pattern_predicate_boolean_combinations() {
    // NOT — anti-semi-apply: the two companies have no out-edge.
    let (root, res) = run(
        "exec_p6_not",
        "MATCH (n) WHERE NOT (n)-->() RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Globex"]);
    let _ = std::fs::remove_dir_all(&root);

    // Conjunction — only Alice both KNOWS-out and WORKS_AT-out.
    let (root, res) = run(
        "exec_p6_and",
        "MATCH (n) WHERE (n)-[:KNOWS]->() AND (n)-[:WORKS_AT]->() RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);

    // Disjunction — WORKS_AT-out (Alice, Carol) OR KNOWS-in (Bob, Carol).
    let (root, res) = run(
        "exec_p6_or",
        "MATCH (n) WHERE (n)-[:WORKS_AT]->() OR (n)<-[:KNOWS]-() RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

// Pattern1 [14]: two bound endpoints — the predicate pins both sides.
#[test]
fn phase6_pattern_predicate_two_bound_nodes() {
    let (root, res) = run(
        "exec_p6_two_node",
        "MATCH (n), (m) WHERE (n)-[:KNOWS]->(m) RETURN n.name AS a, m.name AS b",
    );
    let mut pairs: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("Alice".into(), "Bob".into()),
            ("Alice".into(), "Carol".into()),
            ("Bob".into(), "Carol".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ExistentialSubquery1 [1]/[3]: simple EXISTS, with and without a match.
#[test]
fn phase6_exists_simple() {
    let (root, res) = run(
        "exec_p6_exists_knows",
        "MATCH (n) WHERE EXISTS { (n)-[:KNOWS]->() } RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);

    // A non-existent relationship type yields no matches → empty result.
    let (root, res) = run(
        "exec_p6_exists_none",
        "MATCH (n) WHERE EXISTS { (n)-[:NOSUCHREL]->() } RETURN n.name AS name",
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

// ExistentialSubquery2 [1]: the explicit-MATCH inner form with a label.
#[test]
fn phase6_exists_with_match_keyword() {
    let (root, res) = run(
        "exec_p6_exists_match",
        "MATCH (n) WHERE EXISTS { MATCH (n)-[:WORKS_AT]->(:Company) } RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ExistentialSubquery1 [2]: inner WHERE correlating outer and inner bindings.
#[test]
fn phase6_exists_inner_where_correlated() {
    // Who points at someone older? Only Alice(30)->Bob(25) satisfies n.age >
    // m.age; Acme/Globex have no age so the comparison is NULL (excluded).
    let (root, res) = run(
        "exec_p6_exists_where",
        "MATCH (n) WHERE EXISTS { (n)-->(m) WHERE n.age > m.age } RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);

    // Negated EXISTS — nodes with no outgoing KNOWS edge.
    let (root, res) = run(
        "exec_p6_not_exists",
        "MATCH (n) WHERE NOT EXISTS { (n)-[:KNOWS]->() } RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Carol", "Globex"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── Phase 7 — Val::Path, path functions, shortestPath ────────────────────

// `MATCH p=(…)-[…]->(…) RETURN p` binds a path; nodes()/length() read it back.
// Vectors adapted from FalkorDB tests/flow/test_path.py (read-only fixture).
#[test]
fn phase7_path_binding_and_functions() {
    let (root, res) = run(
        "exec_p7_path_bind",
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b:Person) \
             RETURN [n IN nodes(p) | n.name] AS names, length(p) AS l ORDER BY b.name",
    );
    assert_eq!(res.columns, vec!["names", "l"]);
    assert_eq!(res.rows.len(), 2);
    assert_eq!(render(&res.rows[0][0]), "['Alice','Bob']");
    assert!(matches!(res.rows[0][1], Val::Int(1)));
    assert_eq!(render(&res.rows[1][0]), "['Alice','Carol']");
    assert!(matches!(res.rows[1][1], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);
}

// A variable-length path binds every node along the walk (incl. intermediates).
#[test]
fn phase7_variable_length_path() {
    let (root, res) = run(
        "exec_p7_varlen_path",
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS*]->(b:Person) \
             RETURN [n IN nodes(p) | n.name] AS names ORDER BY length(p), b.name",
    );
    let got: Vec<String> = res.rows.iter().map(|r| render(&r[0])).collect();
    assert_eq!(
        got,
        vec![
            "['Alice','Bob']",
            "['Alice','Carol']",
            "['Alice','Bob','Carol']",
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// relationships(p) yields the edges in walk order; type()/id() read them.
#[test]
fn phase7_relationships_function() {
    let (root, res) = run(
        "exec_p7_rels_fn",
        "MATCH p=(a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}) \
             RETURN [r IN relationships(p) | type(r)] AS types, \
                    [r IN relationships(p) | id(r)] AS ids",
    );
    assert_eq!(render(&res.rows[0][0]), "['KNOWS']");
    assert_eq!(render(&res.rows[0][1]), "[0]");
    let _ = std::fs::remove_dir_all(&root);
}

// Path equality/inequality filters (test_path.py test_path_comparison). Each of
// the 3 KNOWS paths equals only itself, so `p1 = p2` keeps 3 of the 9 pairs.
#[test]
fn phase7_path_equality() {
    let (root, res) = run(
        "exec_p7_path_eq",
        "MATCH p1=(a:Person)-[:KNOWS]->(b:Person) \
             MATCH p2=(c:Person)-[:KNOWS]->(d:Person) WHERE p1 = p2 RETURN count(*) AS c",
    );
    assert!(matches!(res.rows[0][0], Val::Int(3)));
    let _ = std::fs::remove_dir_all(&root);

    let (root, res) = run(
        "exec_p7_path_neq",
        "MATCH p1=(a:Person)-[:KNOWS]->(b:Person) \
             MATCH p2=(c:Person)-[:KNOWS]->(d:Person) WHERE p1 <> p2 RETURN count(*) AS c",
    );
    assert!(matches!(res.rows[0][0], Val::Int(6)));
    let _ = std::fs::remove_dir_all(&root);
}

// shortestPath finds the fewest-hop route: Alice→Carol direct (e4), not via Bob.
// A reversed pattern `(c)<-[*]-(a)` yields the same path (test_shortest_path.py).
#[test]
fn phase7_shortest_path() {
    let (root, res) = run(
        "exec_p7_sp",
        "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Carol'}) \
             RETURN length(shortestPath((a)-[:KNOWS*]->(c))) AS l, \
                    [n IN nodes(shortestPath((a)-[:KNOWS*]->(c))) | n.name] AS names, \
                    [n IN nodes(shortestPath((c)<-[:KNOWS*]-(a))) | n.name] AS rev",
    );
    assert!(matches!(res.rows[0][0], Val::Int(1)));
    assert_eq!(render(&res.rows[0][1]), "['Alice','Carol']");
    assert_eq!(render(&res.rows[0][2]), "['Alice','Carol']");
    let _ = std::fs::remove_dir_all(&root);
}

// `*0..` admits the empty (single-node) path when src == dst; `*` (min 1) does
// not, so a node with no cycle back to itself yields NULL (test05_min_hops).
#[test]
fn phase7_shortest_path_min_zero() {
    let (root, res) = run(
        "exec_p7_sp_zero",
        "MATCH (a:Person {name:'Alice'}) \
             RETURN length(shortestPath((a)-[:KNOWS*0..]->(a))) AS l, \
                    [n IN nodes(shortestPath((a)-[:KNOWS*0..]->(a))) | n.name] AS names, \
                    shortestPath((a)-[:KNOWS*]->(a)) IS NULL AS cyc_null",
    );
    assert!(matches!(res.rows[0][0], Val::Int(0)));
    assert_eq!(render(&res.rows[0][1]), "['Alice']");
    assert!(matches!(res.rows[0][2], Val::Bool(true)));
    let _ = std::fs::remove_dir_all(&root);
}

// No connecting path → NULL (Bob cannot reach Alice over KNOWS).
#[test]
fn phase7_shortest_path_no_path() {
    let (root, res) = run(
        "exec_p7_sp_none",
        "MATCH (a:Person {name:'Bob'}), (c:Person {name:'Alice'}) \
             RETURN shortestPath((a)-[:KNOWS*]->(c)) IS NULL AS np",
    );
    assert!(matches!(res.rows[0][0], Val::Bool(true)));
    let _ = std::fs::remove_dir_all(&root);
}

// shortestPath inside a WHERE filter (test07_shortestPath_in_filter): keep source
// nodes that can reach Carol over KNOWS — Alice and Bob (Carol has no cycle).
#[test]
fn phase7_shortest_path_in_filter() {
    let (root, res) = run(
        "exec_p7_sp_filter",
        "MATCH (a:Person), (c:Person {name:'Carol'}) \
             WHERE length(shortestPath((a)-[:KNOWS*]->(c))) > 0 RETURN a.name AS n",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

// The wrapped-pattern restrictions FalkorDB enforces (test01_invalid_shortest_paths).
#[test]
fn phase7_shortest_path_errors() {
    let pre = "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Carol'}) RETURN ";
    let cases = [
        (
            "exec_p7_sp_e1",
            "shortestPath((a)-[:KNOWS*2..]->(b))",
            "minimal length",
        ),
        (
            "exec_p7_sp_e2",
            "shortestPath((a)-[:KNOWS]->()-[:KNOWS*]->(b))",
            "single relationship",
        ),
        (
            "exec_p7_sp_e3",
            "shortestPath((a)-[:KNOWS* {since:2020}]->(b))",
            "filters on relationships",
        ),
        (
            "exec_p7_sp_e4",
            "shortestPath((a)-[:KNOWS*]->())",
            "requires bound nodes",
        ),
    ];
    for (tag, sp, want) in cases {
        let msg = run_err(tag, &format!("{pre}{sp}"));
        assert!(msg.contains(want), "query `{sp}` → `{msg}` (want `{want}`)");
    }

    // An unbound endpoint variable is likewise rejected.
    let msg = run_err(
        "exec_p7_sp_e5",
        "MATCH (a:Person {name:'Alice'}) RETURN shortestPath((a)-[:KNOWS*]->(z))",
    );
    assert!(msg.contains("requires bound nodes"), "{msg}");
}

// ── Phase 11: metadata procedures (CALL dispatch) ────────────────────────
// Vectors adapted from FalkorDB tests/flow/test_procedures.py (test11/test12)
// onto the read-only fixture: Person(3)/Company(2) nodes, KNOWS(3)/WORKS_AT(2)
// edges, 5 property keys.

fn map_get<'a>(v: &'a Val, key: &str) -> &'a Val {
    match v {
        Val::Map(m) => m
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, val)| val)
            .unwrap_or_else(|| panic!("key {key:?} absent in {v:?}")),
        o => panic!("expected map, got {o:?}"),
    }
}

#[test]
fn phase11_meta_stats_bare() {
    // A bare `CALL db.meta.stats()` (no YIELD/RETURN) returns every output.
    let (root, res) = run("exec_p11_meta", "CALL db.meta.stats()");
    assert_eq!(
        res.columns,
        vec![
            "labels",
            "relTypes",
            "relCount",
            "nodeCount",
            "labelCount",
            "relTypeCount",
            "propertyKeyCount"
        ]
    );
    assert_eq!(res.rows.len(), 1);
    let r = &res.rows[0];
    assert!(matches!(map_get(&r[0], "Person"), Val::Int(3)));
    assert!(matches!(map_get(&r[0], "Company"), Val::Int(2)));
    assert!(matches!(map_get(&r[1], "KNOWS"), Val::Int(3)));
    assert!(matches!(map_get(&r[1], "WORKS_AT"), Val::Int(2)));
    assert!(matches!(r[2], Val::Int(5)), "relCount");
    assert!(matches!(r[3], Val::Int(5)), "nodeCount");
    assert!(matches!(r[4], Val::Int(2)), "labelCount");
    assert!(matches!(r[5], Val::Int(2)), "relTypeCount");
    assert!(matches!(r[6], Val::Int(6)), "propertyKeyCount");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase11_meta_stats_yield_projection() {
    // YIELD selects/reorders outputs into a downstream pipeline.
    let (root, res) = run(
        "exec_p11_meta_yield",
        "CALL db.meta.stats() YIELD nodeCount, relCount, propertyKeyCount \
             RETURN propertyKeyCount AS pk, nodeCount AS n, relCount AS r",
    );
    assert_eq!(res.columns, vec!["pk", "n", "r"]);
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Int(6))); // propertyKeyCount (name/age/city/since/embedding/team)
    assert!(matches!(r[1], Val::Int(5)));
    assert!(matches!(r[2], Val::Int(5)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase11_dbms_procedures_yield_order() {
    // FalkorDB test11 form: YIELD mode, name RETURN mode, name ORDER BY name.
    let (root, res) = run(
        "exec_p11_procs",
        "CALL dbms.procedures() YIELD mode, name RETURN mode, name ORDER BY name",
    );
    assert_eq!(res.columns, vec!["mode", "name"]);
    // Every procedure is READ; names are sorted.
    let names: Vec<String> = res.rows.iter().map(|r| r[1].to_display()).collect();
    assert!(res.rows.iter().all(|r| r[0].to_display() == "READ"));
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "ORDER BY name");
    for want in [
        "db.constraints",
        "db.meta.stats",
        "dbms.functions",
        "dbms.procedures",
    ] {
        assert!(names.iter().any(|n| n == want), "missing {want}");
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase11_dbms_functions_aggregation_flag() {
    // FalkorDB test12 form (literals instead of $param): the aggregation flag
    // distinguishes aggregates from scalars.
    let (root, res) = run(
        "exec_p11_funcs",
        "CALL dbms.functions() YIELD name, aggregation \
             WHERE name IN ['avg', 'count', 'sin'] \
             RETURN name, aggregation ORDER BY name",
    );
    assert_eq!(res.columns, vec!["name", "aggregation"]);
    let got: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("avg".to_string(), "true".to_string()),
            ("count".to_string(), "true".to_string()),
            ("sin".to_string(), "false".to_string()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase11_dbms_functions_coverage_gate() {
    // The self-report is the coverage gate: a representative sample of the
    // functions landed through Phases 1–9 must be present.
    let (root, res) = run(
        "exec_p11_funcs_cov",
        "CALL dbms.functions() YIELD name RETURN name",
    );
    let names: Vec<String> = res.rows.iter().map(|r| r[0].to_display()).collect();
    for want in [
        "sin",
        "tail",
        "point",
        "distance",
        "vec.euclideandistance",
        "tofloatornull",
        "percentilecont",
        "string.matchregex",
        "date",
        "duration",
    ] {
        assert!(
            names.iter().any(|n| n == want),
            "coverage gate missing {want}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase11_db_constraints_empty() {
    // slater enforces no constraints → empty result with the FalkorDB shape.
    let (root, res) = run(
        "exec_p11_constraints",
        "CALL db.constraints() YIELD type, label, properties, entitytype, status \
             RETURN type, label, properties, entitytype, status",
    );
    assert_eq!(
        res.columns,
        vec!["type", "label", "properties", "entitytype", "status"]
    );
    assert!(res.rows.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase11_call_unknown_yield_errors() {
    let (root, graph, _) = testgen::write_basic("exec_p11_badyield");
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let ast = parser::parse("CALL db.meta.stats() YIELD bogus RETURN bogus").unwrap();
    let err = engine.run(&ast).unwrap_err().to_string();
    assert!(err.contains("does not yield 'bogus'"), "{err}");
    let _ = std::fs::remove_dir_all(&root);
}

// ── Phase 12 — CALL { … } subquery ───────────────────────────────────────
// Vectors adapted from FalkorDB `tests/flow/test_call_subquery.py` (test02–07,
// test14, test17) onto the read-only fixture (Person Alice/Bob/Carol with
// name/age/city; their CREATE-based setup is replayed as MATCH over the
// fixture).

#[test]
fn phase12_simple_scan_return() {
    // test02: a plain scan-and-return subquery, with an outer RETURN over it.
    let (root, res) = run(
        "exec_p12_scan",
        "CALL { MATCH (n:Person {name: 'Alice'}) RETURN n } RETURN n.name AS name",
    );
    assert_eq!(res.columns, vec!["name"]);
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_importing_with_correlated() {
    // test04: import an outer variable with a leading `WITH` and reference it
    // inside; the subquery returns one row per outer row.
    let (root, res) = run(
        "exec_p12_import",
        "MATCH (p:Person) CALL { WITH p RETURN p.age AS age } \
             RETURN p.name AS name, age ORDER BY age ASC",
    );
    assert_eq!(res.columns, vec!["name", "age"]);
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(
        rows,
        vec![
            ("Bob".into(), "25".into()),
            ("Alice".into(), "30".into()),
            ("Carol".into(), "40".into()),
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_cardinality_multiplication() {
    // test06: a returning subquery multiplies cardinality (2 outer × 3 inner =
    // 6 rows). The inner does not import `x`, so it is invisible inside.
    let (root, res) = run(
        "exec_p12_card",
        "UNWIND [1, 2] AS x CALL { UNWIND [10, 20, 30] AS y RETURN y } \
             RETURN x, y ORDER BY x ASC, y ASC",
    );
    assert_eq!(res.columns, vec!["x", "y"]);
    let rows: Vec<(i64, i64)> = res
        .rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Val::Int(a), Val::Int(b)) => (*a, *b),
            _ => panic!("expected ints"),
        })
        .collect();
    assert_eq!(
        rows,
        vec![(1, 10), (1, 20), (1, 30), (2, 10), (2, 20), (2, 30)]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_correlated_filter_drops_rows() {
    // test03/test05: a returning subquery that yields nothing for an outer row
    // drops that row entirely (no input passthrough). 'Zztop' matches no node.
    let (root, res) = run(
        "exec_p12_drop",
        "UNWIND ['Alice', 'Zztop'] AS nm \
             CALL { WITH nm MATCH (p:Person {name: nm}) RETURN p } \
             RETURN p.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_optional_match_in_subquery() {
    // test07: OPTIONAL MATCH inside the subquery keeps the row with a null when
    // nothing matches, so cardinality is preserved per outer row.
    let (root, res) = run(
        "exec_p12_optional",
        "UNWIND [25, 99] AS a \
             CALL { WITH a OPTIONAL MATCH (p:Person {age: a}) RETURN p } \
             RETURN a, p.name AS name ORDER BY a ASC",
    );
    let rows: Vec<(String, String)> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(
        rows,
        vec![("25".into(), "Bob".into()), ("99".into(), "null".into())]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_aggregation_in_subquery() {
    // test04/test17: a correlated aggregation. For each threshold `a`, count the
    // Persons with age >= a (Bob 25, Alice 30, Carol 40).
    let (root, res) = run(
        "exec_p12_agg",
        "UNWIND [25, 30] AS a \
             CALL { WITH a MATCH (p:Person) WHERE p.age >= a RETURN count(p) AS c } \
             RETURN a, c ORDER BY a ASC",
    );
    let rows: Vec<(i64, i64)> = res
        .rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Val::Int(a), Val::Int(c)) => (*a, *c),
            _ => panic!("expected ints"),
        })
        .collect();
    assert_eq!(rows, vec![(25, 3), (30, 2)]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_nested_call_subquery() {
    // test14: a CALL {} directly inside another CALL {}.
    let (root, res) = run(
        "exec_p12_nested",
        "CALL { CALL { MATCH (p:Person {name: 'Bob'}) RETURN p } RETURN p } \
             RETURN p.name AS name",
    );
    assert_eq!(col0(&res), vec!["Bob"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_union_in_subquery() {
    // A UNION inside the subquery, each branch importing `p`. DISTINCT union of
    // Alice's name and city.
    let (root, res) = run(
        "exec_p12_union",
        "MATCH (p:Person {name: 'Alice'}) \
             CALL { WITH p RETURN p.name AS x UNION WITH p RETURN p.city AS x } \
             RETURN x ORDER BY x ASC",
    );
    assert_eq!(col0(&res), vec!["Alice", "London"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_unit_subquery_passthrough() {
    // A unit (RETURN-less) subquery preserves the outer cardinality: one outer
    // row stays one row even though the inner MATCH finds three Persons.
    let (root, res) = run(
        "exec_p12_unit",
        "WITH 1 AS a CALL { MATCH (p:Person) } RETURN a",
    );
    assert_eq!(res.columns, vec!["a"]);
    assert_eq!(res.rows.len(), 1);
    assert!(matches!(res.rows[0][0], Val::Int(1)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase12_non_imported_outer_var_is_invisible() {
    // test01: without a leading `WITH`, an outer variable is not visible inside.
    let err = run_err(
        "exec_p12_invisible",
        "WITH 1 AS a CALL { RETURN a AS b } RETURN b",
    );
    assert!(err.contains("'a' is not in scope"), "{err}");
}

#[test]
fn phase12_import_undefined_errors() {
    // test01: importing a variable that does not exist outside is an error.
    let err = run_err(
        "exec_p12_undef",
        "CALL { WITH a RETURN 1 AS one } RETURN one",
    );
    assert!(err.contains("'a' is not in scope"), "{err}");
}

#[test]
fn phase12_outer_scope_collision_errors() {
    // test01: a subquery may not return a name already bound in the outer scope.
    let err = run_err(
        "exec_p12_collision",
        "MATCH (p:Person {name: 'Alice'}) CALL { RETURN 1 AS p } RETURN p",
    );
    assert!(err.contains("already declared in outer scope"), "{err}");
}

// ── Phase 13: algo.* graph-algorithm procedures ──────────────────────────
//
// Tests run over the `write_basic` fixture (dense ids in brackets):
//   [0]Alice [1]Bob [2]Carol :Person ; [3]Acme [4]Globex :Company
//   Alice-KNOWS->Bob, Bob-KNOWS->Carol, Alice-KNOWS->Carol,
//   Alice-WORKS_AT->Acme, Carol-WORKS_AT->Globex
// FalkorDB's own algo tests use CREATE setups we can't replay, so the vectors
// are adapted to this fixture; assertions follow the FalkorDB tests' style
// (orderings, exact-0 for sinks, sum≈1) rather than exact LAGraph float values.

#[test]
fn phase13_bfs_all_reltypes_and_restricted() {
    // BFS from Alice over all relationship types reaches everyone but Alice.
    let (root, res) = run(
        "exec_p13_bfs_all",
        "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, -1, NULL) YIELD nodes \
             UNWIND nodes AS n RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Acme", "Bob", "Carol", "Globex"]);

    // Restricted to KNOWS, only the two reachable Persons appear.
    let (_, res) = run(
        "exec_p13_bfs_knows",
        "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, -1, 'KNOWS') YIELD nodes \
             UNWIND nodes AS n RETURN n.name AS name",
    );
    assert_eq!(col0(&res), vec!["Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_bfs_max_depth_and_edges() {
    // Depth 1 = direct neighbours only; edges parallel the nodes (each is the
    // tree edge that first reached the node).
    let (root, res) = run(
        "exec_p13_bfs_depth",
        "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, 1, 'KNOWS') YIELD nodes, edges \
             RETURN [n IN nodes | n.name] AS ns, [e IN edges | type(e)] AS ts, size(edges) AS k",
    );
    assert_eq!(res.rows.len(), 1);
    // nodes are Bob and Carol (Alice's direct KNOWS neighbours)
    let Val::List(ns) = &res.rows[0][0] else {
        panic!("expected list");
    };
    let mut names: Vec<String> = ns.iter().map(|v| v.to_display()).collect();
    names.sort();
    assert_eq!(names, vec!["Bob", "Carol"]);
    // every tree edge is a KNOWS edge, one per reached node
    let Val::List(ts) = &res.rows[0][1] else {
        panic!("expected list");
    };
    assert!(ts.iter().all(|t| t.to_display() == "KNOWS"));
    assert!(matches!(res.rows[0][2], Val::Int(2)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_bfs_no_results_and_null_source() {
    // A sink node (Globex) reaches nothing → the CALL produces zero rows.
    let (root, res) = run(
        "exec_p13_bfs_sink",
        "MATCH (g:Company {name: 'Globex'}) \
             CALL algo.BFS(g, -1, NULL) YIELD nodes RETURN nodes",
    );
    assert_eq!(res.rows.len(), 0);

    // A missing relationship type → zero rows.
    let (_, res) = run(
        "exec_p13_bfs_missing_rel",
        "MATCH (a:Person {name: 'Alice'}) \
             CALL algo.BFS(a, -1, 'NOPE') YIELD nodes RETURN nodes",
    );
    assert_eq!(res.rows.len(), 0);

    // A NULL source (OPTIONAL MATCH with no hit) → zero rows, no error.
    let (_, res) = run(
        "exec_p13_bfs_null",
        "OPTIONAL MATCH (n:NoSuchLabel) \
             CALL algo.BFS(n, -1, NULL) YIELD nodes RETURN nodes",
    );
    assert_eq!(res.rows.len(), 0);
    let _ = std::fs::remove_dir_all(&root);
}

// ── HIK-88: algo.* must honour the memory budget and the query deadline ──────

#[test]
fn algo_bfs_charges_the_intermediate_budget() {
    // BFS from Alice reaches four nodes (Bob, Carol, Acme, Globex), charging two
    // elements per discovered node (one `Val::Node`, one `Val::Rel`) against
    // `maxIntermediate`. Pre-fix the loop grew `nodes`/`edges`/`visited` with no
    // `charge`, so it ran to completion regardless of the budget; now a tiny
    // budget trips before the whole reachable subgraph is materialised. The query
    // keeps the BFS result unexpanded (`RETURN size(nodes)`, no UNWIND) so only
    // the BFS's own charge — not downstream row-building — can trip the budget.
    let q = "MATCH (a:Person {name: 'Alice'}) \
                 CALL algo.BFS(a, 0, NULL) YIELD nodes RETURN size(nodes) AS k";
    let (root, gen, cache, _) = budgeted_engine("exec_algo_bfs_budget", 0);
    // A generous budget completes and reaches all four nodes.
    let res = Engine::new(&gen, &cache)
        .with_max_intermediate(1_000)
        .run(&parser::parse(q).unwrap())
        .expect("a generous budget lets the BFS finish");
    assert!(matches!(res.rows[0][0], Val::Int(4)));
    // A budget below the retained-node charge must abort with the budget error
    // rather than running the BFS to completion.
    let err = Engine::new(&gen, &cache)
        .with_max_intermediate(3)
        .run(&parser::parse(q).unwrap())
        .expect_err("a tiny budget must bound the BFS");
    assert!(
        format!("{err:#}").contains("intermediate result budget"),
        "expected the intermediate-budget error, got: {err:#}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn algo_bfs_observes_the_deadline() {
    // The BFS pop loop now checks the deadline each iteration, so a runaway
    // `algo.BFS(src, 0, NULL)` aborts at `timeoutMs` instead of materialising the
    // whole reachable subgraph uninterruptibly.
    let q = "MATCH (a:Person {name: 'Alice'}) \
                 CALL algo.BFS(a, 0, NULL) YIELD nodes RETURN nodes";
    let (root, gen, cache, _) = budgeted_engine("exec_algo_bfs_deadline", 0);
    let res = Engine::new(&gen, &cache)
        .run(&parser::parse(q).unwrap())
        .expect("no deadline lets the BFS finish");
    assert_eq!(res.rows.len(), 1);
    let err = Engine::new(&gen, &cache)
        .with_deadline(Instant::now() - std::time::Duration::from_secs(1))
        .run(&parser::parse(q).unwrap())
        .expect_err("an elapsed deadline must abort the BFS");
    assert!(
        format!("{err:#}").contains("time limit"),
        "expected the deadline error, got: {err:#}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The whole `build_view`-backed family (every `algo.*` except BFS) exercised by
/// the budget / deadline guards, so a fix to BFS alone can't pass this.
const ALGO_VIEW_PROCS: [&str; 5] = [
    "CALL algo.WCC() YIELD node RETURN node",
    "CALL algo.pageRank(NULL, NULL) YIELD node RETURN node",
    "CALL algo.HarmonicCentrality() YIELD node RETURN node",
    "CALL algo.betweenness() YIELD node RETURN node",
    "CALL algo.labelPropagation() YIELD node RETURN node",
];

#[test]
fn algo_view_procs_charge_the_intermediate_budget() {
    // `build_view` materialises the whole selected subgraph (nodes + position map
    // + out-adjacency) before the algorithm runs. Pre-fix that ignored
    // `maxIntermediate` entirely — an OOM on a large store. Now it charges the
    // node count up front, so a budget below the 5-node fixture trips each proc.
    let (root, gen, cache, _) = budgeted_engine("exec_algo_view_budget", 0);
    for q in ALGO_VIEW_PROCS {
        let ast = parser::parse(q).unwrap();
        Engine::new(&gen, &cache)
            .with_max_intermediate(10_000)
            .run(&ast)
            .unwrap_or_else(|e| panic!("{q}: a generous budget should succeed: {e:#}"));
        let err = Engine::new(&gen, &cache)
            .with_max_intermediate(1)
            .run(&ast)
            .expect_err("a budget below the node count must bound the view");
        assert!(
            format!("{err:#}").contains("intermediate result budget"),
            "{q}: expected the intermediate-budget error, got: {err:#}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn algo_view_procs_observe_the_deadline() {
    // `build_view` checks the deadline as it fills, and each algorithm kernel is
    // threaded an interrupt it polls while working, so the `O(V·E)` centrality
    // procs abort at `timeoutMs` instead of wedging the connection.
    let (root, gen, cache, _) = budgeted_engine("exec_algo_view_deadline", 0);
    for q in ALGO_VIEW_PROCS {
        let ast = parser::parse(q).unwrap();
        Engine::new(&gen, &cache)
            .run(&ast)
            .unwrap_or_else(|e| panic!("{q}: no deadline should succeed: {e:#}"));
        let err = Engine::new(&gen, &cache)
            .with_deadline(Instant::now() - std::time::Duration::from_secs(1))
            .run(&ast)
            .expect_err("an elapsed deadline must abort the view proc");
        assert!(
            format!("{err:#}").contains("time limit"),
            "{q}: expected the deadline error, got: {err:#}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_wcc_components() {
    // All edges undirected → the whole graph is one component of 5.
    let (root, res) = run(
        "exec_p13_wcc_all",
        "CALL algo.WCC() YIELD node, componentId RETURN node.name AS name, componentId",
    );
    assert_eq!(res.rows.len(), 5);
    let cids: std::collections::HashSet<String> =
        res.rows.iter().map(|r| r[1].to_display()).collect();
    assert_eq!(cids.len(), 1, "one component over the full graph");

    // Restricted to KNOWS: the three Persons form one component; the two
    // Companies (no KNOWS edges) are isolated singletons → 3 components.
    let (_, res) = run(
        "exec_p13_wcc_knows",
        "CALL algo.WCC({relationshipTypes: ['KNOWS']}) YIELD node, componentId \
             RETURN node.name AS name, componentId",
    );
    assert_eq!(res.rows.len(), 5);
    let mut groups: std::collections::HashMap<String, Vec<String>> = Default::default();
    for r in &res.rows {
        groups
            .entry(r[1].to_display())
            .or_default()
            .push(r[0].to_display());
    }
    assert_eq!(groups.len(), 3, "Persons + 2 isolated Companies");
    // the Persons share one component
    let person_comp: Vec<_> = res
        .rows
        .iter()
        .filter(|r| ["Alice", "Bob", "Carol"].contains(&r[0].to_display().as_str()))
        .map(|r| r[1].to_display())
        .collect();
    assert!(
        person_comp.windows(2).all(|w| w[0] == w[1]),
        "Persons in one component"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_wcc_node_label_filter() {
    // nodeLabels=['Person'] selects only the three Persons, connected via KNOWS.
    let (root, res) = run(
        "exec_p13_wcc_person",
        "CALL algo.WCC({nodeLabels: ['Person']}) YIELD node RETURN node.name AS name",
    );
    assert_eq!(col0(&res), vec!["Alice", "Bob", "Carol"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_pagerank_scores() {
    // Over the whole graph: 5 rows, scores positive and summing to ~1
    // (FalkorDB test_pagerank asserts exactly these structural properties).
    let (root, res) = run(
        "exec_p13_pagerank",
        "CALL algo.pageRank(NULL, NULL) YIELD node, score \
             RETURN node.name AS name, score",
    );
    assert_eq!(res.rows.len(), 5);
    let mut sum = 0.0;
    for r in &res.rows {
        let Val::Float(s) = r[1] else {
            panic!("score should be a float");
        };
        assert!(s > 0.0, "scores are positive");
        sum += s;
    }
    assert!((sum - 1.0).abs() < 1e-4, "scores sum to ~1, got {sum}");

    // Over the Person/KNOWS subgraph (Alice->Bob, Alice->Carol, Bob->Carol),
    // Carol — the sink all rank flows toward — scores highest of the three.
    let (_, res) = run(
        "exec_p13_pagerank_knows",
        "CALL algo.pageRank('Person', 'KNOWS') YIELD node, score \
             RETURN node.name AS name, score",
    );
    assert_eq!(res.rows.len(), 3);
    let scores: std::collections::HashMap<String, f64> = res
        .rows
        .iter()
        .map(|r| {
            let Val::Float(s) = r[1] else {
                panic!("score should be a float");
            };
            (r[0].to_display(), s)
        })
        .collect();
    assert!(scores["Carol"] > scores["Alice"], "Carol > Alice");
    assert!(scores["Carol"] > scores["Bob"], "Carol > Bob");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_harmonic_centrality() {
    // Over the Person/KNOWS subgraph (Alice->Bob, Alice->Carol, Bob->Carol):
    //   Alice reaches Bob & Carol at d=1 → score 2.0, reachable 2
    //   Bob reaches Carol at d=1         → score 1.0, reachable 1
    //   Carol is a sink                  → score 0.0, reachable 0
    let (root, res) = run(
        "exec_p13_harmonic",
        "CALL algo.HarmonicCentrality({nodeLabels: ['Person'], relationshipTypes: ['KNOWS']}) \
             YIELD node, score, reachable \
             RETURN node.name AS name, score, reachable ORDER BY score DESC",
    );
    assert_eq!(res.rows.len(), 3);
    assert_eq!(res.rows[0][0].to_display(), "Alice");
    assert_float(&res.rows[0][1], 2.0);
    assert!(matches!(res.rows[0][2], Val::Int(2)));
    assert_eq!(res.rows[1][0].to_display(), "Bob");
    assert_float(&res.rows[1][1], 1.0);
    assert!(matches!(res.rows[1][2], Val::Int(1)));
    assert_eq!(res.rows[2][0].to_display(), "Carol");
    assert_float(&res.rows[2][1], 0.0);
    assert!(matches!(res.rows[2][2], Val::Int(0)));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_betweenness() {
    // Over the whole graph, only Carol lies on a shortest path between other
    // nodes (Alice->Globex and Bob->Globex both pass through Carol); every other
    // node has betweenness exactly 0.
    let (root, res) = run(
        "exec_p13_betweenness",
        "CALL algo.betweenness() YIELD node, score RETURN node.name AS name, score",
    );
    assert_eq!(res.rows.len(), 5);
    let scores: std::collections::HashMap<String, f64> = res
        .rows
        .iter()
        .map(|r| {
            let Val::Float(s) = r[1] else {
                panic!("score should be a float");
            };
            (r[0].to_display(), s)
        })
        .collect();
    assert!(scores["Carol"] > 0.0, "Carol is on shortest paths");
    for name in ["Alice", "Bob", "Acme", "Globex"] {
        assert_eq!(scores[name], 0.0, "{name} is on no shortest path");
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_label_propagation() {
    // Over the KNOWS subgraph the three Persons form one community; the two
    // Companies (no KNOWS edges) stay in their own singleton communities.
    let (root, res) = run(
        "exec_p13_labelprop",
        "CALL algo.labelPropagation({relationshipTypes: ['KNOWS']}) \
             YIELD node, communityId RETURN node.name AS name, communityId",
    );
    assert_eq!(res.rows.len(), 5);
    let comm: std::collections::HashMap<String, String> = res
        .rows
        .iter()
        .map(|r| (r[0].to_display(), r[1].to_display()))
        .collect();
    assert_eq!(comm["Alice"], comm["Bob"]);
    assert_eq!(comm["Bob"], comm["Carol"]);
    assert_ne!(comm["Alice"], comm["Acme"]);
    assert_ne!(comm["Alice"], comm["Globex"]);
    assert_ne!(comm["Acme"], comm["Globex"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn phase13_algo_validation_errors() {
    // Unknown YIELD field.
    let e = run_err(
        "exec_p13_err_yield",
        "CALL algo.WCC() YIELD node, bogus RETURN node",
    );
    assert!(e.contains("does not yield 'bogus'"), "{e}");

    // Non-array nodeLabels.
    let e = run_err(
        "exec_p13_err_labels",
        "CALL algo.WCC({nodeLabels: 'Person'}) YIELD node RETURN node",
    );
    assert!(e.contains("should be an array of strings"), "{e}");

    // Unknown config key.
    let e = run_err(
        "exec_p13_err_key",
        "CALL algo.WCC({bogus: 1}) YIELD node RETURN node",
    );
    assert!(e.contains("unknown key"), "{e}");

    // Non-map config argument.
    let e = run_err(
        "exec_p13_err_cfg",
        "CALL algo.WCC('invalid') YIELD node RETURN node",
    );
    assert!(e.contains("invalid WCC configuration"), "{e}");

    // pageRank requires exactly two scalar arguments.
    let e = run_err(
        "exec_p13_err_pr_arity",
        "CALL algo.pageRank('Person') YIELD node RETURN node",
    );
    assert!(e.contains("expects 2 arguments"), "{e}");

    // betweenness sampling-size validation.
    let e = run_err(
        "exec_p13_err_sampling",
        "CALL algo.betweenness({samplingSize: -1}) YIELD node RETURN node",
    );
    assert!(e.contains("samplingSize"), "{e}");
}

// ── Phase 10 — temporal value types (date/localtime/localdatetime/duration) ──
// Vectors ported from FalkorDB `tests/flow/test_temporal.py`. The inline `run`
// harness has no params, so the `$map`/`$str` inputs become literal map/string
// expressions in the query text.

/// `localtime` from a map and from a string, its `.hour/.minute/.second`
/// components, and `toString` (sub-second is dropped → `HH:MM:SS`).
#[test]
fn phase10_localtime_construction_and_components() {
    let (root, res) = run(
        "exec_p10_lt",
        "WITH localtime({hour: 12, minute: 31, second: 14, nanosecond: 645876123}) AS d \
             RETURN toString(d) AS s, d.hour AS h, d.minute AS mi, d.second AS se, typeOf(d) AS t",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'12:31:14'");
    assert!(matches!(r[1], Val::Int(12)));
    assert!(matches!(r[2], Val::Int(31)));
    assert!(matches!(r[3], Val::Int(14)));
    assert_eq!(render(&r[4]), "'Time'");

    // String forms (compact + colon) and the trailing-fraction drop.
    let (root2, res2) = run(
        "exec_p10_lt_str",
        "RETURN toString(localtime('21')) AS a, toString(localtime('2140')) AS b, \
                    toString(localtime('214032')) AS c, toString(localtime('21:40:32.143')) AS e",
    );
    let r = &res2.rows[0];
    assert_eq!(render(&r[0]), "'21:00:00'");
    assert_eq!(render(&r[1]), "'21:40:00'");
    assert_eq!(render(&r[2]), "'21:40:32'");
    assert_eq!(render(&r[3]), "'21:40:32'");

    // toString round-trips back to an equal value.
    let (root3, res3) = run(
        "exec_p10_lt_rt",
        "WITH localtime({hour: 12, minute: 31, second: 14}) AS d \
             RETURN localtime(toString(d)) = d AS b",
    );
    assert!(matches!(res3.rows[0][0], Val::Bool(true)));
    for p in [root, root2, root3] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// `date` from components (y/m/d, ISO week, quarter) and strings, its many
/// components, and `toString` (`YYYY-MM-DD`).
#[test]
fn phase10_date_construction_and_components() {
    // Component-map and string constructions agree on the rendered date.
    let (root, res) = run(
        "exec_p10_date_build",
        "RETURN toString(date({year:1984})) AS a, \
                    toString(date({year:1984, month:10})) AS b, \
                    toString(date({year:1984, week:10})) AS c, \
                    toString(date({year:1984, month:10, day:11})) AS d, \
                    toString(date({year:1984, week:10, dayOfWeek:3})) AS e, \
                    toString(date({year:1984, quarter:3, dayOfQuarter:45})) AS f, \
                    toString(date({year:1984, quarter:3})) AS g, \
                    toString(date('2015202')) AS h, toString(date('2015-W30-2')) AS i, \
                    toString(date('20150721')) AS j",
    );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'1984-01-01'");
    assert_eq!(render(&r[1]), "'1984-10-01'");
    assert_eq!(render(&r[2]), "'1984-03-05'");
    assert_eq!(render(&r[3]), "'1984-10-11'");
    assert_eq!(render(&r[4]), "'1984-03-07'");
    assert_eq!(render(&r[5]), "'1984-08-14'");
    assert_eq!(render(&r[6]), "'1984-07-01'");
    assert_eq!(render(&r[7]), "'2015-07-21'"); // ordinal day 202
    assert_eq!(render(&r[8]), "'2015-07-21'"); // ISO week 30, Tue
    assert_eq!(render(&r[9]), "'2015-07-21'");

    // Components of date(1984-10-21) — incl. FalkorDB's quirky dayOfQuarter (23).
    let (root2, res2) = run(
        "exec_p10_date_comp",
        "WITH date({year: 1984, month:10, day:21}) AS d \
             RETURN d.year, d.quarter, d.month, d.week, d.day, d.dayOfWeek, \
                    d.dayOfQuarter, d.ordinalDay, typeOf(d)",
    );
    let r = &res2.rows[0];
    let ints: Vec<i64> = (0..8)
        .map(|i| match r[i] {
            Val::Int(v) => v,
            ref o => panic!("col {i}: expected int, got {o:?}"),
        })
        .collect();
    assert_eq!(ints, vec![1984, 4, 10, 42, 21, 0, 23, 295]);
    assert_eq!(render(&r[8]), "'Date'");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// `localdatetime` from components/strings, its `toString` (`…T…`), the
/// ISO-week construction edge cases, and component access.
#[test]
fn phase10_localdatetime_construction_and_components() {
    let (root, res) = run(
            "exec_p10_ldt",
            "RETURN toString(localdatetime({year:1984, month:10, day:11, hour:12, minute:31, second:14, nanosecond:645876123})) AS a, \
                    toString(localdatetime({year:1984, month:10, day:11, hour:12})) AS b, \
                    toString(localdatetime({year:1984})) AS c, \
                    toString(localdatetime({year:1918, week:1})) AS d, \
                    toString(localdatetime({year:1918, week:53})) AS e, \
                    toString(localdatetime('2025-02-18T12:34:56')) AS f, \
                    toString(localdatetime('20250218T123456')) AS g",
        );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'1984-10-11T12:31:14'");
    assert_eq!(render(&r[1]), "'1984-10-11T12:00:00'");
    assert_eq!(render(&r[2]), "'1984-01-01T00:00:00'");
    assert_eq!(render(&r[3]), "'1917-12-31T00:00:00'"); // ISO week 1 of 1918
    assert_eq!(render(&r[4]), "'1918-12-30T00:00:00'"); // lenient week 53
    assert_eq!(render(&r[5]), "'2025-02-18T12:34:56'");
    assert_eq!(render(&r[6]), "'2025-02-18T12:34:56'");

    // Components incl. clock parts + round-trip via toString.
    let (root2, res2) = run(
        "exec_p10_ldt_comp",
        "WITH localdatetime({year:1984, month:10, day:21, hour:10, minute:31, second:46}) AS d \
             RETURN d.year, d.quarter, d.month, d.week, d.day, d.ordinalDay, \
                    d.hour, d.minute, d.second, \
                    localdatetime(toString(d)) = d AS rt, typeOf(d) AS t",
    );
    let r = &res2.rows[0];
    let ints: Vec<i64> = (0..9)
        .map(|i| match r[i] {
            Val::Int(v) => v,
            ref o => panic!("col {i}: expected int, got {o:?}"),
        })
        .collect();
    assert_eq!(ints, vec![1984, 4, 10, 42, 21, 295, 10, 31, 46]);
    assert!(matches!(r[9], Val::Bool(true)), "toString round-trip");
    assert_eq!(render(&r[10]), "'Datetime'");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// `duration` from a map and ISO-8601 string, its components (weeks fold into
/// days), and `toString`.
#[test]
fn phase10_duration_construction_and_components() {
    // Components: weeks fold into days (1 week + 4 days → 11 days, 0 weeks).
    let (root, res) = run(
            "exec_p10_dur_comp",
            "WITH duration({years:2, months:3, weeks:1, days:4, hours:5, minutes:22, seconds:7}) AS d \
             RETURN d.years, d.months, d.weeks, d.days, d.hours, d.minutes, d.seconds, typeOf(d) AS t",
        );
    let r = &res.rows[0];
    // Duration components are doubles (FalkorDB `SI_DoubleVal`) → render as ints.
    let got: Vec<String> = (0..7).map(|i| render(&r[i])).collect();
    assert_eq!(got, vec!["2", "3", "0", "11", "5", "22", "7"]);
    assert_eq!(render(&r[7]), "'Duration'");

    // String form + toString round-trips ('P1M' stays 'P1M').
    let (root2, res2) = run(
            "exec_p10_dur_str",
            "RETURN toString(duration('P1M')) AS a, \
                    toString(duration('P1Y2M3DT4H5M6S')) AS b, \
                    toString(duration({years:2, months:3, days:11, hours:5, minutes:22, seconds:7})) AS c",
        );
    let r = &res2.rows[0];
    assert_eq!(render(&r[0]), "'P1M'");
    assert_eq!(render(&r[1]), "'P1Y2M3DT4H5M6S'");
    assert_eq!(render(&r[2]), "'P2Y3M11DT5H22M7S'");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Comparison operators over each temporal type (test_temporal.py *_compare).
#[test]
fn phase10_temporal_comparison() {
    let (root, res) = run(
            "exec_p10_cmp",
            "WITH date({year:1980, month:12, day:24}) AS d1, date({year:1984, month:10, day:11}) AS d2, \
                  localtime({hour:10, minute:35}) AS t1, localtime({hour:12, minute:31, second:14}) AS t2, \
                  duration({years:1, months:11}) AS u1, duration({years:1, months:10}) AS u2 \
             RETURN d1 < d2, d1 = d2, t1 < t2, t1 >= t2, u1 > u2, u1 = u2, \
                    d1 = d1, t2 = t2",
        );
    let r = &res.rows[0];
    let b: Vec<bool> = (0..8)
        .map(|i| match r[i] {
            Val::Bool(v) => v,
            ref o => panic!("col {i}: {o:?}"),
        })
        .collect();
    // d1<d2 T, d1=d2 F, t1<t2 T, t1>=t2 F, u1>u2 T, u1=u2 F, d1=d1 T, t2=t2 T
    assert_eq!(b, vec![true, false, true, false, true, false, true, true]);

    // Cross-type comparison (date vs duration) is `null`, not an error.
    let (root2, res2) = run(
        "exec_p10_cmp_x",
        "WITH date({year:2000, month:1, day:1}) AS d, duration({days:1}) AS u \
             RETURN d < u AS lt, d = u AS eq",
    );
    let r = &res2.rows[0];
    assert!(matches!(r[0], Val::Null), "date<duration → null");
    assert!(matches!(r[1], Val::Bool(false)), "date=duration → false");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Temporal ± duration and duration ± duration (test_temporal.py
/// test_duration_add + test_month_end_duration_arithmetic).
#[test]
fn phase10_temporal_arithmetic() {
    let (root, res) = run(
            "exec_p10_arith",
            "WITH duration({years:1, months:1, weeks:1, days:1, hours:1, minutes:32, seconds:10}) AS a, \
                  duration({years:2, months:2, weeks:2, days:2, hours:2, minutes:34, seconds:12}) AS b \
             RETURN toString(a + b) AS sum, toString(b - a) AS diff",
        );
    let r = &res.rows[0];
    assert_eq!(render(&r[0]), "'P3Y3M24DT4H6M22S'"); // 66 min normalises to 4h6m
    assert_eq!(render(&r[1]), "'P1Y1M8DT1H2M2S'");

    let (root2, res2) = run(
            "exec_p10_arith2",
            "RETURN toString(date({year:1984, month:10, day:21}) + duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS d, \
                    toString(duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1}) + date({year:1984, month:10, day:21})) AS d2, \
                    toString(localtime({hour:2, minute:34, second:32}) + duration({years:1, months:1, days:1, hours:1, minutes:35, seconds:35})) AS t, \
                    toString(localtime({hour:10, minute:30, second:10}) - duration({hours:2, minutes:40, seconds:30})) AS t2, \
                    toString(localdatetime({year:1984, month:10, day:21, hour:5, minute:30, second:10}) + duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS dt, \
                    toString(localdatetime({year:1984, month:10, day:21, hour:5, minute:30, second:10}) - duration({years:1, months:1, days:1, hours:1, minutes:1, seconds:1})) AS dt2",
        );
    let r = &res2.rows[0];
    assert_eq!(render(&r[0]), "'1985-11-22'"); // date + dur (clock parts ignored)
    assert_eq!(render(&r[1]), "'1985-11-22'"); // commutative
    assert_eq!(render(&r[2]), "'04:10:07'"); // time + dur (calendar parts ignored)
    assert_eq!(render(&r[3]), "'07:49:40'"); // time - dur
    assert_eq!(render(&r[4]), "'1985-11-22T06:31:11'");
    assert_eq!(render(&r[5]), "'1983-09-20T04:29:09'");

    // Month-end overflow normalises forward (Jan 31 + 1mo → Mar 02).
    let (root3, res3) = run(
        "exec_p10_arith_me",
        "RETURN toString(date('2024-01-31') + duration('P1M')) AS d, \
                    toString(localdatetime('2024-01-31T00:00:00') + duration('P1M')) AS l",
    );
    let r = &res3.rows[0];
    assert_eq!(render(&r[0]), "'2024-03-02'");
    assert_eq!(render(&r[1]), "'2024-03-02T00:00:00'");
    for p in [root, root2, root3] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Unsupported temporal arithmetic errors (duration − temporal is invalid),
/// and `null`/unknown-component handling.
#[test]
fn phase10_temporal_errors_and_null() {
    for (tag, q) in [
        (
            "exec_p10_e1",
            "RETURN duration({days:1}) - date({year:1984, month:10, day:21})",
        ),
        (
            "exec_p10_e2",
            "RETURN duration({hours:2}) - localtime({hour:10, minute:30})",
        ),
        (
            "exec_p10_e3",
            "RETURN duration({days:1}) - localdatetime({year:1984})",
        ),
    ] {
        let e = run_err(tag, q);
        assert!(e.contains("cannot be subtracted"), "query `{q}` → `{e}`");
    }

    // Unknown component on a temporal is an error (unlike Point/Map → NULL).
    let e = run_err(
        "exec_p10_e_comp",
        "WITH date({year:2000, month:1, day:1}) AS d RETURN d.bogus",
    );
    assert!(e.contains("unknown date component"), "{e}");

    // NULL / bad-string inputs propagate to NULL.
    let (root, res) = run(
        "exec_p10_null",
        "RETURN date(null) AS a, localtime('nonsense') AS b, duration('not-a-duration') AS c",
    );
    let r = &res.rows[0];
    assert!(matches!(r[0], Val::Null));
    assert!(matches!(r[1], Val::Null));
    assert!(matches!(r[2], Val::Null));
    let _ = std::fs::remove_dir_all(&root);
}

// ── Phase 1b — non-deterministic builtins (rand / randomUUID / timestamp) ──
#[test]
fn phase1b_nondeterministic_functions() {
    let (root, res) = run(
        "exec_p1b_fns",
        "RETURN rand() AS r, randomUUID() AS u, timestamp() AS t",
    );
    let r = &res.rows[0];
    match r[0] {
        Val::Float(x) => assert!((0.0..1.0).contains(&x), "rand() in [0,1): {x}"),
        ref o => panic!("rand() → {o:?}"),
    }
    match &r[1] {
        // RFC-4122 v4: 36 chars, 4 hyphens, version nibble '4'.
        Val::Str(s) => {
            assert_eq!(s.len(), 36, "uuid {s}");
            assert_eq!(s.matches('-').count(), 4, "uuid {s}");
            assert_eq!(s.as_bytes()[14], b'4', "v4 version nibble: {s}");
        }
        o => panic!("randomUUID() → {o:?}"),
    }
    match r[2] {
        // Milliseconds since the epoch — well past 2020 (1.6e12 ms).
        Val::Int(t) => assert!(t > 1_600_000_000_000, "timestamp() ms: {t}"),
        ref o => panic!("timestamp() → {o:?}"),
    }

    // Two randomUUID() calls in one row are distinct.
    let (root2, res2) = run(
        "exec_p1b_uuid2",
        "RETURN randomUUID() AS a, randomUUID() AS b",
    );
    let r = &res2.rows[0];
    assert_ne!(render(&r[0]), render(&r[1]), "two UUIDs differ");
    for p in [root, root2] {
        let _ = std::fs::remove_dir_all(&p);
    }
}

/// Regression (HIK-74): `rand()` must cover the whole of `[0, 1)`, not a
/// sliver of it. The old implementation sliced the *low* 64 bits of a v4
/// UUID, whose two most-significant bits are the fixed RFC-4122 variant
/// (`10`), so every draw landed in `[0.5, 0.75)` — `WHERE rand() < 0.1` could
/// never match, and `ORDER BY rand()` shuffled over a quarter of the range.
///
/// The bounds below are deliberately loose: with a correct uniform generator
/// and `N = 20_000` draws, every assertion here fails with probability far
/// below 1e-9 (an empty octile alone is `(7/8)^20000 ≈ 1e-1160`), so this is
/// a distribution test that cannot realistically flake in CI.
#[test]
fn rand_is_uniform_over_unit_interval() {
    const N: usize = 20_000;
    const BUCKETS: usize = 8;

    let mut hist = [0usize; BUCKETS];
    let mut sum = 0.0f64;
    let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);

    for _ in 0..N {
        let x = random_f64();
        // Hard invariant: the contract is [0, 1), so 1.0 and NaN are bugs.
        assert!(
            (0.0..1.0).contains(&x),
            "rand() escaped [0, 1): {x} (NaN? {})",
            x.is_nan()
        );
        hist[(x * BUCKETS as f64) as usize] += 1;
        sum += x;
        min = min.min(x);
        max = max.max(x);
    }

    // Every octile is hit. This is the assertion the pre-fix code failed:
    // it only ever populated bucket 4 ([0.5, 0.625)).
    for (i, &count) in hist.iter().enumerate() {
        assert!(
                count > 0,
                "octile {i} ([{:.3}, {:.3})) never drawn in {N} samples — rand() is not uniform: {hist:?}",
                i as f64 / BUCKETS as f64,
                (i + 1) as f64 / BUCKETS as f64,
            );
    }

    // The tails are reached, and the mean sits where a uniform mean should.
    // (σ of the mean of N uniforms is 1/√(12N) ≈ 0.002, so ±0.02 is ~10σ.)
    assert!(
        min < 0.05,
        "min draw {min} — low tail unreachable: {hist:?}"
    );
    assert!(
        max > 0.95,
        "max draw {max} — high tail unreachable: {hist:?}"
    );
    let mean = sum / N as f64;
    assert!(
        (0.48..0.52).contains(&mean),
        "mean of {N} draws is {mean}, expected ≈ 0.5: {hist:?}"
    );
}

// ── relationship-type scan: identical results with the posting on vs off ───

/// Run `q` over the sparse-reltype fixture and return the sorted display rows.
/// `postings` toggles the endpoint postings: on ⇒ the planner drives typed
/// first hops from the rel-type posting; off ⇒ the identical graph with no
/// postings, so every query falls back to the label scan.
fn rel_rows(tag: &str, q: &str, postings: bool) -> Vec<String> {
    let (root, graph) = if postings {
        testgen::write_rel_sparse(tag)
    } else {
        testgen::write_rel_sparse_no_postings(tag)
    };
    let gen = Generation::open(&root, &graph).unwrap();
    let cache = BlockCache::new(1 << 20);
    let engine = Engine::new(&gen, &cache);
    let res = parser::parse(q)
        .map_err(|e| e.to_string())
        .and_then(|ast| engine.run(&ast).map_err(|e| e.to_string()))
        .unwrap_or_else(|e| panic!("query failed: {e}\n{q}"));
    let mut rows: Vec<String> = res
        .rows
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| v.to_display())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    let _ = std::fs::remove_dir_all(&root);
    rows
}

#[test]
fn rel_type_scan_matches_label_scan_results() {
    // Every shape the rel-type scan can fire on must return byte-identical rows
    // to the label-scan plan over the same graph. The fixture: 6 :N nodes,
    // T-edges a->b, b->c (sources {a,b}, targets {b,c}), U-edge a->d.
    let cases = [
        // outgoing 1-hop
        "MATCH (a:N)-[:T]->(b) RETURN a.name AS x, b.name AS y",
        // outgoing 1-hop, unlabelled anchor (base AllNodes)
        "MATCH (a)-[:T]->(b) RETURN a.name AS x, b.name AS y",
        // incoming
        "MATCH (a:N)<-[:T]-(b) RETURN a.name AS x, b.name AS y",
        // undirected
        "MATCH (a:N)-[:T]-(b) RETURN a.name AS x, b.name AS y",
        // 2-hop
        "MATCH (a:N)-[:T]->(b)-[:T]->(c) RETURN c.name AS y",
        // with LIMIT (early-exit path)
        "MATCH (a:N)-[:T]->(b) RETURN b.name AS y LIMIT 1",
        // multi-type union
        "MATCH (a:N)-[:T|U]->(b) RETURN a.name AS x, b.name AS y",
        // count (uncapped, parallel-eligible)
        "MATCH (a:N)-[:T]->(b) RETURN count(*) AS n",
        // OPTIONAL with an unbound anchor: edgeless nodes must not change the
        // outcome — both plans yield the same matched set (and the same
        // null-row behaviour, driven by whether anything matched at all).
        "OPTIONAL MATCH (a:N)-[:T]->(b) RETURN a.name AS x, b.name AS y",
    ];
    for (i, q) in cases.iter().enumerate() {
        let on = rel_rows(&format!("exec_relscan_on_{i}"), q, true);
        let off = rel_rows(&format!("exec_relscan_off_{i}"), q, false);
        assert_eq!(on, off, "rel-scan vs label-scan mismatch for: {q}");
    }
}

#[test]
fn rel_type_scan_concrete_rows() {
    // Pin the actual rows (not just on==off), so a bug that breaks *both*
    // plans identically can't hide. T-edges: a->b, b->c.
    let rows = rel_rows(
        "exec_relscan_concrete",
        "MATCH (a:N)-[:T]->(b) RETURN a.name, b.name",
        true,
    );
    assert_eq!(rows, vec!["a|b".to_string(), "b|c".to_string()]);
}
