// SPDX-License-Identifier: Apache-2.0
//! `delta_and_segments` — see the parent module. Split out of the single 12k-line
//! `exec/tests.rs`; a pure relocation, no test logic changed.

use super::*;

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
        builder_max_memory: 0,
        builder_threads: 0,
        consolidate_timeout_secs: 0,
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
                    execute_edge_write(&writer, gen.as_ref(), &w, &HashMap::new(), (5, 4)).unwrap();
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
