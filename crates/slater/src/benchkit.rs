// SPDX-License-Identifier: Apache-2.0
//! Phase 8 read-amp harness support — a scaled base fixture, a stacked-set builder, and
//! a cold-cache reader that reports **read amplification** (the number of core blocks a
//! single read pulls) split into its base and segment-stack halves.
//!
//! The segmented-core thesis is that a bounded upper-segment stack keeps a routine flush
//! O(delta) *without* inflating read cost: a per-segment presence fence lets an untouched
//! id skip the whole stack in resident checks, so an untouched read stays at ~1 block read
//! regardless of stack depth, while only a written id fans out. This module is the
//! measurement rig for that claim — the [`segment_read_amp`](../../benches/segment_read_amp.rs)
//! bench builds fixtures at 0/2/4/8 segments and prints the read-amp of four read shapes
//! (point lookup, 2-hop, label scan, count) against depth.
//!
//! Gated `pub` under `testkit` (like [`crate::testgen`]) so it never ships in a normal build.

#![cfg(any(test, feature = "testkit"))]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use graph_format::columns::PropsWriter;
use graph_format::ids::{EdgeId, Generation as GenId, NodeId, Value};
use graph_format::integrity::hash_file;
use graph_format::isam::write_isam;
use graph_format::manifest::{EntityKind, FileEntry, Manifest, RangeIndexDesc};
use graph_format::nodelabels::NodeLabelsWriter;
use graph_format::store::mem::MemObjectStore;
use graph_format::store::ObjectStore;
use graph_format::topology::{write_csr, Edge};
use graph_format::vectors::VectorStoreWriter;
use graph_format::{FORMAT_VERSION, MAGIC};

use crate::cache::{BlockCache, VectorIndexCache};
use crate::config::DeltaConfig;
use crate::exec::Engine;
use crate::generation::Generation;
use crate::parser;
use crate::read_view::MergedView;
use crate::server::{execute_write, Graphs};
use crate::testgen::fixture_summaries;

const BLOCK: usize = 4096;
const LEVEL: i32 = 3;

/// Nodes each flushed segment patches — a small, contiguous, disjoint band per segment near
/// the top of the id space. Kept small so a segment stays cheap (O(delta)) and its name-fence
/// stays narrow: an untouched lookup below the band falls outside every segment's fence.
const SEG_WRITES: u64 = 16;

/// Build a scaled single-generation `scale` graph of `n` :Person nodes under a fresh temp
/// root, and publish its `current` pointer. Each node `i` carries `name = "p{i:07}"` (a
/// zero-padded business key that sorts in id order) and `age = i % 100`; a `:KNOWS` ring
/// `i -> (i+1) % n` gives every node out-degree 1 (so a 2-hop reaches `i+2`); a range index
/// on `(Person, name)` backs the point lookup. All summary vectors are re-derived from the
/// written stores via [`fixture_summaries`], so the resident count fast paths are exact.
///
/// Returns `(data_dir, graph)`. `n` must be ≥ 2.
pub fn write_scale(tag: &str, n: u64) -> (PathBuf, String) {
    assert!(n >= 2, "write_scale needs at least two nodes");
    let uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_0080);
    let graph = "scale".to_string();
    let root = std::env::temp_dir().join(format!("slater_scalefix_{}_{tag}", std::process::id()));
    let dir = root.join(&graph).join(uuid.to_string());
    std::fs::create_dir_all(dir.join("range")).unwrap();

    // node_props.blk — name(0) + age(1) on every node.
    let mut np = PropsWriter::create(dir.join("node_props.blk"), BLOCK, LEVEL).unwrap();
    for i in 0..n {
        np.append(&[
            (0, Value::Str(format!("p{i:07}"))),
            (1, Value::Int((i % 100) as i64)),
        ])
        .unwrap();
    }
    np.finish().unwrap();

    // node_labels.blk — all :Person(0).
    let mut nl = NodeLabelsWriter::create(dir.join("node_labels.blk"), BLOCK, LEVEL).unwrap();
    for _ in 0..n {
        nl.append(&[0]).unwrap();
    }
    nl.finish().unwrap();

    // edge_props.blk — the ring edges carry no properties.
    PropsWriter::create(dir.join("edge_props.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // topology.csr.blk — the :KNOWS ring (edges already ascending by src).
    let edges: Vec<Edge> = (0..n)
        .map(|i| Edge {
            src: NodeId(i),
            dst: NodeId((i + 1) % n),
            reltype: 0,
            edge: EdgeId(i),
        })
        .collect();
    write_csr(dir.join("topology.csr.blk"), n, &edges, BLOCK, LEVEL).unwrap();

    // vectors.f32.blk — empty (no vector index), but the reader always opens it.
    VectorStoreWriter::create(dir.join("vectors.f32.blk"), BLOCK, LEVEL)
        .unwrap()
        .finish()
        .unwrap();

    // range index on (Person, name); zero-padded names sort in id order.
    let idx: Vec<(Value, u64)> = (0..n)
        .map(|i| (Value::Str(format!("p{i:07}")), i))
        .collect();
    write_isam(
        dir.join("range").join("node_Person_name.isam"),
        idx,
        BLOCK,
        LEVEL,
    )
    .unwrap();

    // Summary vectors re-derived from the written stores (1 label, 1 reltype).
    let s = fixture_summaries(&dir, 1, 1);

    // Inventory + manifest.
    let mut block_sizes = BTreeMap::new();
    let mut files = Vec::new();
    let add = |name: &str, files: &mut Vec<FileEntry>, bs: &mut BTreeMap<String, u32>| {
        let path = dir.join(name);
        let bytes = std::fs::metadata(&path).unwrap().len();
        files.push(FileEntry {
            name: name.to_string(),
            bytes,
            blake3: hash_file(&path).unwrap(),
            sha256: None,
            crc32c: None,
        });
        bs.insert(name.to_string(), BLOCK as u32);
    };
    for name in [
        "node_props.blk",
        "node_labels.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
        "range/node_Person_name.isam",
    ] {
        add(name, &mut files, &mut block_sizes);
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    let inv: Vec<(String, String)> = files
        .iter()
        .map(|f| (f.name.clone(), f.blake3.clone()))
        .collect();
    let content_hash = graph_format::integrity::content_hash(&inv);

    let manifest = Manifest {
        magic: String::from_utf8(MAGIC.to_vec()).unwrap(),
        format_version: FORMAT_VERSION,
        build_uuid: GenId(uuid),
        graph: graph.clone(),
        created_unix: 1_700_000_000,
        content_hash,
        block_sizes,
        codec: "zstd".into(),
        zstd_level: LEVEL,
        compression_profile: String::new(),
        encryption: None,
        node_count: n,
        edge_count: n,
        labels: vec!["Person".into()],
        reltypes: vec!["KNOWS".into()],
        property_keys: vec!["name".into(), "age".into()],
        range_indexes: vec![RangeIndexDesc {
            name: "node_Person_name".into(),
            entity: EntityKind::Node,
            label_or_type: "Person".into(),
            property: "name".into(),
        }],
        vector_indexes: vec![],
        reltype_source_counts: vec![],
        reltype_target_counts: vec![],
        reltype_edge_counts: s.reltype_edge_counts,
        reltype_self_loop_counts: s.reltype_self_loop_counts,
        label_node_counts: s.label_node_counts,
        first_label_counts: s.first_label_counts,
        src_label_reltype_counts: s.src_label_reltype_counts,
        reltype_tgt_label_counts: s.reltype_tgt_label_counts,
        schema_triple_counts: s.schema_triple_counts,
        property_histograms: vec![],
        acl_blake3: None,
        mac: None,
        files,
    };
    manifest.write_to_dir(&dir).unwrap();

    std::fs::write(
        root.join(&graph).join("current"),
        format!("{}\n", uuid.hyphenated()),
    )
    .unwrap();

    (root, graph)
}

fn bench_delta_cfg(wal_dir: &Path) -> DeltaConfig {
    DeltaConfig {
        enabled: true,
        wal_dir: wal_dir.to_string_lossy().into_owned(),
        memtable_bytes: 64 << 20,
        // Leave every auto-trigger off — the harness flushes segments explicitly, one per
        // round, so it controls the exact stack depth it measures.
        l0_compaction_trigger: 0,
        segment_flush_bytes: 0,
        max_upper_segments: 0,
        delta_core_percent: 0,
        delta_hard_bytes: 0,
        consolidate_window: String::new(),
        builder_bin: "slater-build".to_string(),
        off_heap_l0: false,
        segment_gc_grace_secs: 0,
    }
}

/// Build [`write_scale`] and then fold exactly `segments` upper core segments over it — each
/// the O(delta) product of patching one small, contiguous, disjoint band of node names near
/// the top of the id space. The vast base stays untouched, so a read anchored below the
/// patched bands exercises the presence-fence skip at increasing stack depth.
///
/// Returns `(data_dir, graph)`; the served set carries `segments` upper segments and an empty
/// write-delta (each round's writes are flushed out). `segments == 0` returns the bare base.
pub fn build_stacked(tag: &str, n: u64, segments: usize) -> (PathBuf, String) {
    let (root, graph) = write_scale(tag, n);
    if segments == 0 {
        return (root, graph);
    }
    assert!(
        n >= SEG_WRITES * segments as u64,
        "need room for {segments} disjoint {SEG_WRITES}-node write bands in {n} nodes"
    );

    let wal = root.join("_wal");
    let vc = VectorIndexCache::new(1 << 20);
    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&bench_delta_cfg(&wal), &root, None)
        .unwrap();

    for s in 0..segments {
        {
            let gen = graphs.get(&graph).unwrap();
            let writer = graphs.writer(&graph).unwrap();
            let base_off = n - (segments as u64) * SEG_WRITES + (s as u64) * SEG_WRITES;
            for k in 0..SEG_WRITES {
                let id = base_off + k;
                let q = format!(
                    "MATCH (p:Person {{name:'p{id:07}'}}) SET p.age = {}",
                    900 + s
                );
                match parser::parse_statement(&q).unwrap() {
                    parser::ast::Statement::Write(w) => {
                        execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                    }
                    other => panic!("expected a write statement, got {other:?}"),
                }
            }
        }
        graphs
            .flush_graph_to_segment(&graph, &vc, &root)
            .unwrap()
            .expect("a non-empty delta flushes to a segment");
    }

    let gen = graphs.get(&graph).unwrap();
    assert_eq!(
        gen.stack().segments().len(),
        segments,
        "expected exactly {segments} upper segments"
    );
    (root, graph)
}

/// A read amplification sample: the base-side and segment-side block-miss counts a single
/// read drove, and how long it took. `total_blocks` is the read amplification proper.
#[derive(Debug, Clone, Copy)]
pub struct ReadAmp {
    /// Base-generation blocks decompressed to answer the read.
    pub base_blocks: u64,
    /// Upper-segment blocks decompressed to answer the read (0 on a singleton set).
    pub segment_blocks: u64,
    /// Wall time of the single cold run.
    pub elapsed: Duration,
}

impl ReadAmp {
    /// The read amplification: total core blocks pulled (base + segment).
    pub fn total_blocks(&self) -> u64 {
        self.base_blocks + self.segment_blocks
    }
}

/// A reusable reader over a fixed core stack: an owned [`Generation`] (its segment stack has
/// its own block cache) plus a base [`BlockCache`]. Opening is **cold**; repeated [`run`]s
/// warm both caches, so a criterion loop measures warm latency and a fresh `open` measures
/// cold read amplification via [`base_misses`]/[`segment_misses`].
///
/// [`run`]: Reader::run
/// [`base_misses`]: Reader::base_misses
/// [`segment_misses`]: Reader::segment_misses
pub struct Reader {
    gen: Generation,
    cache: BlockCache,
}

impl Reader {
    /// Cold-open `root/graph` with a base block cache of `cache_bytes` (size it past the
    /// working set so a single read never evicts — then a miss count is exactly the distinct
    /// blocks touched).
    pub fn open(root: &Path, graph: &str, cache_bytes: usize) -> Self {
        let gen = Generation::open(root, graph).unwrap();
        Self {
            gen,
            cache: BlockCache::new(cache_bytes),
        }
    }

    /// Execute `query` as a read over the core stack (empty write-delta) and return the row
    /// count (a cheap observable for `black_box`). Panics on a parse/exec error.
    pub fn run(&self, query: &str) -> usize {
        let ast = parser::parse(query).unwrap();
        let view = MergedView::new(&self.gen, slater_delta::DeltaSnapshot::empty());
        // Bind the owned result so the `Engine`/`view` borrows drop before the return.
        let result = Engine::new(&view, &self.cache).run(&ast).unwrap();
        result.rows.len()
    }

    /// Cumulative base-cache block misses so far.
    pub fn base_misses(&self) -> u64 {
        self.cache.metrics().misses
    }

    /// Cold-open `graph` over an arbitrary `ObjectStore` (S3/GCS/in-memory) with a base block
    /// cache of `cache_bytes`. The read path is backend-agnostic: a base or segment block miss
    /// fetches through the store, so the miss counts are identical to the fs reader's — only
    /// the per-block latency differs (see [`read_amp_cold_store`]).
    pub fn open_store(store: &dyn ObjectStore, graph: &str, cache_bytes: usize) -> Self {
        let gen = Generation::open_with_store(store, graph, None).unwrap();
        Self {
            gen,
            cache: BlockCache::new(cache_bytes),
        }
    }

    /// Cumulative segment-stack block misses so far.
    pub fn segment_misses(&self) -> u64 {
        self.gen.stack().cache_metrics().misses
    }
}

/// Cold-run `query` once over `root/graph` and report its read amplification. The reader is
/// opened fresh (cold caches); the miss deltas across the single run are the base and segment
/// blocks the read pulled at this stack depth.
pub fn read_amp_cold(root: &Path, graph: &str, query: &str) -> ReadAmp {
    let r = Reader::open(root, graph, 256 << 20);
    // Snapshot after open so the resident-column loads a segment open performs are excluded —
    // we measure only what the query itself pulls.
    let (b0, s0) = (r.base_misses(), r.segment_misses());
    let t = Instant::now();
    let _ = r.run(query);
    let elapsed = t.elapsed();
    ReadAmp {
        base_blocks: r.base_misses() - b0,
        segment_blocks: r.segment_misses() - s0,
        elapsed,
    }
}

/// Recursively mirror an on-disk data directory into `store` under store-relative keys — the
/// benchkit analogue of the server's test loader, so a fs-built stacked set can be served
/// through an [`ObjectStore`] byte-for-byte.
fn load_dir_into_store(store: &dyn ObjectStore, root: &Path, dir: &Path) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            load_dir_into_store(store, root, &path);
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

/// Build [`build_stacked`] on the local fs, mirror the whole stacked set (base + segments +
/// set manifests + `current`) into a fresh in-memory [`ObjectStore`], tear the fs copy down,
/// and return the store. Reading it back through [`Reader::open_store`] exercises the
/// backend-agnostic read path; because the store image is byte-identical to the fs one, its
/// read-amp block-miss counts match the fs reader's exactly (asserted in the tests).
///
/// A real-S3 read-amp/latency run (the EC2, in-region exercise — never the laptop, never
/// MinIO) points the same [`Reader::open_store`] at an S3-backed `ObjectStore`; the block-miss
/// read-amp is backend-invariant, so this in-memory parity is what a laptop can verify, and
/// only the per-block wall-clock changes on real S3.
pub fn build_stacked_store(tag: &str, n: u64, segments: usize) -> (Arc<MemObjectStore>, String) {
    let (root, graph) = build_stacked(tag, n, segments);
    let store = Arc::new(MemObjectStore::new());
    load_dir_into_store(store.as_ref(), &root, &root);
    std::fs::remove_dir_all(&root).ok();
    (store, graph)
}

/// [`read_amp_cold`] over an [`ObjectStore`]-backed graph — the S3/in-memory read path. Same
/// metric (base + segment block misses across one cold run), served through the store.
pub fn read_amp_cold_store(store: &dyn ObjectStore, graph: &str, query: &str) -> ReadAmp {
    let r = Reader::open_store(store, graph, 256 << 20);
    let (b0, s0) = (r.base_misses(), r.segment_misses());
    let t = Instant::now();
    let _ = r.run(query);
    let elapsed = t.elapsed();
    ReadAmp {
        base_blocks: r.base_misses() - b0,
        segment_blocks: r.segment_misses() - s0,
        elapsed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scaled fixture opens, resolves its business-key index (point lookup), traverses
    /// the ring (2-hop), and counts its label — the four read shapes the harness measures.
    #[test]
    fn write_scale_answers_the_four_shapes() {
        let (root, graph) = write_scale("bk_scale", 2_000);
        let r = Reader::open(&root, &graph, 32 << 20);

        // point lookup: node 700's age is 700 % 100 == 0.
        let ast = parser::parse("MATCH (p:Person {name:'p0000700'}) RETURN p.age").unwrap();
        let view = MergedView::new(&r.gen, slater_delta::DeltaSnapshot::empty());
        let rows = Engine::new(&view, &r.cache).run(&ast).unwrap().rows;
        assert_eq!(rows.len(), 1);

        // 2-hop from 700 along the ring reaches 702.
        let ast = parser::parse(
            "MATCH (p:Person {name:'p0000700'})-[:KNOWS]->()-[:KNOWS]->(q) RETURN q.name",
        )
        .unwrap();
        let view = MergedView::new(&r.gen, slater_delta::DeltaSnapshot::empty());
        let rows = Engine::new(&view, &r.cache).run(&ast).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0][0], crate::exec::Val::Str(s) if s == "p0000702"));

        // count over the label.
        let ast = parser::parse("MATCH (p:Person) RETURN count(p)").unwrap();
        let view = MergedView::new(&r.gen, slater_delta::DeltaSnapshot::empty());
        let rows = Engine::new(&view, &r.cache).run(&ast).unwrap().rows;
        assert!(matches!(&rows[0][0], crate::exec::Val::Int(2_000)));

        std::fs::remove_dir_all(&root).ok();
    }

    /// Folding N segments over the base leaves an N-deep stack, and a read anchored on an
    /// **untouched** node below the patched bands pulls the *same* number of core blocks at
    /// depth 0 and depth 4 — the presence fence keeps an untouched read flat as the stack grows.
    #[test]
    fn untouched_read_amp_is_flat_across_stack_depth() {
        let n = 4_000;
        // Anchor well below the top patched bands (top 4*16 = 64 nodes).
        let q = "MATCH (p:Person {name:'p0001000'}) RETURN p.age";

        let (root0, g0) = build_stacked("bk_flat0", n, 0);
        let amp0 = read_amp_cold(&root0, &g0, q);
        assert_eq!(
            amp0.segment_blocks, 0,
            "a singleton set reads no segment blocks"
        );

        let (root4, g4) = build_stacked("bk_flat4", n, 4);
        let amp4 = read_amp_cold(&root4, &g4, q);

        assert_eq!(
            amp4.base_blocks, amp0.base_blocks,
            "an untouched point lookup pulls the same base blocks at depth 4 as at depth 0"
        );
        assert_eq!(
            amp4.segment_blocks, 0,
            "the presence fence skips every upper segment for an untouched id (no segment block reads)"
        );

        std::fs::remove_dir_all(&root0).ok();
        std::fs::remove_dir_all(&root4).ok();
    }

    /// The label-scan membership gate, measured in isolation: `build_stacked` patches only `age`
    /// (a property), so every segment's `label_membership_touch` is empty and `fold_label_scan`
    /// skips the whole stack — **zero** segment block reads to fold a `:Person` scan at depth 4,
    /// versus one node block per segment without the gate. (A full query that also *materialises*
    /// a segment-resident property still reads those rows for output — the gate zeroes the
    /// membership fold, not the property read; the win is realised for a scan of an untouched
    /// label or a fold-only consumer.)
    #[test]
    fn label_scan_membership_gate_reads_no_segment_blocks() {
        let n = 4_000;
        let (root, graph) = build_stacked("bk_foldgate", n, 4);
        let gen = Generation::open(&root, &graph).unwrap();
        let stack = gen.stack();
        assert_eq!(stack.segments().len(), 4);
        // Every segment is a pure age patch → authoritative empty touch set.
        assert!(stack
            .segments()
            .iter()
            .all(|s| s.manifest.label_membership_touch.as_deref() == Some(&[])));

        let before = stack.cache_metrics().misses;
        let mut ids: Vec<u64> = (0..n).collect();
        stack.fold_label_scan(&mut ids, "Person").unwrap();
        let after = stack.cache_metrics().misses;

        assert_eq!(
            after - before,
            0,
            "membership-preserving segments contribute no fold block reads (gate skips the stack)"
        );
        // The fold left the id set exactly the base scan — no membership changed.
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len() as u64, n);

        std::fs::remove_dir_all(&root).ok();
    }

    /// The safety-critical direction of the gate: a segment that **borns a new `:Person`**
    /// changes Person membership, so its touch set lists Person and `fold_label_scan` must fold
    /// it (never skip) — the born id appears in the scan. Guards against a touch set that
    /// wrongly omits a changed label.
    #[test]
    fn label_scan_gate_folds_a_membership_changing_segment() {
        let n = 2_000;
        let (root, graph) = write_scale("bk_bornlabel", n);
        let wal = root.join("_wal");
        let vc = VectorIndexCache::new(1 << 20);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs
            .enable_writable_layer(&bench_delta_cfg(&wal), &root, None)
            .unwrap();
        {
            let gen = graphs.get(&graph).unwrap();
            let writer = graphs.writer(&graph).unwrap();
            let q = "MERGE (p:Person {name:'zznew'}) SET p.age = 7";
            match parser::parse_statement(q).unwrap() {
                parser::ast::Statement::Write(w) => {
                    execute_write(&writer, gen.as_ref(), &w, &HashMap::new()).unwrap();
                }
                _ => unreachable!(),
            }
        }
        graphs
            .flush_graph_to_segment(&graph, &vc, &root)
            .unwrap()
            .expect("flush");

        let gen = graphs.get(&graph).unwrap();
        let seg = &gen.stack().segments()[0];
        assert_eq!(
            seg.manifest.label_membership_touch.as_deref(),
            Some(&["Person".to_string()][..]),
            "a born :Person makes the segment touch Person membership"
        );

        // The fold must NOT skip: the born id (synthetic id == base node count) joins the scan.
        let mut ids: Vec<u64> = (0..n).collect();
        gen.stack().fold_label_scan(&mut ids, "Person").unwrap();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            ids.len() as u64,
            n + 1,
            "the born :Person is folded into the scan"
        );
        assert!(ids.contains(&n), "born id {n} present in the scan");

        std::fs::remove_dir_all(&root).ok();
    }

    /// Read amplification is **backend-invariant**: serving the same stacked set through an
    /// in-memory `ObjectStore` pulls the identical base + segment block counts as the fs
    /// reader for every read shape. This is the parity a laptop can verify; a real-S3 run adds
    /// only per-block latency (EC2, in-region) — the block counts are these.
    #[test]
    fn read_amp_parity_fs_vs_object_store() {
        let n = 4_000;
        let anchor = n / 3;
        let queries = [
            format!("MATCH (p:Person {{name:'p{anchor:07}'}}) RETURN p.age"),
            format!(
                "MATCH (p:Person {{name:'p{anchor:07}'}})-[:KNOWS]->()-[:KNOWS]->(q) RETURN q.name"
            ),
            "MATCH (p:Person) RETURN p.name LIMIT 500".to_string(),
            "MATCH (p:Person) RETURN count(p)".to_string(),
        ];

        // fs reader over a depth-4 stack.
        let (root, graph) = build_stacked("bk_parity_fs", n, 4);
        let fs_amp: Vec<ReadAmp> = queries
            .iter()
            .map(|q| read_amp_cold(&root, &graph, q))
            .collect();

        // The identical stacked set served through an in-memory object store.
        let store = Arc::new(MemObjectStore::new());
        load_dir_into_store(store.as_ref(), &root, &root);
        let store_amp: Vec<ReadAmp> = queries
            .iter()
            .map(|q| read_amp_cold_store(store.as_ref(), &graph, q))
            .collect();

        for (i, (fs, st)) in fs_amp.iter().zip(store_amp.iter()).enumerate() {
            assert_eq!(
                (fs.base_blocks, fs.segment_blocks),
                (st.base_blocks, st.segment_blocks),
                "shape {i}: object-store read-amp must equal fs read-amp \
                 (fs base+seg {}+{}, store {}+{})",
                fs.base_blocks,
                fs.segment_blocks,
                st.base_blocks,
                st.segment_blocks
            );
        }

        std::fs::remove_dir_all(&root).ok();
    }
}
