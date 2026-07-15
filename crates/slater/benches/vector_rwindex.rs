// SPDX-License-Identifier: Apache-2.0
//! RW-index kill-switch A/B benchmark (HIK-120, feature 1).
//!
//! The FreshDiskANN RW-index (`slater::rwindex`, HIK-112) replaces the delta arm of
//! `db.idx.vector.queryNodes` — which used to rebuild a `ResidentMatrix` over the **entire**
//! write delta and brute-force it on *every* query — with an in-memory proximity graph the
//! query walks. `RwIndexConfig::enabled` is the kill switch: off ⇒ every delta arm brute-forces,
//! the same answer at the pre-HIK-112 cost.
//!
//! This bench measures that A/B **end to end through the real CALL path** (parse →
//! `Engine::with_rw_index` → merged top-k), swept over the number of touched delta nodes:
//!
//!   * **OFF** (`enabled=false`) — allocates + normalises the whole delta into a resident
//!     matrix and scans it per query: cost grows ~linearly with the delta.
//!   * **ON**  (`enabled=true`, floors removed) — a graph walk over the RW-index: ~flat.
//!
//! The delta is populated with **born** nodes (`OpResolution::Node(None)`), so no business-key
//! ISAM is needed and the delta can be grown to 50k cheaply under one group commit. The base
//! index is a tiny brute-force `:Doc(embedding)` cosine index (`testgen::write_vector_docs`),
//! held small so the swept delta dominates.
//!
//! Warm cache: criterion reuses one opened `Generation` + `BlockCache` + `RwIndexCache` per
//! point, so the ON index is built once (during warm-up) and every measured iteration is a
//! steady-state query. Release profile (criterion).
//!
//! Run: `cargo bench -p slater --features testkit --bench vector_rwindex`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use graph_format::ids::Value;
use slater::cache::BlockCache;
use slater::delta_writer::DeltaWriter;
use slater::exec::Engine;
use slater::generation::Generation;
use slater::read_view::MergedView;
use slater::rwindex::{RwIndexCache, RwIndexConfig};
use slater::{parser, testgen};
use slater_delta::{OpResolution, WalOp};

const DIM: usize = 768;
const K: usize = 10;
/// Touched-delta-node counts to sweep. OFF should climb ~linearly across these; ON stay flat.
const DELTAS: &[usize] = &[1_000, 5_000, 20_000, 50_000];

struct SplitMix64(u64);
impl SplitMix64 {
    fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let unit = (z >> 40) as f32 / (1u32 << 24) as f32;
        unit * 2.0 - 1.0
    }
}
fn vec_of(rng: &mut SplitMix64) -> Vec<f32> {
    (0..DIM).map(|_| rng.next_f32()).collect()
}

/// The floors removed so the tiny-relative-to-production sweep actually takes the index path
/// rather than silently brute-forcing under `minVectors` (mirrors exec's `rw_cfg_no_floor`).
fn cfg_on() -> RwIndexConfig {
    RwIndexConfig {
        enabled: true,
        min_vectors: 0,
        max_vectors: 1 << 20,
    }
}
fn cfg_off() -> RwIndexConfig {
    RwIndexConfig {
        enabled: false,
        ..cfg_on()
    }
}

/// A live fixture at one delta size: an opened core generation, a WAL-backed writer whose delta
/// carries `n` born vectors, the query AST (a fixed random query vector), and the RW-index pool.
struct Fixture {
    root: std::path::PathBuf,
    gen: Generation,
    writer: DeltaWriter,
    query: parser::ast::Query,
    graph: String,
}

fn build_fixture(n: usize) -> Fixture {
    let mut rng = SplitMix64(0x5EED_0000 ^ n as u64);
    // A small base so the swept delta is the dominant cost (8 base vectors).
    let base: Vec<Vec<f32>> = (0..8).map(|_| vec_of(&mut rng)).collect();
    let (root, graph) = testgen::write_vector_docs(&format!("rwbench_{n}"), &base);
    let gen = Generation::open(&root, &graph).unwrap();

    let wal = root.join("_wal_rw");
    let _ = std::fs::remove_dir_all(&wal);
    let writer = DeltaWriter::open(
        &wal,
        &graph,
        gen.uuid(),
        gen.node_count(),
        gen.edge_count(),
        // Fresh WAL → replay is empty; born-node resolution besides.
        |_: &WalOp| OpResolution::Node(None),
    )
    .unwrap();

    // Populate the delta with `n` born :Doc nodes carrying a fresh embedding, under one group
    // commit (one fsync for the whole batch).
    let ops: Vec<(WalOp, OpResolution)> = (0..n)
        .map(|i| {
            (
                WalOp::UpsertNode {
                    label: "Doc".into(),
                    key: "name".into(),
                    value: Value::Str(format!("z{i:07}")),
                    patches: vec![("embedding".into(), Value::Vector(vec_of(&mut rng)))],
                },
                OpResolution::Node(None),
            )
        })
        .collect();
    writer.write_batch(&ops).unwrap();

    // A fixed random query vector, baked into the CALL once (parsed once, reused every iter).
    let qv = vec_of(&mut rng);
    let lits = qv
        .iter()
        .map(|x| format!("{x:.6}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = parser::parse(&format!(
        "CALL db.idx.vector.queryNodes('Doc', 'embedding', {K}, vecf32([{lits}])) \
         YIELD node RETURN id(node) AS id"
    ))
    .unwrap();

    Fixture {
        root,
        gen,
        writer,
        query,
        graph,
    }
}

/// Run the KNN once through the merged view with the RW-index arm at `cfg`, returning the row
/// count (a cheap `black_box` observable).
fn run_once(fx: &Fixture, cache: &BlockCache, rw: &RwIndexCache, cfg: RwIndexConfig) -> usize {
    let published = fx.writer.delta_snapshot_at();
    let epoch = published.epoch;
    let view = MergedView::new(&fx.gen, published.delta);
    let engine =
        Engine::new(&view, cache).with_rw_index(rw, fx.writer.touched_journal(), epoch, cfg);
    engine.run(&fx.query).unwrap().rows.len()
}

fn bench_rwindex(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_rwindex/queryNodes/cosine/dim768/k10");
    group.sample_size(10); // a 50k ON build during warm-up is ~100 s; keep the sample count low.

    for &n in DELTAS {
        let fx = build_fixture(n);
        let cache = BlockCache::new(256 << 20);

        // ON: a dedicated pool so warm-up builds the index once and every measured iter reuses it.
        let rw_on = RwIndexCache::new();
        // Prime + assert the index actually served (not a vacuous brute-force fallback).
        let _ = run_once(&fx, &cache, &rw_on, cfg_on());
        let served_epoch = rw_on.index_epoch(fx.gen.uuid(), "Doc", "embedding");
        let want_epoch = fx.writer.delta_snapshot_at().epoch;
        assert_eq!(
            served_epoch,
            Some(want_epoch),
            "delta={n}: the RW-index must serve (index_epoch == query epoch), else the ON arm is \
             a vacuous brute-force"
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("on", n), &n, |b, _| {
            b.iter(|| criterion::black_box(run_once(&fx, &cache, &rw_on, cfg_on())));
        });

        // OFF: the kill switch never builds anything; each query brute-forces the whole delta.
        let rw_off = RwIndexCache::new();
        group.bench_with_input(BenchmarkId::new("off", n), &n, |b, _| {
            b.iter(|| criterion::black_box(run_once(&fx, &cache, &rw_off, cfg_off())));
        });

        // Tear the on-disk fixture down before building the next (larger) size.
        let root = fx.root.clone();
        let _graph = fx.graph.clone();
        drop(fx);
        std::fs::remove_dir_all(&root).ok();
    }
    group.finish();
}

criterion_group!(benches, bench_rwindex);
criterion_main!(benches);
