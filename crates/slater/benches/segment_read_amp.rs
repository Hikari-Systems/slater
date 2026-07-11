// SPDX-License-Identifier: Apache-2.0
//! Phase 8 read-amp harness for the segmented core.
//!
//! The additive-core thesis (see `docs/SEGMENTED-CORE-PLAN.md`) is that a bounded stack of
//! upper segments makes a routine flush O(delta) *without* inflating read cost: a per-segment
//! presence fence lets an **untouched** id skip the whole stack in resident checks, so an
//! untouched read stays at ~1 block read no matter how deep the stack is, while only a
//! **written** id fans out. This bench measures that directly.
//!
//! It builds the same scaled `scale` graph folded to 0, 2, 4 and 8 upper segments (each
//! segment patching a small disjoint band near the top of the id space, so the base bulk
//! stays untouched) and reports, for four read shapes:
//!   * **point_lookup** — index seek + node row for an untouched name;
//!   * **two_hop** — a 2-hop ring traversal from an untouched anchor;
//!   * **label_scan** — the first 1 000 rows of a `:Person` scan;
//!   * **count** — `count(:Person)`, served from resident marginals.
//!
//! Two outputs:
//!   1. a **read-amp matrix** (cold-cache block misses, base + segment) — deterministic, the
//!      headline artifact: an untouched shape's total should stay flat across the depths;
//!   2. **warm latency** per (shape, depth) via criterion.
//!
//! Run: `cargo bench -p slater --features testkit --bench segment_read_amp`

use std::path::PathBuf;

use criterion::{black_box, BenchmarkId, Criterion, Throughput};

use slater::benchkit::{self, Reader};

/// Node count of the base graph. Large enough that node/label/index/topology each span many
/// 4 KiB blocks, so the read shapes reflect real block-cache traffic, but small enough that
/// building four stacked fixtures stays quick.
const N: u64 = 50_000;

/// Segment depths the harness sweeps.
const DEPTHS: &[usize] = &[0, 2, 4, 8];

struct Shape {
    name: &'static str,
    query: String,
}

fn shapes(n: u64) -> Vec<Shape> {
    // Anchor an untouched node well below every segment's top patched band (top 8*16 = 128
    // nodes), so its point lookup / 2-hop should skip every segment via the fence.
    let anchor = n / 3;
    vec![
        Shape {
            name: "point_lookup",
            query: format!("MATCH (p:Person {{name:'p{anchor:07}'}}) RETURN p.age"),
        },
        Shape {
            name: "two_hop",
            query: format!(
                "MATCH (p:Person {{name:'p{anchor:07}'}})-[:KNOWS]->()-[:KNOWS]->(q) RETURN q.name"
            ),
        },
        Shape {
            name: "label_scan",
            query: "MATCH (p:Person) RETURN p.name LIMIT 1000".to_string(),
        },
        Shape {
            name: "count",
            query: "MATCH (p:Person) RETURN count(p)".to_string(),
        },
    ]
}

/// Print the cold-cache read-amplification matrix: for each read shape and stack depth, the
/// base and segment blocks a single cold run pulled. This is deterministic and is the real
/// KPI — an untouched shape's `total` should be flat across depths.
fn print_read_amp_matrix(fixtures: &[(usize, PathBuf, String)], shapes: &[Shape]) {
    eprintln!("\n=== segmented-core read amplification (cold-cache block misses) ===");
    eprintln!("N = {N} nodes; each segment patches a disjoint 16-node top band.\n");
    eprint!("{:<14}", "shape");
    for (d, _, _) in fixtures {
        eprint!("  seg={d:<18}", d = d);
    }
    eprintln!();
    eprint!("{:<14}", "");
    for _ in fixtures {
        eprint!("  {:<20}", "base+seg=total");
    }
    eprintln!();
    for shape in shapes {
        eprint!("{:<14}", shape.name);
        for (_, root, graph) in fixtures {
            let amp = benchkit::read_amp_cold(root, graph, &shape.query);
            let cell = format!(
                "{}+{}={}",
                amp.base_blocks,
                amp.segment_blocks,
                amp.total_blocks()
            );
            eprint!("  {cell:<20}");
        }
        eprintln!();
    }
    eprintln!();
}

/// Warm-latency benchmark: one criterion group per read shape, one point per stack depth. The
/// reader (and its caches) is opened once per depth and reused across iterations, so the
/// caches are warm and the measurement is steady-state read latency over the folded stack.
fn bench_latency(c: &mut Criterion, fixtures: &[(usize, PathBuf, String)], shapes: &[Shape]) {
    for shape in shapes {
        let mut group = c.benchmark_group(format!("warm_latency/{}", shape.name));
        group.throughput(Throughput::Elements(1));
        for (depth, root, graph) in fixtures {
            let reader = Reader::open(root, graph, 256 << 20);
            // Prime the caches so the very first timed iteration is already warm.
            let _ = reader.run(&shape.query);
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("seg={depth}")),
                depth,
                |b, _| {
                    b.iter(|| black_box(reader.run(&shape.query)));
                },
            );
        }
        group.finish();
    }
}

fn main() {
    // Build one stacked fixture per depth.
    let fixtures: Vec<(usize, PathBuf, String)> = DEPTHS
        .iter()
        .map(|&d| {
            let (root, graph) = benchkit::build_stacked(&format!("readamp_d{d}"), N, d);
            (d, root, graph)
        })
        .collect();

    let shapes = shapes(N);

    // 1) The deterministic read-amp matrix (the headline result).
    print_read_amp_matrix(&fixtures, &shapes);

    // 2) Warm latency via criterion.
    let mut c = Criterion::default().configure_from_args();
    bench_latency(&mut c, &fixtures, &shapes);
    c.final_summary();

    // Tear the fixtures down.
    for (_, root, _) in &fixtures {
        std::fs::remove_dir_all(root).ok();
    }
}
