// SPDX-License-Identifier: Apache-2.0
//! Codec CPU/ratio micro-benchmark: zstd levels vs LZ4.
//!
//! Answers the standing question "is a faster-decode / lower-ratio codec (LZ4)
//! ever a win over zstd for slater's blocks?" and confirms zstd's decode speed is
//! ~level-independent (so raising the *build* level shrinks read I/O for free).
//!
//! Payloads are representative `.blk` block shapes by default; set
//! `SLATER_BENCH_GEN=/path/to/<graph>/<generation>` to bench real decompressed
//! blocks from a published generation instead.
//!
//! Compression *ratio* (not a timing) is printed to stderr at startup, since the
//! "read less" half of the trade-off is about size, not speed.

use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graph_format::blockfile::BlockFileReader;
use graph_format::codec;
use graph_format::ids::BlockId;

const ZSTD_LEVELS: &[i32] = &[1, 3, 9, 19];

/// Representative ~256 KiB block payloads, one per `.blk` data shape.
fn synthetic_payloads() -> Vec<(String, Vec<u8>)> {
    let target = 256 * 1024;

    // Property-like: short repeated keys + small values — compresses very well.
    let mut props = Vec::with_capacity(target);
    let row = b"name\x00alice\x01age\x0042\x01city\x00london\x01\x02";
    while props.len() < target {
        props.extend_from_slice(row);
    }
    props.truncate(target);

    // Delta-ish topology: monotonically increasing u32 ids (CSR/postings shape).
    let mut topo = Vec::with_capacity(target);
    let mut v: u32 = 0;
    while topo.len() < target {
        v = v.wrapping_add(1 + (v & 7));
        topo.extend_from_slice(&v.to_le_bytes());
    }
    topo.truncate(target);

    // Vector-like: f32 values with a deterministic spread — the hard case for a
    // general-purpose compressor (near-incompressible high-entropy mantissas).
    let mut vecs = Vec::with_capacity(target);
    let mut s: u64 = 0x1234_5678_9abc_def0;
    while vecs.len() < target {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let f = (s >> 40) as f32 / 16_777_216.0;
        vecs.extend_from_slice(&f.to_le_bytes());
    }
    vecs.truncate(target);

    vec![
        ("props".into(), props),
        ("topology".into(), topo),
        ("vectors".into(), vecs),
    ]
}

/// First block of each `.blk` under `SLATER_BENCH_GEN`, decompressed to raw.
fn real_payloads() -> Option<Vec<(String, Vec<u8>)>> {
    let dir = PathBuf::from(std::env::var("SLATER_BENCH_GEN").ok()?);
    let mut out = Vec::new();
    for name in [
        "node_props.blk",
        "edge_props.blk",
        "topology.csr.blk",
        "vectors.f32.blk",
    ] {
        let path = dir.join(name);
        if !path.exists() {
            continue;
        }
        if let Ok(reader) = BlockFileReader::open(&path) {
            if reader.num_blocks() > 0 {
                if let Ok(raw) = reader.read_block(BlockId(0)) {
                    out.push((name.trim_end_matches(".blk").to_string(), raw));
                }
            }
        }
    }
    (!out.is_empty()).then_some(out)
}

fn bench(c: &mut Criterion) {
    let payloads = real_payloads().unwrap_or_else(synthetic_payloads);

    eprintln!("\n=== compression ratio (raw / compressed) ===");
    for (name, raw) in &payloads {
        let mut line = format!("{name:>10}:");
        for &lvl in ZSTD_LEVELS {
            let r = raw.len() as f64 / codec::compress(raw, lvl).unwrap().len().max(1) as f64;
            line.push_str(&format!("  zstd{lvl}={r:.2}"));
        }
        let lz = lz4_flex::compress_prepend_size(raw);
        line.push_str(&format!(
            "  lz4={:.2}",
            raw.len() as f64 / lz.len().max(1) as f64
        ));
        eprintln!("{line}");
    }
    eprintln!();

    for (name, raw) in &payloads {
        // --- decompress (the hot read-path metric) ---
        let mut g = c.benchmark_group(format!("decompress/{name}"));
        g.throughput(Throughput::Bytes(raw.len() as u64));
        for &lvl in ZSTD_LEVELS {
            let comp = codec::compress(raw, lvl).unwrap();
            g.bench_with_input(BenchmarkId::new("zstd", lvl), &comp, |b, comp| {
                b.iter(|| codec::decompress(comp, raw.len()).unwrap());
            });
        }
        let lz = lz4_flex::compress_prepend_size(raw);
        g.bench_with_input(BenchmarkId::new("lz4", 0), &lz, |b, lz| {
            b.iter(|| lz4_flex::decompress_size_prepended(lz).unwrap());
        });
        g.finish();

        // --- compress (the one-time, offline build cost) ---
        let mut g = c.benchmark_group(format!("compress/{name}"));
        g.throughput(Throughput::Bytes(raw.len() as u64));
        for &lvl in ZSTD_LEVELS {
            g.bench_with_input(BenchmarkId::new("zstd", lvl), raw, |b, raw| {
                b.iter(|| codec::compress(raw, lvl).unwrap());
            });
        }
        g.bench_with_input(BenchmarkId::new("lz4", 0), raw, |b, raw| {
            b.iter(|| lz4_flex::compress_prepend_size(raw));
        });
        g.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
