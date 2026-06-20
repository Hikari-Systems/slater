// SPDX-License-Identifier: Apache-2.0
//! `bench-codec` — measure the real compression trade-off for slater's `.blk`
//! blocks, per file-kind and per backend, so the `--compression-profile` levels
//! can be pinned to the measured knee rather than guessed.
//!
//! It reads a *published generation* (local filesystem or S3) through the exact
//! same positional-read path the query engine uses, samples raw blocks back, and
//! for each candidate zstd level reports:
//!   * **ratio** and average compressed block size,
//!   * **compress** throughput (the one-time, offline build cost),
//!   * **decompress** throughput (confirms zstd decode is ~level-independent — the
//!     whole reason raising the build level is close to free on the read path),
//!   * **GET latency** for that level's compressed byte size, measured with real
//!     positional reads against the backend, and
//!   * **total read time = GET + decompress** per block, with the knee marked.
//!
//! The CPU/ratio leg runs anywhere. The **I/O leg must be run where the backend
//! actually lives**: for S3 that means an EC2 instance *in the bucket's region*,
//! never a laptop over a home connection and never against a MinIO stand-in —
//! either would measure the wrong RTT and push the knee to a bogus level. See the
//! `--no-io` flag to skip the I/O leg on machines where it would be misleading.

use anyhow::{bail, Context, Result};
use clap::Parser;
use graph_format::blockfile::BlockFileReader;
use graph_format::codec;
use graph_format::ids::BlockId;
use graph_format::manifest::Manifest;
use graph_format::store::fs::FsObjectStore;
use graph_format::store::{join_key, ObjectStore, RandomReadAt};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Block files we attempt to bench. Specialised containers (range ISAM, the
/// Vamana/PQ vector index) are skipped if they do not open as a plain block file;
/// skips are reported (never silently dropped).
const CANDIDATE_FILES: &[&str] = &[
    "node_props.blk",
    "node_labels.blk",
    "edge_props.blk",
    "topology.csr.blk",
    "reltype_src.post",
    "reltype_tgt.post",
    "prop_hist.blk",
    "vectors.f32.blk",
];

#[derive(Parser, Debug)]
#[command(
    name = "bench-codec",
    about = "Measure the .blk compression trade-off per backend"
)]
struct Cli {
    /// Graph name (selects `<graph>/<generation>/` under the backend root).
    #[arg(long)]
    graph: String,

    /// Generation UUID. Omit to resolve `<graph>/current` from the backend.
    #[arg(long)]
    generation: Option<String>,

    /// Local data-dir root (filesystem backend). Mutually exclusive with `--s3-bucket`.
    #[arg(long)]
    data_dir: Option<String>,

    /// S3 bucket (S3 backend). Requires the binary be built with `--features s3`.
    #[arg(long)]
    s3_bucket: Option<String>,
    /// S3 region (e.g. `eu-west-2`). Empty ⇒ resolved from the AWS environment.
    #[arg(long, default_value = "")]
    s3_region: String,
    /// Custom S3 endpoint (S3-compatible servers). Empty ⇒ AWS standard endpoint.
    #[arg(long, default_value = "")]
    s3_endpoint: String,
    /// Key prefix for generation keys. May be empty.
    #[arg(long, default_value = "")]
    s3_prefix: String,
    /// Path-style addressing (required by some S3-compatible servers).
    #[arg(long)]
    s3_path_style: bool,

    /// Candidate zstd levels to sweep.
    #[arg(long, value_delimiter = ',', default_value = "1,3,6,9,12,15,19,22")]
    levels: Vec<i32>,

    /// Max blocks sampled per file (spread evenly across the file).
    #[arg(long, default_value_t = 64)]
    blocks_per_file: usize,

    /// Positional-read samples per (file, level) for the I/O leg.
    #[arg(long, default_value_t = 32)]
    io_samples: usize,

    /// Skip the backend I/O leg (CPU/ratio + decompress only). Use this on a
    /// laptop where a remote-S3 GET time would be unrepresentative.
    #[arg(long)]
    no_io: bool,
}

/// Per-(file, level) measurement.
struct Row {
    level: i32,
    ratio: f64,
    avg_comp_bytes: f64,
    compress_mbps: f64,
    decompress_gbps: f64,
    decompress_ms_per_block: f64,
    get_ms: Option<f64>,
    total_ms: Option<f64>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let store = build_store(&cli)?;

    let generation = match &cli.generation {
        Some(g) => g.clone(),
        None => {
            let raw = store
                .read_all(&join_key(&cli.graph, "current"))
                .with_context(|| format!("read {}/current pointer", cli.graph))?;
            String::from_utf8(raw)
                .context("current pointer is not UTF-8")?
                .trim()
                .to_string()
        }
    };
    let base = join_key(&cli.graph, &generation);

    let manifest = Manifest::read_via(store.as_ref(), &base)
        .with_context(|| format!("read MANIFEST for {}/{}", cli.graph, generation))?;
    if manifest.encryption.is_some() {
        bail!("bench-codec supports plaintext generations only (this one is encrypted)");
    }

    println!("# bench-codec");
    println!(
        "# generation: {}/{}  built codec={} level={} profile={:?}",
        cli.graph, generation, manifest.codec, manifest.zstd_level, manifest.compression_profile
    );
    if cli.no_io {
        println!("# I/O leg: SKIPPED (--no-io)");
    } else if cli.s3_bucket.is_some() {
        println!(
            "# I/O leg: S3 — run this in-region on EC2 for valid RTT (NOT a laptop, NOT MinIO)"
        );
    } else {
        println!(
            "# I/O leg: local filesystem (page cache will dominate; for the real curve bench S3)"
        );
    }
    println!();

    let mut skipped: Vec<String> = Vec::new();
    let mut per_file: Vec<(String, u64, Vec<Row>)> = Vec::new();

    for &name in CANDIDATE_FILES {
        let key = join_key(&base, name);
        if !store.exists(&key).unwrap_or(false) {
            continue;
        }
        let src = store.open(&key).with_context(|| format!("open {key}"))?;
        let obj_len = src.len();
        let reader = match BlockFileReader::open_src(src.clone(), None) {
            Ok(r) => r,
            Err(e) => {
                skipped.push(format!("{name} (not a plain block file: {e})"));
                continue;
            }
        };
        let raw_blocks = sample_raw_blocks(&reader, cli.blocks_per_file)?;
        if raw_blocks.is_empty() {
            skipped.push(format!("{name} (no blocks)"));
            continue;
        }
        let raw_total: u64 = raw_blocks.iter().map(|b| b.len() as u64).sum();

        let mut rows = Vec::new();
        for &level in &cli.levels {
            let mut row = measure_cpu(&raw_blocks, raw_total, level)?;
            if !cli.no_io {
                let size = (row.avg_comp_bytes.round() as u64).min(obj_len).max(1);
                let get_ms = measure_get_ms(src.as_ref(), obj_len, size, cli.io_samples)?;
                row.get_ms = Some(get_ms);
                row.total_ms = Some(get_ms + row.decompress_ms_per_block);
            }
            rows.push(row);
        }
        print_table(name, raw_total, raw_blocks.len(), &rows, cli.no_io);
        per_file.push((name.to_string(), raw_total, rows));
    }

    print_aggregate(&per_file, cli.no_io);

    if !skipped.is_empty() {
        println!("\n# skipped files:");
        for s in &skipped {
            println!("#   {s}");
        }
    }
    Ok(())
}

/// Build the backend store from the CLI flags (S3 if `--s3-bucket`, else filesystem).
fn build_store(cli: &Cli) -> Result<Arc<dyn ObjectStore>> {
    if let Some(bucket) = &cli.s3_bucket {
        #[cfg(feature = "s3")]
        {
            let cfg = graph_format::store::s3::S3Config {
                bucket: bucket.clone(),
                region: cli.s3_region.clone(),
                endpoint: (!cli.s3_endpoint.is_empty()).then(|| cli.s3_endpoint.clone()),
                prefix: cli.s3_prefix.clone(),
                path_style: cli.s3_path_style,
            };
            let store = graph_format::store::s3::S3ObjectStore::connect(&cfg)
                .context("connect S3 backend")?;
            return Ok(Arc::new(store));
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = bucket;
            bail!("--s3-bucket requires bench-codec to be built with `--features s3`");
        }
    }
    let root = cli
        .data_dir
        .as_ref()
        .context("one of --data-dir or --s3-bucket is required")?;
    Ok(Arc::new(FsObjectStore::new(root)))
}

/// Read up to `cap` raw (decompressed) blocks, spread evenly across the file.
fn sample_raw_blocks(reader: &BlockFileReader, cap: usize) -> Result<Vec<Vec<u8>>> {
    let n = reader.num_blocks();
    if n == 0 || cap == 0 {
        return Ok(Vec::new());
    }
    let take = cap.min(n);
    let step = (n / take).max(1);
    let mut out = Vec::with_capacity(take);
    let mut i = 0;
    while i < n && out.len() < take {
        out.push(reader.read_block(BlockId(i as u32))?);
        i += step;
    }
    Ok(out)
}

/// Compress/decompress every sampled block at `level`; derive ratio + throughput.
fn measure_cpu(raw_blocks: &[Vec<u8>], raw_total: u64, level: i32) -> Result<Row> {
    let mut comp_blocks = Vec::with_capacity(raw_blocks.len());
    let t0 = Instant::now();
    for raw in raw_blocks {
        comp_blocks.push(codec::compress(raw, level)?);
    }
    let compress_secs = t0.elapsed().as_secs_f64();

    let comp_total: u64 = comp_blocks.iter().map(|c| c.len() as u64).sum();

    let t1 = Instant::now();
    for (comp, raw) in comp_blocks.iter().zip(raw_blocks) {
        let back = codec::decompress(comp, raw.len())?;
        debug_assert_eq!(back.len(), raw.len());
        std::hint::black_box(&back);
    }
    let decompress_secs = t1.elapsed().as_secs_f64();

    let n = raw_blocks.len() as f64;
    let raw_mb = raw_total as f64 / 1e6;
    Ok(Row {
        level,
        ratio: raw_total as f64 / comp_total.max(1) as f64,
        avg_comp_bytes: comp_total as f64 / n,
        compress_mbps: if compress_secs > 0.0 {
            raw_mb / compress_secs
        } else {
            f64::INFINITY
        },
        decompress_gbps: if decompress_secs > 0.0 {
            raw_total as f64 / 1e9 / decompress_secs
        } else {
            f64::INFINITY
        },
        decompress_ms_per_block: decompress_secs * 1e3 / n,
        get_ms: None,
        total_ms: None,
    })
}

/// Median latency of `samples` positional reads of `size` bytes at pseudo-random
/// offsets — a faithful GET cost for a block compressed to `size` on this backend.
fn measure_get_ms(src: &dyn RandomReadAt, obj_len: u64, size: u64, samples: usize) -> Result<f64> {
    let span = obj_len.saturating_sub(size).max(1);
    let mut buf = vec![0u8; size as usize];
    let mut times: Vec<Duration> = Vec::with_capacity(samples);
    // Deterministic LCG so a re-run hits the same offsets (comparable numbers).
    let mut state: u64 = 0x9E3779B97F4A7C15;
    for _ in 0..samples.max(1) {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let offset = state % span;
        let t = Instant::now();
        src.read_exact_at(&mut buf, offset)?;
        times.push(t.elapsed());
        std::hint::black_box(&buf);
    }
    times.sort_unstable();
    Ok(times[times.len() / 2].as_secs_f64() * 1e3)
}

fn print_table(name: &str, raw_total: u64, blocks: usize, rows: &[Row], no_io: bool) {
    println!(
        "## {name}  ({} sampled blocks, {:.2} MiB raw)",
        blocks,
        raw_total as f64 / (1024.0 * 1024.0)
    );
    if no_io {
        println!("  level   ratio   comp/blk   compress    decompress");
        println!("                       KiB      MB/s        GB/s");
    } else {
        println!("  level   ratio   comp/blk   compress    decompress    GET      total");
        println!("                       KiB      MB/s        GB/s         ms       ms/blk");
    }
    let knee = best_total_idx(rows);
    for (i, r) in rows.iter().enumerate() {
        let mark = if Some(i) == knee { " <- knee" } else { "" };
        if no_io {
            println!(
                "  {:>5}  {:>6.2}  {:>8.1}  {:>9.0}  {:>11.2}{}",
                r.level,
                r.ratio,
                r.avg_comp_bytes / 1024.0,
                r.compress_mbps,
                r.decompress_gbps,
                mark
            );
        } else {
            println!(
                "  {:>5}  {:>6.2}  {:>8.1}  {:>9.0}  {:>11.2}  {:>7.3}  {:>7.3}{}",
                r.level,
                r.ratio,
                r.avg_comp_bytes / 1024.0,
                r.compress_mbps,
                r.decompress_gbps,
                r.get_ms.unwrap_or(f64::NAN),
                r.total_ms.unwrap_or(f64::NAN),
                mark
            );
        }
    }
    println!();
}

/// Aggregate the per-file rows weighted by each file's raw bytes, so the knee
/// reflects the whole image, not whichever file happened to be largest.
fn print_aggregate(per_file: &[(String, u64, Vec<Row>)], no_io: bool) {
    if per_file.is_empty() {
        println!("# no block files benched");
        return;
    }
    // Align rows by level position (every file swept the same level list).
    let nlevels = per_file[0].2.len();
    let total_raw: u64 = per_file.iter().map(|(_, w, _)| *w).sum::<u64>().max(1);
    println!(
        "## AGGREGATE (raw-byte weighted across {} files)",
        per_file.len()
    );
    if no_io {
        println!("  level   ratio   decompress GB/s");
    } else {
        println!("  level   ratio   decompress GB/s   total ms/blk");
    }
    let mut agg: Vec<Row> = Vec::new();
    for li in 0..nlevels {
        let level = per_file[0].2[li].level;
        let mut comp_sum = 0.0;
        let mut raw_sum = 0.0;
        let mut dgbps = 0.0;
        let mut total = 0.0;
        let mut has_total = true;
        for (_, w, rows) in per_file {
            let r = &rows[li];
            let weight = *w as f64;
            raw_sum += weight;
            comp_sum += weight / r.ratio.max(1e-9);
            dgbps += r.decompress_gbps * weight / total_raw as f64;
            match r.total_ms {
                Some(t) => total += t * weight / total_raw as f64,
                None => has_total = false,
            }
        }
        let ratio = raw_sum / comp_sum.max(1e-9);
        if no_io {
            println!("  {level:>5}  {ratio:>6.2}  {dgbps:>14.2}");
        } else if has_total {
            println!("  {level:>5}  {ratio:>6.2}  {dgbps:>14.2}  {total:>12.3}");
        }
        agg.push(Row {
            level,
            ratio,
            avg_comp_bytes: 0.0,
            compress_mbps: 0.0,
            decompress_gbps: dgbps,
            decompress_ms_per_block: 0.0,
            get_ms: None,
            total_ms: if has_total { Some(total) } else { None },
        });
    }
    if let Some(k) = best_total_idx(&agg) {
        println!(
            "# recommended level for this backend: {} (lowest total read time/block)",
            agg[k].level
        );
    }
}

/// Index of the row with the lowest `total_ms`; `None` if the I/O leg was skipped.
fn best_total_idx(rows: &[Row]) -> Option<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(i, r)| r.total_ms.map(|t| (i, t)))
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
}
