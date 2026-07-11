// SPDX-License-Identifier: Apache-2.0
//! `slater-build` — the offline writer.
//!
//! Consumes a primitive-Cypher creation script (the dialect emitted by the dump
//! tool) and produces an immutable, generation-numbered on-disk image that the
//! `slater` server serves read-only. Runs offline (build/CI or an admin box),
//! never in the serving hot path, so it may use whatever memory it likes.

mod buckets;
mod build_external;
mod cluster;
mod common;
mod diag;
mod direct_ingest;
mod merge_build;
mod model;
mod overlay;
mod parser;
mod resolve;
mod set_eval;
mod shared;

// On Linux, jemalloc is the global allocator, as it is for the server.
//
// The build's memory accountant (D58) bounds what its sorters *reserve* — at 91.6M
// nodes, peak reserved is exactly the `--max-memory` cap. Peak RSS was still 1.93×
// that, and the excess is not live memory: `emit.topology`'s stitch step held 6.25 GB
// resident against 0.81 GB reserved while doing nothing but concatenating finished
// files. Fourteen band workers churn ~1.5B small `props_blob` allocations and free
// them into per-thread glibc arenas, which are never returned to the OS. jemalloc's
// `background_threads` purge threads return that heap on a decay timer without the
// process making `free()` calls.
//
// Not `malloc_trim`: this crate sets `unsafe_code = "forbid"`, so the libc FFI is not
// available to it — and `slater` migrated away from an idle-gated `malloc_trim` for
// exactly that reason, moving the last `unsafe` into the audited allocator. Non-Linux
// targets keep the system allocator unchanged.
#[cfg(all(target_os = "linux", not(feature = "profiling")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Heap-profiling build only (`--features profiling`, off by default): dhat's
// counting allocator attributes live bytes at peak RSS to allocation call
// stacks. Displaces jemalloc above (only one `#[global_allocator]` may exist),
// which is why the two are mutually exclusive by `cfg`. Use it to attribute the
// residual RSS to allocation sites rather than to guess at it.
#[cfg(feature = "profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::json;

use crate::build_external::build_external;
use crate::cluster::ClusterMode;
use crate::diag::BuildDiag;
use crate::shared::{BuildOptions, InputFormat};

// Candidate zstd levels per backend-aware profile. zstd decode speed is ~level
// independent, so a higher build level shrinks on-disk/on-wire bytes (and thus read
// time) without slowing the hot read path. These are starting points; the
// `bench-codec` harness measures the per-backend knee and these get pinned to it.
const LOCAL_ZSTD_LEVEL: i32 = 9; // balanced: NVMe reads are cheap, keep build CPU sane
const REMOTE_ZSTD_LEVEL: i32 = 19; // object store: every saved byte is network/RTT
const MAX_ZSTD_LEVEL: i32 = 22; // squeeze hardest, build cost no object

/// Backend-aware compression profile. Selects the zstd level for published files
/// when `--zstd-level` is not given explicitly.
#[derive(Copy, Clone, Debug, clap::ValueEnum)]
enum CompressionProfile {
    /// `remote` when a remote publish target (`--publish-s3-bucket` /
    /// `--publish-gcs-bucket`) is configured, else `local`.
    Auto,
    /// Balanced for local/NVMe reads (decompress CPU is a larger share there).
    Local,
    /// Max ratio for remote/object-store reads (bytes-on-the-wire dominate).
    Remote,
    /// Highest ratio regardless of build cost.
    Max,
}

/// Build an immutable Slater graph generation from a primitive-Cypher dump.
#[derive(Debug, Parser)]
#[command(name = "slater-build", version, about)]
struct Cli {
    /// Path to the creation script, or `-` for stdin.
    #[arg(long)]
    input: String,

    /// Logical graph name (selects the `<data-dir>/<graph>/` directory).
    #[arg(long)]
    graph: String,

    /// Root data directory under which `<graph>/<generation>/` is written.
    #[arg(long)]
    data_dir: String,

    /// Primary-key field for single-global-key ("dump_id style") import. When given,
    /// `<FIELD>` is the unique node identity across the whole dump (label-agnostic,
    /// integer-valued) and edges reference endpoints by it; `<FIELD>` is stored as a
    /// queryable node property. `--pk __dump_id__` ingests legacy FalkorDB `GRAPH.DUMP`
    /// files. When omitted (the default), the dump is parsed as business-key `MERGE`
    /// statements (`MERGE (n:L {k:'v'}) [SET …]` for nodes, `MERGE (a:L {k:'v'})-[r:T]->
    /// (b:M {j:'w'}) [SET …]` for edges), where the per-pattern business key is the node
    /// identity and edges resolve endpoints by it; such dumps must be self-contained.
    #[arg(long)]
    pk: Option<String>,

    /// Format of `--input`. `cypher` (default) parses a primitive-Cypher creation
    /// script. `slater-dump` ingests a **binary consolidation dump** directory
    /// (produced by the server during a direct consolidation): dense ids and global
    /// symbol ids are carried in the dump, so parse, node dedup, and endpoint
    /// resolution are skipped and the build enters at clustering. `--pk` must not be
    /// combined with `slater-dump` (a dump has no business-key resolution).
    #[arg(long, value_enum, default_value_t = InputFormat::Cypher)]
    input_format: InputFormat,

    /// Target block size (bytes) for prop/label/topology files.
    #[arg(long, default_value_t = 256 * 1024)]
    block_size: usize,

    /// Target leaf-block size (bytes) for range (ISAM) indexes. Smaller than
    /// `--block-size` on purpose: range indexes are probed by point lookups, and a
    /// lookup decodes a whole leaf, so small leaves keep a probe cheap. See D53.
    #[arg(long, default_value_t = 16 * 1024)]
    range_block_size: usize,

    /// Target block size (bytes) for the vector store.
    #[arg(long, default_value_t = 256 * 1024)]
    vector_block_size: usize,

    /// Explicit zstd level for all published `.blk`/index files. When given it
    /// overrides `--compression-profile` (manifest profile is recorded as
    /// `"manual"`). Omit to let the profile choose the level.
    #[arg(long)]
    zstd_level: Option<i32>,

    /// Backend-aware compression profile selecting the zstd level when
    /// `--zstd-level` is not given. `auto` ⇒ `remote` if a remote publish target
    /// (`--publish-s3-bucket` / `--publish-gcs-bucket`) is set, else `local`. zstd
    /// decode speed is ~level-independent, so a higher level shrinks read I/O
    /// without slowing queries.
    #[arg(long, value_enum, default_value_t = CompressionProfile::Auto)]
    compression_profile: CompressionProfile,

    /// Cap on a per-(label, property) value→count histogram's distinct-key count.
    /// A node range index with more distinct values is not given a histogram (it
    /// would be as large as the index for no benefit); whole-label group-by /
    /// count(DISTINCT) on it then scans the index. `0` disables histograms.
    #[arg(long, default_value_t = graph_format::histogram::DEFAULT_HISTOGRAM_MAX_DISTINCT)]
    histogram_max_distinct: u64,

    /// Optional `VectorIndexSpec[]` JSON sidecar declaring vector indexes.
    #[arg(long)]
    vector_index_json: Option<PathBuf>,

    /// Vector indexes with at least this many vectors are built as a disk-native
    /// Vamana/PQ graph; below it they stay brute-force full-precision.
    #[arg(long, default_value_t = 50_000)]
    ann_threshold: u64,

    /// Vamana out-degree bound `R` (above-threshold indexes).
    #[arg(long, default_value_t = 32)]
    vamana_r: u32,

    /// Vamana robust-prune long-edge factor `alpha` (above-threshold indexes).
    #[arg(long, default_value_t = 1.2)]
    vamana_alpha: f32,

    /// PQ subspace count `m` (must divide each index's dimension).
    #[arg(long, default_value_t = 16)]
    pq_subspaces: u32,

    /// PQ bits per subspace (`k = 2^bits` centroids; 1..=8).
    #[arg(long, default_value_t = 8)]
    pq_bits: u32,

    /// Encrypt every data block at rest (XChaCha20-Poly1305). Requires exactly
    /// one of `--key-file` / `--key-env`. Absent, the image is written plaintext.
    #[arg(long)]
    encrypt: bool,

    /// File holding the at-rest master key as hex (read when `--encrypt`).
    #[arg(long)]
    key_file: Option<PathBuf>,

    /// Environment variable holding the at-rest master key as hex (read when
    /// `--encrypt`).
    #[arg(long)]
    key_env: Option<String>,

    /// Optional path to the live `acl.json`. When given, its BLAKE3 digest is
    /// stamped into the MANIFEST (`aclBlake3`); the server then refuses to serve
    /// this generation if the configured live `acl.json` later differs.
    #[arg(long)]
    acl: Option<PathBuf>,

    /// Working-memory budget for the external build (e.g. `4g`, `512m`). The build
    /// sizes its spill/sort/cluster state to this and aborts rather than exceeding it.
    #[arg(long, default_value = "4g", value_parser = parse_size)]
    max_memory: u64,

    /// Also record each file's SHA-256 and CRC32C in the MANIFEST. These let a
    /// generation served from S3/GCS be verified against the store's own object
    /// checksum without reading the bodies, and are computed automatically whenever
    /// the build publishes to an object store. Pass this when the generation will be
    /// copied to a store by other means (`aws s3 cp`, rsync-then-upload): without
    /// them, the store backend falls back to a size-only completeness check. Costs
    /// wall clock — SHA-256 is the slowest digest of the three and cannot be
    /// parallelised within a file.
    #[arg(long)]
    object_checksums: bool,

    /// Scratch directory for the external build's spill files. Defaults to a
    /// `.slater-scratch-<gen>` under the graph directory; removed on success.
    #[arg(long)]
    temp_dir: Option<PathBuf>,

    /// Node-id reordering for on-disk locality (external build): `ldg` (default)
    /// clusters graph-proximate nodes into the same blocks; `none` keeps dump order.
    #[arg(long, value_enum, default_value_t = ClusterMode::Ldg)]
    cluster: ClusterMode,

    /// LDG refinement passes for `--cluster=ldg`.
    #[arg(long, default_value_t = 3)]
    cluster_passes: u32,

    /// Keep the external build's scratch (buckets/spill) after success, for debugging.
    #[arg(long)]
    keep_temp: bool,

    /// Resume an interrupted external build from its surviving scratch (same
    /// `--graph`/`--data-dir`/`--temp-dir` as the original run), skipping the
    /// phases it already completed.
    #[arg(long)]
    resume: bool,

    /// Suppress the progress log. By default the build emits hs-utils-style
    /// progress lines (phase start/finish with timings, and per-phase work
    /// milestones) to stdout while it runs; `-q`/`--quiet` turns that off (errors
    /// still surface). Independent of `--diagnostics`.
    #[arg(long, short = 'q')]
    quiet: bool,

    /// Diagnostic mode: sample process resource counters (RSS, cgroup memory,
    /// CPU, IO, threads, PSI stall) on a background thread and append a JSONL log
    /// of what the build was doing at each moment, for later bottleneck analysis.
    /// OFF by default (zero overhead). Also enabled by `SLATER_BUILD_DIAG=1`.
    /// Orthogonal to the progress log above — this is the detailed machine-readable
    /// trace, not the human narrative.
    #[arg(long)]
    diagnostics: bool,

    /// Where to write the diagnostics JSONL (with `--diagnostics`). Defaults to
    /// `<data-dir>/build-diag-<graph>-<pid>.jsonl`.
    #[arg(long)]
    diagnostics_log: Option<PathBuf>,

    /// Sampling interval for diagnostic mode, milliseconds.
    #[arg(long, default_value_t = 1000)]
    diagnostics_interval_ms: u64,

    /// Worker-thread cap for the parallel build stages (pass 1, resolve, cluster,
    /// and the external-sort spill pool). Defaults to `max(online_cores - 2, 1)`.
    #[arg(long, short = 'j')]
    threads: Option<usize>,

    /// Also publish the finished generation to this S3 bucket, after the local
    /// publish to `--data-dir`. Requires a binary built with the `s3` feature.
    /// Credentials come from the standard AWS chain (env / profile / instance role).
    #[arg(long)]
    publish_s3_bucket: Option<String>,

    /// AWS region for `--publish-s3-bucket` (e.g. `eu-west-2`).
    #[arg(long, default_value = "")]
    publish_s3_region: String,

    /// Custom endpoint URL for an S3-compatible publish target (MinIO, localstack).
    #[arg(long, default_value = "")]
    publish_s3_endpoint: String,

    /// Key prefix under which the generation is published in the bucket.
    #[arg(long, default_value = "")]
    publish_s3_prefix: String,

    /// Use path-style S3 addressing (required by most S3-compatible servers).
    #[arg(long)]
    publish_s3_path_style: bool,

    /// Also publish the finished generation to this GCS bucket, after the local
    /// publish to `--data-dir`. Requires a binary built with the `gcs` feature.
    /// Credentials come from Application Default Credentials (Workload Identity /
    /// metadata / gcloud) unless `--publish-gcs-credentials` is given. Mutually
    /// exclusive with `--publish-s3-bucket`.
    #[arg(long)]
    publish_gcs_bucket: Option<String>,

    /// Key prefix under which the generation is published in the GCS bucket.
    #[arg(long, default_value = "")]
    publish_gcs_prefix: String,

    /// Path to a service-account JSON key file for `--publish-gcs-bucket`. Empty ⇒
    /// Application Default Credentials.
    #[arg(long, default_value = "")]
    publish_gcs_credentials: String,

    /// Custom endpoint URL for a GCS emulator publish target (`fake-gcs-server`).
    #[arg(long, default_value = "")]
    publish_gcs_endpoint: String,

    /// Use anonymous (unauthenticated) GCS credentials — for a `fake-gcs-server`
    /// emulator publish target only, never against real GCS.
    #[arg(long)]
    publish_gcs_anonymous: bool,
}

/// Resolve the worker-thread cap: the `--threads` value, else
/// `max(online_cores - 2, 1)`.
fn resolve_threads(cli: &Cli) -> usize {
    cli.threads.unwrap_or_else(|| {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        cores.saturating_sub(2).max(1)
    })
}

/// Whether diagnostic mode is on: the `--diagnostics` flag, or a truthy
/// `SLATER_BUILD_DIAG` env var.
fn diagnostics_enabled(cli: &Cli) -> bool {
    if cli.diagnostics {
        return true;
    }
    matches!(
        std::env::var("SLATER_BUILD_DIAG").ok().as_deref(),
        Some("1" | "true" | "on" | "yes")
    )
}

/// Construct the `BuildDiag` for this run. Two independent outputs:
/// * the **progress log** (hs-utils-style phase/milestone lines) — on unless
///   `--quiet`;
/// * the **diagnostics JSONL** (detailed resource sampler) — on only with
///   `--diagnostics` (or `SLATER_BUILD_DIAG=1`), written under `--data-dir`
///   (falling back to the CWD).
fn make_diag(cli: &Cli, data_dir: &std::path::Path) -> Result<BuildDiag> {
    let log_enabled = !cli.quiet;
    let jsonl = if diagnostics_enabled(cli) {
        let log_path = match &cli.diagnostics_log {
            Some(p) => p.clone(),
            None => {
                let name = format!("build-diag-{}-{}.jsonl", cli.graph, std::process::id());
                // Prefer the data dir (create it if missing); fall back to the CWD.
                if std::fs::create_dir_all(data_dir).is_ok() {
                    data_dir.join(name)
                } else {
                    PathBuf::from(name)
                }
            }
        };
        let header = json!({
            "graph": cli.graph,
            "input": cli.input,
            "max_memory_bytes": cli.max_memory,
            "zstd_level": cli.zstd_level,
            "compression_profile": format!("{:?}", cli.compression_profile),
            "block_size": cli.block_size,
            "vector_block_size": cli.vector_block_size,
            "cluster": format!("{:?}", cli.cluster),
            "cluster_passes": cli.cluster_passes,
            "ann_threshold": cli.ann_threshold,
            "resume": cli.resume,
            "threads": resolve_threads(cli),
        });
        eprintln!("slater-build: diagnostics → {}", log_path.display());
        Some(crate::diag::JsonlConfig {
            path: log_path,
            interval: Duration::from_millis(cli.diagnostics_interval_ms.max(1)),
            header,
        })
    } else {
        None
    };
    BuildDiag::start(log_enabled, jsonl).context("start build diagnostics/logging")
}

/// Parse a human byte size like `4g`, `512m`, `1024k`, or a plain byte count.
fn parse_size(s: &str) -> std::result::Result<u64, String> {
    let s = s.trim();
    let (num, mult): (&str, u64) = match s.chars().last() {
        Some('g' | 'G') => (&s[..s.len() - 1], 1 << 30),
        Some('m' | 'M') => (&s[..s.len() - 1], 1 << 20),
        Some('k' | 'K') => (&s[..s.len() - 1], 1 << 10),
        _ => (s, 1),
    };
    num.trim()
        .parse::<u64>()
        .map(|n| n.saturating_mul(mult))
        .map_err(|e| format!("invalid size '{s}': {e}"))
}

/// Resolve the at-rest master key from the CLI flags. Returns `None` unless
/// `--encrypt` is set; otherwise reads exactly one of `--key-file`/`--key-env`,
/// trims it, and hex-decodes it into raw key bytes.
fn resolve_master_key(cli: &Cli) -> Result<Option<Vec<u8>>> {
    if !cli.encrypt {
        if cli.key_file.is_some() || cli.key_env.is_some() {
            anyhow::bail!("--key-file/--key-env given without --encrypt");
        }
        return Ok(None);
    }
    let hex = match (&cli.key_file, &cli.key_env) {
        (Some(_), Some(_)) => anyhow::bail!("give only one of --key-file / --key-env"),
        (Some(path), None) => std::fs::read_to_string(path)
            .with_context(|| format!("read key file {}", path.display()))?,
        (None, Some(var)) => {
            std::env::var(var).with_context(|| format!("read key env var {var}"))?
        }
        (None, None) => anyhow::bail!("--encrypt requires --key-file or --key-env"),
    };
    let key = graph_format::crypto::hex_decode(&hex).context("decode master key hex")?;
    if key.is_empty() {
        anyhow::bail!("the at-rest master key is empty");
    }
    Ok(Some(key))
}

/// Resolve the effective zstd level and the profile name recorded in the manifest.
/// An explicit `--zstd-level N` always wins (recorded as `"manual"`); otherwise the
/// chosen profile maps to a level, with `auto` deferring to whether a remote publish
/// target is configured (`publishing_remote`).
fn resolve_compression(cli: &Cli, publishing_remote: bool) -> (i32, String) {
    if let Some(level) = cli.zstd_level {
        return (level, "manual".into());
    }
    let profile = match cli.compression_profile {
        CompressionProfile::Auto if publishing_remote => CompressionProfile::Remote,
        CompressionProfile::Auto => CompressionProfile::Local,
        p => p,
    };
    match profile {
        CompressionProfile::Local => (LOCAL_ZSTD_LEVEL, "local".into()),
        CompressionProfile::Remote => (REMOTE_ZSTD_LEVEL, "remote".into()),
        CompressionProfile::Max => (MAX_ZSTD_LEVEL, "max".into()),
        // `auto` is resolved to local/remote above.
        CompressionProfile::Auto => unreachable!("auto resolved to local/remote"),
    }
}

/// Build the optional remote publish target from the `--publish-s3-*` /
/// `--publish-gcs-*` flags. `None` ⇒ filesystem-only publish. Errors if a target
/// is requested but the binary was built without the matching backend feature, or
/// if both an S3 and a GCS target are given.
fn resolve_publish_store(cli: &Cli) -> Result<Option<Arc<dyn graph_format::store::ObjectStore>>> {
    if cli.publish_s3_bucket.is_some() && cli.publish_gcs_bucket.is_some() {
        anyhow::bail!("give only one of --publish-s3-bucket / --publish-gcs-bucket");
    }
    if let Some(bucket) = &cli.publish_s3_bucket {
        #[cfg(feature = "s3")]
        {
            let cfg = graph_format::store::s3::S3Config {
                bucket: bucket.clone(),
                region: cli.publish_s3_region.clone(),
                endpoint: (!cli.publish_s3_endpoint.is_empty())
                    .then(|| cli.publish_s3_endpoint.clone()),
                prefix: cli.publish_s3_prefix.clone(),
                path_style: cli.publish_s3_path_style,
                // Publish credentials come from the standard AWS chain.
                access_key: None,
                secret_key: None,
                session_token: None,
            };
            let store = graph_format::store::s3::S3ObjectStore::connect(&cfg)
                .context("connect S3 publish target")?;
            return Ok(Some(Arc::new(store)));
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = bucket;
            anyhow::bail!(
                "--publish-s3-bucket given but slater-build was built without the `s3` cargo feature"
            )
        }
    }
    if let Some(bucket) = &cli.publish_gcs_bucket {
        #[cfg(feature = "gcs")]
        {
            let cfg = graph_format::store::gcs::GcsConfig {
                bucket: bucket.clone(),
                prefix: cli.publish_gcs_prefix.clone(),
                endpoint: (!cli.publish_gcs_endpoint.is_empty())
                    .then(|| cli.publish_gcs_endpoint.clone()),
                credentials_path: (!cli.publish_gcs_credentials.is_empty())
                    .then(|| cli.publish_gcs_credentials.clone()),
                // The builder takes a service-account key by file path (or ADC);
                // inline JSON is a server-side convenience only.
                credentials_json: None,
                anonymous: cli.publish_gcs_anonymous,
            };
            let store = graph_format::store::gcs::GcsObjectStore::connect(&cfg)
                .context("connect GCS publish target")?;
            return Ok(Some(Arc::new(store)));
        }
        #[cfg(not(feature = "gcs"))]
        {
            let _ = bucket;
            anyhow::bail!(
                "--publish-gcs-bucket given but slater-build was built without the `gcs` cargo feature"
            )
        }
    }
    Ok(None)
}

fn main() -> Result<()> {
    // Held for the whole build; its `Drop` writes dhat-heap.json on exit (including
    // early-return error paths, since `?` still runs destructors while unwinding).
    #[cfg(feature = "profiling")]
    let _profiler = dhat::Profiler::new_heap();

    let cli = Cli::parse();

    // Install hs-utils-style logging so the build's progress lines (and any
    // warnings) reach stdout. `-q`/`--quiet` skips the subscriber, so the macros
    // become no-ops and the build runs silent but for errors.
    if !cli.quiet {
        hs_utils::logging::init("info");
    }

    let encryption_key = resolve_master_key(&cli)?;
    let acl_blake3 = match &cli.acl {
        Some(p) => Some(
            graph_format::integrity::hash_file(p)
                .with_context(|| format!("hash acl file {}", p.display()))?,
        ),
        None => None,
    };
    let threads = resolve_threads(&cli);
    let publish_store = resolve_publish_store(&cli)?;
    let (zstd_level, compression_profile) = resolve_compression(&cli, publish_store.is_some());
    if cli.pk.is_some() && matches!(cli.input_format, InputFormat::SlaterDump) {
        anyhow::bail!("--pk cannot be combined with --input-format=slater-dump");
    }
    let opts = BuildOptions {
        pk: cli.pk.clone(),
        input_format: cli.input_format,
        block_size: cli.block_size,
        range_block_size: cli.range_block_size,
        vector_block_size: cli.vector_block_size,
        zstd_level,
        compression_profile,
        histogram_max_distinct: cli.histogram_max_distinct,
        vector_index_json: cli.vector_index_json.clone(),
        encryption_key,
        acl_blake3,
        ann_threshold: cli.ann_threshold,
        vamana_r: cli.vamana_r,
        vamana_alpha: cli.vamana_alpha,
        pq_subspaces: cli.pq_subspaces,
        pq_bits: cli.pq_bits,
        max_memory_bytes: cli.max_memory as usize,
        object_checksums: cli.object_checksums,
        temp_dir: cli.temp_dir.clone(),
        cluster: cli.cluster,
        cluster_passes: cli.cluster_passes,
        keep_temp: cli.keep_temp,
        resume: cli.resume,
        threads,
        publish_store,
    };
    // Pin the global rayon pool and the external-sort spill pool to the cap, so
    // every parallel build stage respects `--threads`.
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global();
    graph_format::extsort::configure_spill_threads(threads);
    graph_format::blockfile::configure_seal_threads(threads);

    let data_dir = PathBuf::from(&cli.data_dir);
    let mut diag = make_diag(&cli, &data_dir)?;

    // The bounded-memory external build is the only build path: it opens the input
    // path itself (so it can seek a file for mid-pass-1 resume) and spills to disk,
    // so it serves graphs of any size without holding the whole graph in RAM.
    let outcome = build_external(&cli.input, &cli.graph, &data_dir, &opts, &diag)?;
    diag.finish();
    // Stdout is the machine-facing channel: print the generation UUID + content
    // hash so a publishing pipeline can record exactly what it built.
    println!(
        "built graph '{}' generation {} ({} nodes, {} edges)\ncontent-hash {}\ndir {}",
        cli.graph,
        outcome.generation,
        outcome.node_count,
        outcome.edge_count,
        outcome.content_hash,
        outcome.dir.display(),
    );
    Ok(())
}
