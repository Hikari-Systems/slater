// SPDX-License-Identifier: Apache-2.0
//! `slater-build` — the offline writer.
//!
//! Consumes a primitive-Cypher creation script (the dialect emitted by the dump
//! tool) and produces an immutable, generation-numbered on-disk image that the
//! `slater` server serves read-only. Runs offline (build/CI or an admin box),
//! never in the serving hot path, so it may use whatever memory it likes.

mod build;
mod model;
mod parser;

use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use crate::build::{build, BuildOptions};

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

    /// Target block size (bytes) for prop/label/topology/range files.
    #[arg(long, default_value_t = 256 * 1024)]
    block_size: usize,

    /// Target block size (bytes) for the vector store.
    #[arg(long, default_value_t = 256 * 1024)]
    vector_block_size: usize,

    /// zstd compression level for all `.blk`/index files.
    #[arg(long, default_value_t = 3)]
    zstd_level: i32,

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

fn main() -> Result<()> {
    let cli = Cli::parse();

    let encryption_key = resolve_master_key(&cli)?;
    let opts = BuildOptions {
        block_size: cli.block_size,
        vector_block_size: cli.vector_block_size,
        zstd_level: cli.zstd_level,
        vector_index_json: cli.vector_index_json.clone(),
        encryption_key,
        ann_threshold: cli.ann_threshold,
        vamana_r: cli.vamana_r,
        vamana_alpha: cli.vamana_alpha,
        pq_subspaces: cli.pq_subspaces,
        pq_bits: cli.pq_bits,
    };
    let data_dir = PathBuf::from(&cli.data_dir);

    let reader: Box<dyn BufRead> = if cli.input == "-" {
        Box::new(BufReader::new(std::io::stdin().lock()))
    } else {
        let f = std::fs::File::open(&cli.input)
            .with_context(|| format!("open input script {}", cli.input))?;
        Box::new(BufReader::new(f))
    };

    let outcome = build(reader, &cli.graph, &data_dir, &opts)?;
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
