// SPDX-License-Identifier: Apache-2.0
//! `slater --consolidate-worker` — the consolidation rebuild, run as a child of the
//! server rather than as a separate `slater-build` binary.
//!
//! # Why a subcommand and not a library call
//! The process boundary is deliberate and stays. A rebuild peaks at ~5.66 GB RSS on the
//! 91.6M-node core — **1.07–2.08× its own `--max-memory` cap**, not repeatable to better
//! than ±1.7 GB — and nothing in the codebase caps RSS (no `setrlimit`, no cgroup
//! enforcement; `MemoryBudget` bounds *reservations*, not residency). Running that inside
//! the server would put a multi-GB, 45-minute workload in the address space of a process
//! whose headline guarantee is a few hundred MB, and an OOM kill there takes every Bolt
//! connection with it. Out of process, the same OOM is a non-zero exit: the old core keeps
//! serving and the delta stays live.
//!
//! It would also silently break the build's own threading. `slater-build` pins the
//! **global** rayon pool with `ThreadPoolBuilder::build_global()`, whose result it
//! discards, and `graph-format`'s `SPILL_POOL`/`SEAL_POOL` are `OnceLock`s that ignore
//! later configuration. The server touches all three before any consolidation could run
//! (generation open, segment flush), so an in-process build would quietly get the
//! *server's* thread counts and ignore `--threads` entirely.
//!
//! # Why re-exec ourselves rather than spawn `slater-build`
//! What the boundary does *not* require is a second binary. Spawning one meant the server
//! had to find it (`/app` is not on the distroless `PATH`), could not consolidate at all
//! where it is not shipped (`slater:latest-lite`), and could silently pair with a
//! different version — a case the code already carries a bespoke error for ("a
//! `builderBin` too old to know `--key-stdin`"). Re-exec'ing `current_exe()` removes all
//! three by construction: same file, same version, nothing to locate.
//!
//! # Argument contract
//! Deliberately identical in *meaning* to the `slater-build` flags it replaces, so the
//! two remain interchangeable and `delta.builderBin` stays a working escape hatch:
//!
//! ```text
//! slater --consolidate-worker --input <dump> --graph <name> --data-dir <dir>
//!        [--max-memory <bytes>] [--threads <n>] [--encrypt --key-stdin]
//! ```
//!
//! The master key arrives on **stdin**, hex-encoded, exactly as it does for
//! `slater-build`: `--key-env` would hold it in `/proc/<pid>/environ` for the whole
//! rebuild (unwipeable — `THREAT_MODEL.md` limitation 7), `--key-file` leaves it on disk
//! after a crash, and `argv` is world-readable through `/proc/<pid>/cmdline`.

use std::io::Read;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use zeroize::Zeroizing;

/// The flag that selects this mode. Long-form and unabbreviated: it is invoked by the
/// server, not by hand, and it should be obvious in `ps` what a multi-GB `slater` process
/// is doing.
pub const WORKER_FLAG: &str = "--consolidate-worker";

/// Run the worker and exit if `argv[1]` selects it; otherwise return so the caller
/// continues into the normal server startup.
///
/// Mirrors [`crate::query::query_subcommand`] — dispatched from `main` *after* config
/// load but *before* the tokio runtime is built, because the rebuild is synchronous and
/// wants no reactor.
pub fn consolidate_worker_subcommand() {
    if std::env::args().nth(1).as_deref() != Some(WORKER_FLAG) {
        return;
    }
    match run() {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            // The parent pipes and drains both streams, so this reaches the server's log
            // and the tail of it is carried into the `CALL slater.consolidate()` failure.
            eprintln!("slater consolidation worker failed: {e:#}");
            std::process::exit(1);
        }
    }
}

/// Parsed worker invocation.
struct Args {
    /// `&str` not `PathBuf`: `build_external` takes the input as a string because it
    /// also accepts `-` for stdin. Consolidation always names a real directory.
    input: String,
    graph: String,
    data_dir: PathBuf,
    max_memory: Option<u64>,
    threads: Option<usize>,
    encrypt: bool,
    key_stdin: bool,
}

/// Parse `argv[2..]`. Hand-rolled rather than clap-derive: the surface is six flags with
/// exactly one producer (the server's `run_builder`), and a parse error here means a bug
/// in that producer, not user input to be helped along.
fn parse_args() -> Result<Args> {
    let (mut input, mut graph, mut data_dir) = (None, None, None);
    let (mut max_memory, mut threads) = (None, None);
    let (mut encrypt, mut key_stdin) = (false, false);
    let mut it = std::env::args().skip(2);
    while let Some(arg) = it.next() {
        let mut value = |name: &str| -> Result<String> {
            it.next()
                .ok_or_else(|| anyhow::anyhow!("{name} requires a value"))
        };
        match arg.as_str() {
            "--input" => input = Some(value("--input")?),
            "--graph" => graph = Some(value("--graph")?),
            "--data-dir" => data_dir = Some(PathBuf::from(value("--data-dir")?)),
            "--max-memory" => {
                let v = value("--max-memory")?;
                max_memory = Some(parse_size(&v)?);
            }
            "--threads" | "-j" => {
                let v = value("--threads")?;
                threads = Some(
                    v.parse()
                        .with_context(|| format!("invalid --threads {v}"))?,
                );
            }
            "--encrypt" => encrypt = true,
            "--key-stdin" => key_stdin = true,
            // Accepted and ignored: the server passes it for `slater-build` compatibility,
            // and this worker only ever ingests a consolidation dump.
            "--input-format" => {
                let v = value("--input-format")?;
                if v != "slater-dump" {
                    bail!("the consolidation worker only accepts --input-format slater-dump");
                }
            }
            other => bail!("unknown consolidation-worker argument '{other}'"),
        }
    }
    Ok(Args {
        input: input.ok_or_else(|| anyhow::anyhow!("--input is required"))?,
        graph: graph.ok_or_else(|| anyhow::anyhow!("--graph is required"))?,
        data_dir: data_dir.ok_or_else(|| anyhow::anyhow!("--data-dir is required"))?,
        max_memory,
        threads,
        encrypt,
        key_stdin,
    })
}

/// Byte sizes with an optional `k`/`m`/`g` suffix — the same grammar `slater-build`'s
/// `--max-memory` accepts, so the two stay interchangeable. A suffix-less number is bytes.
fn parse_size(s: &str) -> Result<u64> {
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
        .with_context(|| format!("invalid size '{s}'"))
}

/// Read the hex-encoded master key from stdin to EOF. The parent closes the pipe after
/// writing, which is the EOF this blocks on.
fn read_key_from_stdin() -> Result<Zeroizing<Vec<u8>>> {
    let mut hex = Zeroizing::new(String::new());
    std::io::stdin()
        .read_to_string(&mut hex)
        .context("read the at-rest master key from stdin")?;
    let hex_trimmed = hex.trim();
    if hex_trimmed.is_empty() {
        bail!("--key-stdin was given but stdin carried no key");
    }
    Ok(Zeroizing::new(
        graph_format::crypto::hex_decode(hex_trimmed).context("decode the master key as hex")?,
    ))
}

fn run() -> Result<()> {
    let args = parse_args()?;
    // The parent drains both streams into its own `tracing` pipeline, so progress lines
    // land in the server's log rather than on an inherited terminal nobody reads.
    hs_utils::logging::init("info");

    if args.encrypt != args.key_stdin {
        bail!("--encrypt and --key-stdin must be given together");
    }
    let encryption_key = if args.key_stdin {
        Some(read_key_from_stdin()?)
    } else {
        None
    };

    // `cores - 2` is `slater-build`'s default and reads the *host's* core count, which
    // over-subscribes a quota-capped container; the server derives a better number and
    // passes it, so an absent flag here means "uncapped host, use the default".
    let threads = args
        .threads
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get().max(3) - 2));
    // Pin the pools before the first sorter or block file exists. These are process-global
    // and read-once (`OnceLock`), which is exactly why the build must not share a process
    // with the server — here the process is ours alone, so the pinning holds.
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global();
    graph_format::extsort::configure_spill_threads(threads);
    graph_format::blockfile::configure_seal_threads(threads);

    // Compression is resolved through the **shared** policy, not through
    // `BuildOptions::default()`. The struct default is `manual`/zstd-3 because a plain
    // struct cannot know the publish target; the invocation the server actually makes —
    // no explicit level, local `--data-dir`, no object store — resolves to `local`/zstd-9
    // with a latency-biased degree margin. Reaching for `..Default::default()` alone would
    // have published every consolidated generation three levels under-compressed, and no
    // "the rebuild worked" assertion would have noticed.
    let (zstd_level, compression_profile, degree_zstd_margin) =
        slater_build::compression::local_publish_defaults();
    let opts = slater_build::BuildOptions {
        input_format: slater_build::InputFormat::SlaterDump,
        encryption_key,
        max_memory_bytes: args.max_memory.unwrap_or(4 << 30) as usize,
        threads,
        zstd_level,
        compression_profile,
        degree_zstd_margin,
        ..Default::default()
    };
    let diag = slater_build::BuildDiag::disabled();
    let outcome =
        slater_build::build_external(&args.input, &args.graph, &args.data_dir, &opts, &diag)?;
    // Same machine-facing line `slater-build` prints, so anything parsing it keeps working.
    println!(
        "built graph '{}' generation {} ({} nodes, {} edges)\ncontent-hash {}\ndir {}",
        args.graph,
        outcome.generation,
        outcome.node_count,
        outcome.edge_count,
        outcome.content_hash,
        outcome.dir.display(),
    );
    Ok(())
}
