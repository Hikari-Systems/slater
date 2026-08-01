// SPDX-License-Identifier: Apache-2.0
//! `slater --consolidate-worker` must publish a generation **identical** to the one an
//! external `slater-build` publishes from the same consolidation dump.
//!
//! # Why this test exists, and why it is an integration test
//! Stage 2 gives the build a second entry point: the pipeline moved into
//! `slater-build`'s library half so the server can re-exec *itself* as the consolidation
//! child instead of hunting for a second binary. A second entry point into a pipeline with
//! ~27 build options is exactly where defaults drift, and the drift is silent — every
//! existing "the rebuild worked" assertion keeps passing while the published bytes differ.
//!
//! It already caught one before it shipped. `BuildOptions::default()` carries
//! `zstd_level: 3` / `compression_profile: "manual"`, because a struct default cannot know
//! the publish target. The CLI never uses those: it resolves the server's flagless,
//! local-`--data-dir` invocation to `local` / zstd-**9**, with a latency-biased
//! degree-column margin. A worker written on `..Default::default()` alone would have
//! published every consolidated generation three compression levels down. The fix was to
//! hoist that policy into `slater_build::compression` so there is one copy; this test is
//! what keeps it that way.
//!
//! **Integration, not unit:** the worker path re-execs `std::env::current_exe()`, which
//! inside a unit test is the *test harness* binary — it rejects `--consolidate-worker` and
//! the test proves nothing. `env!("CARGO_BIN_EXE_slater")` names the real server binary, so
//! this drives the actual production path with no test seam in `server.rs` to get it wrong.
//! (A seam is how HIK-157 hid for its whole life; see `registry.rs`.)
//!
//! Ignored by default because it spawns two real binaries. Run:
//! ```text
//! cargo build -p slater-build
//! SLATER_BUILD_BIN=$CARGO_TARGET_DIR/debug/slater-build \
//!   cargo test -p slater --features testkit --test consolidate_worker_parity -- --ignored
//! ```

#![cfg(feature = "testkit")]

use std::path::{Path, PathBuf};
use std::process::Command;

use slater::cache::{BlockCache, VectorIndexCache};
use slater::config::DeltaConfig;
use slater::server::Graphs;
use slater::testgen;

/// Recursively copy `src` to `dst` — the consolidation dump is a directory, and
/// `consolidate_graph` deletes it on the way out, so the parity run needs its own copy
/// taken from inside the build seam while it still exists.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap().flatten() {
        let (from, to) = (entry.path(), dst.join(entry.file_name()));
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

fn delta_cfg(wal: &Path) -> DeltaConfig {
    DeltaConfig {
        enabled: true,
        wal_dir: wal.to_string_lossy().into_owned(),
        ..DeltaConfig::default()
    }
}

/// The published generation's content hash and compression settings, read **straight off
/// disk** rather than through a reader.
///
/// `content_hash` is a BLAKE3 over the whole data-file inventory, so it covers block
/// contents, not just node and edge counts. `zstd_level`/`compression_profile` come along
/// because they are the specific things that drifted: a mismatch there says *why* the
/// hashes differ instead of leaving a bare "these two blobs are not equal".
/// Note the `unwrap()`s are load-bearing: `serde_json` indexing yields `Null` for a
/// missing key, so reading these as `Option`s and comparing them would let two absent
/// fields compare equal and pass the test having checked nothing. Fail loudly instead.
fn published_identity(data_dir: &Path) -> (String, i64, String) {
    let graph_dir = data_dir.join("people");
    let current = std::fs::read_to_string(graph_dir.join("current"))
        .expect("a published generation should leave a `current` pointer");
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(graph_dir.join(current.trim()).join("MANIFEST.json"))
            .expect("read the published MANIFEST.json"),
    )
    .expect("parse MANIFEST.json");
    (
        manifest["contentHash"].as_str().unwrap().to_string(),
        manifest["zstdLevel"].as_i64().unwrap(),
        manifest["compressionProfile"].as_str().unwrap().to_string(),
    )
}

#[test]
#[ignore = "spawns the real slater-build and the real slater worker; see the module docs"]
fn the_self_exec_worker_publishes_the_same_generation_as_slater_build() {
    let slater_build =
        std::env::var("SLATER_BUILD_BIN").unwrap_or_else(|_| "slater-build".to_string());
    let slater = env!("CARGO_BIN_EXE_slater");

    // ── Arm 1: a normal consolidation through the external `slater-build`, keeping a
    //    copy of the dump it was fed.
    let (root, _graph) = testgen::write_indexed_people("worker_parity");
    let wal = root.join("_wal");
    let dump_copy = std::env::temp_dir().join(format!("worker-parity-dump-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dump_copy);

    let mut graphs = Graphs::open_all(&root, None).unwrap();
    graphs
        .enable_writable_layer(&delta_cfg(&wal), &root, None)
        .unwrap();
    // No delta write is needed, and `execute_write` is `pub(crate)` anyway. The property
    // under test is "same dump in ⇒ same generation out", so what the dump *contains* is
    // beside the point; consolidating the core alone exercises the whole
    // dump → build → publish path either way.
    let cache = BlockCache::new(1 << 20);
    let vc = VectorIndexCache::new(1 << 20);
    let dump_copy_for_seam = dump_copy.clone();
    graphs
        .consolidate_graph("people", &cache, &vc, &root, move |dump, g, dd, key| {
            copy_dir(dump, &dump_copy_for_seam);
            slater::server::run_builder(
                &slater_build,
                dump,
                g,
                dd,
                key,
                slater::server::BuilderLimits::default(),
            )
        })
        .expect("the external slater-build must consolidate");
    let via_slater_build = published_identity(&root);

    // ── Arm 2: the same dump, rebuilt by `slater --consolidate-worker` into a fresh
    //    data dir. Same input bytes in, so the output must match bit for bit.
    let worker_dir: PathBuf =
        std::env::temp_dir().join(format!("worker-parity-out-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&worker_dir);
    std::fs::create_dir_all(&worker_dir).unwrap();
    let out = Command::new(slater)
        .arg("--consolidate-worker")
        .arg("--input")
        .arg(&dump_copy)
        .arg("--input-format")
        .arg("slater-dump")
        .arg("--graph")
        .arg("people")
        .arg("--data-dir")
        .arg(&worker_dir)
        .output()
        .expect("spawn the slater consolidation worker");
    assert!(
        out.status.success(),
        "worker failed: {}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let via_worker = published_identity(&worker_dir);

    // Compare the compression settings first: they are the likeliest drift and they name
    // themselves, so a failure here reads as a cause rather than as two unequal hashes.
    assert_eq!(
        (via_worker.1, via_worker.2.as_str()),
        (via_slater_build.1, via_slater_build.2.as_str()),
        "worker and slater-build resolved different compression settings — \
         `slater_build::compression` is meant to be the single source of truth for this"
    );
    assert_eq!(
        via_worker.0, via_slater_build.0,
        "the self-exec worker published a different generation than slater-build — the two \
         entry points into the build pipeline have drifted (block sizes? cluster mode? \
         thread-dependent output?)"
    );

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&dump_copy);
    let _ = std::fs::remove_dir_all(&worker_dir);
}
