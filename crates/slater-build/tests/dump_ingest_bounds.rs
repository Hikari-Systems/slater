// SPDX-License-Identifier: Apache-2.0
//! A consolidation dump is untrusted input: `--input-format=slater-dump` hands an opaque
//! binary file's bytes straight to the post-resolve pipeline, skipping the parse / dedup /
//! endpoint-resolution the Cypher front-half would put in their way. An edge in it can
//! therefore name a node or a reltype the dump does not contain.
//!
//! The unit tests in `direct_ingest.rs` pin the boundary check itself. This pins the
//! *behaviour of the shipped binary*: a hostile dump is *rejected* — exit 1, with an error
//! naming the offending edge — and does **not** panic.
//!
//! That distinction is the whole fix. Before it, the ids were copied verbatim into the
//! build and blew up deep in a worker thread: on the default `--cluster=ldg` an
//! out-of-range `src` indexed the LDG partition table (`index out of bounds: the len is 3
//! but the index is 999`, exit 101, naming no dump and no edge), and an out-of-range `dst`
//! or `reltype` reached `EndpointPlanes::set`, whose only bounds check is a `debug_assert!`
//! — compiled out of the release builder, leaving a raw `Vec` index that panics, or that
//! silently lands inside a *neighbouring reltype's plane* and sets a posting bit for the
//! wrong relationship type.

use std::path::PathBuf;
use std::process::Command;

use graph_format::consolidate_dump::DumpWriter;
use graph_format::ids::Value;

const NODES: u64 = 3;
const RELTYPES: usize = 2;

/// Write a dump with `NODES` nodes, a well-formed edge, and one edge under test.
/// `DumpWriter` validates neither endpoints nor reltypes, so it will happily write a
/// hostile one — which is the point: a real attacker need not even use it.
fn hostile_dump(tag: &str, src: u64, dst: u64, reltype: u32) -> PathBuf {
    let work = std::env::temp_dir().join(format!("slater_dib_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();

    let dump = work.join("dump");
    let mut w = DumpWriter::create(&dump).unwrap();
    for i in 0..NODES {
        w.append_node(&[0], &[(0, Value::Int(i as i64))]).unwrap();
    }
    w.append_edge(0, 1, 0, &[]).unwrap();
    w.append_edge(src, dst, reltype, &[]).unwrap();
    w.finish(
        vec!["N".into()],
        (0..RELTYPES).map(|i| format!("R{i}")).collect(),
        vec!["k".into()],
        vec![],
        vec![],
    )
    .unwrap();
    work
}

/// Run the real binary over the dump under the *default* cluster mode (`ldg`) and return
/// `(exit_code, stderr)`.
fn build(work: &std::path::Path) -> (Option<i32>, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            work.join("dump").to_str().unwrap(),
            "--input-format",
            "slater-dump",
            "--graph",
            "g",
            "--data-dir",
            work.join("data").to_str().unwrap(),
        ])
        .output()
        .expect("run slater-build");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Rejected cleanly, not panicked. Exit 101 is a Rust panic; exit 1 is `anyhow` reaching
/// `main`. Asserting on the *code* (not the message) is what makes this a regression test
/// for the panic rather than for the wording.
fn assert_rejected(work: &std::path::Path, expect: &str) {
    let (code, stderr) = build(work);
    assert_ne!(
        code,
        Some(101),
        "build panicked on a hostile dump instead of rejecting it:\n{stderr}"
    );
    assert!(
        !stderr.contains("panicked") && !stderr.contains("index out of bounds"),
        "build panicked on a hostile dump instead of rejecting it:\n{stderr}"
    );
    assert_eq!(code, Some(1), "expected a clean rejection:\n{stderr}");
    assert!(
        stderr.contains(expect),
        "error should name the offending edge (wanted {expect:?}):\n{stderr}"
    );
}

#[test]
fn build_rejects_dump_edge_with_out_of_range_src() {
    // Pre-fix: PANIC, exit 101 — `index out of bounds: the len is 3 but the index is 999`
    // in the LDG partition table (`cluster.rs`), from a scoped worker thread.
    let work = hostile_dump("src", 999, 1, 0);
    assert_rejected(&work, "src endpoint 999");
}

#[test]
fn build_rejects_dump_edge_with_out_of_range_dst() {
    // Pre-fix: PANIC in `EndpointPlanes::set` — a `debug_assert!` in a debug build, a raw
    // `Vec` index-out-of-bounds in the release builder that actually ships.
    let work = hostile_dump("dst", 0, 999, 0);
    assert_rejected(&work, "dst endpoint 999");
}

#[test]
fn build_rejects_dump_edge_with_out_of_range_reltype() {
    // Pre-fix: PANIC in `EndpointPlanes::set` (plane path). Endpoints are valid here, so
    // nothing else downstream was even looking at this edge.
    let work = hostile_dump("rt", 0, 1, 7);
    assert_rejected(&work, "reltype id 7");
}

/// The control: the same build, same flags, ids in range — still builds. A bounds check
/// that rejects everything would pass the tests above.
#[test]
fn build_accepts_a_well_formed_dump() {
    let work = hostile_dump("ok", NODES - 1, 0, RELTYPES as u32 - 1);
    let (code, stderr) = build(&work);
    assert_eq!(code, Some(0), "well-formed dump should build:\n{stderr}");
}
