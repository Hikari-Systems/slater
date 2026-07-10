// SPDX-License-Identifier: Apache-2.0
//! `--max-memory` is a budget, not a suggestion.
//!
//! Every sorter in the external build reserves its working bytes from one
//! `MemoryBudget`, so the live reservations can never sum past the cap. Two things
//! have to hold at the extremes, and both are load-bearing:
//!
//! * A cap so small that not even one band worker can be funded must **fail loudly**.
//!   The band workers block on a peer's reservation, and a phase where no worker can
//!   ever be funded would otherwise park forever — a hung build with no output is a
//!   far worse failure than an error message.
//! * A merely *tight* cap must still finish, by throttling how many workers hold a
//!   slice at once rather than by over-committing. The output must be byte-identical
//!   to the same dump built with a roomy cap: memory pressure changes the schedule,
//!   never the bytes.

use std::path::Path;
use std::process::Command;

use graph_format::manifest::Manifest;

fn make_dump(n: usize) -> String {
    let mut s = String::from("CREATE INDEX FOR (n:Concept) ON (n.name);\n");
    for i in 0..n {
        s.push_str(&format!(
            "CREATE (:Concept:__DumpVertex__ {{__dump_id__: {i}, name: 'node{i:04}'}});\n"
        ));
    }
    for i in 0..n.saturating_sub(1) {
        s.push_str(&format!(
            "MATCH (a:__DumpVertex__ {{__dump_id__: {i}}}), (b:__DumpVertex__ {{__dump_id__: {}}}) \
             CREATE (a)-[:NEXT]->(b);\n",
            i + 1
        ));
    }
    s.push_str("MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;\n");
    s
}

/// Run a build at `max_memory`, returning the process output. Bands are forced to one
/// node each so the band-worker pool has many items to hand its slices around.
fn build(work: &Path, tag: &str, max_memory: &str) -> std::process::Output {
    let data_dir = work.join(format!("data_{tag}"));
    let input = work.join(format!("dump_{tag}.cypher"));
    std::fs::write(&input, make_dump(64)).unwrap();
    Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "g",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--max-memory",
            max_memory,
            "--threads",
            "4",
        ])
        .env("SLATER_EMIT_BAND_NODES", "1")
        .output()
        .expect("run slater-build")
}

fn content_hash(work: &Path, tag: &str) -> String {
    let graph_dir = work.join(format!("data_{tag}")).join("g");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let m = Manifest::read_from_dir(graph_dir.join(gen.trim())).unwrap();
    m.verify_content_hash().unwrap();
    m.content_hash
}

fn workdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("slater_membudget_{}_{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A cap below one worker's floor cannot be satisfied by any amount of waiting. The
/// build must say so and exit, not deadlock. `timeout` in the harness would only
/// report "hung", so the assertion is on the *error text*: it names the budget.
#[test]
fn a_cap_too_small_to_fund_one_worker_fails_loudly() {
    let work = workdir("starve");
    // 1 MiB: below `MIN_SORT_BYTES` (8 MiB), so the very first reservation with a
    // floor cannot be granted.
    let out = build(&work, "starve", "1m");
    assert!(
        !out.status.success(),
        "a 1 MiB budget must not produce a generation"
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("memory budget"),
        "expected a budget error naming the cap, got:\n{err}"
    );
    assert!(
        err.contains("--max-memory"),
        "the error must tell the operator which knob to turn, got:\n{err}"
    );
    let _ = std::fs::remove_dir_all(&work);
}

/// A tight-but-workable cap throttles the band workers instead of over-committing,
/// and still emits exactly the same generation as a roomy one.
#[test]
fn a_tight_cap_throttles_workers_without_changing_the_bytes() {
    let work = workdir("tight");
    let tight = build(&work, "tight", "48m");
    assert!(
        tight.status.success(),
        "a 48 MiB budget should throttle, not fail: {}",
        String::from_utf8_lossy(&tight.stderr)
    );
    let roomy = build(&work, "roomy", "4g");
    assert!(
        roomy.status.success(),
        "roomy build failed: {}",
        String::from_utf8_lossy(&roomy.stderr)
    );
    assert_eq!(
        content_hash(&work, "tight"),
        content_hash(&work, "roomy"),
        "memory pressure changed the emitted bytes"
    );
    let _ = std::fs::remove_dir_all(&work);
}
