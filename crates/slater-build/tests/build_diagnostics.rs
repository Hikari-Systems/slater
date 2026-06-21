// SPDX-License-Identifier: Apache-2.0
//! `--diagnostics`: the gated build-diagnostics JSONL log.
//!
//! Drives a real (external) build with diagnostics on over a dump big enough that
//! at least one sample lands mid-phase, then asserts the log is well-formed:
//!   * every line is valid JSON;
//!   * a `header` first and a `footer` last;
//!   * `phase_start`/`phase_end` cover the external pipeline;
//!   * samples are self-describing (carry `phase`, `op`, `progress_*`,
//!     `active_workers`).
//!
//! Also asserts the OFF path writes no log.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_diagtest_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A dump with an index, `n` nodes carrying a range-indexed `val`, and a chain of
/// edges — enough work that a 1ms sampler catches the build mid-flight.
fn make_dump(n: usize) -> String {
    let mut s = String::new();
    s.push_str("CREATE INDEX FOR (n:T) ON (n.val);\n");
    for i in 0..n {
        s.push_str(&format!(
            "CREATE (:T:__DumpVertex__ {{__dump_id__: {i}, val: {}, name: 'node{i}'}});\n",
            i % 97
        ));
    }
    for i in 0..n.saturating_sub(1) {
        s.push_str(&format!(
            "MATCH (a:__DumpVertex__ {{__dump_id__: {i}}}), (b:__DumpVertex__ {{__dump_id__: {}}}) \
             CREATE (a)-[:LINKS]->(b);\n",
            i + 1
        ));
    }
    s.push_str("MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;\n");
    s
}

fn run_build(work: &Path, extra: &[&str]) -> std::process::Output {
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, make_dump(20_000)).unwrap();
    let mut args = vec![
        "--input",
        input.to_str().unwrap(),
        "--graph",
        "g",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ];
    args.extend_from_slice(extra);
    Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args(&args)
        .output()
        .expect("run slater-build")
}

#[test]
fn diagnostics_log_is_well_formed_and_self_describing() {
    let work = unique_dir("on");
    let log = work.join("diag.jsonl");
    let out = run_build(
        &work,
        &[
            "--diagnostics",
            "--diagnostics-log",
            log.to_str().unwrap(),
            "--diagnostics-interval-ms",
            "1",
        ],
    );
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let text = std::fs::read_to_string(&log).expect("diagnostics log must exist");
    let lines: Vec<Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("invalid JSON line `{l}`: {e}")))
        .collect();
    assert!(
        lines.len() >= 3,
        "expected several records, got {}",
        lines.len()
    );

    // Header first, footer last.
    assert_eq!(lines.first().unwrap()["kind"], "header");
    assert!(lines.first().unwrap().get("online_cores").is_some());
    assert!(lines.first().unwrap().get("max_memory_bytes").is_some());
    assert_eq!(lines.last().unwrap()["kind"], "footer");

    // Phases must cover the external pipeline.
    let phases: std::collections::HashSet<String> = lines
        .iter()
        .filter(|r| r["kind"] == "phase_start")
        .filter_map(|r| r["phase"].as_str().map(str::to_string))
        .collect();
    for expected in [
        "pass1",
        "resolve",
        "cluster",
        "emit.node_stores",
        "emit.topology",
        "emit.vectors",
        "emit.range_isam",
        "emit.prop_hist",
        "publish",
    ] {
        assert!(
            phases.contains(expected),
            "missing phase {expected}; saw {phases:?}"
        );
    }

    // Every phase_end carries raw elapsed timing.
    for r in lines.iter().filter(|r| r["kind"] == "phase_end") {
        assert!(
            r.get("elapsed_ms").is_some(),
            "phase_end missing elapsed_ms: {r}"
        );
    }

    // A 1ms interval over 20k nodes must produce samples, and each must be
    // self-describing.
    let samples: Vec<&Value> = lines.iter().filter(|r| r["kind"] == "sample").collect();
    assert!(
        !samples.is_empty(),
        "expected at least one sample at 1ms interval"
    );
    for s in &samples {
        for key in [
            "phase",
            "op",
            "progress_done",
            "progress_total",
            "active_workers",
        ] {
            assert!(s.get(key).is_some(), "sample missing `{key}`: {s}");
        }
        // t_ms monotonic-ish sanity: non-negative.
        assert!(s["t_ms"].as_u64().is_some());
    }

    // At least one sample landed inside a real work phase (not empty string).
    assert!(
        samples
            .iter()
            .any(|s| s["phase"].as_str().map(|p| !p.is_empty()).unwrap_or(false)),
        "no sample was tagged with a phase"
    );
}

#[test]
fn diagnostics_off_writes_no_log() {
    let work = unique_dir("off");
    let out = run_build(&work, &[]);
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // No default log should appear under the data dir.
    let data_dir = work.join("data");
    let stray: Vec<_> = std::fs::read_dir(&data_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("build-diag-"))
        .collect();
    assert!(
        stray.is_empty(),
        "diagnostics log written despite flag off: {stray:?}"
    );
    // And stderr must not advertise a diagnostics log.
    assert!(!String::from_utf8_lossy(&out.stderr).contains("diagnostics →"));
}
