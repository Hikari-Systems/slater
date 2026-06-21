// SPDX-License-Identifier: Apache-2.0
//! Resumability of the external build.
//!
//! Uses the `SLATER_BUILD_FAIL_AFTER` test hook to make the binary exit hard right
//! after a given phase (leaving scratch + checkpoint intact, no `current`
//! published), then re-runs with `--resume` and asserts the build completes,
//! publishes the **same** generation it had started, and is correct — proving the
//! later phases were resumed rather than the whole build redone.

use std::path::Path;
use std::process::Command;

use graph_format::manifest::Manifest;

const DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CREATE INDEX FOR (n:Concept) ON (n.name);
CREATE (:Person:__DumpVertex__ {__dump_id__: 100, name: 'Alice', age: 30});
CREATE (:Person:__DumpVertex__ {__dump_id__: 101, name: 'Bob', age: 25});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 102, name: 'Graphs'});
MATCH (a:__DumpVertex__ {__dump_id__: 100}), (b:__DumpVertex__ {__dump_id__: 102}) CREATE (a)-[:LIKES {since: 2020}]->(b);
MATCH (a:__DumpVertex__ {__dump_id__: 101}), (b:__DumpVertex__ {__dump_id__: 100}) CREATE (a)-[:KNOWS]->(b);
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_resume_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The generation uuid of the in-flight build, read from its scratch dir name.
fn scratch_gen(graph_dir: &Path) -> Option<String> {
    for e in std::fs::read_dir(graph_dir).ok()?.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if let Some(g) = name.strip_prefix(".slater-scratch-") {
            return Some(g.to_string());
        }
    }
    None
}

fn build_args(input: &Path, data_dir: &Path) -> Vec<String> {
    vec![
        "--input".into(),
        input.to_string_lossy().into(),
        "--graph".into(),
        "social".into(),
        "--data-dir".into(),
        data_dir.to_string_lossy().into(),
        "--pk".into(),
        "__dump_id__".into(),
        "--cluster".into(),
        "ldg".into(),
    ]
}

fn run_resume_from(fail_after: &str) {
    let work = unique_dir(fail_after);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();
    let graph_dir = data_dir.join("social");

    // 1) Interrupt the build right after `fail_after`.
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(build_args(&input, &data_dir))
        .env("SLATER_BUILD_FAIL_AFTER", fail_after)
        .output()
        .expect("run slater-build (interrupted)");
    assert_eq!(
        out.status.code(),
        Some(70),
        "expected the fault hook to exit 70 after {fail_after}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Nothing was published; scratch + checkpoint survive.
    assert!(
        !graph_dir.join("current").exists(),
        "an interrupted build must not publish"
    );
    let gen = scratch_gen(&graph_dir).expect("scratch dir with the in-flight generation");
    assert!(graph_dir
        .join(format!(".slater-scratch-{gen}"))
        .join("BUILD-STATE.json")
        .exists());

    // 2) Resume: completes, publishes the SAME generation, cleans scratch.
    let out2 = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(build_args(&input, &data_dir))
        .arg("--resume")
        .output()
        .expect("run slater-build --resume");
    assert!(
        out2.status.success(),
        "resume failed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );

    let current = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    assert_eq!(
        current.trim(),
        gen,
        "resume must continue the same generation, not start a fresh one"
    );
    assert!(
        scratch_gen(&graph_dir).is_none(),
        "scratch must be cleaned up after a successful resume"
    );

    // 3) The published generation is correct.
    let gen_dir = graph_dir.join(current.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    assert_eq!(m.node_count, 3);
    assert_eq!(m.edge_count, 2);
    assert_eq!(m.range_indexes.len(), 1);
    for f in &m.files {
        let got = graph_format::integrity::hash_file(gen_dir.join(&f.name)).unwrap();
        assert_eq!(got, f.blake3, "hash mismatch for {}", f.name);
    }

    let _ = std::fs::remove_dir_all(&work);
}

/// Interrupt *inside* pass 1, after the first shard is durably finalized but with
/// later shards unwritten, then resume — exercising shard-granular recovery (the
/// resume re-reads the input but skips already-complete shards via their sidecars).
#[test]
fn resume_mid_pass1() {
    let work = unique_dir("midp1");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();
    let graph_dir = data_dir.join("social");

    // Tiny shards (so the small dump spans several) + a single worker for a
    // deterministic shard order, and crash right after shard 0 is finalized — so
    // only part of the input is in committed shards.
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(build_args(&input, &data_dir))
        .args(["--threads", "1"])
        .env("SLATER_SHARD_BYTES", "120")
        .env("SLATER_BUILD_FAIL_AFTER_SHARD", "0")
        .output()
        .expect("run slater-build (interrupted mid pass 1)");
    assert_eq!(
        out.status.code(),
        Some(70),
        "expected a mid-pass-1 crash: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!graph_dir.join("current").exists());
    let gen = scratch_gen(&graph_dir).expect("scratch with in-flight generation");

    // Resume (no fault) — completes the same generation, correct counts.
    let out2 = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(build_args(&input, &data_dir))
        .arg("--resume")
        .env("SLATER_SHARD_BYTES", "120")
        .output()
        .expect("run slater-build --resume (mid pass 1)");
    assert!(
        out2.status.success(),
        "mid-pass-1 resume failed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let current = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    assert_eq!(current.trim(), gen, "resume must keep the same generation");
    assert!(
        scratch_gen(&graph_dir).is_none(),
        "scratch must be cleaned up"
    );

    let m = Manifest::read_from_dir(graph_dir.join(current.trim())).unwrap();
    m.verify_content_hash().unwrap();
    assert_eq!(m.node_count, 3);
    assert_eq!(m.edge_count, 2);

    let _ = std::fs::remove_dir_all(&work);
}

// A business-key MERGE dump (--dump-format=merge), mirroring the dump-id fixture: 3
// distinct nodes (Source s1 written twice → collapse), 2 edges, one range index.
const MERGE_DUMP: &str = r#"CREATE INDEX FOR (n:Source) ON (n.sourceId);
MERGE (n:Source {sourceId: 's1'}) SET n.formType = '8-K';
MERGE (n:Source {sourceId: 's1'}) SET n.formType = '10-K';
MERGE (n:Company {ticker: 'ATXI'});
MERGE (n:Person {id: 'p1'});
MERGE (a:Source {sourceId: 's1'})-[r:PUBLISHED_BY]->(b:Company {ticker: 'ATXI'}) SET r.confidence = 'exact';
MERGE (a:Person {id: 'p1'})-[r:SOURCED_FROM]->(b:Source {sourceId: 's1'});
"#;

fn merge_build_args(input: &Path, data_dir: &Path) -> Vec<String> {
    vec![
        "--input".into(),
        input.to_string_lossy().into(),
        "--graph".into(),
        "social".into(),
        "--data-dir".into(),
        data_dir.to_string_lossy().into(),
        "--cluster".into(),
        "ldg".into(),
    ]
}

/// Interrupt a `merge` build after `fail_after`, then resume — exercising the new
/// `deduped` checkpoint (and resolve/cluster) for the business-key path.
fn run_merge_resume_from(fail_after: &str) {
    let work = unique_dir(&format!("merge_{fail_after}"));
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, MERGE_DUMP).unwrap();
    let graph_dir = data_dir.join("social");

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(merge_build_args(&input, &data_dir))
        .env("SLATER_BUILD_FAIL_AFTER", fail_after)
        .output()
        .expect("run slater-build (interrupted merge)");
    assert_eq!(
        out.status.code(),
        Some(70),
        "expected the fault hook to exit 70 after {fail_after}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!graph_dir.join("current").exists());
    let gen = scratch_gen(&graph_dir).expect("scratch with the in-flight generation");

    let out2 = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(merge_build_args(&input, &data_dir))
        .arg("--resume")
        .output()
        .expect("run slater-build --resume (merge)");
    assert!(
        out2.status.success(),
        "merge resume failed: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let current = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    assert_eq!(current.trim(), gen, "resume must keep the same generation");
    assert!(
        scratch_gen(&graph_dir).is_none(),
        "scratch must be cleaned up"
    );

    let gen_dir = graph_dir.join(current.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    assert_eq!(m.node_count, 3);
    assert_eq!(m.edge_count, 2);
    assert_eq!(m.range_indexes.len(), 1);

    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn merge_resume_after_pass1() {
    run_merge_resume_from("pass1");
}

#[test]
fn merge_resume_after_deduped() {
    run_merge_resume_from("deduped");
}

#[test]
fn merge_resume_after_resolve() {
    run_merge_resume_from("resolve");
}

#[test]
fn resume_after_pass1() {
    run_resume_from("pass1");
}

#[test]
fn resume_after_resolve() {
    run_resume_from("resolve");
}

#[test]
fn resume_after_cluster() {
    run_resume_from("cluster");
}
