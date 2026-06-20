// SPDX-License-Identifier: Apache-2.0
//! Determinism of the range-partitioned parallel `emit.topology`.
//!
//! The parallel emit partitions edges into node bands worked concurrently, then
//! stitches the per-band block files. Run order across workers is non-deterministic,
//! so this test builds the *same* dump twice and asserts every published store file
//! is byte-identical — proving the band partition / parallel forward+reverse / concat
//! and the mutex-fed global postings + range sinks are all order-independent.
//!
//! `SLATER_EMIT_BAND_NODES=1` forces one band per node, so even this small fixture
//! exercises many bands, cross-band reverse routing, and a long concat chain.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use graph_format::manifest::Manifest;

/// A dump with a range-indexed property, a chain, and several long-range edges so
/// forward and reverse adjacency cross many bands.
fn make_dump(n: usize) -> String {
    let mut s = String::from("CREATE INDEX FOR (n:Concept) ON (n.name);\n");
    // An edge range index too, so the parallel forward workers' edge-range sink path
    // (pushed into the shared range sorters under a lock) is exercised + checked.
    s.push_str("CREATE INDEX FOR ()-[r:FAR]->() ON (r.w);\n");
    for i in 0..n {
        s.push_str(&format!(
            "CREATE (:Concept:__DumpVertex__ {{__dump_id__: {i}, name: 'node{:04}', val: {}}});\n",
            i,
            i % 7
        ));
    }
    // Chain + a few long-range cross edges (so reverse adjacency lands in far bands).
    for i in 0..n.saturating_sub(1) {
        s.push_str(&format!(
            "MATCH (a:__DumpVertex__ {{__dump_id__: {i}}}), (b:__DumpVertex__ {{__dump_id__: {}}}) \
             CREATE (a)-[:NEXT]->(b);\n",
            i + 1
        ));
    }
    for i in 0..n / 3 {
        s.push_str(&format!(
            "MATCH (a:__DumpVertex__ {{__dump_id__: {i}}}), (b:__DumpVertex__ {{__dump_id__: {}}}) \
             CREATE (a)-[:FAR {{w: {i}}}]->(b);\n",
            n - 1 - i
        ));
    }
    s.push_str("MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;\n");
    s
}

fn build(work: &Path, tag: &str, cluster: &str) -> BTreeMap<String, String> {
    let data_dir = work.join(format!("data_{tag}"));
    let input = work.join(format!("dump_{tag}.cypher"));
    std::fs::write(&input, make_dump(64)).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "g",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            cluster,
        ])
        // Force one band per node so the multi-band partition/concat path is exercised.
        .env("SLATER_EMIT_BAND_NODES", "1")
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build {tag} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("g");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    // The store files and their content hashes, exactly as the manifest recorded them
    // (independently re-hashed on disk so a corrupt write would also be caught).
    let mut map = BTreeMap::new();
    for f in &m.files {
        let got = graph_format::integrity::hash_file(gen_dir.join(&f.name)).unwrap();
        assert_eq!(got, f.blake3, "on-disk hash mismatch for {}", f.name);
        map.insert(f.name.clone(), got);
    }
    map
}

fn assert_identical(cluster: &str) {
    let work =
        std::env::temp_dir().join(format!("slater_emitdet_{}_{cluster}", std::process::id()));
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work).unwrap();

    let a = build(&work, "a", cluster);
    let b = build(&work, "b", cluster);
    assert_eq!(
        a, b,
        "two fresh --cluster {cluster} builds produced different store files"
    );
    // Sanity: the topology + edge_props + postings are actually present.
    for must in [
        "topology.csr.blk",
        "edge_props.blk",
        "reltype_src.post",
        "reltype_tgt.post",
    ] {
        assert!(a.contains_key(must), "missing {must} in build output");
    }
    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn parallel_emit_is_deterministic_ldg() {
    assert_identical("ldg");
}

#[test]
fn parallel_emit_is_deterministic_none() {
    assert_identical("none");
}
