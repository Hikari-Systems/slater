// SPDX-License-Identifier: Apache-2.0
//! Building from a stdin pipe (`--input -`).
//!
//! stdin can't be rewound, but the build reads the input twice (a header pre-scan
//! for vector-index routing, then pass 1). Regression test for the bug where the
//! pre-scan's `BufReader` read ahead past the header and the discarded bytes were
//! lost on the second open, corrupting an early statement. The fix spools stdin to
//! a scratch file first. A vector-index dump exercises the pre-scan path specifically.

use std::io::Write;
use std::process::{Command, Stdio};

use graph_format::manifest::Manifest;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_stdin_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A dump whose header declares a vector index (so the routing pre-scan runs), then
/// many nodes — large enough that a naive pre-scan would over-read past the header
/// into node data.
fn make_dump(n: usize) -> String {
    let mut s = String::from(
        "CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);\n\
         CALL db.idx.vector.createNodeIndex('Chunk', 'embedding', 3, 'cosine');\n\
         CREATE INDEX FOR (n:Chunk) ON (n.name);\n",
    );
    for i in 0..n {
        s.push_str(&format!(
            "CREATE (:Chunk:__DumpVertex__ {{__dump_id__: {i}, name: 'c{i:04}', \
             embedding: vecf32([{}.0, {}.0, 1.0])}});\n",
            i % 5,
            i % 3
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

#[test]
fn builds_from_stdin_pipe() {
    let work = unique_dir("pipe");
    let data_dir = work.join("data");
    let dump = make_dump(200);

    let mut child = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args([
            "--input",
            "-",
            "--graph",
            "fromstdin",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn slater-build");
    // Write the dump down the pipe, then close it (EOF).
    child
        .stdin
        .take()
        .unwrap()
        .write_all(dump.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "stdin build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("fromstdin");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    // All 200 nodes + 199 NEXT edges survived (no bytes dropped on the second read).
    assert_eq!(m.node_count, 200);
    assert_eq!(m.edge_count, 199);
    // The header pre-scan routed the embeddings: a vector index over all 200 chunks.
    assert_eq!(m.vector_indexes.len(), 1);
    assert_eq!(m.vector_indexes[0].count, 200);
    // Embeddings were routed OUT of the column store.
    assert!(!m.property_keys.iter().any(|k| k == "embedding"));

    let _ = std::fs::remove_dir_all(&work);
}
