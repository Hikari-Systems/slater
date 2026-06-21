// SPDX-License-Identifier: Apache-2.0
//! Per-reltype endpoint postings (`reltype_src.post` / `reltype_tgt.post`).
//!
//! One invariant:
//!   **Consistency** — every reltype's posting equals the distinct source/target
//!   node ids independently derived from the CSR topology. This proves the
//!   precomputed posting matches the graph. A shape spot-check then pins the known
//!   per-reltype counts for the fixture.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use graph_format::manifest::Manifest;
use graph_format::postings::decode_endpoint_posting;
use graph_format::topology::TopologyReader;

// reltype DRAWS is sparse: only nodes 100 and 101 are sources; 200 is isolated
// (in no posting). LIKES is a separate type. Self-loop on 101 via LOOPS.
const DUMP: &str = r#"CREATE (:N:__DumpVertex__ {__dump_id__: 100, name: 'a'});
CREATE (:N:__DumpVertex__ {__dump_id__: 101, name: 'b'});
CREATE (:N:__DumpVertex__ {__dump_id__: 102, name: 'c'});
CREATE (:N:__DumpVertex__ {__dump_id__: 200, name: 'iso'});
MATCH (a:__DumpVertex__ {__dump_id__: 100}), (b:__DumpVertex__ {__dump_id__: 102}) CREATE (a)-[:DRAWS]->(b);
MATCH (a:__DumpVertex__ {__dump_id__: 100}), (b:__DumpVertex__ {__dump_id__: 101}) CREATE (a)-[:DRAWS]->(b);
MATCH (a:__DumpVertex__ {__dump_id__: 101}), (b:__DumpVertex__ {__dump_id__: 102}) CREATE (a)-[:DRAWS]->(b);
MATCH (a:__DumpVertex__ {__dump_id__: 100}), (b:__DumpVertex__ {__dump_id__: 101}) CREATE (a)-[:LIKES]->(b);
MATCH (a:__DumpVertex__ {__dump_id__: 101}), (b:__DumpVertex__ {__dump_id__: 101}) CREATE (a)-[:LOOPS]->(b);
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
"#;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_eptest_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Build the DUMP and return the generation dir.
fn build(tag: &str) -> PathBuf {
    let work = unique_dir(tag);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let args = vec![
        "--input".to_string(),
        input.to_str().unwrap().to_string(),
        "--graph".to_string(),
        "g".to_string(),
        "--data-dir".to_string(),
        data_dir.to_str().unwrap().to_string(),
    ];
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(["--pk", "__dump_id__"])
        .args(&args)
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("g");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    graph_dir.join(gen.trim())
}

/// Distinct source / target node id sets per reltype, derived straight from the
/// CSR topology (the ground truth the posting must match).
fn endpoints_from_topology(
    gen_dir: &Path,
    reltype_count: usize,
) -> (Vec<BTreeSet<u64>>, Vec<BTreeSet<u64>>) {
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    let mut src = vec![BTreeSet::new(); reltype_count];
    let mut tgt = vec![BTreeSet::new(); reltype_count];
    for n in 0..topo.node_count() {
        for a in topo.outgoing(graph_format::ids::NodeId(n)).unwrap() {
            src[a.reltype as usize].insert(n);
        }
        for a in topo.incoming(graph_format::ids::NodeId(n)).unwrap() {
            tgt[a.reltype as usize].insert(n);
        }
    }
    (src, tgt)
}

fn decode_all(path: PathBuf, reltype_count: usize) -> Vec<Vec<u64>> {
    let r = graph_format::blockfile::BlockFileReader::open(path).unwrap();
    (0..reltype_count as u64)
        .map(|t| decode_endpoint_posting(&r.read_record_global(t).unwrap()).unwrap())
        .collect()
}

/// Within one build, every reltype's posting equals the topology-derived set.
fn assert_postings_match_topology(gen_dir: &Path, m: &Manifest) {
    let rt = m.reltypes.len();
    let (src, tgt) = endpoints_from_topology(gen_dir, rt);
    let src_post = decode_all(gen_dir.join("reltype_src.post"), rt);
    let tgt_post = decode_all(gen_dir.join("reltype_tgt.post"), rt);
    for t in 0..rt {
        let want_src: Vec<u64> = src[t].iter().copied().collect();
        let want_tgt: Vec<u64> = tgt[t].iter().copied().collect();
        assert_eq!(
            src_post[t], want_src,
            "src posting reltype {} ({})",
            t, m.reltypes[t]
        );
        assert_eq!(
            tgt_post[t], want_tgt,
            "tgt posting reltype {} ({})",
            t, m.reltypes[t]
        );
        assert_eq!(m.reltype_source_counts[t], want_src.len() as u64);
        assert_eq!(m.reltype_target_counts[t], want_tgt.len() as u64);
    }
}

#[test]
fn endpoint_postings_match_topology() {
    let gen_dir = build("ep");

    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();

    // Consistency: the build's postings match its own topology.
    assert_postings_match_topology(&gen_dir, &m);

    // Spot-check the known shape, aligned by reltype NAME (ids intern in any order):
    // DRAWS has 2 distinct sources (100,101) and 2 distinct targets (101,102).
    let count_by_name = |counts: &[u64]| -> std::collections::BTreeMap<String, u64> {
        m.reltypes
            .iter()
            .cloned()
            .zip(counts.iter().copied())
            .collect()
    };
    let src = count_by_name(&m.reltype_source_counts);
    let tgt = count_by_name(&m.reltype_target_counts);
    assert_eq!(src["DRAWS"], 2);
    assert_eq!(tgt["DRAWS"], 2);
    assert_eq!(src["LOOPS"], 1); // self-loop: node 101 is both source and target
    assert_eq!(tgt["LOOPS"], 1);
}
