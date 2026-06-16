// SPDX-License-Identifier: Apache-2.0
//! Per-reltype endpoint postings (`reltype_src.post` / `reltype_tgt.post`).
//!
//! Two invariants:
//!   1. **Parity** — the in-memory and external (bounded-memory) builds produce
//!      identical `reltype_source_counts` / `reltype_target_counts`. Distinct
//!      endpoint counts are permutation-invariant, so even though the external
//!      build permutes node ids the *counts* must match exactly.
//!   2. **Consistency** — within each build, every reltype's posting equals the
//!      distinct source/target node ids independently derived from the CSR
//!      topology. This proves the precomputed posting matches the graph.

use std::collections::BTreeSet;
use std::path::PathBuf;
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

/// Build the DUMP and return the generation dir. `external` toggles the
/// bounded-memory path.
fn build(tag: &str, external: bool) -> PathBuf {
    let work = unique_dir(tag);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let mut args = vec![
        "--input".to_string(),
        input.to_str().unwrap().to_string(),
        "--graph".to_string(),
        "g".to_string(),
        "--data-dir".to_string(),
        data_dir.to_str().unwrap().to_string(),
    ];
    if external {
        args.push("--external".to_string());
        args.push("on".to_string());
    }
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args(&args)
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build (external={external}) failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("g");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    graph_dir.join(gen.trim())
}

/// Distinct source / target node id sets per reltype, derived straight from the
/// CSR topology (the ground truth the posting must match).
fn endpoints_from_topology(
    gen_dir: &PathBuf,
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
fn assert_postings_match_topology(gen_dir: &PathBuf, m: &Manifest) {
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
fn endpoint_postings_consistent_and_parity_across_builds() {
    let inmem = build("inmem", false);
    let extern_ = build("extern", true);

    let mi = Manifest::read_from_dir(&inmem).unwrap();
    let me = Manifest::read_from_dir(&extern_).unwrap();
    mi.verify_content_hash().unwrap();
    me.verify_content_hash().unwrap();

    // Consistency: each build's postings match its own topology.
    assert_postings_match_topology(&inmem, &mi);
    assert_postings_match_topology(&extern_, &me);

    // Parity: counts are permutation-invariant, so they must match across builds.
    // Align by reltype NAME (the two builds may intern reltype ids in any order).
    let count_by_name = |m: &Manifest, counts: &[u64]| -> std::collections::BTreeMap<String, u64> {
        m.reltypes
            .iter()
            .cloned()
            .zip(counts.iter().copied())
            .collect()
    };
    assert_eq!(
        count_by_name(&mi, &mi.reltype_source_counts),
        count_by_name(&me, &me.reltype_source_counts),
        "source counts differ between in-memory and external builds"
    );
    assert_eq!(
        count_by_name(&mi, &mi.reltype_target_counts),
        count_by_name(&me, &me.reltype_target_counts),
        "target counts differ between in-memory and external builds"
    );

    // Spot-check the known shape: DRAWS has 2 distinct sources (100,101) and 2
    // distinct targets (101,102).
    let src = count_by_name(&mi, &mi.reltype_source_counts);
    let tgt = count_by_name(&mi, &mi.reltype_target_counts);
    assert_eq!(src["DRAWS"], 2);
    assert_eq!(tgt["DRAWS"], 2);
    assert_eq!(src["LOOPS"], 1); // self-loop: node 101 is both source and target
    assert_eq!(tgt["LOOPS"], 1);
}
