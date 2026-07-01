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

/// Ground-truth whole-graph metadata summaries derived straight from the CSR +
/// node labels — the same read model the query engine uses, computed independently
/// of what the builder wrote.
#[allow(clippy::type_complexity)]
fn summaries_from_stores(
    gen_dir: &Path,
    n_labels: usize,
    n_reltypes: usize,
) -> (
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<u64>,
    Vec<(u32, u32, u64)>,
    Vec<(u32, u32, u64)>,
) {
    use std::collections::HashMap;
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    let labels =
        graph_format::nodelabels::NodeLabelsReader::open(gen_dir.join("node_labels.blk")).unwrap();
    let mut re = vec![0u64; n_reltypes];
    let mut rs = vec![0u64; n_reltypes];
    let mut ln = vec![0u64; n_labels];
    let mut fl = vec![0u64; n_labels];
    let mut sm: HashMap<(u32, u32), u64> = HashMap::new();
    let mut tm: HashMap<(u32, u32), u64> = HashMap::new();
    for id in 0..topo.node_count() {
        let labs = labels.labels(id).unwrap();
        if let Some(&f) = labs.first() {
            fl[f as usize] += 1;
        }
        for &l in &labs {
            ln[l as usize] += 1;
        }
        for a in topo.outgoing(graph_format::ids::NodeId(id)).unwrap() {
            re[a.reltype as usize] += 1;
            if a.neighbour.0 == id {
                rs[a.reltype as usize] += 1;
            }
            for &x in &labs {
                *sm.entry((x, a.reltype)).or_insert(0) += 1;
            }
        }
        for a in topo.incoming(graph_format::ids::NodeId(id)).unwrap() {
            for &y in &labs {
                *tm.entry((a.reltype, y)).or_insert(0) += 1;
            }
        }
    }
    let mut smv: Vec<(u32, u32, u64)> = sm.into_iter().map(|((a, t), c)| (a, t, c)).collect();
    smv.sort_unstable();
    let mut tmv: Vec<(u32, u32, u64)> = tm.into_iter().map(|((t, b), c)| (t, b, c)).collect();
    tmv.sort_unstable();
    (re, rs, ln, fl, smv, tmv)
}

#[test]
fn metadata_summaries_match_topology_and_sum_invariants() {
    let gen_dir = build("summaries");
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();

    // The builder's persisted summaries equal a fresh scan of the same graph.
    let (re, rs, ln, fl, sm, tm) =
        summaries_from_stores(&gen_dir, m.labels.len(), m.reltypes.len());
    assert_eq!(m.reltype_edge_counts, re, "reltype_edge_counts");
    assert_eq!(m.reltype_self_loop_counts, rs, "reltype_self_loop_counts");
    assert_eq!(m.label_node_counts, ln, "label_node_counts");
    assert_eq!(m.first_label_counts, fl, "first_label_counts");
    assert_eq!(m.src_label_reltype_counts, sm, "src_label_reltype_counts");
    assert_eq!(m.reltype_tgt_label_counts, tm, "reltype_tgt_label_counts");

    // Sum invariants.
    assert_eq!(
        m.reltype_edge_counts.iter().sum::<u64>(),
        m.edge_count,
        "Σ reltype_edge_counts == edge_count"
    );
    assert!(
        m.first_label_counts.iter().sum::<u64>() <= m.node_count,
        "Σ first_label_counts ≤ node_count (remainder = zero-label nodes)"
    );

    // Named spot-checks (independent of intern order): DRAWS×3, LIKES×1, LOOPS×1,
    // and LOOPS is the only self-loop.
    let by_name = |v: &[u64]| -> std::collections::BTreeMap<String, u64> {
        m.reltypes.iter().cloned().zip(v.iter().copied()).collect()
    };
    let edges = by_name(&m.reltype_edge_counts);
    assert_eq!(edges["DRAWS"], 3);
    assert_eq!(edges["LIKES"], 1);
    assert_eq!(edges["LOOPS"], 1);
    let self_loops = by_name(&m.reltype_self_loop_counts);
    assert_eq!(self_loops["LOOPS"], 1);
    assert_eq!(self_loops["DRAWS"], 0);
}
