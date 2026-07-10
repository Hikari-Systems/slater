// SPDX-License-Identifier: Apache-2.0
//! Round-trip for `--dump-format=merge`: a from-scratch build out of business-key
//! MERGE statements (nodes + edges), where the business key is the node identity and
//! edges resolve their endpoints by it.
//!
//! After building (under both `--cluster=none` and `--cluster=ldg`) we re-open the
//! `graph-format` readers and assert against independently-derived truth — node dedup
//! (same business key collapses, SET props last-wins), edge create-on-absent (with and
//! without props), and edge dedup (identical (src,reltype,dst) collapses, props last-wins).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Command;

use graph_format::columns::PropsReader;
use graph_format::ids::{NodeId, Value};
use graph_format::isam::IsamReader;
use graph_format::manifest::Manifest;
use graph_format::topology::TopologyReader;

// A self-contained business-key MERGE dump exercising all three statement forms:
//   • node MERGE … SET (Source s1, written twice → collapse, formType last-wins)
//   • node MERGE, no SET (Company ATXI, Person p1)
//   • edge MERGE … SET (Source -PUBLISHED_BY-> Company, with props incl. a list)
//   • bare edge MERGE, no SET (Person -SOURCED_FROM-> Source)
//   • a duplicate edge MERGE (PUBLISHED_BY again, confidence overwrites → last wins)
// Endpoints resolve against the nodes MERGEd in the same input.
const DUMP: &str = r#"CREATE INDEX FOR (n:Source) ON (n.sourceId);
MERGE (n:Source {sourceId: 's1'}) SET n.companyTicker = 'ATXI', n.formType = '8-K';
MERGE (n:Source {sourceId: 's1'}) SET n.formType = '10-K';
MERGE (n:Company {ticker: 'ATXI'});
MERGE (n:Person {id: 'p1'});
MERGE (a:Source {sourceId: 's1'})-[r:PUBLISHED_BY]->(b:Company {ticker: 'ATXI'}) SET r.confidence = 'exact', r.designations = ['ORPHAN'];
MERGE (a:Person {id: 'p1'})-[r:SOURCED_FROM]->(b:Source {sourceId: 's1'});
MERGE (a:Source {sourceId: 's1'})-[r:PUBLISHED_BY]->(b:Company {ticker: 'ATXI'}) SET r.confidence = 'refined';
"#;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_mergert_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prop<'a>(props: &'a [(u32, Value)], keys: &[String], key_name: &str) -> Option<&'a Value> {
    let kid = keys.iter().position(|k| k == key_name)? as u32;
    props.iter().find(|(k, _)| *k == kid).map(|(_, v)| v)
}

fn run_merge(cluster: &str) {
    let work = unique_dir(cluster);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "merge",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            cluster,
        ])
        .output()
        .expect("run slater-build");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "build ({cluster}) failed: {stderr}");

    let graph_dir = data_dir.join("merge");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());

    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    // s1 collapsed to one Source; plus Company ATXI and Person p1.
    assert_eq!(m.node_count, 3, "same-business-key nodes collapse to one");
    // PUBLISHED_BY collapsed to one edge; SOURCED_FROM is the second.
    assert_eq!(
        m.edge_count, 2,
        "identical (src,reltype,dst) edges collapse"
    );

    // Recover each node's final id by its (per-label) business key.
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let mut id_of: HashMap<String, u64> = HashMap::new();
    for id in 0..m.node_count {
        let props = np.props(id).unwrap();
        if let Some(Value::Str(s)) = prop(&props, &m.property_keys, "sourceId") {
            id_of.insert(format!("Source:{s}"), id);
        } else if let Some(Value::Str(s)) = prop(&props, &m.property_keys, "ticker") {
            id_of.insert(format!("Company:{s}"), id);
        } else if let Some(Value::Str(s)) = prop(&props, &m.property_keys, "id") {
            id_of.insert(format!("Person:{s}"), id);
        }
    }
    assert_eq!(
        id_of.len(),
        3,
        "Source:s1, Company:ATXI, Person:p1 recovered"
    );
    let source = id_of["Source:s1"];
    let company = id_of["Company:ATXI"];
    let person = id_of["Person:p1"];

    // Source s1: the second MERGE's formType wins; companyTicker from the first survives.
    let s_props = np.props(source).unwrap();
    assert_eq!(
        prop(&s_props, &m.property_keys, "formType"),
        Some(&Value::Str("10-K".into())),
        "last write to formType wins"
    );
    assert_eq!(
        prop(&s_props, &m.property_keys, "companyTicker"),
        Some(&Value::Str("ATXI".into())),
        "untouched key survives"
    );

    // Edge resolution + dedup: Source -PUBLISHED_BY-> Company, props last-wins, list kept.
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    let ep = PropsReader::open(gen_dir.join("edge_props.blk")).unwrap();
    let reltype = |a: &graph_format::topology::Adj| m.reltypes[a.reltype as usize].as_str();

    let s_out = topo.outgoing(NodeId(source)).unwrap();
    assert_eq!(s_out.len(), 1, "PUBLISHED_BY collapsed to one edge");
    assert_eq!(reltype(&s_out[0]), "PUBLISHED_BY");
    assert_eq!(s_out[0].neighbour.0, company);
    let pb_props = ep.props(s_out[0].edge.0).unwrap();
    assert_eq!(
        prop(&pb_props, &m.property_keys, "confidence"),
        Some(&Value::Str("refined".into())),
        "last write to edge confidence wins"
    );
    assert_eq!(
        prop(&pb_props, &m.property_keys, "designations"),
        Some(&Value::List(vec![Value::Str("ORPHAN".into())])),
        "list prop set once is kept"
    );

    // Bare edge MERGE (no SET): Person -SOURCED_FROM-> Source, no props.
    let p_out = topo.outgoing(NodeId(person)).unwrap();
    assert_eq!(p_out.len(), 1);
    assert_eq!(reltype(&p_out[0]), "SOURCED_FROM");
    assert_eq!(p_out[0].neighbour.0, source);
    assert!(
        ep.props(p_out[0].edge.0).unwrap().is_empty(),
        "bare edge MERGE has no properties"
    );

    // The Source.sourceId range index resolves the business key to the node id.
    let ri = m
        .range_indexes
        .iter()
        .find(|ri| ri.name == "node_Source_sourceId")
        .expect("Source.sourceId range index");
    let isam = IsamReader::open(gen_dir.join(format!("range/{}.isam", ri.name))).unwrap();
    assert_eq!(
        isam.lookup_eq(&Value::Str("s1".into())).unwrap(),
        vec![source]
    );

    let _ = std::fs::remove_dir_all(&work);
}

// A multi-label node MERGE (`MERGE (n:Ident:Extra {k:v})`) — the shape the consolidation
// dump emits for a node that gained a label via `SET n:Label`. The identity (leading)
// label locates the node and carries the range index; every named label is written to the
// node-label store, so the extra label survives the rebuild.
const MULTI_LABEL_DUMP: &str = r#"CREATE INDEX FOR (n:Source) ON (n.sourceId);
MERGE (n:Source:Company {sourceId: 's1'}) SET n.formType = '10-K';
MERGE (n:Source:Company {sourceId: 's1'}) SET n.formType = '8-K';
"#;

#[test]
fn multi_label_node_merge_writes_every_label() {
    use graph_format::nodelabels::NodeLabelsReader;

    let work = unique_dir("multilabel");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, MULTI_LABEL_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "multilabel",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
        ])
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("multilabel");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    // The two MERGEs of the same business key collapse to one node.
    assert_eq!(m.node_count, 1, "same business key collapses to one node");

    // That node carries BOTH labels; the identity label (Source) is present, and the
    // extra label (Company) survived the rebuild.
    let nl = NodeLabelsReader::open(gen_dir.join("node_labels.blk")).unwrap();
    let ids = nl.labels(0).unwrap();
    let names: Vec<&str> = ids.iter().map(|&l| m.labels[l as usize].as_str()).collect();
    assert!(
        names.contains(&"Source") && names.contains(&"Company"),
        "node should carry both labels, got {names:?}"
    );

    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn merge_dump_cluster_none_roundtrips() {
    run_merge("none");
}

#[test]
fn merge_dump_cluster_ldg_roundtrips() {
    run_merge("ldg");
}

// An edge MERGE whose endpoint has no matching node MERGE in the same input must fail
// loudly: merge dumps are self-contained, so an unresolved endpoint is an error.
const UNRESOLVED_DUMP: &str = r#"MERGE (n:Source {sourceId: 's1'});
MERGE (a:Source {sourceId: 's1'})-[r:PUBLISHED_BY]->(b:Company {ticker: 'MISSING'});
"#;

#[test]
fn merge_unresolved_endpoint_fails() {
    let work = unique_dir("unresolved");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, UNRESOLVED_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "unresolved",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
        ])
        .output()
        .expect("run slater-build");
    assert!(
        !out.status.success(),
        "expected a non-zero exit for an unresolved edge endpoint"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("no matching node") && stderr.contains("self-contained"),
        "expected a clear self-contained-dump error, got: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&work);
}

// A __dump_id__ CREATE statement in a merge dump is rejected (the two identity models
// don't mix).
const MIXED_DUMP: &str = r#"CREATE (:Source:__DumpVertex__ {__dump_id__: 0, sourceId: 's1'});
"#;

#[test]
fn merge_rejects_dump_id_create() {
    let work = unique_dir("mixed");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, MIXED_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "mixed",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .output()
        .expect("run slater-build");
    assert!(
        !out.status.success(),
        "expected rejection of a CREATE statement"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does not accept __dump_id__ CREATE"),
        "expected a clear CREATE-rejection message, got: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&work);
}

// Two fresh builds of the same merge dump must produce byte-identical stores, even
// under maximum parallelism (one statement per shard, one band per node) — guards
// against HashMap-iteration / worker-scheduling order leaking into the output.
fn build_hashes(work: &std::path::Path, tag: &str) -> BTreeMap<String, String> {
    let data_dir = work.join(format!("data_{tag}"));
    let input = work.join(format!("dump_{tag}.cypher"));
    std::fs::write(&input, DUMP).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "merge",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "ldg",
        ])
        .env("SLATER_SHARD_BYTES", "1")
        .env("SLATER_EMIT_BAND_NODES", "1")
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build {tag} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let graph_dir = data_dir.join("merge");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    let mut map = BTreeMap::new();
    for f in &m.files {
        let got = graph_format::integrity::hash_file(gen_dir.join(&f.name)).unwrap();
        assert_eq!(got, f.blake3, "on-disk hash mismatch for {}", f.name);
        map.insert(f.name.clone(), got);
    }
    map
}

#[test]
fn merge_build_is_deterministic() {
    let work = unique_dir("det");
    let a = build_hashes(&work, "a");
    let b = build_hashes(&work, "b");
    assert_eq!(a, b, "two merge builds produced different store files");
    let _ = std::fs::remove_dir_all(&work);
}
