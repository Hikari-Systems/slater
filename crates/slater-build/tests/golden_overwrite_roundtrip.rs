// SPDX-License-Identifier: Apache-2.0
//! Round-trip for the overlay dialect: `MERGE|MATCH … SET …` overwrites.
//!
//! A single dump carries a base CREATE section followed by an overlay patch section.
//! After building it (under both `--cluster=none` and `--cluster=ldg`), we re-open the
//! `graph-format` readers and assert against independently-derived truth — that the
//! overwritten values won, untouched keys survived, a match-all SET hit every node, an
//! edge SET landed, a MERGE created the absent node, and a 0-match MATCH was a no-op.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::process::Command;

use graph_format::columns::PropsReader;
use graph_format::ids::{NodeId, Value};
use graph_format::isam::IsamReader;
use graph_format::manifest::Manifest;
use graph_format::topology::TopologyReader;

// Base section creates A/B/C (A,B share grp='g') and the edge A-LINK->C with w=1.
// Overlay section then:
//   • MATCH name='A'  SET score=99      (overwrite an existing value)
//   • MERGE name='A'  SET extra='x'     (match-existing → add a key, no create)
//   • MATCH grp='g'   SET flag=1        (match-all: hits A and B)
//   • MATCH (A)-LINK->(C) SET w=7       (edge property overwrite)
//   • MERGE name='NEW' SET k=1          (0-match MERGE → create the node)
//   • MATCH name='MISSING' SET z=0      (0-match MATCH → no-op + stderr warning)
const DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CREATE INDEX FOR (n:Concept) ON (n.name);
CREATE (:Concept:__DumpVertex__ {__dump_id__: 0, name: 'A', score: 1, note: 'keep', grp: 'g'});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 1, name: 'B', grp: 'g'});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 2, name: 'C'});
MATCH (a:__DumpVertex__ {__dump_id__: 0}), (b:__DumpVertex__ {__dump_id__: 2}) CREATE (a)-[:LINK {w: 1}]->(b);
MATCH (n:Concept {name: 'A'}) SET n.score = 99;
MERGE (n:Concept {name: 'A'}) SET n.extra = 'x';
MATCH (n:Concept {grp: 'g'}) SET n.flag = 1;
MATCH (a:Concept {name: 'A'})-[r:LINK]->(b:Concept {name: 'C'}) SET r.w = 7;
MERGE (n:Concept {name: 'NEW'}) SET n.k = 1;
MATCH (n:Concept {name: 'MISSING'}) SET n.z = 0;
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_ovrrt_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prop<'a>(props: &'a [(u32, Value)], keys: &[String], key_name: &str) -> Option<&'a Value> {
    let kid = keys.iter().position(|k| k == key_name)? as u32;
    props.iter().find(|(k, _)| *k == kid).map(|(_, v)| v)
}

fn run_overwrite(cluster: &str) {
    let work = unique_dir(cluster);
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "overlay",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            cluster,
        ])
        .output()
        .expect("run slater-build");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "build ({cluster}) failed: {stderr}");
    // The 0-match MATCH is a Cypher-faithful no-op, but warns on stderr.
    assert!(
        stderr.contains("matched no node"),
        "expected a 0-match warning, got: {stderr}"
    );

    let graph_dir = data_dir.join("overlay");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());

    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    // 3 created + 1 MERGE-created ('NEW'); 'MISSING' added nothing.
    assert_eq!(m.node_count, 4, "MERGE created exactly one node");
    assert_eq!(m.edge_count, 1);

    // Recover each node's final id by its (unique) name.
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let mut id_of: HashMap<String, u64> = HashMap::new();
    for id in 0..m.node_count {
        if let Some(Value::Str(s)) = prop(&np.props(id).unwrap(), &m.property_keys, "name") {
            id_of.insert(s.clone(), id);
        }
    }
    assert_eq!(id_of.len(), 4, "A, B, C, NEW all recovered by name");
    let get = |name: &str, key: &str| -> Option<Value> {
        prop(&np.props(id_of[name]).unwrap(), &m.property_keys, key).cloned()
    };

    // A: overwrite won, untouched key kept, new keys added.
    assert_eq!(
        get("A", "score"),
        Some(Value::Int(99)),
        "MATCH SET overwrote"
    );
    assert_eq!(get("A", "note"), Some(Value::Str("keep".into())), "kept");
    assert_eq!(
        get("A", "extra"),
        Some(Value::Str("x".into())),
        "MERGE added"
    );
    assert_eq!(get("A", "flag"), Some(Value::Int(1)), "match-all hit A");
    assert_eq!(get("A", "name"), Some(Value::Str("A".into())), "name kept");

    // Match-all hit B too; C (no grp) was untouched.
    assert_eq!(get("B", "flag"), Some(Value::Int(1)), "match-all hit B");
    assert_eq!(get("C", "flag"), None, "C has no grp → no flag");

    // The MERGE-created node carries its match prop + the SET prop.
    assert_eq!(get("NEW", "k"), Some(Value::Int(1)));
    assert_eq!(get("NEW", "name"), Some(Value::Str("NEW".into())));

    // The 0-match MATCH created/changed nothing.
    assert!(!id_of.contains_key("MISSING"));

    // Edge property overwrite: A -LINK-> C now carries w=7.
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    let a_out = topo.outgoing(NodeId(id_of["A"])).unwrap();
    assert_eq!(a_out.len(), 1);
    assert_eq!(a_out[0].neighbour.0, id_of["C"]);
    let ep = PropsReader::open(gen_dir.join("edge_props.blk")).unwrap();
    assert_eq!(
        prop(&ep.props(a_out[0].edge.0).unwrap(), &m.property_keys, "w"),
        Some(&Value::Int(7)),
        "edge SET overwrote w"
    );

    // The Concept.name range index includes the MERGE-created node.
    let ri = m
        .range_indexes
        .iter()
        .find(|ri| ri.name == "node_Concept_name")
        .expect("Concept.name range index");
    let isam = IsamReader::open(gen_dir.join(format!("range/{}.isam", ri.name))).unwrap();
    assert_eq!(
        isam.lookup_eq(&Value::Str("NEW".into())).unwrap(),
        vec![id_of["NEW"]]
    );

    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn external_overwrite_cluster_none_roundtrips() {
    run_overwrite("none");
}

#[test]
fn external_overwrite_cluster_ldg_roundtrips() {
    run_overwrite("ldg");
}

// An edge overwrite whose relationship does not exist must fail loudly: edge
// create-on-absent is not a v1 feature.
const EDGE_MISSING_DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CREATE (:Concept:__DumpVertex__ {__dump_id__: 0, name: 'A'});
CREATE (:Concept:__DumpVertex__ {__dump_id__: 1, name: 'B'});
MATCH (a:Concept {name: 'A'})-[r:LINK]->(b:Concept {name: 'B'}) SET r.w = 7;
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

// Conflicting writes to the SAME key/edge, in a known order, built with one statement
// per shard (`SLATER_SHARD_BYTES=1`) so consecutive overwrites land in different shards
// processed in parallel. Last-writer-wins must still follow input order: the patch
// stream is reconstructed from shard *index* (= input byte position), not from whichever
// worker finished first.
const ORDER_DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.__dump_id__);
CREATE (:Counter:__DumpVertex__ {__dump_id__: 0, name: 'X', v: 0});
CREATE (:Counter:__DumpVertex__ {__dump_id__: 1, name: 'Y', v: 0});
MATCH (a:__DumpVertex__ {__dump_id__: 0}), (b:__DumpVertex__ {__dump_id__: 1}) CREATE (a)-[:LINK {w: 0}]->(b);
MATCH (n:Counter {name: 'X'}) SET n.v = 1;
MATCH (n:Counter {name: 'X'}) SET n.v = 2;
MATCH (n:Counter {name: 'X'}) SET n.keep = 'a';
MATCH (n:Counter {name: 'X'}) SET n.v = 3;
MATCH (a:Counter {name: 'X'})-[r:LINK]->(b:Counter {name: 'Y'}) SET r.w = 1;
MATCH (a:Counter {name: 'X'})-[r:LINK]->(b:Counter {name: 'Y'}) SET r.w = 2;
MATCH (a:Counter {name: 'X'})-[r:LINK]->(b:Counter {name: 'Y'}) SET r.w = 3;
MATCH (n:__DumpVertex__) REMOVE n:__DumpVertex__, n.__dump_id__;
DROP INDEX ON :__DumpVertex__(__dump_id__);
"#;

#[test]
fn external_overwrite_preserves_order_under_parallel_shards() {
    let work = unique_dir("order");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, ORDER_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "order",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "ldg",
        ])
        // One statement per shard → consecutive overwrites parse in parallel workers.
        .env("SLATER_SHARD_BYTES", "1")
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("order");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();

    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let mut id_of: HashMap<String, u64> = HashMap::new();
    for id in 0..m.node_count {
        if let Some(Value::Str(s)) = prop(&np.props(id).unwrap(), &m.property_keys, "name") {
            id_of.insert(s.clone(), id);
        }
    }
    // Three writes to v in order 1,2,3 (with an unrelated SET interleaved) → 3 wins.
    assert_eq!(
        prop(&np.props(id_of["X"]).unwrap(), &m.property_keys, "v"),
        Some(&Value::Int(3)),
        "last write to v must win"
    );
    assert_eq!(
        prop(&np.props(id_of["X"]).unwrap(), &m.property_keys, "keep"),
        Some(&Value::Str("a".into()))
    );

    // Same for the edge: three writes to w in order 1,2,3 → 3 wins.
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    let edge = topo.outgoing(NodeId(id_of["X"])).unwrap()[0].edge.0;
    let ep = PropsReader::open(gen_dir.join("edge_props.blk")).unwrap();
    assert_eq!(
        prop(&ep.props(edge).unwrap(), &m.property_keys, "w"),
        Some(&Value::Int(3)),
        "last write to edge w must win"
    );

    let _ = std::fs::remove_dir_all(&work);
}

// Two fresh builds of the same overlay dump must produce byte-identical stores, even
// under maximum parallelism (one statement per shard, one band per node). Guards
// against `HashMap`-iteration order in pass-1.9 (match index, node/edge patch maps)
// leaking into the emitted bytes / content hashes.
fn build_overlay_hashes(work: &std::path::Path, tag: &str) -> BTreeMap<String, String> {
    let data_dir = work.join(format!("data_{tag}"));
    let input = work.join(format!("dump_{tag}.cypher"));
    std::fs::write(&input, DUMP).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "overlay",
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
    let graph_dir = data_dir.join("overlay");
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
fn external_overwrite_build_is_deterministic() {
    let work = unique_dir("det");
    let a = build_overlay_hashes(&work, "a");
    let b = build_overlay_hashes(&work, "b");
    assert_eq!(a, b, "two overlay builds produced different store files");
    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn external_overwrite_missing_edge_fails() {
    let work = unique_dir("edgemiss");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, EDGE_MISSING_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "edgemiss",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--cluster",
            "none",
        ])
        .output()
        .expect("run slater-build");
    assert!(
        !out.status.success(),
        "expected a non-zero exit for an unmatched edge overwrite"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("matched no") || stderr.contains("no existing relationship"),
        "expected a clear edge-not-found message, got: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&work);
}
