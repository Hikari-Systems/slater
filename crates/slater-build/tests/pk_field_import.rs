// SPDX-License-Identifier: Apache-2.0
//! `--pk <FIELD>`: single-global-key ("dump_id style") import with a *configurable*,
//! *stored* identity field (not the hardcoded, consumed `__dump_id__`).
//!
//! Verifies: a CREATE / MATCH…CREATE dump keyed on a custom integer field `id` builds
//! when `--pk id` is given; the pk is retained as a queryable property; MERGE…SET
//! overlay patches coexist in the same dump (both grammars); and the same dump is
//! rejected by the default merge mode (no `--pk`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

use graph_format::columns::PropsReader;
use graph_format::ids::{NodeId, Value};
use graph_format::manifest::Manifest;
use graph_format::topology::TopologyReader;

// CREATE nodes keyed on `id` (not __dump_id__), an edge referencing endpoints by `id`,
// and a MERGE…SET overlay patch matching by the stored `id` — exercising both grammars
// under one --pk import.
const DUMP: &str = r#"CREATE (:Person {id: 10, name: 'Alice'});
CREATE (:Person {id: 11, name: 'Bob'});
MATCH (a {id: 10}), (b {id: 11}) CREATE (a)-[:KNOWS {since: 2020}]->(b);
MERGE (n:Person {id: 10}) SET n.age = 30;
"#;

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("slater_pk_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prop<'a>(props: &'a [(u32, Value)], keys: &[String], key_name: &str) -> Option<&'a Value> {
    let kid = keys.iter().position(|k| k == key_name)? as u32;
    props.iter().find(|(k, _)| *k == kid).map(|(_, v)| v)
}

#[test]
fn pk_field_import_with_custom_field() {
    let work = unique_dir("custom");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "pk",
            "--data-dir",
            data_dir.to_str().unwrap(),
            "--pk",
            "id",
            "--cluster",
            "none",
        ])
        .output()
        .expect("run slater-build");
    assert!(
        out.status.success(),
        "pk build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let graph_dir = data_dir.join("pk");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();
    assert_eq!(m.node_count, 2);
    assert_eq!(m.edge_count, 1);

    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    let mut id_of: HashMap<String, u64> = HashMap::new();
    for nid in 0..m.node_count {
        if let Some(Value::Str(s)) = prop(&np.props(nid).unwrap(), &m.property_keys, "name") {
            id_of.insert(s.clone(), nid);
        }
    }
    let alice = id_of["Alice"];
    let bob = id_of["Bob"];

    // The pk field is STORED and queryable (not consumed like legacy __dump_id__).
    assert_eq!(
        prop(&np.props(alice).unwrap(), &m.property_keys, "id"),
        Some(&Value::Int(10)),
        "pk field is retained as a node property"
    );
    // The MERGE…SET overlay patch matched Alice by her stored id and added age.
    assert_eq!(
        prop(&np.props(alice).unwrap(), &m.property_keys, "age"),
        Some(&Value::Int(30)),
        "MERGE overlay patch coexists with CREATE in pk mode"
    );

    // The edge resolved its endpoints by the `id` field.
    let topo = TopologyReader::open(gen_dir.join("topology.csr.blk")).unwrap();
    let a_out = topo.outgoing(NodeId(alice)).unwrap();
    assert_eq!(a_out.len(), 1);
    assert_eq!(a_out[0].neighbour.0, bob);
    let ep = PropsReader::open(gen_dir.join("edge_props.blk")).unwrap();
    assert_eq!(
        prop(
            &ep.props(a_out[0].edge.0).unwrap(),
            &m.property_keys,
            "since"
        ),
        Some(&Value::Int(2020))
    );

    let _ = std::fs::remove_dir_all(&work);
}

#[test]
fn default_merge_mode_rejects_create_without_pk() {
    let work = unique_dir("nopk");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, DUMP).unwrap();

    // Same dump, no --pk ⇒ default merge mode ⇒ CREATE statements are rejected.
    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "nopk",
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .output()
        .expect("run slater-build");
    assert!(
        !out.status.success(),
        "default merge mode must reject a CREATE/__dump_id__ dump"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does not accept __dump_id__ CREATE"),
        "expected a CREATE-rejection message, got: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&work);
}
