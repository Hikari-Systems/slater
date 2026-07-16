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
use graph_format::isam::IsamReader;
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

// A user index ON the pk property, alongside the internal DumpVertex index over the
// same property. Under `--pk id` these differ only by label, which is exactly what the
// DumpVertex filter must discriminate on.
const INDEX_DUMP: &str = r#"CREATE INDEX FOR (n:__DumpVertex__) ON (n.id);
CREATE INDEX FOR (n:Person) ON (n.id);
CREATE INDEX FOR (n:Person) ON (n.name);
CREATE (:Person {id: 10, name: 'Alice'});
CREATE (:Person {id: 11, name: 'Bob'});
"#;

/// A user range index on the pk property must survive `--pk <field>` — while the
/// internal `(:__DumpVertex__)(<pk>)` index is still dropped. Regression: the filter
/// was `label != DUMP_VERTEX && property != pk_field`, whose De Morgan dual dropped
/// *every* index on the pk property, silently degrading `WHERE n.id = …` to a label
/// scan. Note `--pk id` is load-bearing here: under the default `--pk __dump_id__` the
/// first conjunct alone carries the filter and the buggy predicate passes this test.
#[test]
fn pk_field_import_keeps_user_index_on_pk_property() {
    let work = unique_dir("index");
    let data_dir = work.join("data");
    let input = work.join("dump.cypher");
    std::fs::write(&input, INDEX_DUMP).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_slater-build"))
        .args([
            "--input",
            input.to_str().unwrap(),
            "--graph",
            "pkidx",
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

    let graph_dir = data_dir.join("pkidx");
    let gen = std::fs::read_to_string(graph_dir.join("current")).unwrap();
    let gen_dir = graph_dir.join(gen.trim());
    let m = Manifest::read_from_dir(&gen_dir).unwrap();
    m.verify_content_hash().unwrap();

    // The user index on the pk property survives...
    let person_id = m
        .range_indexes
        .iter()
        .find(|ri| ri.label_or_type == "Person" && ri.property == "id")
        .expect("user range index on the pk property :Person(id) must be built");
    // ...and is a real, populated index, not just a manifest entry.
    let isam = IsamReader::open(gen_dir.join(format!("range/{}.isam", person_id.name))).unwrap();
    let hits = isam.lookup_eq(&Value::Int(10)).unwrap();
    assert_eq!(hits.len(), 1, ":Person(id) index must resolve id=10");
    let np = PropsReader::open(gen_dir.join("node_props.blk")).unwrap();
    assert_eq!(
        prop(&np.props(hits[0]).unwrap(), &m.property_keys, "name"),
        Some(&Value::Str("Alice".into())),
        ":Person(id) index must point at the node holding id=10"
    );

    // An index on a non-pk property is unaffected.
    assert!(
        m.range_indexes
            .iter()
            .any(|ri| ri.label_or_type == "Person" && ri.property == "name"),
        ":Person(name) index must be built"
    );

    // The filter still does its actual job: the internal DumpVertex index is dropped.
    assert!(
        !m.range_indexes
            .iter()
            .any(|ri| ri.label_or_type == "__DumpVertex__"),
        "internal (:__DumpVertex__)(id) index must NOT be built, got: {:?}",
        m.range_indexes
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
